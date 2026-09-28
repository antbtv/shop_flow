# Runbook: CDC-стек на ноутбуке и ClickHouse на Pi5

Как поднять и проверить цепочку Postgres → Debezium → Kafka (ноутбук) и ClickHouse (Pi5). Решения: [ADR-0005](adr/0005-cdc-contract.md) (контракт CDC), [ADR-0006](adr/0006-clickhouse-schema-idempotency.md) (схема ClickHouse), [ADR-0007](adr/0007-pi5-deploy.md) (деплой на Pi5), [ADR-0008](adr/0008-spark-streaming-job.md) (Spark). Хост Pi5: [`infra/pi5/README.md`](../infra/pi5/README.md).

Все команды выполняются из корня репозитория на ноутбуке. Сокращение: `D="docker compose -f docker-compose.laptop.yml"`.

## 1. Предусловия

- Docker и Compose v2, пользователь в группе `docker` (после `usermod -aG docker` нужен перелогин).
- `jq`, `curl`, `python3`; для тестов `python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt`.
- `.env` по образцу `.env.example`. Для ноутбучного стека нужны `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `DEBEZIUM_PASSWORD`; для ClickHouse `PI5_HOST`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD` (тот же пароль, что в `~/shopflow/.env` на Pi5). Скрипты читают из `.env` только нужные ключи и ничего не печатают.

## 2. Запуск стека

```bash
$D up -d --wait                 # postgres, kafka, connect -> healthy
scripts/register-connector.sh   # идемпотентный PUT, ждёт "shopflow-pg: RUNNING RUNNING"
```

- При первом старте Postgres выполняет `postgres/init/*`: схема 5.1, `REPLICA IDENTITY FULL`, публикация `shopflow_cdc`, роль `debezium`. Init-скрипты работают только на пустом томе `shopflow_pg-data`; изменения схемы потом накатываются вручную и одновременно со схемой в Spark и DDL ClickHouse (ADR-0005).
- Первая регистрация коннектора делает снапшот существующих строк (`op=r`). **Генератор на это время остановлен** (ADR-0006: изменение, начатое до создания слота, может проиграть строке снапшота).
- Порты ноутбука только на `127.0.0.1`: Postgres 5432, Kafka 9092 (с хоста), Connect REST 8083. Контейнерам Kafka доступна как `kafka:29092`.
- Все сервисы с `restart: unless-stopped`: после перезагрузки ноутбука стек поднимается сам, коннектор продолжает с сохранённых оффсетов без повторного снапшота (проверено в 1.7, сценарий B).

## 3. Генератор нагрузки

```bash
$D --profile generator run --rm generator --duration 60 --rate 5 --seed 1   # разовый прогон
$D --profile generator run -d --name gen generator                         # постоянная нагрузка, 2 действия/с × сезонность
docker stop gen                                                            # SIGTERM, генератор завершается штатно
```

Параметры: `--rate` (действий в секунду при нагрузке 1,0), `--duration` (0 = бесконечно), `--seed`, `--customers` (засев, только в пустую таблицу), `--no-seasonality`.

## 4. Проверки

```bash
curl -s localhost:8083/connectors/shopflow-pg/status | jq -r '[.connector.state, .tasks[].state] | join(" ")'
scripts/check_cdc_counts.sh                                    # orders, customers: Postgres vs состояние из Kafka
scripts/check_cdc_counts.sh products order_items inventory
```

`check_cdc_counts.sh` проигрывает топик по ключам и сравнивает число живых ключей с `count(*)`. Генератор перед проверкой остановить, иначе счётчики гоняются. `duplicates > 0` после аварийной остановки Connect нормален (at-least-once), расхождение `live` и `postgres` нет.

Слот репликации:

```bash
$D exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
  SELECT slot_name, active, wal_status,
         pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag
  FROM pg_replication_slots"'
```

Норма: `active = t`, `wal_status = reserved`, `lag` в пределах КБ–МБ. Пока Connect стоит, `lag` растёт (до `max_slot_wal_keep_size` 4 ГБ).

Чтение топика:

```bash
$D exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic cdc.public.orders --from-beginning --max-messages 3 --formatter-property print.key=true
```

В Kafka 4.3 `--property` устарел и пишет предупреждение в stdout: использовать `--formatter-property`.

## 5. ClickHouse на Pi5

Деплой или обновление (ADR-0007): только явные пути, `.env` на Pi5 не трогаем.

```bash
rsync -av --chmod=D755,F644 docker-compose.pi5.yml infra/pi5/pi5.env.example pi5:shopflow/
rsync -av --delete --chmod=D755,F644 clickhouse/ pi5:shopflow/clickhouse/
ssh pi5 'cd ~/shopflow && docker compose -f docker-compose.pi5.yml up -d --wait'
```

Первый деплой: на Pi5 `cp pi5.env.example .env && chmod 600 .env`, вписать новый пароль. Сверить пароль с ноутбуком без вывода:

```bash
diff <(sed -n 's/^CLICKHOUSE_PASSWORD=//p' .env) <(ssh pi5 "sed -n 's/^CLICKHOUSE_PASSWORD=//p' ~/shopflow/.env") >/dev/null && echo SAME || echo DIFFERENT
```

С ноутбука:

```bash
curl -s http://$PI5_HOST:8123/ping        # Ok.
scripts/apply-ddl.sh                      # clickhouse/ddl/*.sql по порядку, повторный запуск безопасен
scripts/ch-query.sh 'SELECT name, engine FROM system.tables WHERE database = currentDatabase()'
```

Свежие данные в Replacing-таблицах читать через `FINAL` (или `argMax`), `FINAL` по `raw_events` только с фильтром по `event_time` (ADR-0004, CPU Pi5).

`scripts/load_sample_events.sh` (M1, одноразовый) копирует события из Kafka в `raw_events`; повторный запуск безопасен, дубли схлопываются.

## 6. Spark: Kafka → ClickHouse

Один streaming-запрос в контейнере `spark` (ADR-0008): 5 топиков `cdc.public.*` → `raw_events`, `stg_orders`, `stg_order_items`, trigger 30 с, чекпойнт на томе `shopflow_spark-checkpoint`.

Один раз (и после смены пароля или подсети): `CLICKHOUSE_SPARK_PASSWORD` и `LAN_SUBNET` в `.env`, затем

```bash
scripts/create-ch-users.sh      # spark_writer: INSERT/SELECT на 3 таблицы, SELECT на system.clusters/macros
scripts/apply-ddl.sh
```

Запуск и наблюдение:

```bash
$D up -d --wait spark                                   # entrypoint ждёт /ping ClickHouse, потом запускает запрос
docker logs -f shopflow-spark-1 2>&1 | grep shopflow    # строка на батч: batch=, rows=, total_ms=
docker inspect -f '{{.State.Health.Status}} restarts={{.RestartCount}}' shopflow-spark-1
scripts/check_pipeline.sh                               # Kafka vs raw_events, Postgres vs stg_*, задержка
```

- `check_pipeline.sh` точен, когда генератор остановлен и прошёл один trigger (30 с).
- Spark UI: `http://127.0.0.1:4040`. Тесты преобразований: `.venv/bin/python -m pytest -q` (системный `python3` их пропускает).
- Pi5 недоступен: делать ничего не нужно. Запрос падает по таймауту (≤ 2 мин), контейнер перезапускается и ждёт `/ping` с backoff 5 с → потолок 60 с (`waiting for ClickHouse /ping` в логе). События копятся в Kafka. Проверено сценариями 2.9 B (ClickHouse остановлен) и B2 (DROP в `DOCKER-USER`).
- Зависание (в логе нет новых строк `batch=`, маркер не обновляется): watchdog завершает процесс через 10 мин тишины (`no progress for ... exiting`), Docker перезапускает контейнер.
- Падение драйвера для проверки рестарта: `docker exec shopflow-spark-1 pkill -9 -x -f "python3 /opt/shopflow/streaming_to_clickhouse.py"`. `docker kill` не подходит: Docker считает его ручной остановкой и по `unless-stopped` контейнер не поднимает.
- Буфер Kafka: min(7 дней, 1 ГиБ / суточный объём топика), для `orders` при `--rate 5` около 3,4 дня (ADR-0008).

Карантин (`WARN ... quarantined=N sample=[(pk, kafka_offset, reason)]`): строка не легла в типы ClickHouse, в `stg_*` не записана, но есть в `raw_events`. Посмотреть событие:

```bash
scripts/ch-query.sh "SELECT payload FROM raw_events WHERE topic = 'cdc.public.orders' AND kafka_offset = <offset> LIMIT 1"
```

После исправления схемы в `spark-jobs/shopflow_stream/transforms.py` (и DDL) переиграть поток сбросом чекпойнта (раздел 7), пока события ещё в Kafka.

## 7. Аварии

### Слот потерян (`wal_status = lost`)

Connect простоял дольше, чем помещается в 4 ГБ WAL. События за время простоя потеряны, нужен повторный снапшот. DELETE за время простоя снапшот не покажет, их находит сверка FR-8. **Процедура не прогонялась**, проверить при первом применении.

```bash
docker stop gen 2>/dev/null                                           # генератор остановлен на время снапшота
curl -s -X PUT localhost:8083/connectors/shopflow-pg/stop             # коннектор в STOPPED
curl -s -X DELETE localhost:8083/connectors/shopflow-pg/offsets       # сброс оффсетов (Kafka Connect 3.6+)
$D exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT pg_drop_replication_slot('"'"'shopflow_debezium'"'"')"'
curl -s -X PUT localhost:8083/connectors/shopflow-pg/resume           # новый слот и снапшот (op=r)
```

### Потерян том Kafka

Оффсеты Connect хранились в Kafka: коннектор перерегистрировать (`scripts/register-connector.sh`), сделать повторный снапшот по процедуре выше. Оффсеты топиков начнутся с 0; в `raw_events` новые события не затрут старые, потому что `source_lsn` входит в ключ (ADR-0006).

### Сброс чекпойнта Spark

Нужен при потере или порче чекпойнта, после потери тома Kafka (оффсеты чекпойнта больше конца топика, `failOnDataLoss`) и для перезаливки `stg_*` после карантина. Запрос читает Kafka с `earliest`, повторы схлопываются по ключам ADR-0006. Удаление тома хук Claude блокирует: выполняет пользователь.

```bash
$D stop spark && $D rm -f spark
docker volume rm shopflow_spark-checkpoint
$D up -d --wait spark
scripts/check_pipeline.sh
```

### Spark стоял дольше буфера Kafka

Оффсеты чекпойнта за границей хранения, запрос падает на каждом старте (`failOnDataLoss`). События за разрыв потеряны для ClickHouse.

```bash
FAIL_ON_DATA_LOSS=false $D up -d --wait spark    # продолжить с первого доступного оффсета
# дождаться строки batch= в логе, затем вернуть проверку:
$D up -d --wait spark
```

Затем сверка FR-8 (M4) и решение о перезаливке. `check_pipeline.sh` видит только позиции, которые ещё есть в Kafka.

### Событие нарушает контракт (`ContractViolation`)

Батч падает до записи, контейнер перезапускается и падает на том же батче; в логе позиции `topic:partition:offset`. Прочитать событие:

```bash
$D exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic cdc.public.orders --partition 0 --offset <offset> --max-messages 1 --formatter-property print.key=true
```

Если сломан источник (конфиг Debezium, SMT), исправить его: следующие события будут корректны, но битое остаётся в топике. Пропуск события — новый чекпойнт со `startingOffsets` за ним. **Процедура не прогонялась**, до применения согласовать и записать в ADR.

## 8. Остановка

```bash
$D stop            # остановить, данные сохраняются
$D down            # удалить контейнеры, тома сохраняются
```

`down -v` удаляет тома Postgres, Kafka и чекпойнт Spark (данные, оффсеты коннектора, позиции Spark) и заблокирован хуком: только вручную и осознанно.
