# Runbook: CDC-стек на ноутбуке и ClickHouse на Pi5

Как поднять и проверить цепочку Postgres → Debezium → Kafka (ноутбук) и ClickHouse (Pi5). Решения: [ADR-0005](adr/0005-cdc-contract.md) (контракт CDC), [ADR-0006](adr/0006-clickhouse-schema-idempotency.md) (схема ClickHouse), [ADR-0007](adr/0007-pi5-deploy.md) (деплой на Pi5). Хост Pi5: [`infra/pi5/README.md`](../infra/pi5/README.md).

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

## 6. Аварии

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

### Spark стоял дольше ретеншна (M2)

Ретеншн топиков 7 дней или 1 ГиБ. Оффсеты чекпойнта окажутся за границей хранения, и запрос упадёт (`failOnDataLoss=true`). Рестарт с `failOnDataLoss=false`, затем сверка FR-8.

## 7. Остановка

```bash
$D stop            # остановить, данные сохраняются
$D down            # удалить контейнеры, тома сохраняются
```

`down -v` удаляет тома Postgres и Kafka (данные, оффсеты коннектора, слот теряет смысл) и заблокирован хуком: только вручную и осознанно.
