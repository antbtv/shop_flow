# Runbook: CDC-стек на ноутбуке и ClickHouse на Pi5

Как поднять и проверить цепочку Postgres → Debezium → Kafka (ноутбук) и ClickHouse (Pi5). Решения: [ADR-0005](adr/0005-cdc-contract.md) (контракт CDC), [ADR-0006](adr/0006-clickhouse-schema-idempotency.md) (схема ClickHouse), [ADR-0007](adr/0007-pi5-deploy.md) (деплой на Pi5), [ADR-0008](adr/0008-spark-streaming-job.md) (Spark), [ADR-0009](adr/0009-scd2-fact-marts.md) (SCD2, факт, витрины), [ADR-0010](adr/0010-airflow-pi5-postgres-access.md) (Airflow, доступ к Postgres, сверка). Хост Pi5: [`infra/pi5/README.md`](../infra/pi5/README.md).

Все команды выполняются из корня репозитория на ноутбуке. Сокращение: `D="docker compose -f docker-compose.laptop.yml"`.

## 1. Предусловия

- Docker и Compose v2, пользователь в группе `docker` (после `usermod -aG docker` нужен перелогин).
- `jq`, `curl`, `python3`; для тестов `python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt`.
- `.env` по образцу `.env.example`. Для ноутбучного стека нужны `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `DEBEZIUM_PASSWORD`; для ClickHouse `PI5_HOST`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD` (тот же пароль, что в `~/shopflow/.env` на Pi5). Скрипты читают из `.env` только нужные ключи и ничего не печатают.
- С M4 (ADR-0010) в `.env` ноутбука ещё `LAPTOP_HOST`, `LAPTOP_COMPOSE_SUBNET`, `RECON_READER_PASSWORD`, `CLICKHOUSE_AIRFLOW_PASSWORD`, `PI5_COMPOSE_SUBNET`; пароли — `openssl rand -hex 24` (идут в URI подключения). На ноутбуке установлен `infra/laptop/etc/sysctl.d/90-shopflow.conf` (`ip_nonlocal_bind`), у ноутбука DHCP-резервация по MAC `wlo1`.

## 2. Запуск стека

```bash
$D up -d --wait                 # postgres, kafka, connect -> healthy
scripts/register-connector.sh   # идемпотентный PUT, ждёт "shopflow-pg: RUNNING RUNNING"
```

- При первом старте Postgres выполняет `postgres/init/*`: схема 5.1, `REPLICA IDENTITY FULL`, публикация `shopflow_cdc`, роль `debezium`. Init-скрипты работают только на пустом томе `shopflow_pg-data`; изменения схемы потом накатываются вручную и одновременно со схемой в Spark и DDL ClickHouse (ADR-0005).
- Первая регистрация коннектора делает снапшот существующих строк (`op=r`). **Генератор на это время остановлен** (ADR-0006: изменение, начатое до создания слота, может проиграть строке снапшота).
- Порты ноутбука только на `127.0.0.1`: Postgres 5432, Kafka 9092 (с хоста), Connect REST 8083. Контейнерам Kafka доступна как `kafka:29092`.
- Исключение (M4, FR-8): Postgres ещё и на `${LAPTOP_HOST}:5432` для сверки с Pi5. `pg_hba` собирается при старте из `postgres/pg_hba.conf.template`: из LAN пускается только `recon_reader` с IP Pi5, остальное `reject`. На существующем томе роль создаёт `scripts/create-pg-reader.sh` (идемпотентно).
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
rsync -av --delete --chmod=D755,F644 --exclude='__pycache__' airflow/ pi5:shopflow/airflow/
ssh pi5 'cd ~/shopflow && docker compose -f docker-compose.pi5.yml build && docker compose -f docker-compose.pi5.yml up -d --wait'
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

Один streaming-запрос в контейнере `spark` (ADR-0008, ADR-0009): 5 топиков `cdc.public.*` → `raw_events`, затем `stg_orders`, `stg_order_items`, `stg_inventory`, журналы версий `stg_customer_versions`/`stg_product_versions`, затем `stg_order_status_history`; trigger 30 с, чекпойнт на томе `shopflow_spark-checkpoint`.

Один раз (и после смены пароля или подсети): `CLICKHOUSE_SPARK_PASSWORD` и `LAN_SUBNET` в `.env`, затем

```bash
scripts/create-ch-users.sh      # spark_writer: INSERT/SELECT на raw_events и 6 stg, SELECT на system.clusters/macros/parts
scripts/apply-ddl.sh
```

Запуск и наблюдение:

```bash
$D up -d --wait spark                                   # entrypoint ждёт /ping ClickHouse, потом запускает запрос
docker logs -f shopflow-spark-1 2>&1 | grep shopflow    # строка на батч: batch=, rows=, total_ms=
docker inspect -f '{{.State.Health.Status}} restarts={{.RestartCount}}' shopflow-spark-1
scripts/check_pipeline.sh                               # Kafka vs raw_events, Postgres vs stg_*, журналы, dim_*, факт, витрины, задержка
```

- `check_pipeline.sh` точен, когда генератор остановлен и прошёл один trigger (30 с).
- Spark UI: `http://127.0.0.1:4040`. Тесты преобразований: `.venv/bin/python -m pytest -q` (системный `python3` их пропускает).
- Pi5 недоступен: делать ничего не нужно. Запрос падает по таймауту (≤ 2 мин), контейнер перезапускается и ждёт `/ping` с backoff 5 с → потолок 60 с (`waiting for ClickHouse /ping` в логе). События копятся в Kafka. Проверено сценариями 2.9 B (ClickHouse остановлен) и B2 (DROP в `DOCKER-USER`).
- Зависание (в логе нет новых строк `batch=`, маркер не обновляется): watchdog завершает процесс через 10 мин тишины (`no progress for ... exiting`), Docker перезапускает контейнер.
- Падение драйвера для проверки рестарта: `docker exec shopflow-spark-1 pkill -9 -x -f "python3 /opt/shopflow/streaming_to_clickhouse.py"`. `docker kill` не подходит: Docker считает его ручной остановкой и по `unless-stopped` контейнер не поднимает.
- Буфер Kafka: min(7 дней, 1 ГиБ / суточный объём топика). Штатно 7 дней по всем топикам, при `--rate 5` самый короткий у `inventory` ~2,6 дня (ADR-0009, «Последствия»). Простой дольше буфера: раздел 9.

Карантин (`WARN ... quarantined=N sample=[(pk, kafka_offset, reason)]`): строка не легла в типы ClickHouse, в `stg_*` не записана, но есть в `raw_events`. Посмотреть событие:

```bash
scripts/ch-query.sh "SELECT payload FROM raw_events WHERE topic = 'cdc.public.orders' AND kafka_offset = <offset> LIMIT 1"
```

После исправления схемы в `spark-jobs/shopflow_stream/transforms.py` (и DDL) перезалить stg из `raw_events` (раздел 7, «Дозаливка») или сбросить чекпойнт (раздел 9), пока события ещё в Kafka.

## 7. Модель данных: измерения, факт, витрины

Слои и решения: ADR-0009. Всё, что ниже stg, считает ClickHouse; Spark пишет только `raw_events`, stg и журналы.

| Объект | Тип | Источник | Обновление |
|---|---|---|---|
| `dim_customers`, `dim_products` | SCD2, `ReplacingMergeTree` | `stg_*_versions` | `dim_*_mv`, раз в 2 мин |
| `fact_orders` | представление, зерно «позиция заказа» | `stg_order_items` ⋈ `stg_orders`, `ASOF JOIN dim_products` | при чтении |
| `mart_revenue_daily` | `MergeTree` (день, категория) | `fact_orders` | MV раз в 2 мин, `DEPENDS ON dim_products_mv` |
| `mart_funnel_daily` | `MergeTree` (день) | `stg_orders` + `stg_order_status_history` | MV раз в 2 мин, `DEPENDS ON mart_revenue_daily_mv` |

Refreshable MV пересчитывают таблицу целиком во временную и подменяют через `EXCHANGE`: читатель видит старую или новую версию целиком. Состояние и ручное управление (под `shopflow`):

```bash
scripts/ch-query.sh "SELECT view, status, last_success_time, last_success_duration_ms, next_refresh_time, exception
    FROM system.view_refreshes FORMAT PrettyCompact"
scripts/ch-query.sh "SYSTEM REFRESH VIEW shopflow.dim_products_mv"     # внеочередной пересчёт
scripts/ch-query.sh "SYSTEM STOP VIEW shopflow.mart_funnel_daily_mv"   # пауза; SYSTEM START VIEW — снять
```

- Норма: `status = Scheduled`, `exception` пустой, refresh 0,1–0,2 с на Pi5 (3.12). При ошибке ClickHouse повторяет 3 раза (`refresh_retries`), таблица остаётся прежней.
- Свежесть витрин: колонки `refreshed_at` и `source_watermark` (максимальное `event_time` входа). Задержка для читателя `now - source_watermark` ≈ trigger 30 с + период 2 мин, под нагрузкой p95 148 с (3.12). При остановленном генераторе она равна времени простоя — это не сбой.
- Изменить запрос MV: `CREATE ... IF NOT EXISTS` в `apply-ddl.sh` существующую MV не меняет. Новая миграция с `ALTER TABLE shopflow.<mv> MODIFY QUERY ...` (или `DROP VIEW` + `CREATE` той же MV: TO-таблица и её данные остаются), затем `SYSTEM REFRESH VIEW`.
- `mart_revenue_daily.orders` по категориям не суммировать: заказ с позициями разных категорий есть в каждой. Заказов за день — `uniqExact(order_id)` по `fact_orders` или `created` в воронке.
- Запросы refresh в `system.query_log` видны с пустым `user` как `` INSERT INTO shopflow.`.tmp.inner_id.<uuid>` ``.
- `check_pipeline.sh` проверяет: текущие версии `dim_*` = Postgres (MD5), отсутствие разрывов и перекрытий SCD2; `fact_orders` = Postgres и цену на момент заказа (с `FACT_PRICE_SINCE`); витрины по дням = Postgres и = агрегату `fact_orders`; пропуски переходов `paid`/`delivered` в истории статусов; ошибки и застой refresh (> 5 мин); `(key, ts_us)` в нескольких транзакциях (потеря версии SCD2).

### Дозаливка stg из `raw_events`

История, которую поток не записал: таблицы, появившиеся позже событий (так заполнены журналы в 3.8), перезаливка после исправления карантина, разрыв дольше буфера Kafka. `raw_events` хранит 30 дней (NFR-5). Повтор безопасен, можно рядом с потоком: все цели — `ReplacingMergeTree` по ключам ADR-0006/0009.

```bash
$D run --rm --no-deps --entrypoint /opt/spark/bin/spark-submit spark \
    /opt/shopflow/backfill_from_raw.py --from 2026-09-26 --to 2026-09-30 [--tables stg_inventory,stg_customer_versions]
```

Без `--from/--to` берёт последние 30 дней. Через 2 мин `dim_*` и витрины пересчитаются сами (или `SYSTEM REFRESH VIEW`), затем `check_pipeline.sh`.

## 8. Airflow на Pi5: сверка, качество, ретеншн

Airflow 3.1 (LocalExecutor, FAB) в `docker-compose.pi5.yml`, образ `airflow/Dockerfile` собирается на Pi5. UI: `http://$PI5_HOST:8080`, пользователь `admin`, пароль в `~/shopflow/.env` на Pi5 (`AIRFLOW_ADMIN_PASSWORD`). Деплой — раздел 5 (`rsync` `airflow/`, `build`, `up -d`); новые файлы DAG dag-processor видит за ≤ 5 мин или сразу после `restart airflow-dag-processor`.

| DAG | Расписание (МСК) | Что делает | Нужен ноутбук |
| --- | --- | --- | --- |
| `shopflow_reconciliation` | 20:00 | FR-8: сверка 5 таблиц Postgres ↔ ClickHouse | да, ждёт до 2 ч |
| `shopflow_data_quality` | 20:30 | FR-9: 12 проверок `airflow/dags/sql/dq/*.sql` | нет |
| `shopflow_retention` | 04:00 | NFR-5: TTL `raw_events`, размеры таблиц, логи Airflow > 30 дней | нет |
| `shopflow_healthcheck` | вручную | связь с ClickHouse и Postgres | да |

Метабазу Airflow чистит не DAG, а systemd-таймер Pi5 `shopflow-airflow-db-clean.timer` (вс 04:30, > 30 дней): Airflow 3 закрывает метабазу для задач. Установка — в шапке `infra/pi5/etc/systemd/system/shopflow-airflow-db-clean.service`.

После снятия DAG с паузы Airflow один раз запускает последний пропущенный интервал (`scheduled__...`); `catchup=False` остальные не догоняет. Пропущенный из-за выключенного Pi5 запуск — вручную.

### Скрипты (запуск с ноутбука, выполняются на Pi5, секретов не печатают)

```bash
ssh pi5 'bash -s -- trigger shopflow_reconciliation' < scripts/pi5/airflow-api.sh      # запуск и ожидание, состояния задач
ssh pi5 'bash -s -- trigger shopflow_data_quality simulate_violation=true' < scripts/pi5/airflow-api.sh  # проверка пути «нарушение» без данных
ssh pi5 'SKIP_WAIT=1 bash -s -- trigger shopflow_reconciliation' < scripts/pi5/airflow-api.sh  # проверка «ноутбук выключен»
ssh pi5 'bash -s' < scripts/pi5/reconcile-once.sh    # разовая сверка логикой DAG, вывод в терминал
ssh pi5 'bash -s' < scripts/pi5/airflow-smoke.sh     # импорт DAG, подключения, маскирование паролей
ssh pi5 'bash -s' < scripts/pi5/loadtest-m4.sh       # память стека с тремя DAG (15 мин, генератор на ноутбуке)
STAGES=checks scripts/bench-marts.sh               # с ноутбука: сверка и DQ под лимитами airflow_reader на shopflow_bench
```

### Результаты: `dq_check_results`

Одна строка на (запуск, проверка, таблица), сводка — `table_name = ''`; читать с `FINAL`.

```bash
scripts/ch-query.sh "SELECT dag_id, run_id, check_name, table_name, status, violations, details
  FROM dq_check_results FINAL WHERE checked_at > now() - INTERVAL 2 DAY AND status != 'ok'
  ORDER BY checked_at FORMAT Vertical"
```

| Статус | Значит | Что делать |
| --- | --- | --- |
| `ok` | всё сошлось; `in_flight` в `details` — ключи, изменённые после отсечки `T`, это норма | ничего |
| `violation` | сверка: ключи `missing_in_ch` / `different` / `extra_in_ch` (до 20 в `details`); DQ: `sample` и `hint` | `missing_in_ch` — искать ключ в `raw_events` (карантин, ADR-0008), дозалить `backfill_from_raw.py` (раздел 7) |
| `lagging` | Postgres менялся > 5 мин назад, а в `raw_events` ничего после `T`: поток стоит или догоняет (NFR-3) | проверить Spark и коннектор (раздел 6, `/debezium-debug`), после догона запустить сверку вручную |
| `source_unavailable` | ноутбук не ответил за 2 ч; DAG зелёный, второй плановый запуск подряд — красный | включить ноутбук и запустить сверку вручную; серию считают только плановые запуски `scheduled__*`, ручные её не сбрасывают |
| `error` | ошибка, не «нет ноутбука»: `pg_hba`, пароль, лимит соединений, сломанная проверка | текст в `details` и в логе задачи |

Тестовые строки M4 (`lagging`, `source_unavailable`, `simulated_violation` 2026-10-02) оставлены, их видно по `run_id` и `check_name`.

- `pg_value`/`ch_value` сверки — строки, попавшие в сравнение. У `orders`, `order_items`, `inventory` обе стороны режутся отсечкой `T`. У `customers` и `products` ClickHouse отдаёт все живые ключи журнала, а Postgres — только `updated_at < T`, поэтому числа различаются по построению; судить по `violations`, не по разнице.
- Ноутбук уснул посреди сверки (сенсор уже прошёл) — `error`, не `source_unavailable`: запустить вручную.
- Таблицы растут без TTL (NFR-5): DQ и витрины в лимитах до ~4,5 млн позиций (ADR-0009), следить по `table_size` в `dq_check_results`.

### Ноутбук выключен или в другой сети

Сверка ждёт Postgres до 2 ч (сенсор в режиме reschedule, слот не держит), затем пишет `source_unavailable`. `ip_nonlocal_bind` на ноутбуке действует для всех программ, а порт Postgres, опубликованный Docker, идёт мимо файрвола: в чужой сети защита — только `pg_hba` (`/32` Pi5) и пароль `recon_reader`. Ошибки с SQLSTATE (`28000`/`28P01` — `pg_hba` или пароль, `53300`) — это `error`, а не «нет ноутбука». DQ и ретеншн работают без ноутбука. Сменился IP ноутбука или Pi5 — поправить `LAPTOP_HOST`/`PI5_HOST` в обоих `.env`, перезапустить Postgres ноутбука (пересоберётся `pg_hba`) и Airflow на Pi5.

### Тесты и проверки кода

```bash
.venv/bin/python -m pytest -q                         # всё, включая SQL на временных ClickHouse/Postgres (Docker)
SHOPFLOW_SPARK_TESTS=1 .venv/bin/python -m pytest -q tests/sql/test_backfill.py   # дозаливка через Spark, ~30 с
.venv/bin/sqlfluff lint clickhouse/ddl airflow/dags/sql
```

`tests/test_dags.py` разбирает DAG в образе `shopflow-airflow:3.1.0` (`docker build -t shopflow-airflow:3.1.0 airflow/`). `scripts/bench-marts.sh` — замер витрин на 1 млн заказов в отдельной базе `shopflow_bench` (ADR-0009, порог перехода `fact_orders` на таблицу); базу потом удалить вручную.

## 9. Аварии

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

Нужен при потере или порче чекпойнта, после потери тома Kafka (оффсеты чекпойнта больше конца топика, `failOnDataLoss`). Для перезаливки stg после карантина проще дозаливка из `raw_events` (раздел 7). Запрос читает Kafka с `earliest`, повторы схлопываются по ключам ADR-0006. Удаление тома хук Claude блокирует: выполняет пользователь.

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

Затем дозаливка разрыва из `raw_events` (раздел 7), если события успели туда попасть, и сверка FR-8 (M4). `check_pipeline.sh` видит только позиции, которые ещё есть в Kafka.

### Событие нарушает контракт (`ContractViolation`)

Батч падает до записи, контейнер перезапускается и падает на том же батче; в логе позиции `topic:partition:offset`. Прочитать событие:

```bash
$D exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic cdc.public.orders --partition 0 --offset <offset> --max-messages 1 --formatter-property print.key=true
```

Если сломан источник (конфиг Debezium, SMT), исправить его: следующие события будут корректны, но битое остаётся в топике. Пропуск события — новый чекпойнт со `startingOffsets` за ним. **Процедура не прогонялась**, до применения согласовать и записать в ADR.

## 10. Остановка

```bash
$D stop            # остановить, данные сохраняются
$D down            # удалить контейнеры, тома сохраняются
```

`down -v` удаляет тома Postgres, Kafka и чекпойнт Spark (данные, оффсеты коннектора, позиции Spark) и заблокирован хуком: только вручную и осознанно.
