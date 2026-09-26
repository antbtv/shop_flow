# ADR-0004: Бюджет памяти Pi5: ClickHouse и Airflow

- **Статус:** принято (2026-09-25)
- **Контекст:** NFR-7: ClickHouse и Airflow на Pi5 (8 ГБ) должны укладываться в ~6 ГБ с запасом под ОС. Риск не в памяти в покое, а в пиках: запросы и слияния ClickHouse, задачи Airflow. Дашборд (Grafana или Superset) ещё не выбран. Замеры сделаны на стенде `infra/pi5/memtest/` (Milestone 0.9).
- **Решение:**
  - Жёсткие `mem_limit` на каждый контейнер, сумма работающих 5,5 ГБ + резерв 0,5 ГБ под дашборд = 6 ГБ. Memory cgroup включён в `cmdline.txt` (без него лимиты игнорируются, см. 0.6).

    | Контейнер | `mem_limit` | Пик под нагрузкой |
    | --- | --- | --- |
    | ClickHouse 25.8 | 2816m | 1794 МБ |
    | Airflow scheduler (LocalExecutor) | 1152m | 964 МБ |
    | Airflow api-server | 768m | 302 МБ |
    | Airflow dag-processor | 512m | 227 МБ |
    | Postgres (метабаза Airflow) | 256m | 50 МБ |

  - ClickHouse: `max_server_memory_usage` 2 ГБ; зазор ~0,75 ГБ до `mem_limit` под отображённый бинарник и страничный кеш, которые тоже считаются в cgroup. `mark_cache_size` 256 МБ, `index_mark_cache_size` 64 МБ, `uncompressed_cache_size` 0. System-логи `trace_log`, `metric_log`, `asynchronous_metric_log`, `text_log`, `query_thread_log`, `query_views_log`, `processors_profile_log` выключены; `query_log` и `part_log` с TTL 7 дней. `background_pool_size` по умолчанию (уменьшение ломает старт, код 36).
  - Airflow 3.1: LocalExecutor, `parallelism` 2, метабаза на Postgres, 1 воркер api-server, без примеров DAG. Задачи выполняются внутри контейнера scheduler, ~340 МБ на задачу.
  - Дашборд: на Pi5 только лёгкий (Grafana, ~100–200 МБ) в резерв 0,5 ГБ. Superset (≥ 1 ГБ с метабазой и Redis) запускается на ноутбуке; тогда при выключенном ноутбуке FR-10 недоступен.
- **Рассмотренные варианты:**
  - Лимиты не задавать: в пике ClickHouse и задачи Airflow конкурируют за память, OOM-killer хоста убивает случайный процесс.
  - ClickHouse `mem_limit` 2560m: постоянное вытеснение (`memory.events max` > 10 тыс.) и 49 МБ в swap. Отклонено.
  - `parallelism` 4 при 1152m: scheduler упирался в лимит (`max` 144, swap 26 МБ). Для лёгких DAG (сверка, DQ, ретеншн) двух параллельных задач достаточно.
  - CeleryExecutor: нужен брокер (Redis) и воркеры, лишние сотни МБ без выигрыша на одном хосте.
- **Проверка (2026-09-25):** одновременно `SELECT number % 20000000 AS k, count() FROM numbers(1e9) GROUP BY k` и DAG из 4 задач по 50 МБ, 5 минут (`infra/pi5/memtest/loadtest.sh`). `oom_kill` 0, swap 0, min available 4397 МБ, в `dmesg` OOM нет, `throttled=0x0`, 53 °C (активный кулер). В покое все 5 контейнеров ≈ 1 ГБ.
- **Последствия:**
  - Узкое место Pi5 — CPU, а не память: тяжёлый `GROUP BY` на 1 млрд строк идёт ~190 с при свободной памяти. Витрины строим через материализованные представления и агрегаты, а не через `GROUP BY` по сырым событиям (Milestone 3).
  - При добавлении DAG с тяжёлыми Python-задачами пересмотреть лимит scheduler.
  - Итоговый `docker-compose.pi5.yml` (M1, M4) переносит эти лимиты и конфиг ClickHouse.
- **История для интервью:** лимиты подобраны по замерам (`memory.events`, `memory.swap.current`), а не на глаз; первая гипотеза (медленный запрос из-за памяти) проверена и опровергнута.
