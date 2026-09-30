# Статус ShopFlow

Обновлено: 2026-09-29

## Решения
- Брокер: Kafka (KRaft), см. `docs/adr/0001-message-broker-kafka.md`
- Хранилище Pi5: ОС на SD, данные на внешнем USB HDD 500 ГБ, см. `docs/adr/0002-pi5-storage-external-hdd.md`
- Сеть Pi5: bind портов на LAN-IP, ufw для хоста, `DOCKER-USER` для контейнеров, см. `docs/adr/0003-pi5-network-exposure.md`
- Дашборд: Grafana или Superset, не решено (PRD, раздел 13). Допущение: на Pi5 резерв 0,5 ГБ под Grafana; Superset, если выберем, запускается на ноутбуке.
- Память Pi5: жёсткие `mem_limit`, LocalExecutor, урезанный ClickHouse, см. `docs/adr/0004-pi5-memory-budget.md`
- Доступ Pi5 → Postgres на ноутбуке (для FR-8): решаем в Milestone 4 отдельным ADR.
- CDC: полный конверт Debezium в JSON без схем, 1 партиция на топик, fsync Kafka на каждое сообщение, REPLICA IDENTITY FULL на 5 таблицах, см. `docs/adr/0005-cdc-contract.md`
- ClickHouse: версия = `source.lsn` только для таблиц «одна строка Postgres»; `raw_events` по позиции Kafka; `stg_orders` + `stg_order_items`, `fact_orders` поверх них в M3, см. `docs/adr/0006-clickhouse-schema-idempotency.md`
- Деплой Pi5: `rsync` по явному списку путей без `.env`, отдельный `.env` на Pi5, compose `name: shopflow`, см. `docs/adr/0007-pi5-deploy.md`
- Spark: один запрос `foreachBatch` → `raw_events` + `stg_orders`/`stg_order_items`, Spark 4.0.4 + коннектор ClickHouse, raw fail-fast, stg карантин, таймауты и watchdog, см. `docs/adr/0008-spark-streaming-job.md`
- Модель данных M3: SCD2 пересчётом журнала версий из Spark (refreshable MV, порядок по LSN), история статусов с первым временем перехода, `fact_orders` — представление с `ASOF JOIN`, витрины — refreshable MV цепочкой, дозаливка Spark batch из `raw_events`, см. `docs/adr/0009-scd2-fact-marts.md`
- Репозиторий: `docs/` и PRD в git; `.claude/`, `CLAUDE.md`, `.env` локально.

## Milestone 0: подготовка окружения

Ветка: `milestone-0`. Исходное состояние: Debian 13 (trixie) на Pi5, SSH по паролю, NTP синхронизирован, swap на zram, HDD `sda1` уже в ext4, сеть только Wi-Fi (`wlan0`).
Доступа к Pi5 у Claude нет: команды для Pi5 выполняет пользователь и присылает вывод. Файлы репозитория пишет Claude на ноутбуке.

- [x] **0.1. `.gitignore` и первый коммит документации** (~15 мин)
  - Приёмка пройдена: `.env`, `.claude/`, `CLAUDE.md` игнорируются и отсутствуют в `git ls-files`; `docs/`, PRD, `.gitignore` в git; в документации только плейсхолдеры паролей.
- [x] **0.2. `.env.example`** (~20 мин)
  - Переменные раздела 9 PRD + креды ClickHouse и Airflow; `LAPTOP_HOST` закомментирован до M4. PRD (разделы 8, 9) обновлён.
  - Приёмка пройдена: все переменные раздела 9 есть в `.env.example`; `git ls-files .env` пусто.
- [x] **0.3. Базовая настройка ОС Pi5** (~40 мин)
  - Отдельный ключ ed25519, алиас `pi5` в `~/.ssh/config`, `sshd_config.d/00-shopflow.conf` (пароли и root выключены), `apt full-upgrade`.
  - Решить судьбу `unattended-upgrades`: выключить или оставить без автоматического reboot (автономность ≥ 2 недель).
  - Сделано (2026-09-25): вход по ключу, пароль отклоняется (`Permission denied (publickey)`), NTP синхронизирован на Pi5 и ноутбуке. Перезагрузка после `full-upgrade` выполнена в 0.4.
  - `unattended-upgrades` не установлен; предложено оставить так и обновлять вручную (Pi5 не виден из интернета, автономный прогон без неожиданных перезагрузок). Принято 2026-09-25.
  - Приёмка: `ssh pi5 'uname -m; free -m; timedatectl show -p NTPSynchronized'` выводит `aarch64`, ~8 ГБ и `yes`; `ssh -o PubkeyAuthentication=no -o PreferredAuthentications=password pi5` получает `Permission denied`; `timedatectl` на ноутбуке тоже синхронизирован (нужно для замера NFR-3).
  - Закрывает: NFR-2.
  - Риск: потеря доступа. Пока меняется конфиг `sshd`, держать вторую SSH-сессию открытой.
- [x] **0.4. USB HDD** (~45 мин)
  - Синий порт USB 3.0, `/mnt/data` по UUID с `nofail`, `usb_max_current_enable=1`, SMART, `hdparm -S 0` если диск засыпает.
  - Приёмка: `findmnt /mnt/data` показывает ext4; `sudo smartctl -H /dev/disk/by-id/<disk>` (при необходимости `-d sat`) выводит `PASSED`; после `reboot` диск смонтирован; `hdparm -C` после 30 мин простоя показывает `active/idle`, иначе фиксируем, что переходник игнорирует команду.
  - Сделано (2026-09-25): `mkfs.ext4 -L shopdata -m 1`, `/mnt/data` по UUID с `noatime,nofail,x-systemd.device-timeout=30s`; после reboot смонтирован, 453 ГБ свободно, `throttled=0x0`. Проверка `hdparm -C` после простоя перенесена в 0.6.
  - Закрывает: NFR-2, ADR-0002.
  - Данные на `sda1` не нужны (подтверждено 2026-09-25): переформатируем с меткой и уменьшенным резервом (`-m 1`), но только после проверки SMART.
  - Базовая линия (2026-09-25): WD5000LUCT (WD AV, 5400 rpm, мост JMicron, SMART работает без `-d sat`); `PASSED`, Power_On_Hours 21, атрибуты 5/197/198/199 = 0, Load_Cycle_Count 146, 26 °C. `usb_max_current_enable=1` уже выставлен прошивкой (БП 27 Вт), `throttled=0x0`.
- [x] **0.5. Защита SD от износа** (~20 мин)
  - Сделано (2026-09-25): `rpi-swap` переведён на чистый zram (`Mechanism=zram`), backing-файл `/var/swap` на SD удалён самим `rpi-swap`. Журнал: `/var/log/journal` bind mount из `/mnt/data/journal` (переживает сбой питания, SD не пишется), `SystemMaxUse=500M`. log2ram не понадобился: `rsyslog` нет, journald до этого был volatile.
  - Приёмка пройдена: `/proc/swaps` только `/dev/zram0`; `/var/swap` нет; `findmnt /var/log/journal` показывает `/dev/sda1[/journal]`.
  - Компромисс: если HDD не смонтируется, journald пишет в пустой `/var/log/journal` на SD (только при аварии диска).
  - Закрывает: ADR-0002, критерий успеха «≥ 2 недели автономно».
- [x] **0.6. Docker на Pi5** (~40 мин)
  - Сделано (2026-09-25): Docker 29.8.1, Compose v5.5.1 из репозитория Docker для trixie. `daemon.json`: `data-root=/mnt/data/docker`, `log-driver: local` (20m × 3), `containerd-snapshotter: false`. Drop-in `RequiresMountsFor=/mnt/data`. В `cmdline.txt` добавлено `cgroup_enable=memory cgroup_memory=1` (ядро Pi по умолчанию без memory cgroup, лимиты игнорировались). Конфиги в `infra/pi5/`.
  - Приёмка пройдена: `root=/mnt/data/docker driver=overlay2`; `/var/lib/docker` нет, `/var/lib/containerd` 388 КБ; `hello-world` проходит; после reboot `cgroup.controllers` содержит `memory`, предупреждений `docker info` нет, `docker run -m 64m` даёт `memory.max=67108864`; Docker `active` поверх `/dev/sda1`; `hdparm -C` = `active/idle` (журнал на HDD не даёт диску засыпать).
  - Урок: в `cmdline.txt` нет завершающего `\n`, `wc -l` даёт 0; проверять через `grep -c ''`.
  - Закрывает: NFR-2, NFR-7 (лимиты памяти работают).
- [x] **0.7. Фиксированный адрес Pi5** (~20 мин)
  - Ethernet недоступен (решено 2026-09-25), Pi5 на Wi-Fi.
  - Сделано (2026-09-25): DHCP-резервация на роутере по MAC `wlan0`. Профиль NetworkManager `netplan-wlan0-1521` (trixie: netplan + NM): `802-11-wireless.cloned-mac-address permanent`, `802-11-wireless.powersave 2`.
  - Приёмка пройдена: после reboot `wlan0` = `192.168.0.151/24`, MAC не изменился, `Power save: off`.
  - Для 0.8: глобального IPv6 нет; `NetworkManager-wait-online` включён.
  - Закрывает: PRD 6.1.
- [x] **0.8. Файрвол и публикация портов** (~45 мин), по ADR-0003
  - Сделано (2026-09-25): ufw `deny incoming`, 22/tcp только из `192.168.0.0/24`; правило `DOCKER-USER` в `/etc/ufw/after.rules` (DROP с `wlan0` не из LAN). IPv6 глобального нет. `ip_nonlocal_bind` не понадобился: Docker стартует после `network-online.target`.
  - Приёмка пройдена: с ноутбука `nc` на 8123 (контейнер `porttest` на `192.168.0.151`) проходит, HTTP 200; `http.server` на хосте :5000 недоступен (timeout, ufw); `ss -tlnp` показывает 8123 только на `192.168.0.151`; после reboot ufw активен, `DOCKER-USER` восстановлен, `porttest` поднялся сам. На роутере нет виртуальных серверов, UPnP-пробросов нет, SSDP-запрос IGD без ответа.
  - Закрывает: PRD 6.1.
- [x] **0.9. Проверка памяти: ClickHouse и Airflow** (~1 ч), итог в ADR-0004
  - Сделано (2026-09-25): стенд `infra/pi5/memtest/` (ClickHouse 25.8, Airflow 3.1 LocalExecutor, Postgres). Первый прогон выявил нехватку: ClickHouse 2560m и scheduler при `parallelism` 4 упирались в лимит, уходили в swap. Лимиты скорректированы: ClickHouse 2816m, api-server 768m, `parallelism` 2.
  - Приёмка пройдена: сумма `mem_limit` 5,5 ГБ + 0,5 ГБ резерв = 6 ГБ; в покое ≈ 1 ГБ; под нагрузкой (`loadtest.sh`: GROUP BY на 1 млрд строк + DAG из 4 задач) `oom_kill` 0, swap 0, min available 4397 МБ, OOM в `dmesg` нет, `throttled=0x0`, 53 °C; DAG `success`; `/ping` = `Ok.`, порты 8123/9000/8080 доступны с ноутбука.
  - Находки: memory cgroup считает бинарник и кеш ClickHouse (нужен зазор над `max_server_memory_usage`); задача Airflow 3 ≈ 340 МБ; узкое место Pi5 — CPU (GROUP BY 1 млрд строк ≈ 190 с). Активный кулер есть.
  - Закрывает: NFR-7.
- [x] **0.10. Нагрузочный тест диска** (~45 мин)
  - Сделано (2026-09-26): `fio` на `/mnt/data`, 10 ГБ, `--direct=1`.
  - Приёмка пройдена: последовательная запись 107 МБ/с, чтение 110 МБ/с; случайное 4K (QD16) чтение 209 IOPS (средняя задержка 76 мс, максимум 1,1 с), запись 198 IOPS. Журнал ядра за время теста чист (нет `reset`, `disconnect`, `I/O error`); `throttled=0x0` до и после; CPU 36→38 °C, HDD 29 °C; SMART 5/197/198/199 = 0. Активный кулер есть.
  - Закрывает: NFR-7, ADR-0002.
- [x] **0.11. Конфиги Pi5 в git, тест перезагрузки, итоги** (~30 мин)
  - Сделано (2026-09-26): `infra/pi5/` зеркалит пути `/etc` и `/boot` на Pi5, runbook `infra/pi5/README.md` (порядок применения, проверки, базовые замеры), `network.md`, стенд `memtest/`. Реальные IP и секреты только плейсхолдерами.
  - Тест перезагрузки (Pi5 и ноутбук выключались, 2026-09-26): `/mnt/data` и `/var/log/journal` смонтированы с `/dev/sda1`; swap только `/dev/zram0`; ufw `active`, `DOCKER-USER` 4 правила; Docker `active`; `Power save: off`; `throttled=0x0`; с ноутбука ping и SSH проходят. Автоподъём контейнера с `--restart unless-stopped` на `192.168.0.151` проверен в 0.8.
  - Итоги: Pi5 `192.168.0.151` (Wi-Fi, DHCP-резервация); Debian 13 trixie, ядро 6.18.50+rpt-rpi-2712; Docker 29.8.1, Compose v5.5.1; память и диск см. 0.9, 0.10, ADR-0004.
  - Закрывает: ADR-0002 («конфигурацию храним в git»), NFR-6 (Pi5 сам восстанавливается после перезагрузки).

`architect` проверил план (2026-09-25): принят с правками, правки внесены.
`reviewer` (2026-09-26): блокеров нет. Исправлено: решение по Kafka в PRD (разделы 6, 7, 8, 10, 13), пример IP, пароль метабазы в memtest из env, пояснения к `after.rules`. Не подтвердилось: потеря `DOCKER-USER` при `systemctl restart docker` (проверено, правила сохраняются).

**Milestone 0 завершён 2026-09-26.**

## Milestone 1: MVP, связность

Ветка: `milestone-1`. План подтверждён 2026-09-26. Оценка ~11 ч. Шаги на Pi5 выполняет пользователь.

До кода `architect` проверяет: A) ADR-0005, контракт CDC (конвертер, decimal, tombstones, REPLICA IDENTITY FULL на все 5 таблиц, роль и публикация Debezium, `max_slot_wal_keep_size`, ретеншн Kafka при 26 ГБ свободного диска ноутбука, listeners под Spark в M2); B) ADR-0006, поправки к схеме ClickHouse 5.2 (позиция события в `raw_events`, версия SCD2 отдельно от `valid_from`, источник `version` в `fact_orders`); C) деплой на Pi5 (`rsync`, отдельный `.env`, пользователи ClickHouse).

- [x] **1.0. Ветки** (~10 мин)
  - Сделано (2026-09-26): `milestone-0` влит в `master` (PR #1), ветка `milestone-1` от `master`.
- [x] **1.1. Python-инструменты** (~20 мин)
  - Сделано (2026-09-26): `pyproject.toml` (ruff, pytest), `.sqlfluff` (диалект `clickhouse`), `requirements-dev.txt` (pytest 9.1.1, ruff 0.16.9, sqlfluff 4.3.0), `.venv`.
  - Приёмка пройдена: `ruff check .` = `All checks passed!`, `sqlfluff lint clickhouse/ddl` = `All Finished!`. DDL из PRD 5.2 разбирается без ошибок парсинга.
  - Исключения в `.sqlfluff`: CP03 и CP05 выключены (имена функций и типов ClickHouse регистрозависимы), выравнивание колонок в `CREATE TABLE` разрешено, `version` разрешён как идентификатор.
- [x] **1.2. ADR-0005, ADR-0006, ADR-0007, ревью `architect`** (~1 ч)
  - Сделано (2026-09-26): деплой вынесен в отдельный ADR-0007. `architect`: все три «принять с правками», правки внесены. PRD 5.1 (REPLICA IDENTITY на 5 таблиц), 5.2 (новая схема) и раздел 10 обновлены.
  - Главные правки: `fact_orders` из двух топиков терял бы обновления (LSN изменения не сравним между таблицами), заменён на `stg_orders` + `stg_order_items`; правила SCD2 для нескольких UPDATE в одной транзакции; `source_lsn` в ключе `raw_events`; fsync Kafka; явные `publication.name`, `slot.name`, KRaft RF 1; `rsync` без `.env`, `name: shopflow`.
  - Проверить на реальных событиях (1.5, 1.7): разные `source.lsn` у двух UPDATE одной строки в одной транзакции; есть `source.ts_us`; одинаковый `source.lsn` у `op=r`; сдвигается ли слот при простое генератора.
  - Закрывает: подготовку к FR-1, NFR-4, NFR-6.
- [x] **1.3. Postgres в `docker-compose.laptop.yml`** (~45 мин)
  - Сделано (2026-09-26): `docker-compose.laptop.yml` (`name: shopflow`, `postgres:17-bookworm`, `mem_limit` 1g), `postgres/init/001_schema.sql`, `postgres/init/002_debezium_role.sh` (пароль через переменную psql), `DEBEZIUM_PASSWORD` в `.env.example`.
  - Приёмка пройдена: `wal_level` = `logical`, `max_slot_wal_keep_size` = `4GB`; `relreplident` = `f` у 5 таблиц; `pg_publication_tables` = 5; `debezium`: `rolreplication=t`, `rolsuper=f`, только `SELECT` на 5 таблиц; вход по паролю через TCP и `IDENTIFY_SYSTEM` проходят, INSERT даёт `permission denied`; `ss -tlnp`: 5432 только на `127.0.0.1`.
  - Окружение ноутбука: пользователь добавлен в группу `docker`, до перелогина Claude запускает docker через `sg docker -c`.
  - Postgres 17, `wal_level=logical`, `max_slot_wal_keep_size`, порт на `127.0.0.1`; `postgres/init/001_schema.sql`: DDL 5.1, REPLICA IDENTITY, публикация на 5 таблиц, роль `debezium` (`DEBEZIUM_PASSWORD` в `.env.example`).
  - Приёмка: `SHOW wal_level` = `logical`; `relreplident` = `f` у 5 таблиц; `pg_publication_tables` = 5 строк; `rolreplication` у `debezium` = `t`.
  - Закрывает: NFR-1, инвариант Postgres.
  - Риск: init-скрипты выполняются только на пустом томе.
- [x] **1.4. Kafka (KRaft) и Kafka Connect (Debezium)** (~1 ч)
  - Сделано (2026-09-26): `apache/kafka:4.3.1` (KRaft, один узел, статический кворум), `quay.io/debezium/connect:3.6.3.Final` (клиенты Kafka 4.3.0, Java 21). Listeners `INTERNAL://kafka:29092`, `EXTERNAL://localhost:9092`, `CONTROLLER://:9093`. `auto.create.topics.enable=false`. Пароль Debezium через `EnvVarConfigProvider` с `allowlist.pattern` только на `DEBEZIUM_PASSWORD`. `offset.flush.interval.ms` 10 с.
  - Приёмка пройдена: все 3 сервиса `healthy`; `connector-plugins` содержит `io.debezium.connector.postgresql.PostgresConnector`; топики `connect-configs`, `connect-offsets`, `connect-status` с RF 1; у брокера `log.flush.interval.messages=1`, `log.retention.hours=168`, `log.retention.bytes=1073741824`, `log.segment.bytes=268435456`; 9092 и 8083 только на `127.0.0.1`.
  - Память в покое: Connect 842 МиБ из 1,5 ГиБ, Kafka 415 МиБ из 1 ГиБ, Postgres 27 МиБ.
  - Заметки: `retention.*` брокера на compact-топики Connect не действует. `watchtower` на ноутбуке работает с `--label-enable` и контейнеры `shopflow` не обновляет.
  - Kafka 4.x KRaft, внутренний и внешний listener, ретеншн по ADR-0005; Connect на образе Debezium 3.x, RF=1, `mem_limit`, `restart: unless-stopped`.
  - Приёмка: все сервисы `healthy`; `curl -s localhost:8083/connector-plugins` содержит `io.debezium.connector.postgresql.PostgresConnector`.
  - Закрывает: NFR-1.
  - Риск: `advertised.listeners` для хоста и контейнеров.
- [x] **1.5. Регистрация коннектора Debezium** (~45 мин)
  - Сделано (2026-09-26): `debezium/postgres-connector.json` (в git только `${env:DEBEZIUM_PASSWORD}` и `${env:POSTGRES_DB}`, allowlist провайдера на эти две переменные), `scripts/register-connector.sh` (идемпотентный `PUT`, ждёт `RUNNING`, при сбое печатает trace).
  - Приёмка пройдена: два запуска подряд дают `shopflow-pg: RUNNING RUNNING`; REST `/config` показывает плейсхолдеры, а не секреты; созданы `cdc.public.{customers,products,orders,order_items,inventory}` и `__debezium-heartbeat.cdc`; в `cdc.public.orders` `c`, `u` с `before`, `d` с полным `before`, tombstone нет; ключ `{"order_id":1}`; `price_at_order` = `"19.99"`; `timestamptz` в ISO UTC; слот `shopflow_debezium` `active`, `wal_status=reserved`.
  - Проверки ADR-0006: в транзакции `txId 773` два UPDATE одной строки имеют разные `source.lsn` (26741552 < 26741704) и одинаковый `source.ts_us`; поле `source.ts_us` есть. Одинаковый LSN у `op=r` не проверен: при снапшоте таблицы были пусты.
  - Урок: в Kafka 4.3 `kafka-console-consumer.sh --property` устарел и пишет предупреждение в stdout, использовать `--formatter-property`.
  - `debezium/postgres-connector.json` (`topic.prefix=cdc`, `pgoutput`, 5 таблиц, пароль через `${env:...}`), `scripts/register-connector.sh` (идемпотентный `PUT`).
  - Приёмка: коннектор и задача `RUNNING`; после ручных INSERT/UPDATE/DELETE в `orders` в `cdc.public.orders` видны `op` `c`, `u` (с непустым `before`), `d`; есть все 5 топиков `cdc.public.*`; в `source` есть `lsn` и `ts_us`, у двух UPDATE одной строки в одной транзакции разные `lsn`.
  - Закрывает: FR-1.
- [x] **1.6. Генератор нагрузки** (~1 ч)
  - Сделано (2026-09-26): `generator/model.py` (переходы статусов, данные клиентов, сезонность по часам, без psycopg), `generator/generate_orders.py` (psycopg 3, каждое действие отдельной транзакцией, `FOR UPDATE SKIP LOCKED`, пуассоновский поток, SIGTERM), `tests/test_generator.py` (13 тестов). Запуск через сервис compose `generator` (профиль `generator`, `python:3.12-slim`, `USER nobody`): секреты подставляет compose из `.env`, в командной строке их нет.
  - Приёмка пройдена: `python3 -m pytest -q` = `13 passed` (системный Python и `.venv`); `ruff check .` чистый; прогон `--duration 60 --rate 5 --seed 1 --no-seasonality`: 282 действия, заказов 1 → 115, клиентов 1 → 16; статусы только `created/paid/shipped/delivered/cancelled`; в `cdc.public.orders` 116 `c`, 122 `u`, 1 `d`, все 122 перехода `before → after` допустимы по модели, пустых UPDATE нет.
  - Заметка: засев `--customers` срабатывает только на пустой таблице; в прогоне уже был тестовый клиент из 1.5, поэтому засева не было.
  - `generator/generate_orders.py` (psycopg 3, `--rate`, `--duration`, `--seed`): засев `customers`, INSERT `orders`, переходы статусов, UPDATE адреса и сегмента; тесты `tests/test_generator.py`.
  - Приёмка: `python3 -m pytest -q` зелёный, `ruff check .` чистый; после прогона 60 с строки растут, статусы только допустимые.
  - Вне скоупа: `products`, `order_items`, `inventory` генерируем в M3.
- [x] **1.7. Сквозная проверка CDC и перезапуск ноутбука** (~1 ч)
  - Сделано (2026-09-26): `scripts/check_cdc_counts.sh` + `scripts/cdc_state.py` (проигрывает события по ключам: живые ключи, счётчики `op`, повторы `(key, lsn, op)`), тесты `tests/test_cdc_state.py` (всего 17 тестов зелёные).
  - Базовая сверка: все 5 таблиц совпадают.
  - Сценарий A (Connect остановлен на 2 мин под нагрузкой): слот `active=f`, WAL 40 → 243 КБ; после старта `active=t`, отставание 6000 байт; `orders` 469 = 469, `customers` 68 = 68, дублей 0, `op=r` нет.
  - Сценарий B (`down` / `up -d` без `-v`): коннектор сохранился (конфиг в Kafka), `RUNNING`, `op=r` нет; `orders` 532 = 532, `customers` 74 = 74.
  - Сценарий C (SIGKILL Connect под нагрузкой, `rate 10`): дубли `orders` 73, `customers` 17 (≈ 10 с потока = `offset.flush.interval.ms`), живые ключи совпадают (765 = 765, 111 = 111). At-least-once подтверждён, дубли гасит ClickHouse по LSN (ADR-0006).
  - Простой генератора 10 мин (19:07–19:17 UTC): `confirmed_flush_lsn` не двигается (heartbeat без `action.query` слот не сдвигает), но WAL вырос на 264 байта, отставание 7024 байта. `heartbeat.action.query` не нужен; пересмотреть, если при долгом простое отставание уйдёт в мегабайты.
  - `scripts/check_cdc_counts.sh`; остановка `connect` на 2 мин под нагрузкой; `down && up -d` без `-v`.
  - Приёмка: число различных ключей среди `op in (c, r)` без удалённых совпадает с `count(*)` в Postgres (дубли после аварийной остановки Connect допустимы); нового снапшота (`op=r`) нет; слот `active=t`, WAL в слоте после догона в пределах МБ, в том числе после 10 мин простоя генератора (иначе `heartbeat.action.query`).
  - Закрывает: FR-1, NFR-6 (сторона ноутбука).
- [x] **1.8. DDL ClickHouse** (~45 мин)
  - Сделано (2026-09-27): `clickhouse/ddl/000_database.sql` … `005_dim_products.sql` (`raw_events`, `stg_orders`, `stg_order_items`, `dim_customers`, `dim_products`), все `IF NOT EXISTS`. `scripts/apply-ddl.sh`: файлы по порядку через HTTP, креды в заголовках из fd (не в URL и не в `ps`), недостающие переменные берёт из `.env` без `source`, при ошибке печатает ответ ClickHouse.
  - Приёмка пройдена: `sqlfluff lint clickhouse/ddl` = `All Finished!`. На временном ClickHouse 25.8 на ноутбуке (без тома, удалён после проверки): два прогона `apply-ddl.sh` без ошибок; неверный пароль даёт `AUTHENTICATION_FAILED` и остановку; движки и ключи по ADR-0006, TTL 30 дней и `ttl_only_drop_parts = 1`.
  - Семантика на данных: `raw_events` 5 вставок → 3 строки после `FINAL` (повтор позиции Kafka схлопнут, тот же оффсет с другим LSN сохранён); `stg_order_items` версия 150 после 200 проигрывает, `is_deleted = 1` скрывает строку; SCD2 закрытие версии перезаписью с большим LSN, одна текущая версия.
  - Исключения sqlfluff: `SETTINGS` после `TTL` не разбирается парсером (валидный ClickHouse), `-- noqa: PRS` на одной строке; `name` добавлен в `ignore_words`. Комментарий, начинающийся со слова `sqlfluff`, парсер принимает за inline-директиву.
  - `clickhouse/ddl/000_database.sql` … `005_dim_products.sql` по ADR-0006 (`raw_events`, `stg_orders`, `stg_order_items`, `dim_customers`, `dim_products`; `fact_orders` в M3), `IF NOT EXISTS`; `scripts/apply-ddl.sh` (HTTP, креды в заголовках `X-ClickHouse-User`/`X-ClickHouse-Key`, без вывода).
  - Приёмка: `sqlfluff lint clickhouse/ddl` чистый.
  - Закрывает: NFR-4, NFR-5.
- [x] **1.9. `docker-compose.pi5.yml`: только ClickHouse** (~1 ч, `/deploy-pi5`)
  - Сделано (2026-09-27): `docker-compose.pi5.yml` (`name: shopflow`, `clickhouse/clickhouse-server:25.8.33.6` с фиксированной сборкой, `mem_limit` 2816m, `stop_grace_period` 60s, healthcheck `/ping`), `clickhouse/config.d/shopflow.xml` (из memtest, ADR-0004), `clickhouse/users.d/shopflow-profile.xml` (на запрос `max_memory_usage` 1,5 ГиБ, сброс `GROUP BY`/`ORDER BY` на диск с 768 МиБ), `infra/pi5/pi5.env.example`. `scripts/ch-query.sh` и общий `scripts/lib/clickhouse-env.sh` (креды из env или `.env`, не в URL и не в `ps`). Конфиги сначала проверены на ноутбуке в контейнере с теми же монтированиями.
  - Деплой: memtest на Pi5 уже был остановлен вместе с томами; `rsync` по ADR-0007 в `~/shopflow`; `.env` на Pi5 (`600`) с новым паролем, тот же пароль в `.env` ноутбука (сверено без вывода: `SAME`).
  - Приёмка пройдена: `config -q` на Pi5 = `CONFIG_OK`; `up -d --wait` → `healthy` за 12 с; с ноутбука `/ping` = `Ok.`, 9000 открыт, 9009 закрыт; `ss -tlnp` на Pi5: 8123 и 9000 только на `192.168.0.151` (`docker-proxy`); `mem_limit=2952790016`, `restart=unless-stopped`; в `system.users` только `shopflow`, запрос без пароля даёт `REQUIRED_PASSWORD`; `apply-ddl.sh` дважды без ошибок, 5 таблиц с движками по ADR-0006; том `/mnt/data/docker/volumes/shopflow_clickhouse-data`; после `sudo reboot` ClickHouse поднялся сам (`uptime` 76 с), 5 таблиц на месте.
  - Память в покое: 558–576 МиБ из 2,75 ГиБ, CPU 3–4 %.
  - Заметка: пароль memtest (M0) передавался в командной строке и, вероятно, остался в `~/.bash_history` на Pi5; для боевого ClickHouse пароль новый.
  - ClickHouse 25.8, `name: shopflow`, `mem_limit` 2816m, `clickhouse/config.d/shopflow.xml` по ADR-0004, профиль с `max_memory_usage`, `default` без сетевого доступа, порты на `${PI5_HOST}`. Доставка `rsync` по ADR-0007; стенд memtest остановить (`down` без `-v`), проверить `docker volume ls`.
  - Приёмка: `config -q` проходит; `curl -s http://$PI5_HOST:8123/ping` = `Ok.`; `ss -tlnp` показывает 8123/9000 только на `$PI5_HOST`; лимит 2816m; `system.users` без сетевого `default`; `.env` на Pi5 `600`; после reboot отвечает.
  - Закрывает: NFR-2, NFR-7, PRD 6.1, ADR-0003, ADR-0004, ADR-0007.
- [x] **1.10. DDL на Pi5 и ручная заливка тестовых событий** (~1 ч)
  - Сделано (2026-09-27): DDL применён в 1.9. `scripts/load_sample_events.sh` (одноразовый, в M2 заменит Spark): консьюмер печатает партицию, оффсет, ключ и значение, `jq` собирает `JSONEachRow`, ClickHouse сам извлекает `source.lsn`, `source.ts_ms` и `op` из конверта в `INSERT ... SELECT FROM input(...)`.
  - Приёмка пройдена: первая заливка 1984 события (`customers` 332, `orders` 1649, `products`, `order_items`, `inventory` по 1), распределение `op` совпадает со сверкой 1.7, нулевых LSN нет, `event_time` 18:53–19:06 UTC 2026-09-26; транзакция 773 из 1.5 видна как оффсеты 2 и 3 с LSN 26741552 и 26741704. Повторная заливка тех же событий: `FINAL` = 1984 = число различных позиций Kafka, без `FINAL` 3636 (часть дублей уже схлопнута фоновым слиянием). TTL 30 дней в `SHOW CREATE` (1.8).
  - `stg_order_items` на Pi5: версия 150 после 200 проигрывает (`quantity` 5, версия 200), строка с `is_deleted = 1` скрыта `FINAL`; тестовые строки (ключи ≥ 900000000) удалены.
  - Реальные события оставлены в `raw_events`: Spark в M2 при чтении с начала запишет те же позиции, и они схлопнутся (ещё одна проверка NFR-4).
  - `apply-ddl.sh` дважды; `scripts/load_sample_events.sh` (одноразовый, в M2 заменит Spark): события из `cdc.public.*` в `raw_events` через `JSONEachRow`, загрузка дважды; вручную две версии одной строки и удаление в `stg_order_items`.
  - Приёмка: 5 таблиц с ожидаемыми движками; `SELECT topic, op, count() FROM raw_events FINAL GROUP BY ALL` совпадает с выгрузкой после двойной загрузки; TTL 30 дней в `SHOW CREATE`; `FINAL` по `order_item_id` даёт одну строку со старшей версией, строка с `is_deleted = 1` исчезает.
  - Закрывает: пункт M1 про ClickHouse, NFR-4 (smoke), FR-2 (сеть ноутбук → Pi5).
- [x] **1.11. Итоги, ревью, закрытие** (~45 мин)
  - Сделано (2026-09-27): `docs/runbook-laptop.md` (запуск, генератор, проверки, Pi5, аварии, остановка); читающие команды прогнаны на живом стеке, процедура «слот потерян» помечена как непрогнанная (эндпоинт `/offsets` в Connect 4.3 проверен `GET`).
  - `reviewer` (2026-09-27): блокеров нет; pytest 17 passed, ruff и sqlfluff чистые, оба compose валидны, DDL и конфиги соответствуют ADR-0005/0006/0007. Исправлено: обработка аргументов в `check_cdc_counts.sh`; в ADR-0005 записан принятый риск «секреты видны через `docker inspect`».
  - Инцидент: `reviewer` процитировал в отчёте реальное значение `DEBEZIUM_PASSWORD` из `docker inspect`; значение слабое (совпадает с именем проекта). Ротацию пользователь решил не делать (2026-09-27): порт Postgres только на `127.0.0.1`, проект учебный. Принятый риск; вернуться перед публикацией стека за пределы ноутбука.
  - В инструкцию `reviewer` (`.claude/agents/reviewer.md`, локально) добавлено правило: значения секретов не выводить и не цитировать, только имена переменных и канал утечки.
  - `docs/runbook-laptop.md`, STATUS, `reviewer`, PR `milestone-1` → `master`.
  - Приёмка: блокеров нет; pytest, ruff, sqlfluff зелёные; доказательства в STATUS.

Итоги Milestone 1: цепочка Postgres → Debezium → Kafka на ноутбуке и ClickHouse на Pi5 работают и проверены по отдельности; мост между ними (Spark) строится в M2. Условия входа в M2: пользователь-writer ClickHouse для Spark (ADR-0007), подписка Spark на `cdc\.public\..*` (ADR-0005), чекпойнт на постоянном томе.

**Milestone 1 завершён 2026-09-27.** PR `milestone-1` → `master` создаёт пользователь (`gh` не установлен, push по SSH из сессии Claude недоступен).

## Milestone 2: потоковая обработка

Ветка: `milestone-2`. План подтверждён 2026-09-27. Оценка ~10 ч. Шаги на Pi5 выполняет пользователь.
Граница M2/M3: в M2 Spark пишет `raw_events`, `stg_orders`, `stg_order_items`; SCD2, `fact_orders`, MV и генерация `products`/`order_items`/`inventory` в M3.

`architect` проверил ADR-0008 (2026-09-27): принять с правками, правки внесены. Главные: политика «ядовитых» событий (raw fail-fast, stg карантин), таймауты сокета и watchdog против зависания при пропаже Pi5 из Wi-Fi, ожидание `/ping` в entrypoint вместо цикла рестартов, `ingested_at` из Spark (часы ноутбука), явный список 5 топиков вместо шаблона, `max_by` по `(lsn, offset)`, `quantity Int32`, буфер Kafka = min(7 дней, 1 ГиБ / объём в сутки): для `orders` при `--rate 5` ≈ 3,4 дня.

- [x] **2.0. Ветка** (~10 мин)
  - Сделано (2026-09-27): PR #2 `milestone-1` → `master` влит пользователем, `master` fast-forward до `22140ed`, ветка `milestone-2` от `master`.
  - Приёмка пройдена: `git log master..milestone-1` пусто, текущая ветка `milestone-2`.
- [x] **2.1. ADR-0008, ревью `architect`** (~1 ч)
  - Сделано (2026-09-27): ADR-0008 принят с правками (14 правок, в том числе 2 блокера), PRD разделы 8 и 10 обновлены. Замер для буфера: `cdc.public.orders` 1 447 915 байт на ~1650 событий.
  - `docs/adr/0008-spark-streaming-job.md`; PRD, разделы 8 и 10 (граница M2/M3, `spark-jobs/`).
  - Приёмка: вердикт `architect` «принять» или «принять с правками», правки внесены.
  - Закрывает: подготовку к FR-2, NFR-4, NFR-6.
- [x] **2.2. Образ Spark и сервис в `docker-compose.laptop.yml`** (~1 ч)
  - Сделано (2026-09-27): `spark-jobs/Dockerfile` на `spark:4.0.4-scala2.13-java17-python3-ubuntu` (Ubuntu 22.04, Python 3.10, uid 185). Jar-файлы через `ADD --checksum=sha256`: `spark-sql-kafka-0-10_2.13` и `spark-token-provider-kafka-0-10_2.13` 4.0.4, `kafka-clients` 3.9.1, `commons-pool2` 2.12.0 (версии из POM Spark 4.0.4), `clickhouse-spark-runtime-4.0_2.13` 0.10.1. Коннектор — fat jar (21 МБ) со встроенным `client-v2` 0.9.5, отдельный клиент не нужен. Хеши сверены с `.sha256`/`.sha1` Maven Central. `spark-defaults.conf`: `local[2]`, драйвер 1g, UTC для сессии и JVM. Сервис `spark` под профилем `spark` до 2.6: `mem_limit` 2g, том `spark-checkpoint`, UI на `127.0.0.1:4040`, ротация логов 3×10 МБ. В ruff для `spark-jobs/` целевая версия py310.
  - Приёмка пройдена: `spark-jobs/smoke_kafka_count.py` (batch-чтение 5 топиков) даёт `customers` 332, `inventory` 1, `order_items` 1, `orders` 1649, `products` 1, это совпадает с latest − earliest в `kafka-get-offsets`; `__debezium-heartbeat.cdc` (378) в выборку не попал. Клиент Kafka 3.9.1 работает с брокером 4.3.
  - `spark-jobs/Dockerfile` (Spark 4.0.4, jar-файлы при сборке с sha256), сервис `spark` (`mem_limit`, том `spark-checkpoint`, UTC, UI на `127.0.0.1:4040`).
  - Приёмка: batch-чтение Kafka по `cdc\.public\..*` даёт по топикам столько сообщений, сколько `kafka-get-offsets` (end − start); heartbeat-топика нет.
  - Закрывает: NFR-1. Риск: клиент Kafka в Spark и брокер 4.3.
- [x] **2.3. Writer ClickHouse для Spark** (~45 мин, Pi5)
  - Сделано (2026-09-27): `scripts/create-ch-users.sh` (идемпотентный: каждый прогон приводит пароль, подсеть, профиль и права к целевым; `REVOKE ALL` + `GRANT INSERT` на 3 таблицы; профиль `spark_writer_profile` с `max_memory_usage` 512 МиБ; хеш sha256 считается локально). `CLICKHOUSE_SPARK_PASSWORD`, `LAN_SUBNET` в `.env.example`. Миграция `clickhouse/ddl/006_stg_order_items_quantity_int32.sql` (`ALTER ... MODIFY COLUMN quantity Int32`) вместо пересоздания: таблица была пустой (`count()` = 0), по конвенции `clickhouse-ddl` схема меняется только новой миграцией. PRD 5.2 обновлён.
  - Приёмка пройдена: `apply-ddl.sh` и `create-ch-users.sh` по два прогона без ошибок; `DESCRIBE` `quantity Int32`; `SHOW GRANTS FOR spark_writer` = `INSERT` на `raw_events`, `stg_orders`, `stg_order_items`; `host_ip` `192.168.0.0/24`; от `spark_writer` `INSERT` в `raw_events` проходит, а `SELECT`, `INSERT` в `dim_customers`, `CREATE TABLE`, `ALTER ... DELETE` дают `ACCESS_DENIED`; тестовая строка удалена (1 → 0). Пароль: 0 совпадений в `git grep` и рабочих файлах; в `query_log` ClickHouse скрывает хеш (`IDENTIFIED WITH sha256_password HOST IP ...`).
  - Проверка `DROP` (выполнил пользователь, хук `guard-bash` не пропускает её у Claude): `DROP TABLE shopflow.stg_orders` от `spark_writer` даёт `Code: 497` (`ACCESS_DENIED` по `system.errors`), все 5 таблиц на месте.
  - Инцидент: sha256 пароля `spark_writer` попал в вывод сессии Claude (текст ошибочного запроса к `query_log`). Пароль случайный, 192 бита, по хешу не восстанавливается.
  - `scripts/create-ch-users.sh` (идемпотентный, `sha256_hash`, `HOST IP` подсети из переменной, профиль с `max_memory_usage` 512 МиБ), `CLICKHOUSE_SPARK_PASSWORD` и переменная подсети в `.env.example`.
  - `stg_order_items.quantity` → `Int32` (DDL 003, PRD 5.2); на Pi5 пустую таблицу пересоздать.
  - Приёмка: `SHOW GRANTS FOR spark_writer` только `INSERT` на 3 таблицы; `INSERT` проходит, `DROP`/`CREATE` дают `ACCESS_DENIED`; пароля нет в `git grep` и `system.query_log`; `quantity` = `Int32` в `DESCRIBE`.
  - Закрывает: условие входа M2 (ADR-0007).
- [x] **2.4. Spike: запись из Spark в ClickHouse на Pi5** (~1 ч)
  - Сделано (2026-09-27): `spark-jobs/shopflow_stream/sink.py` (каталог `clickhouse` из env: `spark_writer`, `connection_timeout` 10 с, `socket_timeout` 120 с), `spark-jobs/spike_clickhouse_write.py` (случаи `raw`, `raw_ingested`, `stg`, `null_uint`, `negative_uint`). В сервис `spark` переданы `PI5_HOST`, `CLICKHOUSE_HTTP_PORT`, `CLICKHOUSE_SPARK_PASSWORD`. Коннектор 0.10.1 оставлен, запасной путь не понадобился.
  - Права коннектора (по ошибкам `ACCESS_DENIED` и строкам `FROM system.*` в jar): `SELECT` на `system.clusters`, `system.macros` и на 3 целевые таблицы (`loadTable` читает схему через `SELECT`). Добавлены в `create-ch-users.sh`, итог `SHOW GRANTS`: `SELECT, INSERT` на 3 таблицы, `SELECT` на `system.clusters`, `system.macros`.
  - Приёмка пройдена: `raw` 3 строки за 2,9 с; `2026-09-27T12:34:56.123456Z` → `12:34:56.123`, `23:59:59.999Z` в партиции 20260927, `00:00:00Z` в 20260928; LSN `9007199254740993` без потерь; `ingested_at` по DEFAULT и из Spark `current_timestamp()` оба работают; `stg`: `quantity` −2 в `Int32`, `price_at_order` 19.99 и 0.01. Тестовые строки удалены (0), `raw_events` 1984 уникальных позиции, как после 1.10.
  - Находка: коннектор не проверяет значения: `NULL` в `UInt64` → `0`, `-1` → `18446744073709551615`, без ошибки. Проверки fail-fast и карантина обязательны в Spark (ADR-0008 дополнен).
  - Таймауты: хост без ответа на SYN (`10.255.255.1`) → ошибка через 40 с (4 попытки по 10 с); слушатель, который молчит (`busybox nc` в сети compose) → ошибка через 120 с (1 попытка, ошибку чтения клиент не повторяет). Зависания нет. Сценарий с DROP в `DOCKER-USER` на Pi5 остаётся в 2.9 B2.
  - Урок: хук `guard-bash` блокирует слова `DROP`/`TRUNCATE` в любом тексте команды, правки с ними Claude делает через Edit.
  - 3 строки с `topic='test.spike'` в `raw_events` через коннектор, затем удалить. Версия коннектора 0.10.1 или 0.10.0, клиент по POM коннектора.
  - Приёмка: `count()` = 3, типы `DateTime64(3)` (из ISO с микросекундами и `Z`), `UInt64`, `Decimal(10,2)` без искажений; поведение null и отрицательного значения в `UInt*` записано; права коннектора по `system.query_log` (нужен ли `SELECT`); имена опций таймаутов найдены, при DROP-правиле на Pi5 вставка падает по таймауту, а не висит; после очистки 0.
  - Риск: главное неизвестное. Запасные варианты: JDBC, затем `foreachPartition` + HTTP `JSONEachRow`, правка ADR-0008.
- [x] **2.5. Модуль преобразований и тесты** (~1 ч)
  - Сделано (2026-09-27): `spark-jobs/shopflow_stream/transforms.py`: `to_raw_events` (разбор только `source.lsn`, `source.ts_ms`, `op`; `ingested_at` из Spark), `raw_violations` (fail-fast: нет payload, LSN, `ts_ms` или `op`), `stg_rows` (схема конверта по `StgSpec` для `orders`/`order_items`, `op in (c,u,d,r)`, для `d` PK из ключа и `before` со значениями по умолчанию, `quarantine_reason`), `latest_per_key` (`max_by` по `(version, kafka_offset)`). Spark 4 в ANSI-режиме: значения разбираются через `try_cast`/`try_to_timestamp`, битое значение уходит в карантин, а не роняет батч. `pyspark==4.0.4` в `requirements-dev.txt`, `spark-jobs` в `pythonpath` pytest.
  - Приёмка пройдена: `.venv/bin/python -m pytest -q` = `37 passed` (20 новых); `python3 -m pytest -q` (Stop-хук, без pyspark) = `17 passed, 1 skipped`; `ruff check .` чистый; модуль импортируется в образе (Python 3.10.12). Случаи: `c/u/d/r`; `d` с PK из ключа и `before = null`; два UPDATE транзакции 773 (LSN 26741552 < 26741704) → старшая версия; повтор Debezium с тем же LSN → развязка по оффсету; `t` и чужие топики не попадают в stg; карантин: null/отрицательный PK, нет LSN, нет `after`, null/отрицательный `customer_id`, битая дата, `price` `abc` и переполнение `Decimal(10,2)`, `quantity` 2^31; raw fail-fast: нет LSN, tombstone, не-JSON, нет `op`.
  - Решение по тестам: вместо маркера `spark` `pytest.importorskip("pyspark")`: Stop-хук запускает системный `python3` без pyspark, Spark-тесты там пропускаются, полный прогон через `.venv`.
  - `spark-jobs/shopflow_stream/transforms.py`, `tests/test_transforms.py` (локальная `SparkSession`, `pyspark` в `requirements-dev.txt`).
  - Приёмка: pytest и ruff зелёные; случаи `c/u/d/r`, `d` с PK из ключа, два UPDATE в одной транзакции (LSN 26741552 < 26741704), повтор Debezium с тем же LSN (развязка по оффсету), карантин (null в PK, отрицательное значение в `UInt*`), raw fail-fast без `source.lsn`.
  - Закрывает: NFR-4 (логика). Риск: Stop-хук станет медленнее, Spark-тесты под маркером `spark`.
- [x] **2.6. Streaming job: `raw_events`** (~1 ч)
  - Сделано (2026-09-27): `spark-jobs/streaming_to_clickhouse.py` (один запрос, `subscribe` на 5 топиков, `earliest`, `failOnDataLoss=true`, `maxOffsetsPerTrigger` 20000, trigger 30 с, `foreachBatch`: `raw_violations` → `ContractViolation` с позициями, иначе `writeTo(raw_events).append()`; `spark.clickhouse.write.batchSize` = 20000), `shopflow_stream/liveness.py` (`StreamingQueryListener`: строка лога на батч, heartbeat на progress и idle, маркер `/tmp/shopflow-alive`; watchdog `os._exit(1)` после 600 с тишины), `spark-jobs/entrypoint.sh` (ждёт `/ping` с backoff 5 → 300 с, затем `exec spark-submit`). Сервис `spark` без профиля: `restart: unless-stopped`, `stop_grace_period` 60s, healthcheck по возрасту маркера (< 3 мин). `scripts/check_pipeline.sh`: позиции Kafka [earliest, latest) против `uniqExact(kafka_offset)` в `raw_events` по топикам + задержка за 10 мин.
  - Приёмка (2026-09-27): старт 17:17:29 UTC, `/ping` с первой попытки, `healthy` без рестартов. Батч 0: 1984 события за 6,5 с. Все 1984 позиции из 1.10 получили строку от Spark (`ingested_at` после старта), `FINAL` = 1984, без `FINAL` 1987 (слияние почти всё схлопнуло), чужих топиков 0. Генератор 60 с (`--rate 5 --seed 2`, 472 действия) → батчи 1–3 по 172/235/65 строк за ~1,5 с; `check_pipeline.sh` OK по 5 топикам (`customers` 402, `orders` 2051, остальные по 1). Задержка p50 16,3 с, p95 28,7 с, max 30,5 с (≈ интервал trigger).
  - Память в работе: `spark` 1,07 из 2 ГиБ, CPU < 1 % в простое. Образ 1,33 ГБ, на диске ноутбука свободно 26 ГБ.
  - Заметка: коннектор на каждый батч пишет WARN `Ignoring unsupported ClickHouse partition/sharding expression: toYYYYMMDD(event_time)`: Spark не делит батч по партициям ClickHouse, ClickHouse делит сам. На корректность не влияет.
  - Fail-fast по контракту проверен тестами 2.5. На живом топике не проверяем: битое событие из Kafka не удалить, оно остановило бы поток до ручного вмешательства.
  - Простой генератора 17:19:08–17:35:21 UTC (16 мин): `healthy`, `restarts=0`, ошибок и `no progress` в логе 0, маркер обновлён в 17:35:00 (`onQueryIdle` работает, ложного срабатывания watchdog нет).
  - Runbook, раздел 6 «Spark» и аварии: сброс чекпойнта, простой дольше буфера (`FAIL_ON_DATA_LOSS=false`, переменная добавлена в job и compose), `ContractViolation` (пропуск события не прогонялся).
  - `spark-jobs/streaming_to_clickhouse.py` (подписка на 5 топиков, таймауты, ожидание `/ping` в entrypoint, watchdog + маркер для healthcheck, ротация логов), `scripts/check_pipeline.sh`.
  - Приёмка: `raw_events FINAL` по топикам = позиции Kafka; события из 1.10 схлопнулись; heartbeat не попал; при простое генератора 15 мин контейнер `healthy` и без рестартов.
  - Закрывает: FR-2, NFR-4.
- [x] **2.7. `stg_orders`, `stg_order_items`** (~1 ч)
  - Сделано (2026-09-27): в `foreachBatch` после `raw_events` для каждого `StgSpec`: `stg_rows` → карантин в лог (`WARN batch=N <table> quarantined=K sample=[(pk, offset, reason)]`) → `latest_per_key` → `writeTo(stg_*)`. `check_pipeline.sh` сверяет `stg_*` с Postgres: `orders` (строки, `sum(order_id)`, `max(updated_at)` в мс, распределение по статусам), `order_items` (строки, `sum(quantity)`, `sum(quantity * price_at_order)`).
  - Чекпойнт сброшен пользователем (удаление тома хук Claude не пропускает): батчи 0–3 из 2.6 писали только `raw_events`, stg без истории. Заодно проверена процедура «потеря чекпойнта»: батч 0 перечитал Kafka с начала (2456 событий, 9,4 с), `raw_events` схлопнулся, позиции OK по 5 топикам.
  - Приёмка пройдена (генератор остановлен): `orders` 947 = 947, `sum(order_id)` 449824, `max(updated_at)` 1790529547734 мс, статусы `cancelled=107,created=384,delivered=164,paid=171,shipped=121` совпадают; 20 случайных `order_id` совпадают построчно (`status`, `updated_at` до мс, `customer_id`).
  - Ручной DML: позиция 2: INSERT (5; 10.00), затем в одной транзакции UPDATE → 6 и UPDATE → 7; 12.50 (LSN 27857920 < 27858064). Три события схлопнулись в батче: в `stg_order_items` одна строка `7 / 12.50 / 27858064` даже без `FINAL`. Позиция 3: INSERT + DELETE в одном батче → одна строка `is_deleted = 1` (значения из `before`), `FINAL` её скрывает. Итог `order_items` 2 строки, `sum(qty)` 9, сумма 127.48 = Postgres.
  - Карантин: заказ `order_id = -5` (Postgres принимает, `UInt64` нет) → `WARN batch=1 stg_orders quarantined=1 sample=[(-5, 2051, 'bad order_id')]`, событие в `raw_events`, остальные записи батча прошли; его DELETE → то же в батче 2 (оффсет 2052). Оба события остаются в Kafka и `raw_events` и уйдут в карантин при каждом повторном чтении: это ожидаемо.
  - Приёмка (генератор остановлен): `stg_orders FINAL WHERE is_deleted=0` = `count(*)` в Postgres; 20 случайных `order_id` совпадают по `status` и `updated_at`; ручные INSERT → UPDATE → UPDATE → DELETE в `order_items` дают правильный `FINAL`; ручное «ядовитое» событие уходит в карантин (счётчик в логе), остальные топики пишутся.
  - Закрывает: NFR-4 (версия = LSN).
- [x] **2.8. Задержка и память под нагрузкой** (~45 мин)
  - Сделано (2026-09-27): генератор 18:16:19–18:46:20 UTC (`--rate 5 --seed 3 --no-seasonality`, 8920 действий: 3618 новых заказов, 4024 смены статуса, 426 клиентов, 852 изменения клиентов). Сэмплер каждые 30 с (56 замеров): `docker stats`, активные парты на Pi5, `LoadAverage1` и `MemoryResident` ClickHouse из `system.asynchronous_metrics`.
  - Приёмка пройдена: задержка `ingested_at - event_time` по 8920 событиям окна: p50 15,3 с, p95 29,0 с, p99 30,4 с, max 30,9 с (≈ интервал trigger, NFR-3 ≤ 5 мин). `check_pipeline.sh` после прогона OK: `orders` 9695 позиций, `customers` 1680; `orders` 4565 = 4565, статусы совпадают. Рестартов и карантина нет.
  - Память (пик / медиана): `spark` 1357 / 1155 МиБ из 2 ГиБ, `kafka` 836 / 716 МиБ из 1 ГиБ, `connect` 1001 / 1000 МиБ из 1,5 ГиБ, `postgres` 75 / 72 МиБ; OOM нет. Батч ~160 строк за ~2,2 с.
  - Pi5: активные парты `raw_events` 4–9 (в конце 8), `stg_orders` 1–5 (в конце 3), слияния успевают, роста нет; `LoadAverage1` max 0,34, медиана 0,06; RSS ClickHouse max 633 МБ.
  - Объём топиков за 30 мин: `orders` +6,85 МБ (313 МиБ в сутки, 1 ГиБ за 3,3 дня), `customers` +1,31 МБ (60 МиБ в сутки, 17 дней). Штатный режим (`--rate 2` × сезонность 0,83) ≈ 104 МиБ в сутки для `orders`, 1 ГиБ за ~9,8 дня: буфер NFR-6 = 7 дней (ограничение по времени). Решение: `retention.bytes` не меняем, для нагрузочных прогонов буфер ~3,5 дня принят явно (ADR-0008).
  - Генератор 30 мин, `--rate 5`.
  - Приёмка: p95 и max `ingested_at - event_time` < 5 мин; пик памяти `spark` без OOM; `system.parts WHERE active` по 3 таблицам не растёт; суточный объём каждого топика в байтах записан, буфер NFR-6 посчитан, решение по `retention.bytes` для `orders`.
  - Закрывает: NFR-3, FR-2, NFR-6.
- [x] **2.9. Сценарии отказов** (~1,5 ч, частично Pi5)
  - Как ронять драйвер: `docker kill` Docker считает ручной остановкой и по `unless-stopped` не перезапускает, поэтому убиваем Python-драйвер внутри (`pkill -9 -x -f "python3 /opt/shopflow/streaming_to_clickhouse.py"`): `spark-submit` выходит с ошибкой, как при настоящем падении.
  - **A1, SIGKILL до записи (2026-09-28):** backlog 3 мин (`--rate 10`, 1686 действий) при остановленном Spark; драйвер убит, когда `offsets/64` уже есть, а `commits/64` нет (0,7 с после старта запроса). Рестарт → батч 64 с тем же диапазоном: 1686 строк, 1686 позиций, одна попытка записи.
  - **A2, SIGKILL после записи `raw_events`, до `stg_*` (2026-09-28):** backlog 2 мин (1116 событий); драйвер убит, как только строки батча появились в `raw_events` (18:37:30). По `query_log`: первая попытка — `raw_events` 166 + 950, `stg_orders` нет; после рестарта батч 65 — `raw_events` 166 + 950 ещё раз и `stg_orders` 419 + 482. Слияние 18:37:42 схлопнуло повторы (`part_log`). `check_pipeline.sh` OK: `orders` 12045 позиций, 5688 = 5688; повторов `(order_id, version)` в `stg_orders` 0.
  - **C, перезагрузка ноутбука (2026-09-28, жёстче, чем `down`/`up`):** стек поднялся сам (`restart: unless-stopped`); Spark ждал `/ping` ~50 с, пока поднималась сеть (backoff 5 → 10 → 20 с), затем продолжил тот же запрос с чекпойнта (`id` тот же, новый `run`), повторного чтения с `earliest` нет. Коннектор `RUNNING RUNNING`, слот `active`, отставание 672 байта; `check_pipeline.sh` OK (`orders` 9695 позиций, 4565 = 4565).
  - **D, повторы Debezium (2026-09-28):** генератор 90 с (`--rate 10`), через 25 с `SIGKILL` Connect, через 20 с `start`. В `raw_events` за окно 56 повторов с новыми позициями Kafka (`orders` 768 позиций / 721 изменение, `customers` 160 / 151), `op=r` 0 (без снапшота). `check_pipeline.sh` OK: `orders` 12813 позиций, 6029 = 6029, статусы совпадают. Повторы остаются в `raw_events` по дизайну и гасятся в `stg_*` по LSN (ADR-0006).
  - **B, ClickHouse на Pi5 остановлен 5 мин (2026-09-28, 18:44:38–18:49:39 UTC, генератор `--rate 5`):** батч 76 упал через 40 с (`Connect timed out`, 4 попытки по 10 с: остановленный контейнер на Pi5 даёт таймаут, а не отказ, ufw отбрасывает SYN), процесс вышел, Docker перезапустил контейнер один раз (`restarts` 1 → 2), entrypoint ждал `/ping` 5 → 10 → 20 → 40 → 80 → 160 с, `unhealthy` на время ожидания. Запрос поднялся в 18:51:27, батч 76 переигран (144 строки), батч 77 догнал backlog (2003 строки за 2,7 с). `check_pipeline.sh` OK: `orders` 15771 позиция, 7442 = 7442, статусы совпадают. Задержка по минутам: max 425 с для событий начала простоя, к 18:52 снова ≤ 31 с.
  - Находка B: потолок backoff 300 с добавил ~1 мин 48 с к простою (ClickHouse поднялся в 18:49:39, следующий `/ping` в 18:51:27). Потолок снижен до 60 с: дорогим был цикл перезапусков JVM, а `curl` раз в минуту почти бесплатен.
  - **B2, «чёрная дыра» (2026-09-28):** на Pi5 `iptables -I DOCKER-USER 1 -s 192.168.0.105 -j DROP` с 18:57:16 до 19:12:16 UTC (снято автоматически, `iptables -S DOCKER-USER` после — только правила ADR-0003), генератор 25 мин `--rate 5`. Батч 85 упал через 40 с (`Connect timed out`), один рестарт, `/ping` каждые ≤ 60 с (новый потолок), запрос поднялся в 19:12:47 (через 31 с после снятия правила), батч 86 догнал 4470 событий за 3,3 с. Зависаний нет, watchdog не понадобился. `check_pipeline.sh` OK: `orders` 21849 позиций, 10245 = 10245, статусы совпадают. Задержка по 5-мин окнам: p95 942 → 762 → 463 → 167 → 29 с (max 954 с = 15 мин простоя + 31 с).
  - Ограничение B2: DROP встал между батчами, поэтому сработал таймаут подключения. Ветка «соединение установлено, сервер молчит» (`socket_timeout` 120 с) проверена только в spike 2.4 (`busybox nc`): попасть в 2-секундную запись можно лишь случайно.
  - Итог 2.9: во всех сценариях сверка Kafka ↔ `raw_events` и Postgres ↔ `stg_*` сходится, потерь и дублей в `stg_*` нет, ручного вмешательства не потребовалось.
  - A) `SIGKILL` Spark; B) ClickHouse на Pi5 остановлен на 5 мин; B2) «чёрная дыра»: DROP для ноутбука в `DOCKER-USER` на Pi5 на 15 мин; C) `down`/`up` ноутбука без `-v`; D) повторы Debezium.
  - Приёмка: после каждого `check_pipeline.sh` сходится, вывод в STATUS; в B и B2 нет цикла быстрых рестартов (ожидание `/ping` в логе), в B2 watchdog или таймаут завершили зависший батч.
  - Закрывает: NFR-6, NFR-4.
- [x] **2.10. Итоги, ревью, закрытие** (~45 мин)
  - Сделано (2026-09-28): runbook, раздел 6 «Spark» и раздел 7 «Аварии» (сброс чекпойнта, простой дольше буфера, `ContractViolation`, как ронять драйвер для проверки); `reviewer`, PR `milestone-2` → `master`.
  - Доказательства перед ревью: `.venv/bin/python -m pytest -q` = 37 passed, `python3 -m pytest -q` = 17 passed, 1 skipped; `ruff check .` и `sqlfluff lint clickhouse/ddl` чистые; оба compose валидны.
  - `reviewer` (2026-09-28): блокеров нет. Исправлено: в runbook устаревший потолок backoff «5 мин» → 60 с; поведение «самая новая версия PK в батче ушла в карантин → в `stg_*` последняя валидная» записано в ADR-0008 и покрыто тестом `test_quarantined_newest_version_leaves_last_valid_one` (итого 38 passed). Инцидент с хешем пароля `spark_writer` (2.3) ревьюер отметил как корректно раскрытый.

Итоги Milestone 2: Spark Structured Streaming (4.0.4, один запрос `foreachBatch`) пишет все события 5 топиков в `raw_events` и последнее состояние `orders`/`order_items` в `stg_*` на Pi5; задержка p95 29 с (NFR-3); повтор батча, повторы Debezium, падение драйвера, перезагрузка ноутбука, недоступность и «чёрная дыра» Pi5 проходят без потерь и дублей в `stg_*` (NFR-4, NFR-6). Неочевидное: коннектор ClickHouse не проверяет значения (проверки в Spark), Spark 4 в ANSI-режиме (`try_cast`), права коннектора (`system.clusters`, `system.macros`, `SELECT`). Условия входа в M3: SCD2 для `dim_*` и `fact_orders` поверх `stg_*`, генерация `products`/`order_items`/`inventory`, решение по `retention.bytes` при новом потоке, дозаливка `dim_*` из `raw_events` (ADR на M3).

**Milestone 2 завершён 2026-09-28.** PR `milestone-2` → `master` создаёт пользователь (push по SSH из сессии Claude недоступен).

## Milestone 3: моделирование данных

Ветка: `milestone-3`. План подтверждён 2026-09-29. Оценка ~12 ч. Шаги на Pi5 выполняет пользователь.
Скоуп: SCD2 `dim_customers`/`dim_products` (FR-3), `fact_orders` поверх `stg_*`, генерация `products`/`order_items`/`inventory`, дневные витрины выручки (FR-4) и воронки (FR-5), решение по `retention.bytes`, дозаливка `dim_*` из `raw_events`. Представления для FR-6/FR-7 перенесены в M5 (PRD, раздел 10).

До кода `architect` проверяет ADR-0009:
1. Источник `valid_from` и закрытие версии SCD2. Кандидат: Spark пишет журнал версий `stg_<table>_versions` (ключ `(id, valid_from)`, RMT по LSN), `dim_*` пересчитывает refreshable MV (`valid_to` через `leadInFrame`). Альтернативы: чтение текущей версии из ClickHouse по ключам батча; состояние Spark (`transformWithState`).
2. Зерно и материализация `fact_orders`: позиция заказа, refreshable MV или представление с `FINAL`, `ASOF JOIN` к `dim_products`.
3. Агрегаты: refreshable MV (инкрементальная MV поверх RMT считает повторы и обновления дважды); полный пересчёт или последние N дней при ~2 млн позиций за 2 недели (ADR-0004).
4. Воронка: `stg_order_status_history` из Spark (ключ `(order_id, status)`, время из `source.ts_ms`), а не разбор `raw_events` в ClickHouse (ADR-0008).
5. Новые stg: `stg_inventory`, журналы версий `customers`/`products`.
6. Дозаливка `dim_*`: Spark batch из `raw_events` с теми же `transforms.py` или сброс чекпойнта (первые события уходят из Kafka ~2026-10-03).
7. SQL SECURITY refreshable MV, гранты `spark_writer`, память и CPU refresh в бюджете ClickHouse, задержка витрин = NFR-3 + период refresh.

- [x] **3.0. Ветка** (~10 мин)
  - Сделано (2026-09-29): PR #3 `milestone-2` → `master` влит пользователем, `master` fast-forward до `47c75b2`, ветка `milestone-3` от `master`.
  - Приёмка пройдена: `git log master..milestone-2` пусто, текущая ветка `milestone-3`.
- [x] **3.1. ADR-0009 «SCD2, факт и витрины», ревью `architect`** (~1 ч)
  - Сделано (2026-09-29): ADR-0009 принят с правками (4 блокера, 7 желательных), PRD 5.2 обновлён, в ADR-0006 правила обработчика SCD2 помечены как заменённые.
  - Блокеры: окно SCD2 по LSN, а не по `ts_us` (часы ноутбука), явная рамка окна и `toNullable` для `leadInFrame`, отбрасывание строк без изменений до `leadInFrame`; история статусов с ключом `(order_id, status, version)` и `min(changed_at)` (снапшот и повтор статуса не перетирают переход), воронка монотонная; свои функции схлопывания для журналов (`latest_per_key` сжал бы историю); `stg_orders.created_at` → `DateTime64(6)` и `join_use_nulls = 1` для ASOF.
  - Желательные: цепочка `DEPENDS ON` (`dim_products_mv` → выручка → воронка), лимит витрин 768 МиБ, `refresh_retries`; `refreshed_at` и `source_watermark` в витринах для NFR-3; `event_time` в `stg_*` и журналах (окно лага FR-8/FR-9 в M4, DQ читает `stg_*`, а не факт); первая версия из снапшота с `valid_from = 1970-01-01`; проверка фильтра дозаливки по `query_log`; порог перехода `fact_orders` на таблицу.
  - Приёмка: вердикт «принять» или «принять с правками», правки внесены.
  - Закрывает: подготовку к FR-3, FR-4, FR-5.
- [x] **3.2. Генератор: каталог товаров** (~1 ч)
  - Сделано (2026-09-29): `generator/model.py`: `Product`, 5 категорий с диапазонами цен, `new_product`, `change_price` (распродажа −20…−40 % в 30 % случаев, иначе дрейф ±3…15 %; всегда отличается, в пределах `NUMERIC(10,2)`), `change_product` (5 % смена категории, иначе цена; ровно один атрибут SCD2). Веса действий: `update_product` 0,025, `new_product` 0,005, клиенты 0,08/0,04 (сумма 1). `generate_orders.py`: `seed_products` доливает каталог до `--products` (по умолчанию 100), а не только в пустую таблицу: тестовый товар из 1.5 засев не блокирует. `updated_at = now()` у всех UPDATE (было у `orders`/`customers`, добавлено у `products`).
  - Приёмка пройдена: `.venv/bin/python -m pytest -q` = 44 passed (6 новых: сумма весов, обработчик на каждое действие, валидность цен, распродажи и дрейф, один атрибут за изменение); `python3 -m pytest -q` = 22 passed, 2 skipped; `ruff check .` чистый. Прогон 120 с (`--rate 5 --seed 4 --no-seasonality`): засеяно 99 товаров; в `cdc.public.products` 102 `c` и 9 `u` (7 смен цены, например 6961.90 → 4713.21, 2 смены категории, обоих сразу 0); UPDATE без сдвига `updated_at`: `products` 0 из 9, `customers` 0 из 42, `orders` 0 из 278. `check_pipeline.sh` OK по 5 топикам, `orders` 10463 = 10463, задержка p95 29,1 с.
  - Засев `products` (категории, цены), изменения цен (обычные и «распродажа»), редкая смена категории; `updated_at = now()` при каждом UPDATE.
  - Приёмка: pytest зелёный; прогон 60 с даёт в `cdc.public.products` события `u` с `before.price ≠ after.price`; `updated_at` сдвигается у всех UPDATE.
  - Закрывает: FR-1, подготовку к FR-3.
- [x] **3.3. Генератор: `order_items` и `inventory`** (~1 ч)
  - Сделано (2026-09-29): `generator/model.py`: `StockRow`, `OrderLine`, `choose_order_lines` (1–5 разных товаров из перемешанных строк с остатком, количество 1–3, но не больше остатка, цена = текущая), `initial_stock` (20–200), `restock_amount` (50–200), склады 1–3. `generate_orders.py`: `new_order` одной транзакцией (строки остатков `FOR UPDATE OF i SKIP LOCKED` → заказ → позиции → списание; нет остатков — нет заказа); новый товар вместе с остатками на 3 складах; `seed_inventory` досоздаёт недостающие пары (товар, склад); действие `restock` (вес 0,02 за счёт `advance_order` 0,45 → 0,43) пополняет одну из 10 самых пустых полок. Возврат остатка при отмене не моделируется (позиция не хранит склад).
  - Приёмка пройдена: `.venv/bin/python -m pytest -q` = 50 passed (6 новых: разные товары в заказе, количество в пределах остатка, цена = прайс, 1–5 позиций, пустой склад, баланс пополнения и расхода); `ruff check .` чистый. Прогон 120 с (`--rate 5 --seed 5 --no-seasonality`, заказы с `order_id > 10464`): засеяно 308 строк остатков; 247 заказов, 744 позиции (3,01 на заказ, от 1 до 5 примерно поровну), заказов без позиций 0, повторов товара в заказе 0; `min(inventory.quantity)` = 15, строк 318 = 106 товаров × 3.
  - `price_at_order`: по Postgres для товаров, не менявшихся после заказа, 685 из 685 совпадают. По истории `cdc.public.products` на момент заказа (`source.ts_us` ≤ `created_at`) 744 из 744 совпадают, позиций без версии товара 0. Сохранение остатков по событиям `cdc.public.inventory`: списано 1493 = продано 1493, пополнено 1074.
  - `check_pipeline.sh` после прогона не запускался (сбой проверки команд в сессии), сверка Kafka ↔ `raw_events` для нового потока будет в 3.4.
  - 1–5 позиций в одной транзакции с заказом, `price_at_order` = текущая цена; засев остатков по складам, списание при заказе, пополнение.
  - Приёмка: pytest; заказов без позиций, созданных после старта, 0; `min(inventory.quantity) >= 0`; `price_at_order` = `products.price` на момент вставки.
  - Риск: старые заказы M1/M2 без позиций, это не ошибка DQ.
- [x] **3.4. Объём Kafka и `retention.bytes`** (~45 мин)
  - Сделано (2026-09-29): генератор 18:27:29–18:57:34 UTC (`--rate 5 --seed 6 --no-seasonality`: 3286 заказов, 3630 смен статуса, 214 изменений товаров, 169 пополнений); размеры партиций по `kafka-log-dirs` до и после, окно 1805 с.
  - Приёмка пройдена: в сутки при `--rate 5` / штатно: `inventory` 400 / 133 МиБ (1 ГиБ за 2,6 / 7,7 дня), `order_items` 347 / 115 (2,9 / 8,9), `orders` 284 / 94 (3,6 / 10,9), `customers` 48 / 16, `products` 11 / 4. Буфер NFR-6 штатно = 7 дней по всем топикам (запас у `inventory` 10 %), при `--rate 5` ~2,6 дня. **Решение: `retention.bytes` не меняем** (ADR-0009, «Последствия»; отметка в ADR-0008).
  - `check_pipeline.sh` после прогона OK: позиции Kafka = `raw_events` по 5 топикам (`order_items` 10494, `inventory` 11089, `orders` 29769); `orders` 13996 = 13996, статусы совпадают; `order_items` 10490 строк, `sum(qty)` 21082, сумма 120706403.11 = Postgres (первая сверка `stg_order_items` на реальном потоке). Задержка p95 29 с, max 30,8 с.
  - Находка: на диске ноутбука свободно 12 ГБ (92 %), в M1 было 26 ГБ. Тома ShopFlow ~125 МБ; Docker-образы 18,7 ГБ (11,2 ГБ освобождаемы) и файлы вне проекта. Худший случай Kafka ≈ 6,3 ГиБ. Чистку решает пользователь.
  - Генератор 30 мин `--rate 5`, пересчёт на штатный режим.
  - Приёмка: байты в сутки по 5 топикам и буфер NFR-6 = min(7 дней, 1 ГиБ / объём) записаны; решение в ADR-0008/0009.
  - Риск: `order_items` с `REPLICA IDENTITY FULL` может стать самым тяжёлым топиком.
- [x] **3.5. DDL новых таблиц** (~45 мин, `/clickhouse-ddl`, Pi5)
  - Миграции `007+` по ADR-0009: журналы версий, `stg_order_status_history`, `stg_inventory`, `event_time` в `stg_orders`/`stg_order_items`, `stg_orders.created_at` → `DateTime64(6)`. Гранты в `create-ch-users.sh`. MV и `fact_orders` — в 3.8–3.11.
  - Сначала на временном ClickHouse 25.8 на ноутбуке (как в 1.8): права определителя и атомарность refresh без `APPEND` с TO-таблицей; `DEPENDS ON` при цепочке; ASOF JOIN с `DateTime64(3)`/`(6)`; `leadInFrame` с рамкой и `toNullable`. Результаты в ADR-0009.
  - Приёмка: `sqlfluff lint clickhouse/ddl` чистый; `apply-ddl.sh` и `create-ch-users.sh` дважды без ошибок; `SHOW GRANTS FOR spark_writer` только нужные таблицы; проверки стенда записаны.
  - Сделано на ноутбуке (2026-09-30): миграции `007_stg_customer_versions`, `008_stg_product_versions`, `009_stg_order_status_history`, `010_stg_inventory`, `011_stg_orders_event_time`, `012_stg_orders_created_at_us`, `013_stg_order_items_event_time` (у `event_time` явный DEFAULT, чтобы текущий job писал без неё). `ALTER` с двумя действиями ClickHouse принимает, но `sqlfluff` не разбирает, поэтому одно действие на файл. Гранты `SELECT, INSERT` на 4 новые таблицы в `create-ch-users.sh`. Skill `clickhouse-ddl` (локально) приведён к ADR-0009.
  - Стенд: временный ClickHouse 25.8.33.6 (`ch-m3-test`, конфиги Pi5, только `127.0.0.1:18123`, в сети `shopflow_default`). `sqlfluff` чистый; `apply-ddl.sh` и `create-ch-users.sh` по два прогона без ошибок; `SHOW GRANTS FOR spark_writer` = `SELECT, INSERT` на 7 таблиц `shopflow` + `system.clusters`/`system.macros`. Результаты проверок в ADR-0009, «Проверки стенда»: SCD2-прототип корректен на всех граничных случаях; refresh подменяет TO-таблицу через `EXCHANGE`, определителю нужны права уровня базы → `shopflow`; одна refreshable MV на таблицу; `DEPENDS ON` ждёт окончания зависимости (3034 мс при 3-секундной зависимости); ASOF с `DateTime64(6)` верен, с мс — старая цена; `join_use_nulls` даёт NULL; Spark пишет `stg_orders` без `event_time` после миграций, микросекунды сохраняются. Прототипы MV в scratchpad, перейдут в DDL в 3.8–3.11.
  - Pi5 (2026-09-30, выполнил пользователь с ноутбука): `apply-ddl.sh` и `create-ch-users.sh` по два прогона без ошибок; `SHOW GRANTS FOR spark_writer` = `SELECT, INSERT` на 7 таблиц + `system.clusters`/`system.macros`; 9 таблиц с ключами по ADR-0009; мутация `MODIFY COLUMN created_at DateTime64(6, 'UTC')` на `stg_orders` `is_done = 1`.
  - Приёмка пройдена: после миграций текущий job пишет без ошибок: генератор 30 с (`--seed 7`), `check_pipeline.sh` OK по 5 топикам, `orders` 14057 = 14057, статусы совпадают, `order_items` 10680 строк, сумма 122641173.69 = Postgres; задержка p95 28,9 с; `spark` `healthy`, рестартов 0, ошибок в логе 0.
- [x] **3.6. Преобразования Spark и тесты** (~1 ч)
  - Сделано (2026-09-30): `transforms.py`: `StgSpec` с составным ключом `keys` (из ключа сообщения, есть и у `d`) и флагом `versioned`; `Field.nullable` (NULL в nullable-колонке Postgres → `''`, а не карантин), вид `uint32`; `event_time` = `source.ts_ms` во всех stg; спецификации `INVENTORY`, `CUSTOMER_VERSIONS` (атрибуты `name`, `address`, `segment`), `PRODUCT_VERSIONS` (`name`, `category`, `price`). Для журналов `valid_from` = `source.ts_us`, `is_snapshot` = `op = r`, карантин `bad valid_from` без `ts_us`. `latest_per_key` группирует по `spec.dedup_key`: PK для stg, `(PK, valid_from)` для журналов. `status_history_rows` (строка для `c`/`r` и для `u` со сменой статуса или без `before`) и `latest_status_rows` (схлопывание по `(order_id, status, version)`). В job — логирование карантина по составному ключу; запись новых таблиц — в 3.7.
  - Приёмка пройдена: `.venv/bin/python -m pytest -q` = 72 passed (22 новых), `python3 -m pytest -q` = 28 passed, 2 skipped; `ruff check .` чистый; модуль импортируется в образе Spark (Python 3.10.12). Случаи: колонки каждой спецификации и истории статусов = колонкам DDL после миграций (тест разбирает `clickhouse/ddl`, мутант с пропущенной колонкой ловится); `event_time` из `ts_ms`; микросекунды `created_at`; составной ключ `inventory`, `d` с ключом из сообщения, карантин `warehouse_id` −1, 2^31 и без ключа; журнал: `valid_from` из `ts_us`, `is_snapshot`, три транзакции одного ключа в батче дают A / D (310) / удаление (500), два UPDATE с одним `ts_us` схлопнуты, повтор Debezium детерминирован, NULL `address`/`segment` → `''`, без `ts_us` и с `price` `abc` — карантин; история: `created`, `paid`, UPDATE другой колонки и `d` не пишутся, снапшот с `is_snapshot = 1`, без `before` пишется, повтор схлопнут, повторный снапшот (LSN 900) остаётся отдельной строкой, NULL `status` и отрицательный `order_id` — карантин.
  - `StgSpec` для `customers`, `products`, `inventory`, история статусов; `valid_from` из `source.ts_us`; правила ADR-0006 (схлопывание `(id, ts_us)` до max LSN, `d` закрывает, `r` без изменений не открывает).
  - Приёмка: `.venv/bin/python -m pytest -q` зелёный, случаи: два UPDATE в транзакции, DELETE, `op=r` без изменений, переход статуса, повтор Debezium.
  - Закрывает: FR-3 (логика), NFR-4.
- [x] **3.7. Подключение к streaming job и сверка** (~1 ч)
  - Сделано (2026-09-30): `streaming_to_clickhouse.py`: общая `_write_quarantined` (карантин в лог, схлопывание, запись), поверх неё `write_stg` для 5 спецификаций (`stg_orders`, `stg_order_items`, `stg_inventory`, 2 журнала) и `write_status_history`; порядок в батче: `raw_events` → stg и журналы → история статусов. `check_pipeline.sh`: `stg_inventory` против Postgres (строки, `sum(quantity)`); журналы против `raw_events` (`uniqExact(event_key, source.ts_us)` по `c/u/d/r` с первого события журнала); заказы без текущего статуса в истории (с первого события истории); `CLICKHOUSE_URL` переопределяет сервер.
  - Стенд (временный ClickHouse, разовый job с `earliest` и одноразовым чекпойнтом в контейнере, боевой том не тронут): 3 батча, 57298 событий = вся Kafka; карантин только известный `order_id = -5` (2.7) в `stg_orders` и истории; запись `warehouse_id` Spark `Int` → `UInt32` проходит. `check_pipeline.sh` против стенда OK целиком: позиции по 5 топикам, `orders` 14057, `order_items` 10680, `inventory` 426 строк / 48323 = Postgres, журналы 5019 / 384 пар `(key, ts_us)` = `raw_events` (у `customers` 5045 событий: 26 повторов Debezium из 1.7 C и 2.9 D схлопнулись), заказов без текущего статуса в истории 0.
  - Pi5: образ пересобран, `spark` пересоздан, запрос продолжил с боевого чекпойнта (тот же `id`). Генератор 90 с (`--seed 8`): позиции по 5 топикам OK, `orders` 14233 = 14233, `order_items` 11206 = 11206; журналы с 19:39 UTC: 50 / 11 = `raw_events`; заказов без текущего статуса 0; задержка p95 28,6 с; `healthy`, рестартов 0, ошибок в логе 0.
  - Не закрыто в 3.7: `stg_inventory` на Pi5 316 из 429 строк (MISMATCH ожидаем): это таблица состояния, в ней только строки, изменённые после выкатки. Полная сверка — после дозаливки из `raw_events` в 3.8; на стенде с полной историей она сходится.
  - `foreachBatch` пишет новые таблицы; `check_pipeline.sh` расширен.
  - Приёмка (генератор остановлен): `stg_inventory FINAL` = Postgres (строки, `sum(quantity)`); число версий = числу различных `(id, ts_us)` изменений атрибутов в `raw_events`.
- [ ] **3.8. `dim_customers`, `dim_products` и дозаливка** (~1 ч, Pi5)
  - Материализация SCD2 по ADR-0009, дозаливка истории из `raw_events`.
  - Приёмка: одна `is_current=1` на живой ключ, их число = `count(*)` Postgres; атрибуты текущей версии = Postgres; нет разрывов и перекрытий (`valid_to` i-й = `valid_from` (i+1)-й); ручной тест: три смены адреса, две в одной транзакции.
  - Закрывает: FR-3.
  - Сделано на ноутбуке (2026-09-30): `014_dim_customers_mv.sql`, `015_dim_products_mv.sql` по прототипу 3.5 (`REFRESH EVERY 2 MINUTE SETTINGS refresh_retries = 3`, `max_memory_usage` 512 МиБ, `max_threads = 2`). `spark-jobs/backfill_from_raw.py`: по дням читает `raw_events` через каталог, прогоняет через `write_stg`/`write_status_history` потока, `--from/--to/--tables`. Грант `SELECT(partition, partition_id, rows, bytes_on_disk, database, table, active) ON system.parts` для `spark_writer`: коннектор при чтении планирует задачи по партициям (без него `ACCESS_DENIED`). `check_pipeline.sh`: раздел измерений — текущие версии = Postgres (число + MD5 отсортированных атрибутов), ключей с >1 текущей, разрывов/перекрытий и `valid_to < valid_from` — 0.
  - Стенд: после догона `check_pipeline.sh` OK целиком, `dim_customers` 1692 и `dim_products` 143 текущих версий = Postgres по MD5, структура 0/0/0; refresh ~25 мс. Дозаливка 2026-09-26…30 за 35 с (104169 строк `raw_events` без `FINAL`, в том числе дубли двух прогонов потока), повторно поверх полных данных — сверка без изменений (идемпотентность). Фильтр по `topic` и `event_time` доходит до ClickHouse (`query_log`: запрос на партицию, чужие партиции читают 0 строк). Урок: в образе Python 3.10, `datetime.UTC` нет (3.11+), ruff это не ловит; `UInt64` коннектор читает как `Decimal(20,0)`.
- [ ] **3.9. `fact_orders`** (~1 ч)
  - Зерно «позиция заказа», `ASOF JOIN` к `dim_products`.
  - Приёмка (старше окна лага 10 мин): строки и `sum(quantity * price_at_order)` = Postgres; строк с `price_at_order` ≠ цены `dim_products` на момент заказа 0 (сквозная проверка SCD2).
- [ ] **3.10. Витрина выручки FR-4** (~45 мин)
  - `mart_revenue_daily` (день, категория, выручка, заказы, позиции), refreshable MV.
  - Приёмка: по дням старше окна лага = запросу к Postgres; время refresh (`system.view_refreshes`) и память записаны.
  - Закрывает: FR-4.
- [ ] **3.11. Витрина воронки FR-5** (~45 мин)
  - `mart_funnel_daily`: created → paid → delivered, конверсии, медиана времени до оплаты.
  - Приёмка: `created` по дням = Postgres по `created_at`; у заказов в `paid`/`shipped`/`delivered` есть переход `paid`, у `delivered` есть `delivered` (пропусков 0).
  - Закрывает: FR-5. Риск: история до 3.7 только из дозаливки `raw_events`.
- [ ] **3.12. Нагрузка на Pi5 и задержка витрин** (~45 мин, Pi5)
  - Генератор 30 мин `--rate 5` со всеми refresh.
  - Приёмка: `LoadAverage1` и RSS ClickHouse в рамках ADR-0004; парты не растут; p95 refresh < периода; задержка `stg_*` < 5 мин (NFR-3); задержка витрин записана.
- [ ] **3.13. Итоги, ревью, закрытие** (~45 мин)
  - Runbook, STATUS, `dq-tester` (предварительно FR-9 по `fact_orders`), `reviewer`, PR.
  - Приёмка: pytest, ruff, sqlfluff зелёные; блокеров у `reviewer` нет.
