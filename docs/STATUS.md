# Статус ShopFlow

Обновлено: 2026-09-26

## Решения
- Брокер: Kafka (KRaft), см. `docs/adr/0001-message-broker-kafka.md`
- Хранилище Pi5: ОС на SD, данные на внешнем USB HDD 500 ГБ, см. `docs/adr/0002-pi5-storage-external-hdd.md`
- Сеть Pi5: bind портов на LAN-IP, ufw для хоста, `DOCKER-USER` для контейнеров, см. `docs/adr/0003-pi5-network-exposure.md`
- Дашборд: Grafana или Superset, не решено (PRD, раздел 13). Допущение: на Pi5 резерв 0,5 ГБ под Grafana; Superset, если выберем, запускается на ноутбуке.
- Память Pi5: жёсткие `mem_limit`, LocalExecutor, урезанный ClickHouse, см. `docs/adr/0004-pi5-memory-budget.md`
- Доступ Pi5 → Postgres на ноутбуке (для FR-8): решаем в Milestone 4 отдельным ADR.
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
- [ ] **1.1. Python-инструменты** (~20 мин)
  - `pyproject.toml` (ruff, pytest), `.sqlfluff` (диалект `clickhouse`), `requirements-dev.txt`, `.venv`.
  - Приёмка: `ruff check .` и `sqlfluff lint clickhouse/ddl` запускаются без ошибок конфигурации.
  - Риск: неполная поддержка ClickHouse в sqlfluff, исключения фиксируем в `.sqlfluff`.
- [ ] **1.2. ADR-0005 и ADR-0006, ревью `architect`** (~1 ч)
  - Приёмка: вердикт «принять» или «принять с правками», правки внесены; PRD 5.1/5.2 совпадает с ADR.
  - Закрывает: подготовку к FR-1, NFR-4, NFR-6.
- [ ] **1.3. Postgres в `docker-compose.laptop.yml`** (~45 мин)
  - Postgres 17, `wal_level=logical`, `max_slot_wal_keep_size`, порт на `127.0.0.1`; `postgres/init/001_schema.sql`: DDL 5.1, REPLICA IDENTITY, публикация на 5 таблиц, роль `debezium` (`DEBEZIUM_PASSWORD` в `.env.example`).
  - Приёмка: `SHOW wal_level` = `logical`; `relreplident` = `f` у 5 таблиц; `pg_publication_tables` = 5 строк; `rolreplication` у `debezium` = `t`.
  - Закрывает: NFR-1, инвариант Postgres.
  - Риск: init-скрипты выполняются только на пустом томе.
- [ ] **1.4. Kafka (KRaft) и Kafka Connect (Debezium)** (~1 ч)
  - Kafka 4.x KRaft, внутренний и внешний listener, ретеншн по ADR-0005; Connect на образе Debezium 3.x, RF=1, `mem_limit`, `restart: unless-stopped`.
  - Приёмка: все сервисы `healthy`; `curl -s localhost:8083/connector-plugins` содержит `io.debezium.connector.postgresql.PostgresConnector`.
  - Закрывает: NFR-1.
  - Риск: `advertised.listeners` для хоста и контейнеров.
- [ ] **1.5. Регистрация коннектора Debezium** (~45 мин)
  - `debezium/postgres-connector.json` (`topic.prefix=cdc`, `pgoutput`, 5 таблиц, пароль через `${env:...}`), `scripts/register-connector.sh` (идемпотентный `PUT`).
  - Приёмка: коннектор и задача `RUNNING`; после ручных INSERT/UPDATE/DELETE в `orders` в `cdc.public.orders` видны `op` `c`, `u` (с непустым `before`), `d`; есть все 5 топиков `cdc.public.*`.
  - Закрывает: FR-1.
- [ ] **1.6. Генератор нагрузки** (~1 ч)
  - `generator/generate_orders.py` (psycopg 3, `--rate`, `--duration`, `--seed`): засев `customers`, INSERT `orders`, переходы статусов, UPDATE адреса и сегмента; тесты `tests/test_generator.py`.
  - Приёмка: `python3 -m pytest -q` зелёный, `ruff check .` чистый; после прогона 60 с строки растут, статусы только допустимые.
  - Вне скоупа: `products`, `order_items`, `inventory` генерируем в M3.
- [ ] **1.7. Сквозная проверка CDC и перезапуск ноутбука** (~1 ч)
  - `scripts/check_cdc_counts.sh`; остановка `connect` на 2 мин под нагрузкой; `down && up -d` без `-v`.
  - Приёмка: число событий `op=c` совпадает с `count(*)` в Postgres; нового снапшота (`op=r`) нет; слот `active=t`, WAL в слоте после догона в пределах МБ.
  - Закрывает: FR-1, NFR-6 (сторона ноутбука).
- [ ] **1.8. DDL ClickHouse** (~45 мин)
  - `clickhouse/ddl/000_database.sql` … `004_fact_orders.sql` по ADR-0006, `IF NOT EXISTS`; `scripts/apply-ddl.sh` (HTTP, креды из env, без вывода).
  - Приёмка: `sqlfluff lint clickhouse/ddl` чистый.
  - Закрывает: NFR-4, NFR-5.
- [ ] **1.9. `docker-compose.pi5.yml`: только ClickHouse** (~1 ч, `/deploy-pi5`)
  - ClickHouse 25.8, `mem_limit` 2816m, `clickhouse/config.d/shopflow.xml` по ADR-0004, профиль с `max_memory_usage`, порты на `${PI5_HOST}`. Доставка `rsync` в `~/shopflow`; стенд memtest остановить (`down` без `-v`).
  - Приёмка: `config -q` проходит; `curl -s http://$PI5_HOST:8123/ping` = `Ok.`; `ss -tlnp` показывает 8123/9000 только на `$PI5_HOST`; лимит 2816m; после reboot отвечает.
  - Закрывает: NFR-2, NFR-7, PRD 6.1, ADR-0003, ADR-0004.
- [ ] **1.10. DDL на Pi5 и ручная заливка тестовых событий** (~1 ч)
  - `apply-ddl.sh` дважды; `scripts/load_sample_events.sh` (одноразовый, в M2 заменит Spark): события из `cdc.public.*` в `raw_events` через `JSONEachRow`; две версии одной строки в `fact_orders`.
  - Приёмка: 4 таблицы с ожидаемыми движками; `SELECT topic, op, count() FROM raw_events GROUP BY ALL` совпадает с выгрузкой; TTL 30 дней в `SHOW CREATE`; `FINAL` по `order_item_id = 1` даёт одну строку со старшей версией.
  - Закрывает: пункт M1 про ClickHouse, NFR-4 (smoke), FR-2 (сеть ноутбук → Pi5).
- [ ] **1.11. Итоги, ревью, закрытие** (~45 мин)
  - `docs/runbook-laptop.md`, STATUS, `reviewer`, PR `milestone-1` → `master`.
  - Приёмка: блокеров нет; pytest, ruff, sqlfluff зелёные; доказательства в STATUS.
