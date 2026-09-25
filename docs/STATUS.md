# Статус ShopFlow

Обновлено: 2026-09-25

## Решения
- Брокер: Kafka (KRaft), см. `docs/adr/0001-message-broker-kafka.md`
- Хранилище Pi5: ОС на SD, данные на внешнем USB HDD 500 ГБ, см. `docs/adr/0002-pi5-storage-external-hdd.md`
- Сеть Pi5: bind портов на LAN-IP, ufw для хоста, `DOCKER-USER` для контейнеров, см. `docs/adr/0003-pi5-network-exposure.md`
- Дашборд: Grafana или Superset, не решено (PRD, раздел 13). Допущение: на Pi5 резерв 0,5 ГБ под Grafana; Superset, если выберем, запускается на ноутбуке.
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
- [ ] **0.3. Базовая настройка ОС Pi5** (~40 мин)
  - Отдельный ключ ed25519, алиас `pi5` в `~/.ssh/config`, `sshd_config.d/00-shopflow.conf` (пароли и root выключены), `apt full-upgrade`.
  - Решить судьбу `unattended-upgrades`: выключить или оставить без автоматического reboot (автономность ≥ 2 недель).
  - Сделано (2026-09-25): вход по ключу, пароль отклоняется (`Permission denied (publickey)`), NTP синхронизирован на Pi5 и ноутбуке. Осталось: перезагрузка после `full-upgrade` (совместим с 0.4).
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
- [ ] **0.9. Проверка памяти: ClickHouse и Airflow** (~1 ч)
  - Пробный запуск arm64-образов, тома на `/mnt/data`, порты на `$PI5_HOST`. Разовая проверка, не итоговый compose.
  - ClickHouse: `max_server_memory_usage` ~2 ГБ, уменьшенный `mark_cache_size`, system-логи (`trace_log`, `metric_log`, `asynchronous_metric_log`, `query_log`) выключены или с TTL. Airflow: LocalExecutor, метабаза Postgres, 1–2 воркера веб-сервера, `load_examples=False`.
  - Приёмка: сумма `mem_limit` ≤ 6 ГБ (включая резерв 0,5 ГБ под дашборд); через 10 мин после старта `docker stats --no-stream` ≤ 3 ГБ; под нагрузкой (`SELECT ... FROM numbers(1e9) GROUP BY` + тестовый DAG) `dmesg | grep -i oom` пуст и `free -m` показывает available ≥ 1 ГБ; `curl http://$PI5_HOST:8123/ping` возвращает `Ok.`; `nc -zv` проходит для 8123, 9000, 8080.
  - Закрывает: NFR-7. Итог в ADR-0004 (версия Airflow, executor, лимиты).
- [ ] **0.10. Нагрузочный тест диска** (~45 мин)
  - `fio` или запись 10–20 ГБ на `/mnt/data`, параллельно `dmesg -w`.
  - Приёмка: в `dmesg` нет `over-current`, `USB disconnect`, `I/O error`; `vcgencmd get_throttled` до и после теста выводит `throttled=0x0`; скорость последовательной записи и случайного чтения записана; наличие активного кулера зафиксировано.
  - Закрывает: NFR-7, ADR-0002.
- [ ] **0.11. Конфиги Pi5 в git, тест перезагрузки, итоги** (~30 мин)
  - В `infra/pi5/`: фрагмент `fstab`, `daemon.json`, systemd drop-in, правила ufw, правки `config.txt` и `README.md` (runbook: порядок применения и команды проверки). Только плейсхолдеры вместо IP и секретов.
  - Тест: после `sudo reboot` на Pi5 с ноутбука проходят `ssh pi5 'findmnt /mnt/data && docker ps'` и `nc -zv $PI5_HOST 8123` (тестовый контейнер поднялся сам).
  - Записать сюда: IP, версии ОС и Docker, замеры памяти, диска и `get_throttled`.
  - Закрывает: ADR-0002 («конфигурацию храним в git»), NFR-6 (Pi5 сам восстанавливается после перезагрузки).

`architect` проверил план (2026-09-25): принят с правками, правки внесены.
После 0.11: `reviewer` по диффу ветки.
