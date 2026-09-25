# Статус ShopFlow

Обновлено: 2026-09-25

## Решения
- Брокер: Kafka (KRaft), см. `docs/adr/0001-message-broker-kafka.md`
- Хранилище Pi5: ОС на SD, данные на внешнем USB HDD 500 ГБ, см. `docs/adr/0002-pi5-storage-external-hdd.md`
- Дашборд: Grafana или Superset, не решено (PRD, раздел 13)
- Репозиторий: `docs/` и PRD в git; `.claude/`, `CLAUDE.md`, `.env` локально

## Milestone 0: подготовка окружения

Ветка: `milestone-0`. Исходное состояние: ОС на Pi5 установлена, SSH работает.
Доступа к Pi5 у Claude нет: команды для Pi5 выполняет пользователь и присылает вывод. Файлы репозитория пишет Claude на ноутбуке.

- [ ] **0.1. `.gitignore` и первый коммит документации** (~15 мин)
  - Игнорировать `.env`, `.claude/`, `CLAUDE.md`; в git: `.gitignore`, `docs/`, PRD.
  - Приёмка: `git check-ignore .env .claude/settings.json CLAUDE.md` выводит все три пути; `git check-ignore docs/STATUS.md PRD-retail-cdc-platform.md .gitignore` ничего не выводит.
  - Риск: секреты в документации. Перед коммитом `grep -rnE '(PASSWORD|TOKEN)=.+' docs PRD-*.md` находит только плейсхолдеры.
- [ ] **0.2. `.env.example`** (~20 мин)
  - Переменные раздела 9 PRD, плюс `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `AIRFLOW_ADMIN_USER`, `AIRFLOW_ADMIN_PASSWORD`, `LAPTOP_HOST`. Значения только плейсхолдеры.
  - Папки из раздела 8 создаём по мере надобности в следующих milestone.
  - Приёмка: `git ls-files .env` пусто; все переменные раздела 9 есть в `.env.example`.
- [ ] **0.3. Базовая настройка ОС Pi5** (~40 мин)
  - `apt full-upgrade`, вход только по ключу (`PasswordAuthentication no`), алиас `pi5` в `~/.ssh/config` на ноутбуке, NTP.
  - Приёмка: `ssh pi5 'uname -m; free -m; timedatectl show -p NTPSynchronized'` выводит `aarch64`, ~8 ГБ и `yes`; `ssh -o PubkeyAuthentication=no pi5` получает `Permission denied`.
  - Закрывает: NFR-2.
  - Риск: потеря доступа. Пока меняется конфиг `sshd`, держать вторую SSH-сессию открытой.
- [ ] **0.4. USB HDD** (~45 мин)
  - Синий порт USB 3.0, ext4, `/mnt/data` по UUID с `nofail`, `usb_max_current_enable=1`, SMART, `hdparm -S 0` при засыпании диска.
  - Приёмка: `findmnt /mnt/data` показывает ext4; `sudo smartctl -H /dev/sda` (при необходимости `-d sat`) выводит `PASSED`; после `reboot` диск смонтирован.
  - Закрывает: NFR-2, ADR-0002.
  - Риск: форматирование стирает диск. Сначала устройство по `lsblk`.
- [ ] **0.5. Защита SD от износа** (~20 мин)
  - journald в память (`Storage=volatile`) или `log2ram`; swap убрать с SD (`dphys-swapfile` заменить на zram или отключить).
  - Приёмка: в `swapon --show` нет файла на SD; `journalctl --disk-usage` показывает журнал в `/run`.
  - Закрывает: ADR-0002, критерий успеха «≥2 недели автономно».
- [ ] **0.6. Docker на Pi5** (~30 мин)
  - Docker Engine и compose plugin, пользователь в группе `docker`, `data-root=/mnt/data/docker`, systemd drop-in `RequiresMountsFor=/mnt/data`.
  - Приёмка: `docker info -f '{{.DockerRootDir}}'` выводит `/mnt/data/docker`; `docker run --rm hello-world` проходит; `systemctl show docker -p RequiresMountsFor` содержит `/mnt/data`.
  - Закрывает: NFR-2.
- [ ] **0.7. Фиксированные адреса Pi5 и ноутбука** (~20 мин)
  - DHCP-резервация на роутере для Pi5 и ноутбука (адрес ноутбука нужен Airflow для сверки в Milestone 4).
  - Приёмка: после перезагрузки обоих `ping -c 3 $PI5_HOST` с ноутбука и `ping -c 3 $LAPTOP_HOST` с Pi5 отвечают с теми же адресами.
  - Закрывает: PRD 6.1, NFR-6.
  - Риск: вне домашней сети сверка работать не будет. Приемлемо, но зафиксировать.
- [ ] **0.8. Файрвол и публикация портов** (~40 мин)
  - `ufw default deny incoming`; разрешить 22, 8123, 9000, 8080 только из LAN-подсети. Порты в compose публиковать на LAN-адрес, так как Docker обходит ufw.
  - Приёмка: на тестовом контейнере (`docker run -d --rm -p $PI5_IP:8123:80 nginx`) `nc -zv $PI5_HOST 8123` с ноутбука проходит; `nc -zv $PI5_HOST 5000` не проходит; на роутере нет проброса портов.
  - Закрывает: PRD 6.1.
  - Риск: потеря SSH. Сначала `ufw allow`, потом `ufw enable`.
- [ ] **0.9. Проверка памяти: ClickHouse и Airflow** (~1 ч)
  - Пробный запуск arm64-образов без данных, тома на `/mnt/data`, замер `docker stats`. Разовая проверка, не итоговый compose.
  - Приёмка: `curl http://$PI5_HOST:8123/ping` возвращает `Ok.`; ClickHouse + Airflow ≲ 3 ГБ в покое; `nc -zv` проходит для 8123, 9000, 8080.
  - Закрывает: NFR-7.
  - Риск: Airflow не влезает в бюджет. Тогда LocalExecutor и лёгкая метабаза, решение в ADR. Дашборд на Pi5 (Superset) добавит ~1 ГБ.
- [ ] **0.10. Нагрузочный тест диска** (~45 мин)
  - `fio` или запись 10-20 ГБ на `/mnt/data`, параллельно `dmesg -w`.
  - Приёмка: в `dmesg` нет `over-current`, `USB disconnect`, `I/O error`; скорость последовательной записи и случайного чтения записана.
  - Закрывает: NFR-7, ADR-0002.
- [ ] **0.11. Конфиги Pi5 в git, тест перезагрузки, итоги** (~30 мин)
  - В `infra/pi5/`: фрагмент `fstab`, `daemon.json`, systemd drop-in, правила ufw, правки `config.txt`.
  - Тест: после `sudo reboot` на Pi5 с ноутбука проходят `ping`, `ssh pi5 'findmnt /mnt/data && docker run --rm hello-world'` и `nc -zv` на тестовый порт.
  - Записать сюда: IP, версии ОС и Docker, замеры памяти и диска.
  - Закрывает: ADR-0002 («конфигурацию храним в git»), NFR-6.

Проверка `architect` до начала: сетевая схема (публикация на LAN-адрес + ufw, доступ Pi5 → Postgres на ноутбуке), бюджет памяти ~6 ГБ с учётом дашборда, каталог `infra/pi5/` вне раздела 8 PRD.

После 0.11: `reviewer` по диффу ветки.
