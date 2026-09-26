# Pi5 host setup runbook

Raspberry Pi 5 (8 GB) runs ClickHouse and Airflow 24/7. This runbook rebuilds the host from a fresh
Raspberry Pi OS (Debian 13 trixie, 64-bit) with SSH enabled. Files under `etc/` and `boot/` mirror their
paths on the Pi; `*.snippet` files are appended, the rest are copied as is.

Placeholders: `<PI5_IP>` (e.g. `192.168.0.151`), `<LAN_CIDR>` (e.g. `192.168.0.0/24`), `<HDD_UUID>`,
`<disk>` (from `/dev/disk/by-id/`). Real values live in `.env`, never in git.

Decisions: [ADR-0002](../../docs/adr/0002-pi5-storage-external-hdd.md) (storage),
[ADR-0003](../../docs/adr/0003-pi5-network-exposure.md) (network),
[ADR-0004](../../docs/adr/0004-pi5-memory-budget.md) (memory).

## 1. SSH by key only

On the laptop:

```bash
ssh-keygen -t ed25519 -f ~/.ssh/id_ed25519_pi5
ssh-copy-id -i ~/.ssh/id_ed25519_pi5.pub <user>@<PI5_IP>
# ~/.ssh/config: Host pi5 / HostName <PI5_IP> / IdentityFile ~/.ssh/id_ed25519_pi5 / IdentitiesOnly yes
```

On the Pi (keep a second SSH session open): copy `etc/ssh/sshd_config.d/00-shopflow.conf`, then
`sudo sshd -t && sudo systemctl reload ssh`. The `00-` prefix matters: sshd takes the first value it reads.

Check: `ssh -o PubkeyAuthentication=no -o PreferredAuthentications=password pi5` -> `Permission denied (publickey)`.

## 2. Data disk (USB HDD)

```bash
DISK=/dev/disk/by-id/<disk>
readlink -f $DISK-part1                     # must be the HDD partition, not the SD card
sudo mkfs.ext4 -L shopdata -m 1 -E lazy_itable_init=0,lazy_journal_init=0 $DISK-part1
sudo mkdir -p /mnt/data
```

Append the first line of `etc/fstab.snippet` with `<HDD_UUID>` from `sudo blkid -s UUID -o value $DISK-part1`,
then `sudo systemctl daemon-reload && sudo mount /mnt/data`.
`usb_max_current_enable=1` is set by firmware with the official 27 W PSU; check with `vcgencmd get_config usb_max_current_enable`.

Check: `findmnt /mnt/data`; `sudo smartctl -H /dev/sda` -> `PASSED`.

## 3. Keep writes off the SD card

- Swap: copy `etc/rpi/swap.conf.d/90-shopflow.conf` (zram only, no `/var/swap` file).
- Journal on the HDD: `sudo mkdir -p /mnt/data/journal /var/log/journal`, append the second line of
  `etc/fstab.snippet`, copy `etc/systemd/journald.conf.d/90-shopflow.conf`.

Reboot. Check: `cat /proc/swaps` shows only `/dev/zram0`; `findmnt /var/log/journal` shows `/dev/sda1[/journal]`.

## 4. Docker

Configure before installing, so the first start already uses the HDD:

- copy `etc/docker/daemon.json` (`data-root` on the HDD, log rotation, containerd snapshotter off);
- copy `etc/systemd/system/docker.service.d/10-shopflow.conf` (start only after `/mnt/data` is mounted);
- install Docker Engine and the compose plugin from the official Debian repository, `sudo usermod -aG docker <user>`;
- append `boot/firmware/cmdline.txt.snippet` to the single line of `/boot/firmware/cmdline.txt`
  (the file has no trailing newline: verify with `grep -c ''`, not `wc -l`), then reboot.

Check: `docker info -f '{{.DockerRootDir}} {{.Driver}}'` -> `/mnt/data/docker overlay2`; `docker info` has no
`No memory limit support` warning; `docker run --rm -m 64m alpine cat /sys/fs/cgroup/memory.max` -> `67108864`.

## 5. Network (Wi-Fi)

See [network.md](network.md): DHCP reservation on the router, hardware MAC, Wi-Fi power saving off.

## 6. Firewall

See [etc/ufw/rules.md](etc/ufw/rules.md). Allow SSH before `ufw enable`. Append `etc/ufw/after.rules.snippet`
for the `DOCKER-USER` chain. Container ports are published only on `<PI5_IP>` (`"${PI5_HOST}:8123:8123"`).

Check from the laptop: a container published on `<PI5_IP>` is reachable; a host service on another port
(`python3 -m http.server 5000`) is not; `sudo ss -tlnp` shows no `0.0.0.0` for 8123/9000/8080.

## 7. Memory test (optional)

[memtest/](memtest/) is the one-off stand used to size container limits (ADR-0004):

```bash
scp -r infra/pi5/memtest pi5:~/          # on the laptop
cd ~/memtest                             # on the Pi: create .env with PI5_HOST, CLICKHOUSE_PASSWORD, AIRFLOW_JWT_SECRET, AIRFLOW_DB_PASSWORD
docker compose -f docker-compose.memtest.yml up -d && sleep 90 && ./loadtest.sh
docker compose -f docker-compose.memtest.yml down -v
```

## Measured baseline (2026-09-26)

| Item | Value |
| --- | --- |
| OS / kernel | Debian 13 trixie, 6.18.50+rpt-rpi-2712 |
| Docker / Compose | 29.8.1 / v5.5.1 |
| HDD | WD5000LUCT 5400 rpm via JMicron USB bridge |
| Sequential write / read (fio, 1M, direct) | 107 / 110 MB/s |
| Random read / write (fio, 4K, QD16) | 209 / 198 IOPS, read latency avg 76 ms |
| ClickHouse + Airflow idle / peak | ~1 GB / ~3.3 GB, see ADR-0004 |
| Throttling, temperature under load | `0x0`, 53 °C CPU (active cooler), 29 °C HDD |
