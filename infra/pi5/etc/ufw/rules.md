# ufw on Pi5 (ADR-0003)

```bash
sudo ufw default deny incoming
sudo ufw default allow outgoing
sudo ufw allow from <LAN_CIDR> to any port 22 proto tcp comment 'ssh from LAN'
sudo ufw enable
```

ufw protects host services only. Container ports (8123, 9000, 8080) are protected by binding to `$PI5_HOST` in compose and by the `DOCKER-USER` rule from `after.rules.snippet`.
