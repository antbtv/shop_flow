# ufw on Pi5 (ADR-0003)

```bash
sudo ufw default deny incoming
sudo ufw default allow outgoing
sudo ufw allow from <LAN_CIDR> to any port 22 proto tcp comment 'ssh from LAN'
sudo ufw enable
```

ufw protects host services only. Container ports (8123, 9000, 8080) are protected by binding to `$PI5_HOST` in compose and by the `DOCKER-USER` rule from `after.rules.snippet`.

## `after.rules` snippet

`after.rules.snippet` is appended as a separate `*filter ... COMMIT` block after the existing one in
`/etc/ufw/after.rules`. This is intentional (`iptables-restore` accepts several blocks for the same table);
do not merge the `DOCKER-USER` lines into ufw's own block.

## Check after any Docker or ufw restart

```bash
sudo iptables -S DOCKER-USER   # must contain the "! -s <LAN_CIDR> -i wlan0 -j DROP" rule
```

If the DROP rule is missing, restore it with `sudo ufw reload`.
