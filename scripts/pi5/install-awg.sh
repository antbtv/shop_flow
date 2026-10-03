#!/usr/bin/env bash
# Install the AmneziaWG tunnel to Telegram on Pi5 (ADR-0012). Runs ON Pi5 as root, after the
# laptop shipped the files into ~/awg-stage (see docs in infra/pi5/README.md):
#   ssh -t pi5 'sudo bash ~/awg-stage/install-awg.sh'
#   ssh -t pi5 'sudo bash ~/awg-stage/install-awg.sh uninstall'
# It checks sha256 of the binaries, refuses a config that is not narrow (DNS=, default route,
# IPv6, ranges other than Telegram's), adds the DOCKER-USER rule for awg0, starts the unit and
# prints the checks of ADR-0012. Key material is never printed. Paths can be overridden (tests).
set -euo pipefail

STAGE=${STAGE:-$(dirname "$(readlink -f "$0")")}
BIN_DIR=${BIN_DIR:-/usr/local/bin}
UNIT_DIR=${UNIT_DIR:-/etc/systemd/system}
CONF_DIR=${CONF_DIR:-/etc/amnezia/amneziawg}
UFW_AFTER=${UFW_AFTER:-/etc/ufw/after.rules}
OWNER=${OWNER-root:root}            # empty: do not chown (tests run without root)
SKIP_SYSTEM=${SKIP_SYSTEM:-0}       # 1: no systemctl, ufw, iptables, probes (tests)
IFACE=awg0
PROBE_URL=https://api.telegram.org/
# The IPv4 ranges of Telegram (https://core.telegram.org/resources/cidr.txt).
ALLOWED='91.108.4.0/22 91.108.8.0/22 91.108.12.0/22 91.108.16.0/22 91.108.20.0/22 91.108.56.0/22 91.105.192.0/23 149.154.160.0/20 185.76.151.0/24'

die() { echo "error: $*" >&2; exit 1; }
[[ $SKIP_SYSTEM == 1 || $EUID -eq 0 ]] || die "run as root (sudo)"

if [[ ${1:-install} == uninstall ]]; then
    if [[ $SKIP_SYSTEM != 1 ]]; then
        systemctl disable --now "awg-quick@$IFACE" 2>/dev/null || true
    fi
    rm -f "$UNIT_DIR/awg-quick@.service" "$BIN_DIR/amneziawg-go" "$BIN_DIR/awg" "$BIN_DIR/awg-quick"
    [[ $SKIP_SYSTEM == 1 ]] || systemctl daemon-reload
    echo "removed the unit and the binaries; $CONF_DIR/$IFACE.conf and the ufw rule are left as they are"
    exit 0
fi

echo "== 1. binaries: sha256"
(cd "$STAGE" && sha256sum -c SHA256SUMS) || die "sha256 mismatch: rebuild and ship again"

echo "== 2. config check (key material is not printed)"
CONF_SRC=$STAGE/$IFACE.conf
if [[ -f $CONF_SRC ]]; then
    python3 - "$CONF_SRC" $ALLOWED <<'PY' || die "the config is not acceptable, see above"
import ipaddress, re, sys
path, allowed = sys.argv[1], {ipaddress.ip_network(n) for n in sys.argv[2:]}
bad = []
text = open(path).read()
if "<" in text:
    bad.append("an unfilled <placeholder>")
if re.search(r"^\s*DNS\s*=", text, flags=re.M | re.I):
    bad.append("a DNS= line (it would take over the DNS of the host and of Docker)")
if re.search(r"^\s*Table\s*=", text, flags=re.M | re.I):
    bad.append("a Table= line")
lines = re.findall(r"^\s*AllowedIPs\s*=\s*(.+)$", text, flags=re.M | re.I)
if len(lines) != 1:
    bad.append("AllowedIPs must be given exactly once")
for net in (n.strip() for n in ",".join(lines).split(",") if n.strip()):
    try:
        if ipaddress.ip_network(net, strict=False) not in allowed:
            bad.append(f"AllowedIPs entry {net} is not a Telegram range")
    except ValueError:
        bad.append(f"AllowedIPs entry {net!r} is not a network")
addr = re.search(r"^\s*Address\s*=\s*(.+)$", text, flags=re.M | re.I)
if not addr or ":" in addr.group(1):
    bad.append("Address must be given and IPv4 only")
for key in ("PrivateKey", "PublicKey", "Endpoint"):
    if not re.search(rf"^\s*{key}\s*=\s*\S+", text, flags=re.M | re.I):
        bad.append(f"{key} is missing")
for item in bad:
    print("  refused:", item, file=sys.stderr)
sys.exit(1 if bad else 0)
PY
    echo "  ok: $IFACE.conf is narrow (Telegram ranges only, no DNS, IPv4)"
elif [[ -f $CONF_DIR/$IFACE.conf ]]; then
    echo "  no staged $IFACE.conf: keeping the installed $CONF_DIR/$IFACE.conf"
else
    die "no $CONF_SRC and no installed config"
fi

echo "== 3. install files"
install -d -m 0755 "$BIN_DIR" "$UNIT_DIR"
for f in amneziawg-go awg awg-quick; do install -m 0755 "$STAGE/$f" "$BIN_DIR/$f"; done
install -m 0644 "$STAGE/awg-quick@.service" "$UNIT_DIR/awg-quick@.service"
install -d -m 0700 "$CONF_DIR"
if [[ -f $CONF_SRC ]]; then
    install -m 0600 "$CONF_SRC" "$CONF_DIR/$IFACE.conf"
    rm -f "$CONF_SRC"                # the staged copy held the keys
fi
if [[ -n $OWNER ]]; then
    chown "$OWNER" "$CONF_DIR" "$CONF_DIR/$IFACE.conf"
fi
stat -c '  %U:%G %a %n' "$CONF_DIR/$IFACE.conf" "$BIN_DIR/awg-quick"

echo "== 4. DOCKER-USER: nothing opens a NEW connection to a container through $IFACE"
RULE="-A DOCKER-USER -i $IFACE -m conntrack --ctstate NEW -j DROP"
if grep -qxF -- "$RULE" "$UFW_AFTER"; then
    echo "  rule already in $UFW_AFTER"
else
    ANCHOR='-A DOCKER-USER -m conntrack --ctstate RELATED,ESTABLISHED -j RETURN'
    grep -qxF -- "$ANCHOR" "$UFW_AFTER" || die "$UFW_AFTER has no DOCKER-USER block (ADR-0003)"
    cp -p "$UFW_AFTER" "$UFW_AFTER.bak-awg-$(date +%Y%m%d%H%M%S)"
    python3 - "$UFW_AFTER" "$ANCHOR" "$RULE" <<'PY'
import sys
path, anchor, rule = sys.argv[1:4]
lines = open(path).read().split("\n")
out = []
for line in lines:
    out.append(line)
    if line == anchor:
        out.append(rule)
open(path, "w").write("\n".join(out))
PY
    echo "  rule added to $UFW_AFTER (backup next to it)"
fi

if [[ $SKIP_SYSTEM == 1 ]]; then
    echo "SKIP_SYSTEM=1: not starting anything"
    exit 0
fi
ufw reload >/dev/null
iptables -S DOCKER-USER | grep -- "-i $IFACE" | grep -q DROP || die "no DROP rule for $IFACE in DOCKER-USER after ufw reload"
echo "  iptables DOCKER-USER has the $IFACE DROP rule"

echo "== 5. start the tunnel"
systemctl daemon-reload
systemctl enable --now "awg-quick@$IFACE"
for _ in $(seq 20); do
    stamp=$(/usr/local/bin/awg show "$IFACE" latest-handshakes 2>/dev/null | awk '{print $2; exit}')
    [[ -n ${stamp:-} && $stamp -gt 0 ]] && break
    sleep 2
done
echo "  unit: $(systemctl is-active "awg-quick@$IFACE")"
echo "  last handshake: ${stamp:-none} (epoch seconds, 0 or none = no handshake)"

echo "== 6. routes (expected: Telegram via $IFACE, the rest and the default route as before)"
ip route get 149.154.166.110 | head -1
ip route get 1.1.1.1 | head -1
ip route show default
echo "  resolv.conf nameservers: $(awk '/^nameserver/{printf "%s ", $2}' /etc/resolv.conf)"

echo "== 7. Telegram from the host and from the scheduler container (any HTTP code is good)"
curl -sS -m 10 -o /dev/null -w '  host: HTTP %{http_code}\n' "$PROBE_URL" 2>&1 | tail -1
home=$(getent passwd "${SUDO_USER:-root}" | cut -d: -f6)
if [[ -f $home/shopflow/docker-compose.pi5.yml ]]; then
    (cd "$home/shopflow" && docker compose -f docker-compose.pi5.yml exec -T airflow-scheduler python -c "
import urllib.request, urllib.error
try:
    code = urllib.request.urlopen('$PROBE_URL', timeout=10).status
except urllib.error.HTTPError as e:
    code = e.code
except Exception as e:
    code = type(e).__name__
print('  scheduler container: HTTP', code)" </dev/null) || echo "  scheduler container: not reachable for the probe"
fi
