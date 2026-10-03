#!/usr/bin/env bash
# Local test of the AmneziaWG setup for Pi5 (ADR-0012): two containers with the amd64 build,
# the client config is rendered FROM infra/pi5/etc/amnezia/awg0.conf.example, so the template
# itself is tested. The "Telegram" server owns 149.154.166.110 and answers HTTP only through the
# tunnel. Needs: ARCH=amd64 scripts/pi5/build-awg.sh, docker. Nothing touches the host network.
#   scripts/pi5/test-awg-local.sh
set -uo pipefail
cd "$(dirname "$0")/../.."
BIN=$PWD/build/awg/amd64
TEMPLATE=infra/pi5/etc/amnezia/awg0.conf.example
[[ -x $BIN/amneziawg-go ]] || { echo "build first: ARCH=amd64 scripts/pi5/build-awg.sh" >&2; exit 2; }
NET=awgtest-net SRV=awgtest-srv CLI=awgtest-cli
fails=0
check() {  # description, command...
    local what=$1; shift
    if "$@" >/dev/null 2>&1; then echo "ok    $what"; else echo "FAIL  $what"; fails=$((fails + 1)); fi
}
# Throwaway keys of the test: a private directory, not fixed names in /tmp with the default umask.
KEYS=$(mktemp -d)
chmod 700 "$KEYS"
reset_docker() { docker rm -f "$SRV" "$CLI" >/dev/null 2>&1; docker network rm "$NET" >/dev/null 2>&1; }
cleanup() { reset_docker; rm -f "$KEYS"/*.conf; rmdir "$KEYS" 2>/dev/null; }
[[ ${KEEP:-0} == 1 ]] || trap cleanup EXIT
reset_docker

docker build -q -t awgtest-base - >/dev/null <<'DOCKERFILE'
FROM debian:trixie-slim
RUN apt-get update -qq && apt-get install -y -qq --no-install-recommends \
    bash iproute2 iptables procps curl python3 >/dev/null
DOCKERFILE
docker network create "$NET" >/dev/null
run() {  # name
    docker run -d --name "$1" --network "$NET" --cap-add NET_ADMIN --device /dev/net/tun \
        --sysctl net.ipv4.ip_forward=1 -v "$BIN:/opt/awg:ro" \
        -e WG_QUICK_USERSPACE_IMPLEMENTATION=/opt/awg/amneziawg-go \
        awgtest-base sleep 600 >/dev/null
}
run "$SRV"; run "$CLI"
x() { docker exec "$1" bash -c "$2"; }

# keys and obfuscation parameters (any values: both sides must agree)
srv_priv=$(x $SRV "/opt/awg/awg genkey"); cli_priv=$(x $CLI "/opt/awg/awg genkey")
srv_pub=$(echo "$srv_priv" | docker exec -i $SRV /opt/awg/awg pubkey)
cli_pub=$(echo "$cli_priv" | docker exec -i $CLI /opt/awg/awg pubkey)
psk=$(x $SRV "/opt/awg/awg genpsk")
srv_ip=$(docker inspect "$SRV" --format "{{(index .NetworkSettings.Networks \"$NET\").IPAddress}}")
params="Jc = 5\nJmin = 10\nJmax = 50\nS1 = 66\nS2 = 77\nH1 = 1234567891\nH2 = 1234567892\nH3 = 1234567893\nH4 = 1234567894"

printf "[Interface]\nAddress = 10.9.0.1/24\nPrivateKey = %s\nListenPort = 51820\n$params\n\n[Peer]\nPublicKey = %s\nPresharedKey = %s\nAllowedIPs = 10.9.0.2/32\n" \
    "$srv_priv" "$cli_pub" "$psk" > "$KEYS/srv.conf"
# client: the template with its <placeholders> filled in
python3 - "$TEMPLATE" "$cli_priv" "$srv_pub" "$psk" "$srv_ip" >"$KEYS/cli.conf" <<'PY'
import re, sys
text, priv, pub, psk, ip = open(sys.argv[1]).read(), *sys.argv[2:6]
text = text.replace("<PrivateKey of this device>", priv).replace("<PublicKey of the server>", pub)
text = text.replace("<PresharedKey, if the exported config has one>", psk)
text = text.replace("<server address>:<port>", f"{ip}:51820").replace("Address = 10.8.1.2/32", "Address = 10.9.0.2/32")
values = {"Jc": 5, "Jmin": 10, "Jmax": 50, "S1": 66, "S2": 77,
          "H1": 1234567891, "H2": 1234567892, "H3": 1234567893, "H4": 1234567894}
for key, value in values.items():
    text = re.sub(rf"^{key} = <copy>$", f"{key} = {value}", text, flags=re.M)
assert "<" not in text.replace("<PRIVATE", ""), "unfilled placeholder in the template"
sys.stdout.write(text)
PY
for c in $SRV $CLI; do x $c "mkdir -p /etc/amnezia/amneziawg && chmod 700 /etc/amnezia/amneziawg"; done
docker cp "$KEYS/srv.conf" $SRV:/etc/amnezia/amneziawg/awg0.conf
docker cp "$KEYS/cli.conf" $CLI:/etc/amnezia/amneziawg/awg0.conf
x $SRV "chmod 600 /etc/amnezia/amneziawg/awg0.conf"; x $CLI "chmod 600 /etc/amnezia/amneziawg/awg0.conf"
rm -f "$KEYS/srv.conf" "$KEYS/cli.conf"

# the "Telegram" side
x $SRV "ip addr add 149.154.166.110/32 dev lo"
# socketserver, not http.server: its server_bind() does a reverse DNS lookup that hangs here
docker exec -d $SRV python3 -c "import http.server, socketserver
socketserver.TCPServer.allow_reuse_address = True
socketserver.ThreadingTCPServer(('149.154.166.110', 80), http.server.SimpleHTTPRequestHandler).serve_forever()"
for _ in $(seq 20); do x $SRV "ss -tln | grep -q ':80 '" && break; sleep 0.3; done
check "server tunnel comes up" x $SRV "/opt/awg/awg-quick up awg0"

before_default=$(x $CLI "ip route show default")
before_resolv=$(x $CLI "md5sum < /etc/resolv.conf")
before_route_tg=$(x $CLI "ip route get 149.154.166.110 | head -1")
check "before: Telegram is not routed through awg0" bash -c "! echo '$before_route_tg' | grep -q awg0"
check "client tunnel comes up from the template" x $CLI "/opt/awg/awg-quick up awg0"
check "Telegram subnet goes through awg0" x $CLI "ip route get 149.154.166.110 | grep -q 'dev awg0'"
check "other traffic does not (1.1.1.1 stays on eth0)" x $CLI "ip route get 1.1.1.1 | grep -q 'dev eth0'"
check "default route unchanged" bash -c "[[ '$before_default' == \"\$(docker exec $CLI ip route show default)\" ]]"
check "resolv.conf unchanged (no DNS= taken over)" bash -c "[[ \"\$(docker exec $CLI bash -c 'md5sum < /etc/resolv.conf')\" == '$before_resolv' ]]"
check "tunnel MTU is 1280" x $CLI "ip link show awg0 | grep -q 'mtu 1280'"
check "TCPMSS clamp rule is installed" x $CLI "iptables -t mangle -S FORWARD | grep -q TCPMSS"
check "HTTP to 'Telegram' works through the tunnel" x $CLI "test \"\$(curl -sS -m 10 -o /dev/null -w '%{http_code}' http://149.154.166.110/)\" = 200"
# awk must have seen a line: on empty input (no interface) a bare `{exit ...}` action never runs and exits 0
check "handshake happened" x $CLI "/opt/awg/awg show awg0 latest-handshakes | awk '{ok = (\$2 > 0)} END{exit !ok}'"
check "config has AllowedIPs, none of them IPv6 or the default route" x $CLI "grep -q '^AllowedIPs' /etc/amnezia/amneziawg/awg0.conf && ! grep '^AllowedIPs' /etc/amnezia/amneziawg/awg0.conf | grep -qE '0\.0\.0\.0/0|::'"
check "config exists and has no DNS line" x $CLI "test -f /etc/amnezia/amneziawg/awg0.conf && ! grep -qi '^DNS' /etc/amnezia/amneziawg/awg0.conf"

check "client tunnel goes down" x $CLI "/opt/awg/awg-quick down awg0"
check "after down: the route is gone" bash -c "! docker exec $CLI ip route get 149.154.166.110 | grep -q awg0"
check "after down: the TCPMSS rule is gone" bash -c "! docker exec $CLI iptables -t mangle -S FORWARD | grep -q TCPMSS"
echo
((fails == 0)) && echo "all checks passed" || { echo "$fails check(s) failed"; exit 1; }
