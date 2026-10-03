"""AmneziaWG files for Pi5 (ADR-0012): the template keeps the tunnel narrow and holds no secrets.

The tunnel itself is tested by scripts/pi5/test-awg-local.sh (two containers).
"""

import ipaddress
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / "infra/pi5/etc/amnezia/awg0.conf.example"
UNIT = ROOT / "infra/pi5/etc/systemd/system/awg-quick@.service"
AFTER_RULES = ROOT / "infra/pi5/etc/ufw/after.rules.snippet"
# Telegram's published ranges (https://core.telegram.org/resources/cidr.txt, IPv4 part).
TELEGRAM = {"91.108.4.0/22", "91.108.8.0/22", "91.108.12.0/22", "91.108.16.0/22",
            "91.108.20.0/22", "91.108.56.0/22", "91.105.192.0/23", "149.154.160.0/20",
            "185.76.151.0/24"}


def settings(section: str) -> dict[str, str]:
    text = TEMPLATE.read_text()
    block = re.split(r"^\[", text, flags=re.M)
    (body,) = [b for b in block if b.startswith(section + "]")]
    pairs = (line.partition("=") for line in body.splitlines() if "=" in line
             and not line.lstrip().startswith("#"))
    return {k.strip(): v.strip() for k, _, v in pairs}


def test_allowed_ips_are_only_telegram_ranges_and_never_the_default_route():
    nets = {n.strip() for n in settings("Peer")["AllowedIPs"].split(",")}
    assert nets == TELEGRAM
    assert all(ipaddress.ip_network(n).version == 4 and ipaddress.ip_network(n).prefixlen >= 20
               for n in nets)


def test_no_dns_no_ipv6_and_the_tunnel_parameters_that_matter():
    interface, peer = settings("Interface"), settings("Peer")
    assert not any(k.upper() == "DNS" for k in interface)
    assert ":" not in interface["Address"]
    assert interface["MTU"] == "1280"
    assert peer["PersistentKeepalive"] == "25"
    assert "Table" not in interface  # default "auto" adds only the AllowedIPs routes


def test_mss_clamp_is_added_and_removed_with_the_tunnel():
    interface = settings("Interface")
    assert "TCPMSS --clamp-mss-to-pmtu" in interface["PostUp"]
    assert interface["PostDown"] == interface["PostUp"].replace(" -A ", " -D ")


def test_template_holds_placeholders_only_no_key_material():
    text = TEMPLATE.read_text()
    assert not re.search(r"=\s*[A-Za-z0-9+/]{43}=\s*$", text, flags=re.M)  # base64 key shape
    for line in text.splitlines():
        if line.startswith(("PrivateKey", "PublicKey", "PresharedKey", "Endpoint")):
            assert "<" in line, line


def test_real_configs_are_ignored_by_git_and_the_template_is_not():
    def ignored(path):
        return subprocess.run(["git", "check-ignore", "-q", path], cwd=ROOT).returncode == 0
    assert ignored("infra/pi5/etc/amnezia/awg0.conf")
    assert ignored("infra/pi5/etc/amnezia/anything.conf")
    assert not ignored("infra/pi5/etc/amnezia/awg0.conf.example")
    assert ignored("build/awg/arm64/awg")


def test_unit_waits_for_the_network_and_retries():
    unit = UNIT.read_text()
    assert "After=network-online.target" in unit and "Wants=network-online.target" in unit
    assert "Restart=on-failure" in unit and "RestartSec=30" in unit
    assert "WG_QUICK_USERSPACE_IMPLEMENTATION=/usr/local/bin/amneziawg-go" in unit


def test_nothing_may_open_a_new_connection_to_containers_through_the_tunnel():
    rules = [line for line in AFTER_RULES.read_text().splitlines()
             if line.startswith("-A DOCKER-USER")]
    drop = "-A DOCKER-USER -i awg0 -m conntrack --ctstate NEW -j DROP"
    established = "-A DOCKER-USER -m conntrack --ctstate RELATED,ESTABLISHED -j RETURN"
    assert drop in rules and established in rules
    assert rules.index(established) < rules.index(drop) < rules.index("-A DOCKER-USER -j RETURN")
