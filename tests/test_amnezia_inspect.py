"""scripts/amnezia_inspect.py: structure is shown, secrets never are (ADR-0012)."""

import base64
import json
import struct
import subprocess
import sys
import zlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SECRETS = ["PRIV" + "a" * 40, "PUB" + "b" * 41, "PSK" + "c" * 41, "203.0.113.77", "tok-secret-123"]
CONFIG = (
    "[Interface]\nAddress = 10.8.1.5/32\nDNS = 1.1.1.1, 1.0.0.1\n"
    f"PrivateKey = {SECRETS[0]}\nJc = 4\nJmin = 10\nJmax = 50\nS1 = 100\nS2 = 120\nH1 = 1234567\n"
    f"\n[Peer]\nPublicKey = {SECRETS[1]}\nPresharedKey = {SECRETS[2]}\n"
    f"AllowedIPs = 0.0.0.0/0, ::/0\nEndpoint = {SECRETS[3]}:51820\nPersistentKeepalive = 25\n"
)


def make_key(payload: dict, qt_prefix: bool = True) -> str:
    packed = zlib.compress(json.dumps(payload).encode())
    if qt_prefix:
        packed = struct.pack(">I", len(json.dumps(payload))) + packed
    return "vpn://" + base64.urlsafe_b64encode(packed).decode().rstrip("=")


def inspect(tmp_path, key: str):
    path = tmp_path / "key.txt"
    path.write_text(key)
    return subprocess.run([sys.executable, str(ROOT / "scripts/amnezia_inspect.py"), str(path)],
                          capture_output=True, text=True)


def payload():
    inner = json.dumps({"config": CONFIG, "client_priv_key": SECRETS[0], "mtu": "1280"})
    return {"hostName": SECRETS[3], "defaultContainer": "amnezia-awg", "description": "Premium",
            "api_key": SECRETS[4],
            "containers": [{"container": "amnezia-awg", "awg": {"last_config": inner}}]}


def test_shows_protocol_obfuscation_and_routes_but_no_secret(tmp_path):
    done = inspect(tmp_path, make_key(payload()))
    assert done.returncode == 0, done.stderr
    for secret in SECRETS:
        assert secret not in done.stdout and secret not in done.stderr
    assert "amnezia-awg" in done.stdout
    assert "Jc = 4" in done.stdout and "H1 = 1234567" in done.stdout
    assert "AllowedIPs = 0.0.0.0/0, ::/0" in done.stdout
    assert "DNS = 1.1.1.1, 1.0.0.1" in done.stdout
    assert "Endpoint = <host masked>:51820" in done.stdout
    assert "PrivateKey = <masked," in done.stdout


def test_key_without_qt_length_prefix_and_without_config_is_still_safe(tmp_path):
    key = make_key({"api_config": {"service_type": "amnezia-premium", "token": SECRETS[4]}},
                   qt_prefix=False)
    done = inspect(tmp_path, key)
    assert done.returncode == 0, done.stderr
    assert SECRETS[4] not in done.stdout
    assert "api_config.token: string" in done.stdout


def test_garbage_is_refused_without_echoing_it(tmp_path):
    done = inspect(tmp_path, "vpn://" + "A" * 40)
    assert done.returncode != 0
    assert "A" * 40 not in done.stdout + done.stderr
