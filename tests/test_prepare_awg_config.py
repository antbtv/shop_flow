"""scripts/pi5/prepare-awg-config.py (ADR-0012): the exported config becomes the narrow one."""

import stat
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/pi5/prepare-awg-config.py"
PRIVATE = "PRIVATEKEYPRIVATEKEYPRIVATEKEYPRIVATEKEY0="
EXPORTED = f"""[Interface]
Address = 10.8.1.7/32, fd58:baa6:dead::7/128
DNS = 1.1.1.1, 1.0.0.1
PrivateKey = {PRIVATE}
Jc = 4
Jmin = 10
Jmax = 50
S1 = 100
S2 = 120
S3 = 15
S4 = 20
H1 = 1000-2000
I1 = <b 0xdeadbeef>
MTU = 1376
PostUp = echo original

[Peer]
PublicKey = PUBLICKEYPUBLICKEYPUBLICKEYPUBLICKEYPUBLIC0=
PresharedKey = PSKPSKPSKPSKPSKPSKPSKPSKPSKPSKPSKPSKPSK000=
AllowedIPs = 0.0.0.0/0, ::/0
Endpoint = 203.0.113.9:51820
PersistentKeepalive = 60
"""


def run(tmp_path, text):
    src, out = tmp_path / "in.conf", tmp_path / "out.conf"
    src.write_text(text)
    done = subprocess.run([sys.executable, str(SCRIPT), str(src), str(out)],
                          capture_output=True, text=True)
    return done, out


def test_narrows_the_config_and_keeps_keys_endpoint_and_parameters(tmp_path):
    done, out = run(tmp_path, EXPORTED)
    assert done.returncode == 0, done.stderr
    text = out.read_text()
    for kept in (f"PrivateKey = {PRIVATE}", "Jc = 4", "S3 = 15", "H1 = 1000-2000",
                 "I1 = <b 0xdeadbeef>", "Endpoint = 203.0.113.9:51820",
                 "PresharedKey = PSKPSK", "Address = 10.8.1.7/32"):
        assert kept in text, kept
    assert "DNS" not in text and "fd58" not in text and "echo original" not in text
    assert "0.0.0.0/0" not in text and "::/0" not in text
    assert text.count("MTU = 1280") == 1 and "1376" not in text
    assert text.count("AllowedIPs") == 1 and "149.154.160.0/20" in text
    assert "PersistentKeepalive = 25" in text and "= 60" not in text
    assert "-A FORWARD -o %i" in text and "-D FORWARD -o %i" in text
    assert stat.S_IMODE(out.stat().st_mode) == 0o600


def test_nothing_secret_is_printed(tmp_path):
    done, _ = run(tmp_path, EXPORTED)
    for secret in (PRIVATE, "PUBLICKEY", "PSKPSK", "203.0.113.9"):
        assert secret not in done.stdout + done.stderr


def test_the_result_passes_the_installer_check(tmp_path):
    """install-awg.sh refuses anything not narrow: the prepared config must pass it."""
    done, out = run(tmp_path, EXPORTED)
    assert done.returncode == 0
    stage = tmp_path / "stage"
    stage.mkdir()
    for name in ("amneziawg-go", "awg", "awg-quick"):
        (stage / name).write_text(name)
    import hashlib
    (stage / "SHA256SUMS").write_text("".join(
        f"{hashlib.sha256((stage / n).read_bytes()).hexdigest()}  {n}\n"
        for n in ("amneziawg-go", "awg", "awg-quick")))
    (stage / "awg-quick@.service").write_text("[Unit]\n")
    (stage / "awg0.conf").write_text(out.read_text())
    ufw = tmp_path / "after.rules"
    ufw.write_text("*filter\n-A DOCKER-USER -m conntrack --ctstate RELATED,ESTABLISHED -j RETURN\n"
                   "COMMIT\n")
    result = subprocess.run(
        ["bash", str(ROOT / "scripts/pi5/install-awg.sh")], capture_output=True, text=True,
        env={"PATH": "/usr/bin:/bin", "STAGE": str(stage), "BIN_DIR": str(tmp_path / "b"),
             "UNIT_DIR": str(tmp_path / "u"), "CONF_DIR": str(tmp_path / "c"),
             "UFW_AFTER": str(ufw), "OWNER": "", "SKIP_SYSTEM": "1"})
    assert result.returncode == 0, result.stderr


def test_a_config_without_a_peer_endpoint_is_refused_without_echoing_it(tmp_path):
    done, out = run(tmp_path, EXPORTED.replace("Endpoint = 203.0.113.9:51820\n", ""))
    assert done.returncode != 0 and "endpoint" in done.stderr
    assert not out.exists()
    assert PRIVATE not in done.stdout + done.stderr


def test_garbage_line_is_refused_without_echoing_it(tmp_path):
    done, _ = run(tmp_path, EXPORTED + "THIS-IS-SECRET-LOOKING-GARBAGE\n")
    assert done.returncode != 0
    assert "SECRET-LOOKING" not in done.stdout + done.stderr
