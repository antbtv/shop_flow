"""Turn the AmneziaWG config exported from the Amnezia app into the narrow one for Pi5 (ADR-0012).

    python3 scripts/pi5/prepare-awg-config.py EXPORTED.conf OUT.conf

Keeps keys, endpoint and every AmneziaWG parameter as they are. Changes only what ADR-0012 asks:
AllowedIPs = the Telegram ranges, no DNS / Table / PostUp / PostDown of the original, IPv4-only
Address, MTU 1280, PersistentKeepalive 25, the MSS clamp as PostUp/PostDown. The output is written
with mode 600; nothing secret is printed (only the names of the keys that were kept or changed).
"""

import os
import re
import sys

TELEGRAM = ("91.108.4.0/22, 91.108.8.0/22, 91.108.12.0/22, 91.108.16.0/22, 91.108.20.0/22, "
            "91.108.56.0/22, 91.105.192.0/23, 149.154.160.0/20, 185.76.151.0/24")
MSS = ("iptables -t mangle {op} FORWARD -o %i -p tcp --tcp-flags SYN,RST SYN "
       "-j TCPMSS --clamp-mss-to-pmtu")
DROP_INTERFACE = {"dns", "table", "mtu", "postup", "postdown", "preup", "predown", "saveconfig"}
DROP_PEER = {"allowedips", "persistentkeepalive"}
REQUIRED = {"Interface": ("address", "privatekey"), "Peer": ("publickey", "endpoint")}


def parse(text: str) -> list[tuple[str, list[tuple[str, str]]]]:
    sections: list[tuple[str, list[tuple[str, str]]]] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        header = re.fullmatch(r"\[(\w+)\]", line)
        if header:
            sections.append((header.group(1), []))
        elif "=" in line and sections:
            key, _, value = line.partition("=")
            sections[-1][1].append((key.strip(), value.strip()))
        else:
            raise SystemExit("unexpected line in the config (content not shown)")
    return sections


def ipv4_only(address: str) -> str:
    kept = [a.strip() for a in address.split(",") if a.strip() and ":" not in a]
    if not kept:
        raise SystemExit("the config has no IPv4 Address")
    return ", ".join(kept)


def narrow(text: str) -> tuple[str, list[str]]:
    sections = parse(text)
    names = [n for n, _ in sections]
    if names.count("Interface") != 1 or names.count("Peer") != 1:
        raise SystemExit("expected exactly one [Interface] and one [Peer]")
    notes, out = [], []
    for name, items in sections:
        lower = {k.lower() for k, _ in items}
        missing = [k for k in REQUIRED[name] if k not in lower]
        if missing:
            raise SystemExit(f"[{name}] lacks: {', '.join(missing)}")
        out.append(f"[{name}]")
        for key, value in items:
            k = key.lower()
            if name == "Interface" and k in DROP_INTERFACE:
                notes.append(f"removed {key}")
            elif name == "Peer" and k in DROP_PEER:
                notes.append(f"replaced {key}")
            elif k == "address":
                given = [a.strip() for a in value.split(",") if a.strip()]
                kept = ipv4_only(value)
                # compared as lists: "a,b" without a space is not a change
                if kept.split(", ") != given:
                    notes.append("Address: IPv6 removed")
                out.append(f"{key} = {kept}")
            else:
                out.append(f"{key} = {value}")
        if name == "Interface":
            out += ["MTU = 1280", f"PostUp = {MSS.format(op='-A')}",
                    f"PostDown = {MSS.format(op='-D')}"]
            notes.append("set MTU = 1280 and the MSS clamp")
        else:
            out += [f"AllowedIPs = {TELEGRAM}", "PersistentKeepalive = 25"]
            notes.append("set AllowedIPs = Telegram ranges, PersistentKeepalive = 25")
        out.append("")
    return "\n".join(out), notes


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit(__doc__)
    with open(sys.argv[1], encoding="utf-8") as handle:
        text, notes = narrow(handle.read())
    fd = os.open(sys.argv[2], os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    # An existing file keeps its old (maybe wider) mode: narrow it before any key is written.
    os.fchmod(fd, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(text)
    print(f"wrote {sys.argv[2]} (mode 600)")
    for note in dict.fromkeys(notes):
        print(" -", note)


if __name__ == "__main__":
    main()
