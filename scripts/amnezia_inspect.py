"""Show the structure of an Amnezia key (vpn://...) with every secret masked (ADR-0012).

    python3 scripts/amnezia_inspect.py PATH_TO_FILE_WITH_THE_KEY

The key is read from a file, never from argv, and never printed: only field names, the protocol,
the obfuscation parameters (Jc, S1, H1...), ports and masked key material are shown. Use it to see
whether a subscription key contains a ready WireGuard/AmneziaWG config before touching Pi5.
Format (Amnezia client): "vpn://" + base64url(4-byte big-endian length + zlib(JSON))
"""

import base64
import json
import re
import sys
import zlib

SECRET_KEYS = {"privatekey", "publickey", "presharedkey"}
# Obfuscation parameters of AmneziaWG are not secrets and tell which software is needed.
AWG_PARAMS = ("Jc", "Jmin", "Jmax", "S1", "S2", "S3", "S4", "H1", "H2", "H3", "H4",
              "I1", "I2", "I3", "I4", "I5")
IP = re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}\b")
NAME = re.compile(r"[A-Za-z][A-Za-z0-9]{0,24}")


def decode(text: str) -> object:
    raw = text.strip().removeprefix("vpn://")
    raw += "=" * (-len(raw) % 4)
    blob = base64.urlsafe_b64decode(raw)
    for payload in (blob[4:], blob):
        try:
            return json.loads(zlib.decompress(payload))
        except (zlib.error, ValueError):
            continue
    try:
        return json.loads(blob)
    except ValueError as exc:
        raise SystemExit("not an Amnezia key: cannot decode (is it a vpn:// string?)") from exc


def mask_endpoint(value: str) -> str:
    host, _, port = value.rpartition(":")
    return f"<host masked>:{port}" if host else "<masked>"


def describe_config(text: str) -> list[str]:
    """Lines of a WireGuard-style [Interface]/[Peer] config, secrets masked."""
    out = []
    for line in text.splitlines():
        key, sep, value = line.partition("=")
        name = key.strip()
        if not sep:
            # Only section headers are shown; anything else might be a piece of a secret.
            out.append(name if name in ("[Interface]", "[Peer]") else "<line hidden>")
        elif not NAME.fullmatch(name):
            out.append("<line hidden>")
        elif name.lower() in SECRET_KEYS:
            out.append(f"{name} = <masked, {len(value.strip())} chars>")
        elif name.lower() == "endpoint":
            out.append(f"{name} = {mask_endpoint(value.strip())}")
        elif name in AWG_PARAMS or name.lower() in ("allowedips", "mtu", "dns", "address",
                                                    "persistentkeepalive", "listenport"):
            out.append(f"{name} = {value.strip()}")
        else:
            out.append(f"{name} = <value hidden>")
    return out


def walk(node, path: str, report: list[str], seen: set) -> None:
    if isinstance(node, dict):
        for key, value in node.items():
            walk(value, f"{path}.{key}" if path else key, report, seen)
    elif isinstance(node, list):
        for i, value in enumerate(node):
            walk(value, f"{path}[{i}]", report, seen)
    elif isinstance(node, str):
        stripped = node.strip()
        parsed = None
        if stripped.startswith("{") and stripped.endswith("}"):
            try:
                parsed = json.loads(stripped)
            except ValueError:
                parsed = None
        if parsed is not None:
            walk(parsed, path + "{json}", report, seen)
        elif stripped.startswith("[Interface]"):
            report.append(f"{path}: WireGuard-style config")
            report.extend("    " + line for line in describe_config(node))
        elif path.rsplit(".", 1)[-1] in ("container", "protocol", "defaultContainer", "type",
                                         "description", "name") and len(node) < 60:
            report.append(f"{path} = {node}")
        elif path not in seen:
            seen.add(path)
            what = "contains an IP address" if IP.search(node) else "string"
            report.append(f"{path}: {what}, {len(node)} chars (value hidden)")
    else:
        report.append(f"{path} = {node!r}")


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)
    with open(sys.argv[1], encoding="utf-8") as handle:
        data = decode(handle.read())
    report: list[str] = []
    walk(data, "", report, set())
    print("\n".join(report))


if __name__ == "__main__":
    main()
