#!/usr/bin/env python3
"""Runs every panel query of dashboards/*.json through the Grafana API (the same path a browser
takes: plugin macros expanded, queries run as grafana_reader) and prints rows or the error.

Usage: GRAFANA_URL=http://host:3000 GRAFANA_PASSWORD=... scripts/check_dashboard.py [from] [to]
       (user admin or GRAFANA_USER; from/to are Grafana time strings, default now-30d / now)
Exit code 1 if any panel fails. The password is read from the environment, never printed.
On Pi5 the password is GRAFANA_ADMIN_PASSWORD in ~/shopflow/.env (read it with sed, never print it).
No dependency beyond the standard library.
"""

import base64
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def iter_panels(items):
    for item in items:
        yield item
        yield from iter_panels(item.get("panels", []))


def run(url: str, auth: str, dashboard: str, panel: dict, start: str, end: str) -> tuple[bool, str]:
    body = {"queries": [{**t, "datasource": panel.get("datasource", t.get("datasource"))}
                        for t in panel["targets"]], "from": start, "to": end}
    req = urllib.request.Request(
        f"{url}/api/ds/query", data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "Authorization": f"Basic {auth}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            results = json.load(resp)["results"]
    except urllib.error.HTTPError as exc:
        return False, f"HTTP {exc.code} {exc.read().decode()[:200]}"
    problems, rows = [], 0
    for ref, res in results.items():
        if res.get("error"):
            problems.append(f"{ref}: {res['error'][:200]}")
        for frame in res.get("frames", []):
            values = frame["data"]["values"]
            rows += len(values[0]) if values else 0
    return (not problems), (" | ".join(problems) if problems else f"{rows} rows")


def main() -> int:
    url = os.environ.get("GRAFANA_URL", "http://127.0.0.1:3000").rstrip("/")
    password = os.environ.get("GRAFANA_PASSWORD")
    if not password:
        print("set GRAFANA_PASSWORD", file=sys.stderr)
        return 2
    user = os.environ.get("GRAFANA_USER", "admin")
    auth = base64.b64encode(f"{user}:{password}".encode()).decode()
    start = sys.argv[1] if len(sys.argv) > 1 else "now-30d"
    end = sys.argv[2] if len(sys.argv) > 2 else "now"
    failed = 0
    for path in sorted((ROOT / "dashboards").glob("*.json")):
        dash = json.loads(path.read_text())
        for panel in iter_panels(dash["panels"]):
            if "targets" not in panel:
                continue
            ok, text = run(url, auth, dash["uid"], panel, start, end)
            failed += not ok
            mark = "ok  " if ok else "FAIL"
            print(f"{mark} {dash['uid']} #{panel['id']:<3} {panel['title'][:46]:<46} {text}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
