"""Static checks of dashboards/*.json (ADR-0011): no Docker, no Grafana.

The live behaviour (macros, values against fact_orders, grafana_reader limits) is checked with
scripts/check_dashboard.py against a real Grafana. Here: the JSON is what scripts/build_dashboard.py
produces, and every query stays inside what grafana_reader may read (the trust boundary).
"""

import importlib.util
import json
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DASHBOARDS = sorted((ROOT / "dashboards").glob("*.json"))
DS_UID = "shopflow-clickhouse"


def granted_tables() -> set[str]:
    script = (ROOT / "scripts/create-ch-users.sh").read_text()
    return set(re.findall(r'"GRANT SELECT ON shopflow\.(\w+) TO grafana_reader"', script))


def panels(dash):
    stack = list(dash["panels"])
    while stack:
        item = stack.pop(0)
        yield item
        stack = item.get("panels", []) + stack


def builder():
    path = ROOT / "scripts/build_dashboard.py"
    spec = importlib.util.spec_from_file_location("build_dashboard", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_there_is_a_dashboard_and_the_grants_were_found():
    assert DASHBOARDS, "dashboards/*.json is empty"
    assert {"mart_revenue_daily", "dq_check_results"} <= granted_tables()


def test_json_is_what_the_builder_produces():
    # Edit scripts/build_dashboard.py and run it; do not edit the JSON by hand.
    assert (ROOT / "dashboards/shopflow.json").read_text() == builder().render()


@pytest.mark.parametrize("path", DASHBOARDS, ids=lambda p: p.name)
def test_queries_read_only_what_grafana_reader_may_read(path):
    allowed = granted_tables()
    dash = json.loads(path.read_text())
    for panel in panels(dash):
        for target in panel.get("targets", []):
            sql = target["rawSql"]
            used = set(re.findall(r"\bshopflow\.(\w+)", sql))
            assert used, f"panel {panel['id']} reads no shopflow table"
            assert used <= allowed, f"panel {panel['id']} reads {sorted(used - allowed)}"
            assert "system." not in sql.lower(), f"panel {panel['id']} reads system tables"


@pytest.mark.parametrize("path", DASHBOARDS, ids=lambda p: p.name)
def test_every_panel_uses_the_provisioned_datasource(path):
    dash = json.loads(path.read_text())
    for panel in panels(dash):
        if panel["type"] == "row":
            continue
        assert panel["datasource"]["uid"] == DS_UID, panel["id"]
        assert panel["targets"], panel["id"]
        for target in panel["targets"]:
            assert target["datasource"]["uid"] == DS_UID, panel["id"]
            assert target["rawSql"].strip(), panel["id"]


@pytest.mark.parametrize("path", DASHBOARDS, ids=lambda p: p.name)
def test_dashboard_settings(path):
    dash = json.loads(path.read_text())
    ids = [p["id"] for p in panels(dash)]
    assert len(ids) == len(set(ids)), "duplicate panel ids"
    assert dash["uid"] and dash["title"]
    assert dash["timezone"] == "utc"  # marts are UTC days
    assert dash["editable"] is False  # provisioned from git, ADR-0011
    # Marts refresh every 2 minutes: a faster dashboard only loads the Pi5
    # (GF_DASHBOARDS_MIN_REFRESH_INTERVAL).
    seconds = {"s": 1, "m": 60, "h": 3600}[dash["refresh"][-1]] * int(dash["refresh"][:-1])
    assert seconds >= 30


@pytest.mark.parametrize("path", DASHBOARDS, ids=lambda p: p.name)
def test_daily_marts_are_filtered_by_the_dashboard_time_range(path):
    dash = json.loads(path.read_text())
    for panel in panels(dash):
        for target in panel.get("targets", []):
            sql = target["rawSql"]
            if "max(refreshed_at)" in sql:
                continue  # a freshness probe looks at the newest refresh, not at a period
            if re.search(r"mart_(revenue|funnel|top_products)_daily", sql):
                # The picker range, or an explicit fixed window (top products: 7 and 30 days).
                assert ("$__dateFilter(order_date)" in sql
                        or "order_date >= toDate(now('UTC'))" in sql), (
                    f"panel {panel['id']} has no time window")


@pytest.mark.parametrize("path", DASHBOARDS, ids=lambda p: p.name)
def test_dq_results_are_always_read_with_final(path):
    # ReplacingMergeTree(checked_at): without FINAL a rewritten row shows twice.
    dash = json.loads(path.read_text())
    for panel in panels(dash):
        for target in panel.get("targets", []):
            sql = target["rawSql"]
            for use in re.finditer(r"shopflow\.dq_check_results\b(?: AS \w+)?( FINAL)?", sql):
                assert use.group(1), f"panel {panel['id']} reads dq_check_results without FINAL"


@pytest.mark.parametrize("path", DASHBOARDS, ids=lambda p: p.name)
def test_panels_fit_the_grid_and_do_not_overlap(path):
    # Nobody looks at the picture in CI: two panels on the same cells are caught here.
    dash = json.loads(path.read_text())
    boxes = []
    for panel in panels(dash):
        g = panel["gridPos"]
        assert g["x"] >= 0 and g["w"] > 0 and g["h"] > 0 and g["x"] + g["w"] <= 24, panel["id"]
        boxes.append((panel["id"], g["x"], g["y"], g["x"] + g["w"], g["y"] + g["h"]))
    for i, a in enumerate(boxes):
        for b in boxes[i + 1:]:
            apart = a[3] <= b[1] or b[3] <= a[1] or a[4] <= b[2] or b[4] <= a[2]
            assert apart, f"panels {a[0]} and {b[0]} overlap"
