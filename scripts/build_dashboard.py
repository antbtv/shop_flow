#!/usr/bin/env python3
"""Builds dashboards/shopflow.json (FR-4..FR-7 and pipeline health, ADR-0011).

The dashboard is provisioned from git and read-only in the UI, so the JSON is the source of truth;
this script only keeps 20-odd panels consistent (datasource, grid, units) instead of hand-editing
JSON. tests/test_dashboards.py fails when dashboards/shopflow.json drifts from this script's output.
Every query reads only the tables granted to grafana_reader (scripts/create-ch-users.sh).

Usage: python3 scripts/build_dashboard.py            (writes dashboards/shopflow.json)
"""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "dashboards/shopflow.json"

DS = {"type": "grafana-clickhouse-datasource", "uid": "shopflow-clickhouse"}
# Grafana ClickHouse plugin: Format 0 = time series (long format: time, series key, metrics),
# 1 = table.
TIME_SERIES, TABLE = 0, 1

REVENUE = "shopflow.mart_revenue_daily"
FUNNEL = "shopflow.mart_funnel_daily"


def target(sql: str, fmt: int) -> dict:
    return {
        "refId": "A",
        "datasource": DS,
        "editorType": "sql",
        "format": fmt,
        "queryType": "timeseries" if fmt == TIME_SERIES else "table",
        "rawSql": " ".join(sql.split()),
    }


def panel(pid, kind, title, x, y, w, h, sql, fmt=TABLE, description="", **extra) -> dict:
    out = {
        "id": pid,
        "type": kind,
        "title": title,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "datasource": DS,
        "targets": [target(sql, fmt)],
        "fieldConfig": {"defaults": {}, "overrides": []},
        "options": {},
    }
    if description:
        out["description"] = description
    out.update(extra)
    return out


def stat(pid, title, x, y, sql, unit="none", decimals=0, description=""):
    p = panel(pid, "stat", title, x, y, 6, 4, sql, description=description)
    p["fieldConfig"]["defaults"] = {"unit": unit, "decimals": decimals,
                                    "color": {"mode": "fixed", "fixedColor": "blue"}}
    p["options"] = {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                    "colorMode": "value", "graphMode": "none", "textMode": "auto"}
    return p


def timeseries(pid, title, x, y, w, h, sql, unit="none", bars=False, stacked=False,
               description="", fmt=TIME_SERIES):
    p = panel(pid, "timeseries", title, x, y, w, h, sql, fmt, description)
    custom = {"drawStyle": "bars" if bars else "line", "lineWidth": 2,
              "fillOpacity": 60 if bars else 10,
              "showPoints": "auto", "spanNulls": True,
              "stacking": {"mode": "normal" if stacked else "none", "group": "A"}}
    p["fieldConfig"]["defaults"] = {"unit": unit, "custom": custom}
    p["options"] = {"legend": {"displayMode": "table" if bars else "list", "placement": "bottom",
                               "calcs": ["sum"] if bars else []},
                    "tooltip": {"mode": "multi", "sort": "desc"}}
    return p


def rows_5_7() -> list[dict]:
    window = "$__dateFilter(order_date)"
    return [
        {"id": 1, "type": "row", "title": "Выручка (FR-4)", "collapsed": False,
         "gridPos": {"x": 0, "y": 0, "w": 24, "h": 1}, "panels": []},
        stat(2, "Выручка без отменённых", 0, 1,
             f"SELECT sum(revenue_net) AS revenue_net FROM {REVENUE} WHERE {window}",
             description="Сумма revenue_net витрины mart_revenue_daily за выбранный период."),
        stat(3, "Заказов создано", 6, 1,
             f"SELECT sum(created) AS created FROM {FUNNEL} WHERE {window}",
             description="Из воронки: orders в витрине выручки считает заказ "
                         "в каждой его категории."),
        stat(4, "Оплачено, % от созданных", 12, 1,
             f"SELECT round(100 * sum(paid) / nullIf(sum(created), 0), 1) AS paid_pct "
             f"FROM {FUNNEL} WHERE {window}", unit="percent", decimals=1),
        stat(5, "Доставлено, % от созданных", 18, 1,
             f"SELECT round(100 * sum(delivered) / nullIf(sum(created), 0), 1) AS delivered_pct "
             f"FROM {FUNNEL} WHERE {window}", unit="percent", decimals=1),
        timeseries(6, "Выручка по дням и категориям (без отменённых)", 0, 5, 16, 9,
                   f"SELECT toDateTime(order_date) AS time, category, "
                   f"sum(revenue_net) AS revenue_net FROM {REVENUE} WHERE {window} "
                   f"GROUP BY time, category ORDER BY time",
                   bars=True, stacked=True,
                   description="День — UTC-дата создания заказа, "
                               "категория на момент заказа (SCD2)."),
        panel(7, "table", "Выручка по категориям за период", 16, 5, 8, 9,
              f"SELECT category, sum(revenue_net) AS revenue_net, sum(revenue) AS revenue, "
              f"sum(items) AS items FROM {REVENUE} WHERE {window} GROUP BY category "
              f"ORDER BY revenue_net DESC"),
        {"id": 8, "type": "row", "title": "Воронка (FR-5)", "collapsed": False,
         "gridPos": {"x": 0, "y": 14, "w": 24, "h": 1}, "panels": []},
        timeseries(9, "Заказы по статусам по дням", 0, 15, 12, 9,
                   f"SELECT toDateTime(order_date) AS time, created, paid, shipped, delivered, "
                   f"cancelled FROM {FUNNEL} WHERE {window} ORDER BY time",
                   description="Дошли до статуса (монотонно: доставленный считается и оплаченным). "
                               "Оплаченный заказ может потом попасть в отменённые."),
        timeseries(10, "Время переходов: медиана и p90", 12, 15, 12, 9,
                   f"SELECT toDateTime(order_date) AS time, to_paid_median_s, to_paid_p90_s, "
                   f"paid_to_delivered_median_s, paid_to_delivered_p90_s FROM {FUNNEL} "
                   f"WHERE {window} ORDER BY time", unit="s",
                   description="Только реальные переходы (не снимки); "
                               "день без переходов — пропуск, не 0."),
        bargauge(11, "Воронка за период", 0, 24, 24, 6,
                 f"SELECT sum(created) AS `создан`, sum(paid) AS `оплачен`, "
                 f"sum(shipped) AS `отгружен`, sum(delivered) AS `доставлен`, "
                 f"sum(cancelled) AS `отменён` FROM {FUNNEL} "
                 f"WHERE {window}"),
    ]


def bargauge(pid, title, x, y, w, h, sql):
    p = panel(pid, "bargauge", title, x, y, w, h, sql)
    p["fieldConfig"]["defaults"] = {"unit": "none", "decimals": 0, "min": 0,
                                    "color": {"mode": "palette-classic"}}
    p["options"] = {"orientation": "horizontal", "displayMode": "gradient", "showUnfilled": True,
                    "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}}
    return p


def dashboard() -> dict:
    return {
        "uid": "shopflow-main",
        "title": "ShopFlow",
        "description": "Операционная аналитика ритейла: данные из Postgres через CDC. ADR-0011.",
        "tags": ["shopflow"],
        "timezone": "utc",
        "schemaVersion": 41,
        "version": 1,
        "editable": False,
        "graphTooltip": 1,
        "refresh": "1m",
        "time": {"from": "now-30d", "to": "now"},
        "timepicker": {"refresh_intervals": ["1m", "5m", "15m"]},
        "templating": {"list": []},
        "annotations": {"list": []},
        "links": [],
        "panels": rows_5_7(),
    }


def render() -> str:
    return json.dumps(dashboard(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"


if __name__ == "__main__":
    OUT.write_text(render())
    print(f"wrote {OUT.relative_to(ROOT)}")
