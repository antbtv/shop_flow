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


def thresholds(*steps):
    """steps: (color, from) pairs; the first one has no lower bound."""
    return {"mode": "absolute",
            "steps": [{"color": c, "value": v} for c, v in steps]}


def stat(pid, title, x, y, sql, unit="none", decimals=0, description="", w=6, limits=None):
    p = panel(pid, "stat", title, x, y, w, 4, sql, description=description)
    color = ({"mode": "thresholds"} if limits else {"mode": "fixed", "fixedColor": "blue"})
    p["fieldConfig"]["defaults"] = {"unit": unit, "decimals": decimals, "color": color}
    if limits:
        p["fieldConfig"]["defaults"]["thresholds"] = limits
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


def override(field, *props, regex=False):
    return {"matcher": {"id": "byRegexp" if regex else "byName", "options": field},
            "properties": [{"id": k, "value": v} for k, v in props]}


def colored(field, limits, unit=None, regex=False):
    """Cell background by thresholds (green/orange/red), optional unit."""
    props = [("custom.cellOptions", {"type": "color-background", "mode": "basic"}),
             ("thresholds", limits), ("color", {"mode": "thresholds"})]
    if unit:
        props.append(("unit", unit))
    return override(field, *props, regex=regex)


STATUS_COLORS = {"ok": "green", "violation": "red", "lagging": "orange",
                 "source_unavailable": "yellow", "error": "dark-red"}


def status_cells(field="status"):
    mapping = {k: {"color": c, "text": k, "index": i}
               for i, (k, c) in enumerate(STATUS_COLORS.items())}
    return override(field, ("custom.cellOptions", {"type": "color-background", "mode": "basic"}),
                    ("mappings", [{"type": "value", "options": mapping}]))


def table(pid, title, x, y, w, h, sql, overrides=(), description=""):
    p = panel(pid, "table", title, x, y, w, h, sql, description=description)
    p["fieldConfig"]["overrides"] = list(overrides)
    p["options"] = {"showHeader": True, "cellHeight": "sm"}
    return p


def section(pid, title, y):
    return {"id": pid, "type": "row", "title": title, "collapsed": False,
            "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}, "panels": []}


def error_cells(field):
    """Empty text is green '—', any text is red: a failed refresh shows its message."""
    mappings = [{"type": "value", "options": {"": {"text": "—", "color": "green", "index": 0}}},
                {"type": "regex", "options": {"pattern": ".+",
                                              "result": {"color": "red", "index": 1}}}]
    return override(field, ("custom.cellOptions", {"type": "color-background", "mode": "basic"}),
                    ("mappings", mappings))


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


def bargauge(pid, title, x, y, w, h, sql, by_rows=False):
    p = panel(pid, "bargauge", title, x, y, w, h, sql)
    p["fieldConfig"]["defaults"] = {"unit": "none", "decimals": 0, "min": 0,
                                    "color": {"mode": "palette-classic"}}
    # by_rows: one bar per row, labelled by the string column (all values, not a reduction).
    reduce = ({"calcs": [], "fields": "/^units$/", "values": True} if by_rows
              else {"calcs": ["lastNotNull"], "fields": "", "values": False})
    p["options"] = {"orientation": "horizontal", "displayMode": "gradient", "showUnfilled": True,
                    "reduceOptions": reduce}
    return p


COHORTS = "shopflow.mart_cohort_retention"
TOP = "shopflow.mart_top_products_daily"
STOCK = "shopflow.mart_inventory_current"
HEALTH = "shopflow.mart_pipeline_health"
DQ = "shopflow.dq_check_results"
MARTS = ("mart_revenue_daily", "mart_funnel_daily", "mart_cohort_retention",
         "mart_top_products_daily", "mart_inventory_current")
SCHEDULED_DAGS = ("shopflow_reconciliation", "shopflow_data_quality", "shopflow_retention")
# Marts refresh every 2 minutes (ADR-0009): late by 4 min is a stuck chain, by 10 min a dead one.
REFRESH_LIMITS = thresholds(("green", None), ("orange", 240), ("red", 600))
# A scheduled DAG runs daily: more than 26 hours without a run is a missed day (ADR-0011).
AGE_HOURS_LIMITS = thresholds(("green", None), ("red", 26))
# The alert channel is probed every 3 hours (ADR-0012): one missed probe is orange, two are red.
CHANNEL_AGE_LIMITS = thresholds(("green", None), ("orange", 5), ("red", 7))
LOW_STOCK_LIMITS = thresholds(("green", None), ("orange", 1))
# stg_order_items beyond ~4.5M rows: fact_orders moves to a table (ADR-0009, ADR-0011).
ITEMS_LIMITS = thresholds(("green", None), ("orange", 3_500_000), ("red", 4_500_000))


def retention_columns(periods: int = 12) -> str:
    return ", ".join(
        f"round(100 * sumIf(returned, period = {n} AND is_partial = 0) / "
        f"nullIf(sumIf(customers, period = {n} AND is_partial = 0), 0), 1) AS `M+{n}`"
        for n in range(periods + 1)
    )


def top_sql(days: int) -> str:
    return (f"SELECT product_id, argMax(product_name, order_date) AS `товар`, "
            f"sum(quantity_net) AS `штук`, sum(revenue_net) AS `выручка` FROM {TOP} "
            f"WHERE order_date >= toDate(now('UTC')) - {days - 1} GROUP BY product_id "
            f"ORDER BY `выручка` DESC LIMIT 10")


def freshness_sql() -> str:
    parts = [
        f"SELECT '{m}' AS `витрина`, "
        f"dateDiff('second', max(refreshed_at), now('UTC')) AS `возраст refresh, с`, "
        f"dateDiff('second', max(source_watermark), now('UTC')) AS `отставание от источника, с` "
        f"FROM shopflow.{m}"
        for m in MARTS
    ]
    return " UNION ALL ".join(parts)


def rows_5_8() -> list[dict]:
    last_runs = (f"WITH last_runs AS (SELECT dag_id, argMax(run_id, checked_at) AS last_run_id "
                 f"FROM {DQ} FINAL WHERE table_name = '' AND startsWith(run_id, 'scheduled__') "
                 f"GROUP BY dag_id) ")
    dags = ", ".join(f"'{d}'" for d in SCHEDULED_DAGS)
    return [
        section(12, "Когорты (FR-6)", 30),
        table(13, "Удержание по когортам, % от размера когорты", 0, 31, 16, 8,
              f"SELECT formatDateTime(cohort_month, '%Y-%m') AS `когорта`, "
              f"max(customers) AS `клиентов`, {retention_columns()} FROM {COHORTS} "
              f"GROUP BY cohort_month ORDER BY cohort_month DESC",
              overrides=[override("клиентов", ("unit", "none")),
                         override("^M\\+", ("unit", "percent"), ("min", 0), ("max", 100),
                                  ("color", {"mode": "continuous-RdYlGr"}),
                                  ("custom.cellOptions", {"type": "color-background",
                                                          "mode": "gradient"}), regex=True)],
              description="Когорта — месяц первого неотменённого заказа клиента; M+N — доля "
                          "клиентов когорты с неотменённым заказом через N месяцев. Незаконченный "
                          "текущий месяц не показан (пусто), повторный заказ в месяц первого "
                          "заказа возвратом не считается. Первые 12 месяцев."),
        timeseries(14, "Новых клиентов по месяцам (размер когорты)", 16, 31, 8, 8,
                   f"SELECT toDateTime(cohort_month) AS time, max(customers) AS customers "
                   f"FROM {COHORTS} GROUP BY time ORDER BY time", bars=True,
                   fmt=TIME_SERIES),
        section(15, "Топ товаров и остатки (FR-7)", 39),
        table(16, "Топ-10 товаров за 7 дней (без отменённых)", 0, 40, 12, 9, top_sql(7),
              overrides=[override("выручка", ("unit", "none"), ("decimals", 2))],
              description="Окно 7 суток по UTC-дню создания заказа, включая сегодня; "
                          "сумма по дням витрины mart_top_products_daily."),
        table(17, "Топ-10 товаров за 30 дней (без отменённых)", 12, 40, 12, 9, top_sql(30),
              overrides=[override("выручка", ("unit", "none"), ("decimals", 2))]),
        stat(18, "Позиций с низким остатком", 0, 49,
             f"SELECT countIf(is_low = 1) AS low FROM {STOCK}", limits=LOW_STOCK_LIMITS, w=4,
             description="Остаток меньше 20 штук (порог в DDL mart_inventory_current)."),
        stat(19, "Единиц на складах", 4, 49, f"SELECT sum(quantity) AS units FROM {STOCK}", w=4),
        bargauge(20, "Остатки по категориям", 8, 49, 16, 4,
                 f"SELECT category, sum(quantity) AS units FROM {STOCK} GROUP BY category "
                 f"ORDER BY units DESC", by_rows=True),
        table(21, "Низкие остатки (50 наименьших)", 0, 53, 24, 8,
              f"SELECT product_name AS `товар`, category AS `категория`, "
              f"warehouse_id AS `склад`, quantity AS `остаток` FROM {STOCK} WHERE is_low = 1 "
              f"ORDER BY quantity, product_id LIMIT 50",
              overrides=[colored("остаток", thresholds(("red", None), ("orange", 5),
                                                       ("yellow", 10)))]),
        section(22, "Здоровье пайплайна (NFR-3, FR-8, FR-9)", 61),
        stat(23, "Возраст последнего refresh витрин", 0, 62,
             f"SELECT dateDiff('second', max(refreshed_at), now('UTC')) AS refresh_age_s "
             f"FROM {HEALTH}", unit="s", limits=REFRESH_LIMITS, w=6,
             description="Витрины пересчитываются раз в 2 минуты цепочкой. Оранжевый — "
                         "цепочка отстаёт (> 4 мин), красный — стоит (> 10 мин)."),
        stat(24, "stg_order_items, строк (порог ~4,5 млн)", 6, 62,
             f"SELECT toUInt64(argMax(ch_value, checked_at)) AS items FROM {DQ} FINAL "
             f"WHERE check_name = 'table_size' AND table_name = 'stg_order_items'",
             limits=ITEMS_LIMITS, w=6,
             description="После ~4,5 млн строк fact_orders переходит на таблицу (ADR-0009)."),
        table(25, "Свежесть витрин", 12, 62, 12, 5, freshness_sql(),
              overrides=[colored("возраст refresh, с", REFRESH_LIMITS, "s"),
                         override("отставание от источника, с", ("unit", "s"))],
              description="Возраст refresh — здоровье цепочки. Отставание от источника "
                          "(now − source_watermark) растёт, когда в Postgres тихо или ноутбук "
                          "выключен: это не поломка пайплайна."),
        table(26, "Источники: последнее событие", 0, 67, 12, 8,
              f"SELECT name AS `источник`, last_event_time AS `последнее событие`, "
              f"dateDiff('second', last_event_time, now('UTC')) AS `давность, с` FROM {HEALTH} "
              f"WHERE kind = 'source' ORDER BY name",
              overrides=[override("давность, с", ("unit", "s"))],
              description="Пусто в давности (NULL) — в таблице ещё нет строк."),
        table(27, "Refreshable MV", 12, 67, 12, 8,
              f"SELECT name AS `MV`, status AS `статус`, last_success_time AS `последний успех`, "
              f"dateDiff('second', last_success_time, now('UTC')) AS `давность, с`, "
              f"ifNull(last_error, '') AS `ошибка` FROM {HEALTH} WHERE kind = 'view' "
              f"ORDER BY name",
              overrides=[colored("давность, с", REFRESH_LIMITS, "s"), error_cells("ошибка")]),
        table(28, "Возраст последнего планового запуска, ч", 0, 75, 8, 6,
              f"WITH ages AS (SELECT toString(dag_id) AS dag_id, "
              f"dateDiff('hour', max(checked_at), now('UTC')) AS age_h FROM {DQ} FINAL "
              f"WHERE table_name = '' AND startsWith(run_id, 'scheduled__') GROUP BY dag_id), "
              f"known AS (SELECT mapFromArrays(groupArray(dag_id), groupArray(age_h)) AS m "
              f"FROM ages) SELECT d AS dag_id, if(mapContains(m, d), m[d], 99999) AS `часов` "
              f"FROM known ARRAY JOIN [{dags}] AS d ORDER BY dag_id",
              overrides=[colored("часов", AGE_HOURS_LIMITS)],
              description="Сторож самого Airflow: красный при > 26 ч без планового запуска; "
                          "99999 — планового запуска ещё не было. Падение задачи из-за "
                          "перезапуска scheduler алерт не шлёт (ADR-0011), здесь это видно."),
        table(29, "Статус последних плановых запусков", 8, 75, 16, 10,
              last_runs +
              f"SELECT r.dag_id AS dag_id, r.check_name AS `проверка`, r.table_name AS `таблица`, "
              f"toString(r.status) AS status, r.violations AS `нарушений`, "
              f"r.checked_at AS `когда` FROM {DQ} AS r FINAL "
              f"WHERE (r.dag_id, r.run_id) IN (SELECT dag_id, last_run_id FROM last_runs) "
              f"AND r.check_name != 'table_size' "
              f"ORDER BY r.dag_id, r.status DESC, r.check_name, r.table_name",
              overrides=[status_cells()],
              description="Последний плановый запуск каждого DAG: сверка (FR-8), качество "
                          "данных (FR-9), ретеншн (NFR-5). Худшие статусы сверху."),
        table(30, "Ручные и тестовые запуски (последние 10)", 0, 81, 8, 8,
              f"SELECT dag_id, run_id, toString(status) AS status, checked_at AS `когда` "
              f"FROM {DQ} FINAL WHERE table_name = '' AND check_name != 'table_size' "
              f"AND NOT startsWith(run_id, 'scheduled__') ORDER BY checked_at DESC LIMIT 10",
              overrides=[status_cells()],
              description="Отдельно от плановых: ручной запуск (например, с "
                          "simulate_violation) не красит плановую секцию."),
        table(31, "Размеры таблиц (по данным ретеншн-DAG)", 8, 85, 16, 8,
              f"SELECT table_name AS `таблица`, toUInt64(ch_value) AS `строк`, "
              f"JSONExtractUInt(details, 'bytes_on_disk') AS `на диске` FROM {DQ} FINAL "
              f"WHERE check_name = 'table_size' AND run_id = (SELECT argMax(run_id, checked_at) "
              f"FROM {DQ} FINAL WHERE check_name = 'table_size') ORDER BY `на диске` DESC",
              overrides=[override("на диске", ("unit", "bytes"))],
              description="Тренд диска и порог ADR-0009; пишет shopflow_retention."),
        stat(32, "Канал алертов: часов с последней успешной проверки Telegram", 0, 93,
             f"SELECT if(countIf(status = 'ok') = 0, 99999, "
             f"dateDiff('hour', maxIf(checked_at, status = 'ok'), now('UTC'))) AS age_h "
             f"FROM {DQ} FINAL WHERE check_name = 'telegram_reachable' AND table_name = ''",
             unit="h", limits=CHANNEL_AGE_LIMITS, w=8,
             description="Плановая проба shopflow_alert_channel раз в 3 часа: GET без токена на "
                         "Bot API из контейнера scheduler. Красный — алерты в Telegram не "
                         "доходят (туннель, ADR-0012), хотя DAG-и зелёные; 99999 — проверок "
                         "ещё не было."),
    ]


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
        "panels": rows_5_7() + rows_5_8(),
    }


def render() -> str:
    return json.dumps(dashboard(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"


if __name__ == "__main__":
    OUT.write_text(render())
    print(f"wrote {OUT.relative_to(ROOT)}")
