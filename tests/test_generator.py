import random
from collections import Counter
from decimal import Decimal

import pytest

from generator.model import (
    ACTION_WEIGHTS,
    CATEGORIES,
    FINAL_STATUSES,
    MAX_PRICE,
    MIN_PRICE,
    OPEN_STATUSES,
    STATUSES,
    TRANSITIONS,
    Customer,
    OrderLine,
    Product,
    StockRow,
    change_customer,
    change_price,
    change_product,
    choose_action,
    choose_order_lines,
    initial_stock,
    is_valid_transition,
    load_multiplier,
    new_customer,
    new_product,
    next_status,
    restock_amount,
)


def test_statuses_match_prd():
    assert STATUSES == {"created", "paid", "shipped", "delivered", "cancelled"}
    assert FINAL_STATUSES == {"delivered", "cancelled"}


def test_transition_weights_sum_to_one():
    for status, options in TRANSITIONS.items():
        if options:
            assert sum(options.values()) == pytest.approx(1.0), status


@pytest.mark.parametrize("status", sorted(OPEN_STATUSES))
def test_next_status_is_valid(status):
    rng = random.Random(1)
    for _ in range(200):
        new = next_status(status, rng)
        assert is_valid_transition(status, new)


@pytest.mark.parametrize("status", sorted(FINAL_STATUSES))
def test_final_status_has_no_next(status):
    assert next_status(status, random.Random(1)) is None


def test_no_backward_or_skipping_transitions():
    assert not is_valid_transition("paid", "created")
    assert not is_valid_transition("created", "delivered")
    assert not is_valid_transition("shipped", "cancelled")
    assert not is_valid_transition("delivered", "created")


def test_every_order_reaches_a_final_status():
    rng = random.Random(7)
    for _ in range(500):
        status, steps = "created", 0
        while status not in FINAL_STATUSES:
            status = next_status(status, rng)
            steps += 1
        assert steps <= 3


def test_choose_action_follows_weights():
    rng = random.Random(42)
    counts = Counter(choose_action(rng) for _ in range(20_000))
    assert set(counts) == set(ACTION_WEIGHTS)
    for action, weight in ACTION_WEIGHTS.items():
        assert counts[action] / 20_000 == pytest.approx(weight, abs=0.02)


def test_same_seed_gives_same_data():
    a = [new_customer(random.Random(3)) for _ in range(3)]
    b = [new_customer(random.Random(3)) for _ in range(3)]
    assert a == b


def test_change_customer_always_changes_one_scd2_attribute():
    rng = random.Random(5)
    current = Customer("Anna Ivanova", "anna@example.com", "Kazan, Mira st., 1", "vip")
    for _ in range(300):
        changed = change_customer(current, rng)
        assert (changed.name, changed.email) == (current.name, current.email)
        diffs = (changed.address != current.address) + (changed.segment != current.segment)
        assert diffs == 1
        current = changed


def test_load_multiplier_has_evening_peak_and_quiet_night():
    assert load_multiplier(19) > load_multiplier(12) > load_multiplier(3)
    assert all(load_multiplier(h) > 0 for h in range(24))


def test_action_weights_sum_to_one():
    assert sum(ACTION_WEIGHTS.values()) == pytest.approx(1.0)


def test_every_action_has_a_handler():
    pytest.importorskip("psycopg")
    from generator.generate_orders import ACTIONS

    assert set(ACTIONS) == set(ACTION_WEIGHTS)


def _fits_numeric_10_2(price: Decimal) -> bool:
    return MIN_PRICE <= price <= MAX_PRICE and price == price.quantize(Decimal("0.01"))


def test_new_product_is_valid():
    rng = random.Random(11)
    for _ in range(500):
        p = new_product(rng)
        assert p.category in CATEGORIES
        low, high = CATEGORIES[p.category][1]
        assert low - 1 <= p.price <= high
        assert _fits_numeric_10_2(p.price)


def test_change_price_always_differs_and_stays_in_range():
    rng = random.Random(12)
    for start in (MIN_PRICE, Decimal("1.01"), Decimal("499.90"), MAX_PRICE):
        price = start
        for _ in range(300):
            new = change_price(price, rng)
            assert new != price
            assert _fits_numeric_10_2(new)
            price = new


def test_price_changes_include_sales_and_drifts():
    rng = random.Random(13)
    ratios = [change_price(Decimal("1000.00"), rng) / Decimal("1000.00") for _ in range(2000)]
    sales = sum(r <= Decimal("0.81") for r in ratios)
    drifts = sum(Decimal("0.84") <= r <= Decimal("1.16") for r in ratios)
    assert sales + drifts == len(ratios)
    assert 0.2 < sales / len(ratios) < 0.4


def test_change_product_changes_exactly_one_scd2_attribute():
    rng = random.Random(14)
    current = Product("Nord Kettle 1", "home", Decimal("1999.90"))
    category_changes = 0
    for _ in range(2000):
        changed = change_product(current, rng)
        assert changed.name == current.name
        diffs = (changed.category != current.category) + (changed.price != current.price)
        assert diffs == 1
        category_changes += changed.category != current.category
        current = changed
    assert 0.02 < category_changes / 2000 < 0.08


def _stock(rng: random.Random, n: int = 20) -> list[StockRow]:
    return [
        StockRow(rng.randint(1, 8), rng.randint(1, 3), rng.randint(0, 3), Decimal("10.00") + i)
        for i in range(n)
    ]


def test_order_lines_are_distinct_products_within_stock():
    rng = random.Random(21)
    for _ in range(1000):
        candidates = _stock(rng)
        by_row = {(r.product_id, r.warehouse_id, r.price): r for r in candidates}
        lines = choose_order_lines(candidates, rng)
        assert len(lines) <= 5
        assert len({line.product_id for line in lines}) == len(lines)
        for line in lines:
            row = by_row[(line.product_id, line.warehouse_id, line.price)]
            assert 1 <= line.quantity <= min(3, row.quantity)
            assert line.price == row.price  # price_at_order = list price (ADR-0009)


def test_order_lines_cover_one_to_five_when_stock_allows():
    rng = random.Random(22)
    many = [StockRow(i, 1, 100, Decimal("5.00")) for i in range(1, 21)]
    sizes = Counter(len(choose_order_lines(many, rng)) for _ in range(2000))
    assert set(sizes) == {1, 2, 3, 4, 5}


def test_no_lines_without_stock():
    rng = random.Random(23)
    assert choose_order_lines([], rng) == []
    empty = [StockRow(1, 1, 0, Decimal("5.00")), StockRow(2, 2, 0, Decimal("7.00"))]
    assert choose_order_lines(empty, rng) == []


def test_order_line_takes_first_warehouse_of_a_product():
    rows = [StockRow(1, 2, 5, Decimal("5.00")), StockRow(1, 3, 5, Decimal("5.00"))]
    for seed in range(50):
        lines = choose_order_lines(rows, random.Random(seed))
        assert lines == [OrderLine(1, 2, lines[0].quantity, Decimal("5.00"))]


def test_restock_roughly_balances_consumption():
    rng = random.Random(24)
    many = [StockRow(i, 1, 100, Decimal("5.00")) for i in range(1, 21)]
    units_per_order = sum(
        line.quantity for _ in range(2000) for line in choose_order_lines(many, rng)
    ) / 2000
    consumed = ACTION_WEIGHTS["new_order"] * units_per_order
    supplied = ACTION_WEIGHTS["restock"] * sum(restock_amount(rng) for _ in range(2000)) / 2000
    assert 0.7 < supplied / consumed < 1.4


def test_initial_stock_is_positive():
    rng = random.Random(25)
    assert all(initial_stock(rng) > 0 for _ in range(500))
