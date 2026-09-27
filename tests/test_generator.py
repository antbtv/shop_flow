import random
from collections import Counter

import pytest

from generator.model import (
    ACTION_WEIGHTS,
    FINAL_STATUSES,
    OPEN_STATUSES,
    STATUSES,
    TRANSITIONS,
    Customer,
    change_customer,
    choose_action,
    is_valid_transition,
    load_multiplier,
    new_customer,
    next_status,
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
