"""Pure load-generation logic: order status transitions and synthetic customer data.

No database access here, so it is unit-tested without Postgres.
"""

import random
from dataclasses import dataclass

# Order lifecycle (PRD 5.1): created -> paid -> shipped -> delivered, cancel before shipping.
TRANSITIONS: dict[str, dict[str, float]] = {
    "created": {"paid": 0.85, "cancelled": 0.15},
    "paid": {"shipped": 0.95, "cancelled": 0.05},
    "shipped": {"delivered": 1.0},
    "delivered": {},
    "cancelled": {},
}
STATUSES = frozenset(TRANSITIONS)
FINAL_STATUSES = frozenset(s for s, nxt in TRANSITIONS.items() if not nxt)
OPEN_STATUSES = STATUSES - FINAL_STATUSES

SEGMENTS = ("new", "regular", "vip", "churn_risk")
CITIES = ("Moscow", "Saint Petersburg", "Kazan", "Novosibirsk", "Yekaterinburg", "Samara")
STREETS = ("Lenina", "Mira", "Sadovaya", "Tverskaya", "Pushkina", "Gagarina")
FIRST_NAMES = ("Anna", "Ivan", "Olga", "Petr", "Maria", "Sergey", "Elena", "Dmitry")
LAST_NAMES = ("Ivanova", "Petrov", "Smirnova", "Kuznetsov", "Popova", "Sokolov")

# Relative weight of each action per tick: status changes dominate, like a real shop.
ACTION_WEIGHTS: dict[str, float] = {
    "new_order": 0.40,
    "advance_order": 0.45,
    "update_customer": 0.10,
    "new_customer": 0.05,
}

# Hourly load multiplier (UTC+3 shop): quiet night, evening peak.
_HOURLY = (
    0.2, 0.1, 0.1, 0.1, 0.1, 0.2, 0.4, 0.6, 0.8, 1.0, 1.0, 1.1,
    1.2, 1.1, 1.0, 1.0, 1.1, 1.3, 1.6, 1.8, 1.7, 1.3, 0.8, 0.4,
)  # fmt: skip


@dataclass(frozen=True)
class Customer:
    name: str
    email: str
    address: str
    segment: str


def is_valid_transition(current: str, new: str) -> bool:
    return new in TRANSITIONS.get(current, {})


def next_status(current: str, rng: random.Random) -> str | None:
    """Pick the next status for an order, or None if the status is final."""
    options = TRANSITIONS[current]
    if not options:
        return None
    statuses = list(options)
    return rng.choices(statuses, weights=[options[s] for s in statuses])[0]


def choose_action(rng: random.Random) -> str:
    actions = list(ACTION_WEIGHTS)
    return rng.choices(actions, weights=[ACTION_WEIGHTS[a] for a in actions])[0]


def load_multiplier(hour: int) -> float:
    """Seasonality: relative load for an hour of day (0-23)."""
    return _HOURLY[hour % 24]


def random_address(rng: random.Random) -> str:
    return f"{rng.choice(CITIES)}, {rng.choice(STREETS)} st., {rng.randint(1, 150)}"


def new_customer(rng: random.Random) -> Customer:
    first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
    return Customer(
        name=f"{first} {last}",
        email=f"{first.lower()}.{last.lower()}.{rng.randrange(10**6)}@example.com",
        address=random_address(rng),
        segment=rng.choice(SEGMENTS),
    )


def change_customer(current: Customer, rng: random.Random) -> Customer:
    """Change address or segment (the SCD2 attributes); the result always differs."""
    if rng.random() < 0.5:
        address = current.address
        while address == current.address:
            address = random_address(rng)
        return Customer(current.name, current.email, address, current.segment)
    segment = rng.choice([s for s in SEGMENTS if s != current.segment])
    return Customer(current.name, current.email, current.address, segment)
