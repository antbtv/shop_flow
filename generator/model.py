"""Pure load-generation logic: order status transitions, synthetic customers and products.

No database access here, so it is unit-tested without Postgres.
"""

import random
from dataclasses import dataclass, replace
from decimal import ROUND_HALF_UP, Decimal

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

# Catalog: category -> (name stems, list price range in rubles).
CATEGORIES: dict[str, tuple[tuple[str, ...], tuple[int, int]]] = {
    "books": (("Novel", "Cookbook", "Atlas", "Biography"), (300, 2500)),
    "electronics": (("Headphones", "Charger", "Keyboard", "Speaker"), (1500, 30000)),
    "home": (("Kettle", "Lamp", "Blanket", "Pan"), (500, 8000)),
    "toys": (("Puzzle", "Robot", "Doll", "Blocks"), (300, 5000)),
    "sports": (("Ball", "Yoga Mat", "Dumbbell", "Bottle"), (400, 7000)),
}
BRANDS = ("Aurora", "Nord", "Volga", "Taiga", "Luna", "Sever")
MIN_PRICE = Decimal("1.00")
MAX_PRICE = Decimal("99999999.99")  # NUMERIC(10,2)
CATEGORY_CHANGE_SHARE = 0.05  # rare re-categorization, the rest are price changes
SALE_SHARE = 0.3  # of price changes: a sale discount instead of a small drift

# Relative weight of each action per tick, sums to 1: status changes dominate, like a real
# shop. Price changes are rarer than orders but frequent enough for SCD2 history (ADR-0009).
ACTION_WEIGHTS: dict[str, float] = {
    "new_order": 0.40,
    "advance_order": 0.45,
    "update_customer": 0.08,
    "new_customer": 0.04,
    "update_product": 0.025,
    "new_product": 0.005,
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


@dataclass(frozen=True)
class Product:
    name: str
    category: str
    price: Decimal


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


def _money(value: Decimal) -> Decimal:
    return min(max(value, MIN_PRICE), MAX_PRICE).quantize(Decimal("0.01"), ROUND_HALF_UP)


def new_product(rng: random.Random) -> Product:
    category = rng.choice(sorted(CATEGORIES))
    stems, (low, high) = CATEGORIES[category]
    # Prices end in .90 or .99, like shelf prices.
    price = Decimal(rng.randint(low, high)) - rng.choice((Decimal("0.10"), Decimal("0.01")))
    return Product(
        name=f"{rng.choice(BRANDS)} {rng.choice(stems)} {rng.randint(1, 999)}",
        category=category,
        price=_money(price),
    )


def change_price(price: Decimal, rng: random.Random) -> Decimal:
    """New list price: a sale (-20..-40 %) or a drift (+-3..15 %). Always differs."""
    for _ in range(100):
        if rng.random() < SALE_SHARE:
            factor = 1 - rng.uniform(0.20, 0.40)
        else:
            factor = 1 + rng.choice((-1, 1)) * rng.uniform(0.03, 0.15)
        new = _money(price * Decimal(str(round(factor, 4))))
        if new != price:
            return new
    # Clamped at a bound (for example MIN_PRICE and every factor < 1): step away from it.
    step = Decimal("0.01") if price == MIN_PRICE else Decimal("-0.01")
    return _money(price + step)


def change_product(current: Product, rng: random.Random) -> Product:
    """Change price or, rarely, category (the SCD2 attributes); the result always differs."""
    if rng.random() < CATEGORY_CHANGE_SHARE:
        category = rng.choice([c for c in sorted(CATEGORIES) if c != current.category])
        return replace(current, category=category)
    return replace(current, price=change_price(current.price, rng))
