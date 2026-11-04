"""Generates realistic e-commerce events, including deliberately faulty ones.

Valid events follow schemas/event_schema.json. The generator also produces the
situations the pipeline must survive: invalid events, duplicates and late arrivals.

Revenue is carried by `purchase` events. A `payment` event describes the settlement
of an order: downstream aggregations must not add both.
"""

from __future__ import annotations

import json
import random
import uuid
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

Event = dict[str, Any]

USER_COUNT = 10_000
PRODUCT_COUNT = 500
RECENT_EVENTS_KEPT = 1_000

# Price range for each category, in euros.
CATEGORY_PRICES: dict[str, tuple[float, float]] = {
    "electronics": (29.0, 1_499.0),
    "fashion": (9.0, 249.0),
    "home": (7.0, 399.0),
    "beauty": (4.0, 89.0),
    "sports": (8.0, 299.0),
    "books": (5.0, 45.0),
    "groceries": (1.0, 39.0),
    "toys": (5.0, 129.0),
}
EVENT_TYPE_WEIGHTS: dict[str, float] = {
    "page_view": 0.55,
    "add_to_cart": 0.18,
    "remove_from_cart": 0.04,
    "checkout": 0.08,
    "purchase": 0.09,
    "payment": 0.06,
}
COUNTRY_WEIGHTS: dict[str, float] = {
    "FR": 0.30,
    "SN": 0.12,
    "CI": 0.12,
    "ML": 0.06,
    "MA": 0.08,
    "BE": 0.08,
    "CA": 0.07,
    "DE": 0.07,
    "GB": 0.05,
    "US": 0.05,
}
MOBILE_MONEY_COUNTRIES = frozenset({"SN", "CI", "ML"})
PAYMENT_WEIGHTS: dict[str, float] = {
    "card": 0.55,
    "paypal": 0.20,
    "bank_transfer": 0.10,
    "cash_on_delivery": 0.15,
}
MOBILE_MONEY_PAYMENT_WEIGHTS: dict[str, float] = {
    "mobile_money": 0.55,
    "card": 0.20,
    "cash_on_delivery": 0.25,
}
TRANSACTION_EVENT_TYPES = frozenset({"purchase", "payment"})
REQUIRED_FIELDS = (
    "event_id",
    "user_id",
    "product_id",
    "category",
    "amount",
    "quantity",
    "event_type",
    "timestamp",
    "payment_method",
    "country",
)


@dataclass(frozen=True)
class Product:
    product_id: str
    category: str
    price: float


@dataclass(frozen=True)
class GeneratedEvent:
    """An event ready to publish. `kind` is valid, invalid, duplicate or late."""

    payload: Event | str
    kind: str

    @property
    def key(self) -> str | None:
        """Return the partition key, so that a user's events stay ordered."""
        if isinstance(self.payload, str):
            return None
        return self.payload.get("user_id")

    @property
    def value(self) -> bytes:
        """Return the Kafka message value. Malformed payloads are sent as they are."""
        if isinstance(self.payload, str):
            return self.payload.encode("utf-8")
        return json.dumps(self.payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _drop_field(event: Event, rng: random.Random) -> None:
    del event[rng.choice(REQUIRED_FIELDS)]


def _negative_amount(event: Event, rng: random.Random) -> None:
    event["amount"] = -round(rng.uniform(1, 500), 2)


def _amount_as_text(event: Event, rng: random.Random) -> None:
    event["amount"] = "twelve euros"


def _zero_quantity(event: Event, rng: random.Random) -> None:
    event["quantity"] = 0


def _unknown_category(event: Event, rng: random.Random) -> None:
    event["category"] = "unknown"


def _unknown_event_type(event: Event, rng: random.Random) -> None:
    event["event_type"] = "teleport"


def _bad_timestamp(event: Event, rng: random.Random) -> None:
    event["timestamp"] = "not-a-timestamp"


def _bad_country(event: Event, rng: random.Random) -> None:
    event["country"] = "FRANCE"


def _purchase_without_payment_method(event: Event, rng: random.Random) -> None:
    event["event_type"] = "purchase"
    event["amount"] = max(event["amount"], 1.0)
    event["payment_method"] = None


FIELD_CORRUPTIONS: tuple[Callable[[Event, random.Random], None], ...] = (
    _drop_field,
    _negative_amount,
    _amount_as_text,
    _zero_quantity,
    _unknown_category,
    _unknown_event_type,
    _bad_timestamp,
    _bad_country,
    _purchase_without_payment_method,
)


class EventGenerator:
    """Infinite, reproducible source of events.

    Args:
        invalid_ratio: Share of events that break the contract.
        duplicate_ratio: Share of events that repeat a recent valid event.
        late_ratio: Share of valid events with a timestamp in the past.
        max_lateness_seconds: Upper bound of that delay.
        seed: Makes the sequence reproducible. None gives a different run each time.
        clock: Returns the current time. Injected by the tests.

    Raises:
        ValueError: If the ratios add up to more than 1.
    """

    def __init__(
        self,
        invalid_ratio: float = 0.0,
        duplicate_ratio: float = 0.0,
        late_ratio: float = 0.0,
        max_lateness_seconds: int = 180,
        seed: int | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if invalid_ratio + duplicate_ratio + late_ratio > 1:
            raise ValueError("invalid_ratio + duplicate_ratio + late_ratio must not exceed 1")
        self._invalid_ratio = invalid_ratio
        self._duplicate_ratio = duplicate_ratio
        self._late_ratio = late_ratio
        self._max_lateness_seconds = max_lateness_seconds
        self._clock = clock
        self._rng = random.Random(seed)
        self._recent: deque[Event] = deque(maxlen=RECENT_EVENTS_KEPT)

        self._products = self._build_catalog()
        # A few products take most of the traffic, as in a real shop.
        self._product_weights = [1 / rank for rank in range(1, PRODUCT_COUNT + 1)]
        self._user_countries = self._rng.choices(
            list(COUNTRY_WEIGHTS), weights=list(COUNTRY_WEIGHTS.values()), k=USER_COUNT
        )

    def _build_catalog(self) -> list[Product]:
        categories = list(CATEGORY_PRICES)
        products = []
        for index in range(PRODUCT_COUNT):
            category = self._rng.choice(categories)
            low, high = CATEGORY_PRICES[category]
            price = round(self._rng.uniform(low, high), 2)
            products.append(Product(f"P{index:05d}", category, price))
        return products

    def _payment_method(self, country: str) -> str:
        weights = (
            MOBILE_MONEY_PAYMENT_WEIGHTS if country in MOBILE_MONEY_COUNTRIES else PAYMENT_WEIGHTS
        )
        return self._rng.choices(list(weights), weights=list(weights.values()))[0]

    def _valid_event(self, timestamp: datetime) -> Event:
        user_index = self._rng.randrange(USER_COUNT)
        country = self._user_countries[user_index]
        product = self._rng.choices(self._products, weights=self._product_weights)[0]
        event_type = self._rng.choices(
            list(EVENT_TYPE_WEIGHTS), weights=list(EVENT_TYPE_WEIGHTS.values())
        )[0]
        is_transaction = event_type in TRANSACTION_EVENT_TYPES
        quantity = 1 if event_type == "page_view" else self._rng.randint(1, 4)
        amount = 0.0 if event_type == "page_view" else round(product.price * quantity, 2)
        return {
            "event_id": str(uuid.UUID(int=self._rng.getrandbits(128), version=4)),
            "user_id": f"U{user_index:06d}",
            "product_id": product.product_id,
            "category": product.category,
            "amount": amount,
            "quantity": quantity,
            "event_type": event_type,
            "timestamp": timestamp.isoformat(timespec="milliseconds"),
            "payment_method": self._payment_method(country) if is_transaction else None,
            "country": country,
        }

    def _invalid_event(self, now: datetime) -> GeneratedEvent:
        event = self._valid_event(now)
        # One extra choice for payloads that are not even JSON.
        corruption_index = self._rng.randrange(len(FIELD_CORRUPTIONS) + 1)
        if corruption_index == len(FIELD_CORRUPTIONS):
            return GeneratedEvent(json.dumps(event)[:-12], "invalid")
        FIELD_CORRUPTIONS[corruption_index](event, self._rng)
        return GeneratedEvent(event, "invalid")

    def next_event(self) -> GeneratedEvent:
        """Return the next event of the stream."""
        now = self._clock()
        draw = self._rng.random()
        if draw < self._invalid_ratio:
            return self._invalid_event(now)

        draw -= self._invalid_ratio
        if draw < self._duplicate_ratio and self._recent:
            return GeneratedEvent(dict(self._rng.choice(self._recent)), "duplicate")

        draw -= self._duplicate_ratio
        kind = "valid"
        timestamp = now
        if draw < self._late_ratio:
            kind = "late"
            delay = self._rng.uniform(1, self._max_lateness_seconds)
            timestamp = now - timedelta(seconds=delay)

        event = self._valid_event(timestamp)
        self._recent.append(event)
        return GeneratedEvent(event, kind)
