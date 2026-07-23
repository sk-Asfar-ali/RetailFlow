"""
events.py
---------
Defines the clickstream event schema and generates realistic
random events. Kept separate from the producer so the same event
logic can be reused regardless of which backend (Event Hubs / Kafka)
ships the data.

Event types modeled (typical e-commerce clickstream):
  - page_view
  - product_view
  - search
  - add_to_cart
  - remove_from_cart
  - checkout_start

Each event carries a session_id so downstream you can reconstruct
user sessions / funnels.
"""

import json
import random
import uuid
from datetime import datetime, timezone

from faker import Faker

fake = Faker()

EVENT_TYPES_WEIGHTED = [
    ("page_view", 40),
    ("product_view", 30),
    ("search", 12),
    ("add_to_cart", 10),
    ("remove_from_cart", 3),
    ("checkout_start", 5),
]

DEVICE_TYPES = ["mobile", "desktop", "tablet"]
PAGES = ["/home", "/category/electronics", "/category/fashion", "/category/home",
         "/deals", "/cart", "/checkout", "/account"]
SEARCH_TERMS = ["wireless earbuds", "running shoes", "laptop bag", "smart watch",
                "office chair", "bluetooth speaker", "yoga mat", "led lights"]


class SessionPool:
    """
    Keeps a rolling pool of active sessions so events aren't all
    single-shot — mimics real users browsing across multiple events
    per session before it expires.
    """

    def __init__(self, customer_ids=None, pool_size=200, max_events_per_session=15):
        self.pool_size = pool_size
        self.max_events_per_session = max_events_per_session
        self.customer_ids = customer_ids or []
        self.sessions = {}  # session_id -> {customer_id, event_count}
        self._fill_pool()

    def _new_session(self):
        session_id = str(uuid.uuid4())
        # ~60% of sessions are logged-in customers, 40% anonymous
        customer_id = random.choice(self.customer_ids) if self.customer_ids and random.random() < 0.6 else None
        self.sessions[session_id] = {
            "customer_id": customer_id,
            "event_count": 0,
            "device": random.choice(DEVICE_TYPES),
        }
        return session_id

    def _fill_pool(self):
        while len(self.sessions) < self.pool_size:
            self._new_session()

    def get_session(self):
        session_id = random.choice(list(self.sessions.keys()))
        session = self.sessions[session_id]
        session["event_count"] += 1

        # expire session after N events, replace with a fresh one
        if session["event_count"] >= self.max_events_per_session:
            del self.sessions[session_id]
            self._fill_pool()

        return session_id, session


def generate_event(session_pool: SessionPool) -> dict:
    session_id, session = session_pool.get_session()
    event_type = random.choices(
        [e[0] for e in EVENT_TYPES_WEIGHTED],
        weights=[e[1] for e in EVENT_TYPES_WEIGHTED],
        k=1,
    )[0]

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "session_id": session_id,
        "customer_id": session["customer_id"],
        "device": session["device"],
        "page_url": random.choice(PAGES),
        "referrer": fake.uri() if random.random() < 0.3 else None,
        "user_agent": fake.user_agent(),
        "ip_address": fake.ipv4(),
    }

    if event_type in ("product_view", "add_to_cart", "remove_from_cart"):
        event["product_id"] = random.randint(1, 200)  # aligns with seeded product range
        if event_type in ("add_to_cart", "remove_from_cart"):
            event["quantity"] = random.randint(1, 3)

    if event_type == "search":
        event["search_term"] = random.choice(SEARCH_TERMS)

    if event_type == "checkout_start":
        event["cart_value"] = round(random.uniform(10, 500), 2)

    return event


def event_to_json(event: dict) -> str:
    return json.dumps(event, default=str)
