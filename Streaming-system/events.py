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

    # Timestamp format drift (~15% drift: ISO string vs Epoch Unix ms vs formatted datetime)
    now_dt = datetime.now(timezone.utc)
    t_rand = random.random()
    if t_rand < 0.08:
        event_time = str(int(now_dt.timestamp() * 1000))  # Epoch milliseconds as string
    elif t_rand < 0.15:
        event_time = now_dt.strftime("%Y/%m/%d %H:%M:%S")  # Non-standard date format
    else:
        event_time = now_dt.isoformat()

    # Event type casing drift (~10% UPPERCASE e.g. ADD_TO_CART)
    final_event_type = event_type.upper() if random.random() < 0.10 else event_type

    # Device name drift
    device = session["device"]
    if random.random() < 0.12:
        device = random.choice(["iPhone 15", "Android Phone", "DESKTOP", "Mobile Web", " Tablet "])

    # Customer ID representation drift
    cust_id = session["customer_id"]
    if cust_id is not None and random.random() < 0.10:
        cust_id = str(cust_id)  # string representation "102"
    elif cust_id is None and random.random() < 0.05:
        cust_id = "null"  # string "null" instead of actual None

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": final_event_type,
        "event_time": event_time,
        "session_id": session_id,
        "customer_id": cust_id,
        "device": device,
        "page_url": random.choice(PAGES),
        "referrer": fake.uri() if random.random() < 0.3 else None,
        "user_agent": fake.user_agent() if random.random() > 0.08 else None,  # ~8% missing user_agent
        "ip_address": fake.ipv4() if random.random() > 0.05 else "127.0.0.1",
    }

    if event_type in ("product_view", "add_to_cart", "remove_from_cart"):
        event["product_id"] = str(random.randint(1, 200)) if random.random() < 0.10 else random.randint(1, 200)
        if event_type in ("add_to_cart", "remove_from_cart"):
            qty = random.randint(1, 3)
            # ~15% type drift: quantity sent as string "2" or float "2.0"
            q_rand = random.random()
            if q_rand < 0.10:
                event["quantity"] = str(qty)
            elif q_rand < 0.15:
                event["quantity"] = float(qty)
            else:
                event["quantity"] = qty

    if event_type == "search":
        search_term = random.choice(SEARCH_TERMS)
        event["search_term"] = f"  {search_term}  " if random.random() < 0.20 else search_term

    if event_type == "checkout_start":
        val = round(random.uniform(10, 500), 2)
        # ~15% type drift: cart_value formatted as currency string "$149.99" or string "149.99"
        c_rand = random.random()
        if c_rand < 0.10:
            event["cart_value"] = f"${val:.2f}"
        elif c_rand < 0.15:
            event["cart_value"] = str(val)
        else:
            event["cart_value"] = val

    # Schema drift: ~5% unexpected extra metadata key
    if random.random() < 0.05:
        event["_meta_source_version"] = "v1.2.4-dirty"

    return event


def event_to_json(event: dict) -> str:
    return json.dumps(event, default=str)
