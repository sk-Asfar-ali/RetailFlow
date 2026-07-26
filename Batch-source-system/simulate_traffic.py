"""
simulate_traffic.py
--------------------
Continuously simulates live production traffic against the source
Postgres DB:
  - New customers signing up (occasionally)
  - New orders + order_items + payments being created
  - Order status transitions (PENDING -> CONFIRMED -> SHIPPED -> DELIVERED)
  - Payment status transitions (INITIATED -> SUCCESS/FAILED)
  - Product stock decrementing on order, occasional restocks

This is what your CDC / ingestion pipeline (Databricks Autoloader,
Debezium, JDBC batch pull, etc.) will point at. Every insert/update
naturally advances `updated_at`, giving you realistic incremental
extraction signals.

Usage:
    python simulate_traffic.py --interval 2
"""

import argparse
import os
import random
import time
from datetime import datetime

import psycopg2
from dotenv import load_dotenv
from faker import Faker

load_dotenv()
fake = Faker()

PAYMENT_METHODS = ["CARD", "UPI", "NETBANKING", "COD"]
ORDER_STATUS_FLOW = ["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED"]


def get_connection():
    return psycopg2.connect(
        host=os.getenv("PGHOST"),
        port=os.getenv("PGPORT"),
        dbname=os.getenv("PGDATABASE"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
        sslmode=os.getenv("PGSSLMODE", "require"),  # Azure Postgres requires SSL
    )


def fetch_random_ids(cur, table, id_col, limit=1):
    cur.execute(f"SELECT {id_col} FROM retail.{table} ORDER BY random() LIMIT %s", (limit,))
    return [row[0] for row in cur.fetchall()]


def messy_text(val):
    if not val or not isinstance(val, str):
        return val
    r = random.random()
    if r < 0.08:
        return val.upper()
    elif r < 0.16:
        return val.lower()
    elif r < 0.24:
        return f"  {val}  "
    return val


def messy_email(email):
    r = random.random()
    if r < 0.10:
        return email.upper()
    elif r < 0.18:
        return f" {email} "
    return email


def messy_phone(phone):
    r = random.random()
    if r < 0.15:
        return "".join(filter(str.isdigit, phone))[:10]
    elif r < 0.30:
        return "N/A"
    return phone[:30]


def messy_status(status):
    r = random.random()
    if r < 0.15:
        return status.lower()
    elif r < 0.25:
        return f" {status} "
    return status


def create_new_customer(cur):
    cur.execute(
        """
        INSERT INTO retail.customers
            (first_name, last_name, email, phone, address_line,
             city, state, country, postal_code, signup_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, now())
        """,
        (
            messy_text(fake.first_name()),
            messy_text(fake.last_name()),
            messy_email(fake.unique.email()),
            messy_phone(fake.phone_number()) if random.random() > 0.10 else None,
            messy_text(fake.street_address()),
            messy_text(fake.city()),
            messy_text(fake.state()),
            messy_text(fake.country()),
            fake.postcode() if random.random() > 0.08 else None,
        ),
    )
    print(f"[{datetime.now()}] NEW CUSTOMER inserted (messy format)")


def create_new_order(cur):
    customer_ids = fetch_random_ids(cur, "customers", "customer_id")
    if not customer_ids:
        return
    customer_id = customer_ids[0]

    ship_city = messy_text(fake.city()) if random.random() > 0.05 else None  # ~5% null city
    ship_country = messy_text(fake.country()) if random.random() > 0.05 else None

    cur.execute(
        """
        INSERT INTO retail.orders (customer_id, order_date, status, shipping_city, shipping_country)
        VALUES (%s, now(), 'PENDING', %s, %s)
        RETURNING order_id
        """,
        (customer_id, ship_city, ship_country),
    )
    order_id = cur.fetchone()[0]

    num_items = random.randint(1, 4)
    total_amount = 0
    for _ in range(num_items):
        product_ids = fetch_random_ids(cur, "products", "product_id")
        if not product_ids:
            continue
        product_id = product_ids[0]

        cur.execute("SELECT price, stock_qty FROM retail.products WHERE product_id = %s", (product_id,))
        row = cur.fetchone()
        if not row:
            continue
        price, stock_qty = row
        qty = random.randint(1, 3)

        cur.execute(
            """
            INSERT INTO retail.order_items (order_id, product_id, quantity, unit_price)
            VALUES (%s, %s, %s, %s)
            """,
            (order_id, product_id, qty, price),
        )
        total_amount += float(price) * qty

        # decrement stock (won't go negative)
        new_stock = max(stock_qty - qty, 0)
        cur.execute(
            "UPDATE retail.products SET stock_qty = %s WHERE product_id = %s",
            (new_stock, product_id),
        )

    cur.execute(
        "UPDATE retail.orders SET total_amount = %s WHERE order_id = %s",
        (round(total_amount, 2), order_id),
    )

    # create a payment attempt with messy method format
    method = random.choice(PAYMENT_METHODS)
    if random.random() < 0.20:
        method = method.lower() if random.random() < 0.5 else f" {method} "

    cur.execute(
        """
        INSERT INTO retail.payments (order_id, amount, method, status)
        VALUES (%s, %s, %s, 'INITIATED')
        """,
        (order_id, round(total_amount, 2), method),
    )

    print(f"[{datetime.now()}] NEW ORDER #{order_id} created with {num_items} items (${total_amount:.2f})")


def progress_order_status(cur):
    """Advance a random in-flight order to its next status."""
    cur.execute(
        """
        SELECT order_id, status FROM retail.orders
        WHERE status NOT IN ('DELIVERED', 'CANCELLED', 'delivered', 'cancelled')
        ORDER BY random() LIMIT 1
        """
    )
    row = cur.fetchone()
    if not row:
        return
    order_id, current_status = row
    current_status_clean = current_status.strip().upper()

    if random.random() < 0.05:
        new_status = "CANCELLED"
    else:
        idx = ORDER_STATUS_FLOW.index(current_status_clean) if current_status_clean in ORDER_STATUS_FLOW else 0
        new_status = ORDER_STATUS_FLOW[min(idx + 1, len(ORDER_STATUS_FLOW) - 1)]

    # Inject status casing messy drift
    final_status = messy_status(new_status)

    cur.execute("UPDATE retail.orders SET status = %s WHERE order_id = %s", (final_status, order_id))
    print(f"[{datetime.now()}] ORDER #{order_id} status {current_status} -> {final_status}")


def progress_payment_status(cur):
    """Resolve a random INITIATED payment to SUCCESS or FAILED."""
    cur.execute(
        "SELECT payment_id FROM retail.payments WHERE UPPER(TRIM(status)) = 'INITIATED' ORDER BY random() LIMIT 1"
    )
    row = cur.fetchone()
    if not row:
        return
    payment_id = row[0]
    outcome = "SUCCESS" if random.random() < 0.9 else "FAILED"
    final_outcome = messy_status(outcome)
    paid_at_clause = "paid_at = now()," if outcome == "SUCCESS" else ""

    cur.execute(
        f"UPDATE retail.payments SET {paid_at_clause} status = %s WHERE payment_id = %s",
        (final_outcome, payment_id),
    )
    print(f"[{datetime.now()}] PAYMENT #{payment_id} -> {final_outcome}")


def restock_product(cur):
    product_ids = fetch_random_ids(cur, "products", "product_id")
    if not product_ids:
        return
    product_id = product_ids[0]
    restock_amount = random.randint(10, 100)
    cur.execute(
        "UPDATE retail.products SET stock_qty = stock_qty + %s WHERE product_id = %s",
        (restock_amount, product_id),
    )
    print(f"[{datetime.now()}] PRODUCT #{product_id} restocked +{restock_amount}")


ACTIONS_WEIGHTED = [
    (create_new_order, 40),
    (progress_order_status, 25),
    (progress_payment_status, 20),
    (restock_product, 8),
    (create_new_customer, 7),
]


def pick_action():
    actions, weights = zip(*ACTIONS_WEIGHTED)
    return random.choices(actions, weights=weights, k=1)[0]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval", type=float, default=2.0, help="seconds between events")
    args = parser.parse_args()

    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()

    print("Starting live traffic simulation. Ctrl+C to stop.")
    try:
        while True:
            action = pick_action()
            try:
                action(cur)
            except Exception as e:
                print(f"Error during {action.__name__}: {e}")
                conn.rollback()
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nStopping simulation.")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
