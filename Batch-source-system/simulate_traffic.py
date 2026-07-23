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

COUNTRY_VARIANTS = {
    "United States": ["USA", "United States", "U.S.A.", "united states", "us"],
    "United Kingdom": ["UK", "United Kingdom", "U.K.", "great britain", "GB"],
    "Canada": ["Canada", "CA", "can", "canada"],
    "India": ["India", "IN", "IND", "india"],
    "Australia": ["Australia", "AU", "AUS", "australia"],
}


def get_connection():
    return psycopg2.connect(
        host=os.getenv("PGHOST"),
        port=os.getenv("PGPORT"),
        dbname=os.getenv("PGDATABASE"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
    )


def fetch_random_ids(cur, table, id_col, limit=1):
    cur.execute(f"SELECT {id_col} FROM retail.{table} ORDER BY random() LIMIT %s", (limit,))
    return [row[0] for row in cur.fetchall()]


def create_new_customer(cur, messy=True):
    fname = fake.first_name()
    lname = fake.last_name()
    email = fake.unique.email()
    phone = fake.phone_number()[:30]
    country = fake.country()
    postcode = fake.postcode()

    if messy:
        if random.random() < 0.2:
            fname = f"  {fname.lower()}  "
        if random.random() < 0.2:
            email = email.upper()
        if random.random() < 0.15:
            phone = None
        for key, variants in COUNTRY_VARIANTS.items():
            if key.lower() in country.lower():
                country = random.choice(variants)
                break

    cur.execute(
        """
        INSERT INTO retail.customers
            (first_name, last_name, email, phone, address_line,
             city, state, country, postal_code, signup_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, now())
        """,
        (
            fname,
            lname,
            email,
            phone,
            fake.street_address(),
            fake.city(),
            fake.state(),
            country,
            postcode,
        ),
    )
    print(f"[{datetime.now()}] NEW CUSTOMER inserted (email: {email})")


def create_new_order(cur, messy=True):
    customer_ids = fetch_random_ids(cur, "customers", "customer_id")
    if not customer_ids:
        return
    customer_id = customer_ids[0]

    country = fake.country()
    if messy:
        for key, variants in COUNTRY_VARIANTS.items():
            if key.lower() in country.lower():
                country = random.choice(variants)
                break

    cur.execute(
        """
        INSERT INTO retail.orders (customer_id, order_date, status, shipping_city, shipping_country)
        VALUES (%s, now(), 'PENDING', %s, %s)
        RETURNING order_id
        """,
        (customer_id, fake.city(), country),
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
        price, stock_qty = cur.fetchone()
        qty = random.randint(1, 3)

        cur.execute(
            """
            INSERT INTO retail.order_items (order_id, product_id, quantity, unit_price)
            VALUES (%s, %s, %s, %s)
            """,
            (order_id, product_id, qty, price),
        )
        total_amount += float(price) * qty

        new_stock = max(stock_qty - qty, 0)
        cur.execute(
            "UPDATE retail.products SET stock_qty = %s WHERE product_id = %s",
            (new_stock, product_id),
        )

    order_total = round(total_amount, 2)
    cur.execute(
        "UPDATE retail.orders SET total_amount = %s WHERE order_id = %s",
        (order_total, order_id),
    )

    # Calculate payment amount (inject minor rounding/fee discrepancy if messy)
    payment_amount = order_total
    if messy and random.random() < 0.1:
        payment_amount = round(order_total + random.choice([-0.01, 0.01, 0.05]), 2)

    cur.execute(
        """
        INSERT INTO retail.payments (order_id, amount, method, status)
        VALUES (%s, %s, %s, 'INITIATED')
        """,
        (order_id, payment_amount, random.choice(PAYMENT_METHODS)),
    )

    # Occasional duplicate payment gateway retry record if messy
    if messy and random.random() < 0.08:
        cur.execute(
            """
            INSERT INTO retail.payments (order_id, amount, method, status)
            VALUES (%s, %s, %s, 'FAILED')
            """,
            (order_id, payment_amount, random.choice(PAYMENT_METHODS)),
        )
        print(f"[{datetime.now()}] MESSY NOISE: Injected duplicate payment retry attempt for ORDER #{order_id}")

    print(f"[{datetime.now()}] NEW ORDER #{order_id} created with {num_items} items (${order_total:.2f})")


def progress_order_status(cur, messy=True):
    """Advance a random in-flight order to its next status."""
    cur.execute(
        """
        SELECT order_id, status FROM retail.orders
        WHERE status != 'DELIVERED' AND status != 'CANCELLED'
        ORDER BY random() LIMIT 1
        """
    )
    row = cur.fetchone()
    if not row:
        return
    order_id, current_status = row

    if random.random() < 0.05:
        new_status = "CANCELLED"
    else:
        idx = ORDER_STATUS_FLOW.index(current_status)
        new_status = ORDER_STATUS_FLOW[min(idx + 1, len(ORDER_STATUS_FLOW) - 1)]

    # Late-arriving timestamp anomaly if messy
    time_clause = "updated_at = now() - interval '5 minutes'," if messy and random.random() < 0.05 else ""

    cur.execute(f"UPDATE retail.orders SET {time_clause} status = %s WHERE order_id = %s", (new_status, order_id))
    print(f"[{datetime.now()}] ORDER #{order_id} status {current_status} -> {new_status}")


def progress_payment_status(cur, messy=True):
    """Resolve a random INITIATED payment to SUCCESS or FAILED."""
    cur.execute(
        "SELECT payment_id FROM retail.payments WHERE status = 'INITIATED' ORDER BY random() LIMIT 1"
    )
    row = cur.fetchone()
    if not row:
        return
    payment_id = row[0]
    outcome = "SUCCESS" if random.random() < 0.9 else "FAILED"
    paid_at_clause = "paid_at = now()," if outcome == "SUCCESS" else ""

    cur.execute(
        f"UPDATE retail.payments SET {paid_at_clause} status = %s WHERE payment_id = %s",
        (outcome, payment_id),
    )
    print(f"[{datetime.now()}] PAYMENT #{payment_id} -> {outcome}")


def restock_product(cur, messy=True):
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
    parser.add_argument("--messy", action="store_true", default=True, help="Enable realistic dirty data anomalies")
    parser.add_argument("--no-messy", action="store_false", dest="messy", help="Disable dirty data anomalies")
    args = parser.parse_args()

    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()

    print(f"Starting live traffic simulation (interval={args.interval}s, messy={args.messy}). Ctrl+C to stop.")
    try:
        while True:
            action = pick_action()
            try:
                action(cur, messy=args.messy)
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

