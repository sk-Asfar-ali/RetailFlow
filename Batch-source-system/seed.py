"""
seed.py
--------
One-time bulk load of the source Postgres DB with an initial batch
of customers and products. Run this once after the containers are up.

Usage:
    python seed.py --customers 500 --products 200
"""

import argparse
import os
import random

import psycopg2
from dotenv import load_dotenv
from faker import Faker

load_dotenv()
fake = Faker()

CATEGORIES = {
    "Electronics": ["Mobiles", "Laptops", "Headphones", "Cameras"],
    "Fashion": ["Men", "Women", "Kids", "Footwear"],
    "Home": ["Furniture", "Kitchen", "Decor"],
    "Grocery": ["Snacks", "Beverages", "Staples"],
    "Sports": ["Fitness", "Outdoor", "Team Sports"],
}

BRANDS = ["Acme", "Zenith", "NovaTech", "Urban", "Pinnacle", "Everest", "Bluewave"]


def get_connection():
    return psycopg2.connect(
        host=os.getenv("PGHOST"),
        port=os.getenv("PGPORT"),
        dbname=os.getenv("PGDATABASE"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
        sslmode=os.getenv("PGSSLMODE", "require"),  # Azure Postgres requires SSL
    )


def messy_text(val):
    """Inject messy casing, leading/trailing whitespace, or double spaces."""
    if not val or not isinstance(val, str):
        return val
    r = random.random()
    if r < 0.08:
        return val.upper()
    elif r < 0.16:
        return val.lower()
    elif r < 0.24:
        return f"  {val}  "
    elif r < 0.30:
        return val.swapcase()
    return val


def messy_email(email):
    """Inject messy email formats like UPPERCASE, untrimmed whitespace, or missing @."""
    r = random.random()
    if r < 0.10:
        return email.upper()
    elif r < 0.18:
        return f" {email} "
    elif r < 0.22:
        return email.replace("@", " AT ")
    return email


def messy_phone(phone):
    """Inject inconsistent phone number formats."""
    r = random.random()
    if r < 0.15:
        return "".join(filter(str.isdigit, phone))[:10]  # unformatted numbers e.g. 9876543210
    elif r < 0.25:
        return f"+1 ({phone[:3]}) {phone[3:6]}-{phone[6:10]}"
    elif r < 0.30:
        return "N/A"
    return phone[:30]


def messy_category(category):
    """Inject inconsistent category casing, typos, or trailing spaces."""
    r = random.random()
    if r < 0.10:
        return category.lower()
    elif r < 0.18:
        return category.upper()
    elif r < 0.24:
        return f"{category} "
    elif r < 0.28:
        return f"{category}_DEPT"
    return category


def seed_customers(cur, n):
    print(f"Seeding {n} customers (with messy production data)...")
    for _ in range(n):
        first_name = messy_text(fake.first_name())
        last_name = messy_text(fake.last_name())
        email = messy_email(fake.unique.email())
        phone = messy_phone(fake.phone_number()) if random.random() > 0.10 else None  # ~10% null phone
        address = messy_text(fake.street_address())
        city = messy_text(fake.city())
        state = messy_text(fake.state())
        country = messy_text(fake.country())
        postal_code = fake.postcode() if random.random() > 0.08 else None  # ~8% null postal code

        cur.execute(
            """
            INSERT INTO retail.customers
                (first_name, last_name, email, phone, address_line,
                 city, state, country, postal_code, signup_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                first_name,
                last_name,
                email,
                phone,
                address,
                city,
                state,
                country,
                postal_code,
                fake.date_time_between(start_date="-2y", end_date="now"),
            ),
        )


def seed_products(cur, n):
    print(f"Seeding {n} products (with messy production data)...")
    for _ in range(n):
        category = random.choice(list(CATEGORIES.keys()))
        sub_category = random.choice(CATEGORIES[category])
        brand = messy_text(random.choice(BRANDS)) if random.random() > 0.05 else None  # ~5% null brand
        prod_name = messy_text(f"{brand or 'Generic'} {fake.word().capitalize()} {sub_category}")
        
        # Inject bad price / stock anomalies (~5% negative price or 0 price glitch)
        r = random.random()
        if r < 0.03:
            price = -round(random.uniform(5, 100), 2)  # negative price error
        elif r < 0.06:
            price = 0.00  # 0 price glitch
        else:
            price = round(random.uniform(5, 2000), 2)

        stock_qty = -random.randint(1, 10) if random.random() < 0.04 else random.randint(0, 500)

        cur.execute(
            """
            INSERT INTO retail.products
                (product_name, category, sub_category, brand, price, stock_qty)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                prod_name,
                messy_category(category),
                messy_category(sub_category),
                brand,
                price,
                stock_qty,
            ),
        )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--customers", type=int, default=500)
    parser.add_argument("--products", type=int, default=200)
    args = parser.parse_args()

    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()

    seed_customers(cur, args.customers)
    seed_products(cur, args.products)

    cur.close()
    conn.close()
    print("Seeding complete.")


if __name__ == "__main__":
    main()
