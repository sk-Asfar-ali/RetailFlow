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


def apply_messy_customer_data(first_name, last_name, email, phone, country, postal_code):
    # 1. Mixed casing & extra whitespace
    if random.random() < 0.2:
        first_name = f"  {first_name.lower()}  " if random.random() < 0.5 else first_name.upper()
    if random.random() < 0.15:
        last_name = f"{last_name.lower()} "

    # 2. Email mixed casing
    if random.random() < 0.25:
        parts = email.split("@")
        email = f"{parts[0].upper()}@{parts[1]}" if random.random() < 0.5 else email.upper()

    # 3. Phone formatting noise or null
    if random.random() < 0.15:
        phone = None
    elif random.random() < 0.25:
        digits = ''.join(filter(str.isdigit, phone))[:10]
        formats = [f"({digits[:3]}) {digits[3:6]}-{digits[6:]}", f"+1-{digits[:3]}-{digits[3:]}", f"{digits}", "N/A"]
        phone = random.choice(formats)

    # 4. Inconsistent country naming
    for key, variants in COUNTRY_VARIANTS.items():
        if key.lower() in country.lower():
            country = random.choice(variants)
            break

    # 5. Missing postal code
    if random.random() < 0.15:
        postal_code = None

    return first_name, last_name, email, phone, country, postal_code


def seed_customers(cur, n, messy=True):
    print(f"Seeding {n} customers (messy={messy})...")
    for i in range(n):
        fname = fake.first_name()
        lname = fake.last_name()
        email = fake.unique.email()
        phone = fake.phone_number()[:30]
        country = fake.country()
        postcode = fake.postcode()

        if messy:
            fname, lname, email, phone, country, postcode = apply_messy_customer_data(
                fname, lname, email, phone, country, postcode
            )

        cur.execute(
            """
            INSERT INTO retail.customers
                (first_name, last_name, email, phone, address_line,
                 city, state, country, postal_code, signup_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                fake.date_time_between(start_date="-2y", end_date="now"),
            ),
        )


def seed_products(cur, n, messy=True):
    print(f"Seeding {n} products (messy={messy})...")
    for _ in range(n):
        category = random.choice(list(CATEGORIES.keys()))
        sub_category = random.choice(CATEGORIES[category])
        price = round(random.uniform(5, 2000), 2)
        stock_qty = random.randint(0, 500)
        brand = random.choice(BRANDS)

        if messy:
            # 1. Occasional 0.00 price (promotional glitch)
            if random.random() < 0.03:
                price = 0.00
            # 2. Negative stock quantity (oversold inventory anomaly)
            elif random.random() < 0.03:
                stock_qty = -random.randint(1, 10)
            # 3. Unstandardized brand casing
            if random.random() < 0.15:
                brand = brand.lower()

        cur.execute(
            """
            INSERT INTO retail.products
                (product_name, category, sub_category, brand, price, stock_qty)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                f"{brand} {fake.word().capitalize()} {sub_category[:-1] if sub_category.endswith('s') else sub_category}",
                category,
                sub_category,
                brand,
                price,
                stock_qty,
            ),
        )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--customers", type=int, default=500)
    parser.add_argument("--products", type=int, default=200)
    parser.add_argument("--messy", action="store_true", default=True, help="Enable realistic dirty data anomalies")
    parser.add_argument("--no-messy", action="store_false", dest="messy", help="Disable dirty data anomalies")
    args = parser.parse_args()

    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()

    seed_customers(cur, args.customers, messy=args.messy)
    seed_products(cur, args.products, messy=args.messy)

    cur.close()
    conn.close()
    print("Seeding complete.")


if __name__ == "__main__":
    main()

