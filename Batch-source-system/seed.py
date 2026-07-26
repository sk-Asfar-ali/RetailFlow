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


def seed_customers(cur, n):
    print(f"Seeding {n} customers...")
    for _ in range(n):
        cur.execute(
            """
            INSERT INTO retail.customers
                (first_name, last_name, email, phone, address_line,
                 city, state, country, postal_code, signup_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                fake.first_name(),
                fake.last_name(),
                fake.unique.email(),
                fake.phone_number()[:30],
                fake.street_address(),
                fake.city(),
                fake.state(),
                fake.country(),
                fake.postcode(),
                fake.date_time_between(start_date="-2y", end_date="now"),
            ),
        )


def seed_products(cur, n):
    print(f"Seeding {n} products...")
    for _ in range(n):
        category = random.choice(list(CATEGORIES.keys()))
        sub_category = random.choice(CATEGORIES[category])
        cur.execute(
            """
            INSERT INTO retail.products
                (product_name, category, sub_category, brand, price, stock_qty)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                f"{random.choice(BRANDS)} {fake.word().capitalize()} {sub_category[:-1] if sub_category.endswith('s') else sub_category}",
                category,
                sub_category,
                random.choice(BRANDS),
                round(random.uniform(5, 2000), 2),
                random.randint(0, 500),
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
