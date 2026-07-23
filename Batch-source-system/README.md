# Source System — Simulated E-commerce OLTP DB

This is the "production application database" for the project. It's a
Postgres instance with a realistic e-commerce schema (customers, products,
orders, order_items, payments), plus scripts that simulate live traffic —
new orders, status transitions, stock updates — so downstream pipelines
have something real to ingest incrementally.

## 1. Start Postgres

```bash
docker compose up -d
```

This spins up:
- **postgres** on `localhost:5432` (db: `ecommerce_source`, user: `source_admin`, pass: `source_pass`)
- **pgadmin** on `localhost:5050` (login: admin@admin.com / admin) — optional GUI to browse tables

Schema in `sql/01_schema.sql` is auto-applied on first container start.

## 2. Install Python deps

```bash
python -m venv venv
source venv/bin/activate      # venv\Scripts\activate on Windows
pip install -r requirements.txt
```

## 3. Seed initial data

```bash
python seed.py --customers 500 --products 200
```

## 4. Start live traffic simulation

```bash
python simulate_traffic.py --interval 2
```

This runs forever (Ctrl+C to stop), randomly:
- creating new orders (with items + payment)
- advancing order status: PENDING → CONFIRMED → SHIPPED → DELIVERED (or CANCELLED)
- resolving payments: INITIATED → SUCCESS / FAILED
- decrementing stock on order, restocking occasionally
- occasionally signing up new customers

Every insert/update touches `updated_at`, so this is what your batch
ingestion (JDBC pull / Autoloader) or CDC tool will key off for
incremental extraction later.

## Next steps

- Point a batch extraction job (Databricks JDBC read on `updated_at` watermark,
  or Debezium CDC) at this DB to land data into a Bronze Delta table.
- This will pair with a separate streaming source (Event Hubs / Kafka) for
  the streaming half of the project — coming next.
