# RetailFlow — Architecture

## Overview

RetailFlow is an end-to-end data lakehouse platform simulating a real
e-commerce business, combining **batch OLTP ingestion** and **real-time
clickstream processing**, transformed through a **medallion architecture**
(Landing → Bronze → Silver → Gold) using **Azure Databricks**, **Unity
Catalog**, **ADLS Gen2**, and **dbt**.

The project was deliberately built to include both major data-integration
patterns found in production data platforms:

- **Batch, incremental extraction** from a transactional (OLTP) source
- **Streaming ingestion** from an event-driven source

```
┌─────────────────┐         ┌──────────────────┐
│  Azure Postgres  │         │  Azure Event Hubs │
│  (OLTP source)   │         │  (clickstream)     │
└────────┬─────────┘         └─────────┬──────────┘
         │ JDBC (incremental)          │ Structured Streaming
         │                             │ (Kafka-compatible endpoint)
         ▼                             ▼
┌─────────────────────────────────────────────────┐
│              Landing Zone (raw files)             │
│         retailflow.landing.raw_files (Volume)     │
└─────────────────────┬─────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────┐
│                  Bronze (Delta)                    │
│  customers, products, orders, order_items,         │
│  payments, clickstream_events                       │
└─────────────────────┬─────────────────────────────┘
                       ▼  (dbt)
┌─────────────────────────────────────────────────┐
│                  Silver (Delta)                    │
│  Cleaned, deduplicated, conformed, sessionized     │
└─────────────────────┬─────────────────────────────┘
                       ▼  (dbt)
┌─────────────────────────────────────────────────┐
│                   Gold (Delta)                     │
│  daily_sales_summary, customer_ltv,                │
│  clickstream_funnel                                 │
└─────────────────────┬─────────────────────────────┘
                       ▼
              Databricks Dashboard
           (RetailFlow - Business Overview)
```

All physical data (Bronze/Silver/Gold Delta tables, raw Landing files)
lives in a dedicated **ADLS Gen2 container** (`retailflow-lake`), owned
and managed by Unity Catalog via a registered External Location and
Managed Identity — not Databricks' default internal storage.

---

## 1. Source Systems

### 1.1 Batch source: Azure Database for PostgreSQL

A realistic e-commerce OLTP schema (`Batch-source-system/`):

| Table          | Purpose                                                |
|----------------|---------------------------------------------------------|
| `customers`    | Customer profiles                                       |
| `products`     | Product catalog                                         |
| `orders`       | Orders with in-flight status (PENDING→CONFIRMED→SHIPPED→DELIVERED/CANCELLED) |
| `order_items`  | Line items per order (insert-only)                       |
| `payments`     | Payment attempts per order (INITIATED→SUCCESS/FAILED)   |

A Python/Faker-based **traffic simulator** (`simulate_traffic.py`) continuously
generates realistic production-like activity: new orders, status transitions,
stock changes, and payment resolutions — giving the batch ingestion pipeline
genuine incremental signal via `updated_at` timestamps and database triggers.

Hosted on **Azure Database for PostgreSQL Flexible Server** (Burstable B1ms
tier) rather than local Docker, so it's genuinely reachable from Databricks
in the cloud — matching how a real production source would be deployed.

### 1.2 Streaming source: Azure Event Hubs

A Python producer (`Streaming-system/produce_clickstream.py`) continuously
generates clickstream events (page views, product views, searches, cart
actions, checkout starts), grouped into simulated user sessions, and
publishes them to **Azure Event Hubs** via its Kafka-compatible endpoint
(requires **Standard tier** — Basic tier does not support the Kafka
protocol).

Roughly 60% of sessions are tied to real `customer_id`s pulled from the
Postgres source, enabling downstream joins between clickstream behavior
and actual customer/order data in Gold.

---

## 2. Why a message broker (Event Hubs) instead of writing directly to Delta

A natural question: why not have the producer write straight into a Delta
table? Event Hubs (the broker) provides:

- **Decoupling** — the producer doesn't need to know anything about
  Databricks/Spark; any number of downstream consumers could read the
  same stream independently.
- **Buffering** — if the Databricks consumer lags or is down temporarily,
  events aren't lost; they sit in the broker (24hr retention) until
  consumed.
- **Replayability** — checkpoints can be reset to reprocess historical
  events through updated ingestion logic, without needing to regenerate
  data from the source.
- **Ordering** — partitioning guarantees in-order delivery per partition
  key (e.g. per session), important for reconstructing user journeys.

This is the standard **producer → broker → stream processor → sink**
pattern used in real production streaming architectures.

---

## 3. Unity Catalog & Storage Architecture

### 3.1 The trust chain

Since Databricks compute needs to securely read/write into an Azure
storage account, RetailFlow uses Unity Catalog's Managed Identity
pattern (not the legacy mount-point / access-key pattern):

1. **Access Connector for Databricks** — an Azure resource providing a
   system-assigned Managed Identity for Databricks to authenticate as.
2. **IAM Role Assignment** — grants that Managed Identity the
   `Storage Blob Data Contributor` role on the `datalakealli` storage
   account.
3. **Storage Credential** (in Unity Catalog) — registers the Access
   Connector's Managed Identity as an authorized credential inside
   Databricks' governance layer.
4. **External Location** — ties a specific ABFSS path
   (`abfss://retailflow-lake@datalakealli.dfs.core.windows.net/`) to
   that Storage Credential.
5. **Catalog / Schemas with `MANAGED LOCATION`** — the `retailflow`
   catalog and its `landing`/`bronze`/`silver`/`gold` schemas are each
   explicitly anchored to subfolders within the External Location, so
   every table's data physically lands inside the owned ADLS container.

This means the project's entire dataset is verifiable and portable —
independent of Databricks' own internal managed storage.

### 3.2 Catalog structure

```
retailflow (catalog)
├── landing   -- raw files (Volume: raw_files)
├── bronze    -- minimally-transformed Delta tables + _ingestion_control
├── silver    -- cleaned/conformed dbt models
└── gold      -- business-level aggregates (dbt models)
```

### 3.3 Secrets management

All credentials (Postgres connection details, Event Hubs connection
string) are stored in **Azure Key Vault**, linked to Databricks via a
Key Vault-backed secret scope (`retailflow_scope`). No credentials are
ever hardcoded in notebooks or committed to source control.

---

## 4. Ingestion Design

### 4.1 Batch: incremental JDBC (Postgres → Bronze)

Notebook: `databricks-notebooks/01_bronze/01_bronze_ingestion_postgres.py`

Pattern per table:
1. Read the last processed watermark (`updated_at`) from a control table
   (`retailflow.bronze._ingestion_control`).
2. JDBC-read only rows where `updated_at > last_watermark`.
3. Land the raw extract as Parquet in the Landing volume (audit trail /
   replay safety net).
4. `MERGE` the incremental batch into the Bronze Delta table — upserting
   on primary key, so updated OLTP rows (e.g. order status changes)
   overwrite in place rather than duplicating.
5. Advance the watermark.

This avoids full-table reloads on every run and correctly reflects the
OLTP source's current state (not an append-only history of every change).

### 4.2 Streaming: Structured Streaming with `availableNow` (Event Hubs → Bronze)

Notebook: `databricks-notebooks/01_bronze/02_bronze_ingestion_clickstream.py`

Rather than running as an always-on continuous stream (which would
require 24/7 compute), this uses:

```python
.trigger(availableNow=True)
```

This processes everything currently available in Event Hubs since the
last checkpoint, then **stops** — behaving like a normal batch job that
can be scheduled. A checkpoint location tracks exactly which Kafka
offsets have been consumed, making re-runs safe (no duplication, no data
loss) across scheduled executions.

**Design tradeoff, explicitly**: true 24/7 streaming (sub-second latency)
was considered and rejected for this project. Given the data volumes
involved (a Faker-driven demo producer, not a system requiring
split-second decisions), scheduled near-real-time processing (every 15
minutes) delivers freshness that's more than adequate for BI/reporting
use cases, at a fraction of the compute cost of an always-on cluster.
This mirrors how most real companies actually run "real-time analytics"
in practice.

### 4.3 Orchestration

A single **Databricks Job** (`retailflow-bronze-ingestion`) orchestrates
the full pipeline on a **15-minute schedule**:

```
setup_catalog
      │
      ├──► bronze_postgres ─────┐
      │                          ├──► dbt_transform (dbt run + dbt test)
      └──► bronze_clickstream ──┘
```

- `setup_catalog` and `bronze_postgres`/`bronze_clickstream` run on a
  shared Job Cluster (single-node, small VM) to stay within Azure
  subscription core quotas.
- `dbt_transform` uses Databricks' native **dbt task type**, running
  against a Serverless SQL Warehouse, executing `dbt deps → dbt seed →
  dbt snapshot → dbt run → dbt test` in sequence.
- Both ingestion tasks depend only on `setup_catalog`, so they run in
  parallel; `dbt_transform` depends on both, ensuring Silver/Gold only
  rebuild after fresh Bronze data lands.

---

## 5. Transformation Layer (dbt)

dbt Core with the `dbt-databricks` adapter runs directly against the
Databricks SQL Warehouse (no separate transformation engine).

### Silver layer — cleaning & conforming
- Deduplication (via `row_number()` windowed on the latest `updated_at`)
- Standardized text casing, trimmed whitespace
- **`silver_clickstream_sessions`**: reconstructs session sequences
  (`event_sequence_in_session`, `previous_event_type`,
  `seconds_since_previous_event`) — the foundation for funnel analysis.
  Also normalizes an inconsistent-casing data quality issue discovered
  in Bronze (`PAGE_VIEW` vs `page_view` etc., from earlier producer test
  runs) rather than silently dropping affected rows.

### Slowly Changing Dimensions (SCD Type 2)

Two dbt **snapshots** track historical versions of dimension attributes
that change over time — something the standard Silver models (which
always reflect only the *current* state) cannot answer:

- **`customers_snapshot`** — tracks `city`, `state`, `address_line`,
  `postal_code`, and other profile fields.
- **`products_snapshot`** — tracks `price`, `category`, `sub_category`,
  `brand`, `is_discontinued`.

Both use dbt's **`check` strategy** rather than `timestamp`: instead of
trusting a single `updated_at` column, dbt computes a hash across an
explicit `check_cols` list on every run and compares it to the last
captured version. Any difference — even one not reflected in
`updated_at` (e.g. a bypassed trigger, a manual fix) — triggers a new
version. This is more robust than timestamp-based snapshotting for a
production-style pipeline.

`stock_qty` is deliberately **excluded** from `products_snapshot`'s
`check_cols` — it changes on nearly every order/restock and is a
fast-moving operational metric, not a dimensional attribute worth
versioning; including it would create a new SCD version almost every
run, defeating the purpose.

The source system (`Batch-source-system/simulate_traffic.py`) was
extended with two new actions specifically to generate genuine SCD2
history: `update_customer_profile` (simulates a customer moving) and
`update_product_details` (simulates repricing) — both perform real
`UPDATE`s on existing dimension rows, distinct from the messy-data
injection applied at record creation time.

A derived model, **`silver_customer_profile_history`**, demonstrates
practical use of this history — counting how many times each customer's
profile has changed and flagging customers who have relocated. This is
a question genuinely unanswerable from the current-state
`silver_customers` table alone.

```sql
-- Example: what did customer 18's profile look like before their most
-- recent move, and when did it change?
SELECT customer_id, city, state, dbt_valid_from, dbt_valid_to
FROM retailflow.silver.customers_snapshot
WHERE customer_id = 18
ORDER BY dbt_valid_from;
```

Snapshots run as part of the orchestrated pipeline (`dbt snapshot`,
before `dbt run`, in the `dbt_transform` Job task), so history
accumulates automatically on every scheduled run.

### Gold layer — business aggregates
- **`gold_daily_sales_summary`** — daily order volume, gross/net revenue,
  status breakdown, average order value.
- **`gold_customer_ltv`** — lifetime revenue, order history, payment
  reliability, and recency per customer.
- **`gold_clickstream_funnel`** — session counts at each funnel stage
  (page_view → product_view → add_to_cart → checkout_start) with
  stage-to-stage conversion percentages.

All models are documented and tested (`unique`, `not_null`,
`accepted_values`) via `schema.yml`, with full source-to-Gold lineage
visualized through `dbt docs`.

---

## 6. Visualization

A **Databricks Dashboard** ("RetailFlow - Business Overview") is built
directly on the three Gold tables, with three tabs:
- **Sales Overview** — revenue trend, order status breakdown, AOV
- **Customer Insights** — top customers by LTV, gross-vs-net revenue,
  recency-vs-value scatter
- **Clickstream Funnel** — funnel stage counts, conversion rates,
  anonymous vs. identified session split

---

## 7. Key Design Decisions & Tradeoffs

| Decision | Rationale |
|---|---|
| Managed Identity over Service Principal for storage auth | No secrets to rotate/leak; Unity Catalog's recommended current pattern |
| Scheduled `availableNow` streaming over always-on | Matches actual data volume/latency needs; avoids 24/7 compute cost |
| Azure Postgres over local Docker | Genuinely reachable from cloud compute, matching a real production topology |
| Single-node Job Cluster (not autoscaling) | Data volumes don't warrant distributed compute; avoids Azure core quota limits |
| dbt for Silver/Gold, not raw PySpark | Version-controlled, testable, documented SQL transformations with lineage |
| Landing zone before Bronze | Raw audit trail; enables reprocessing without re-hitting live sources |
| `check` strategy over `timestamp` for SCD2 snapshots | Doesn't rely solely on `updated_at` being trustworthy; detects any actual attribute change via hashing |

---

## 8. Repository Structure

```
RetailFlow/
├── Batch-source-system/       -- Postgres schema, Faker seed/simulator scripts
├── Streaming-system/           -- Clickstream event producer (Event Hubs/Kafka)
├── databricks-notebooks/        -- Bronze ingestion notebooks (batch + streaming)
│   ├── 00_setup/
│   └── 01_bronze/
├── dbt/                          -- Silver + Gold transformation models
│   ├── models/silver/
│   ├── models/gold/
│   └── snapshots/                 -- SCD Type 2 history (customers, products)
├── docs/
│   └── architecture.md            -- this file
└── README.md
```
