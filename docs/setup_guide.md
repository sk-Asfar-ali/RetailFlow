# RetailFlow — Complete Infrastructure Setup Guide

This document walks through every Azure resource created for this
project, and every connection point between them, step by step, low
level. Follow in order — later sections depend on resources created in
earlier ones.

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Resource Group](#2-resource-group)
3. [Azure Database for PostgreSQL (Flexible Server)](#3-azure-database-for-postgresql-flexible-server)
4. [Azure Event Hubs](#4-azure-event-hubs)
5. [Azure Data Lake Storage Gen2](#5-azure-data-lake-storage-gen2)
6. [Azure Databricks Workspace](#6-azure-databricks-workspace)
7. [Databricks Compute (Clusters)](#7-databricks-compute-clusters)
8. [Unity Catalog: Connecting Databricks to ADLS Gen2](#8-unity-catalog-connecting-databricks-to-adls-gen2)
9. [Azure Key Vault + Databricks Secret Scope](#9-azure-key-vault--databricks-secret-scope)
10. [Connecting Databricks to GitHub (Repos)](#10-connecting-databricks-to-github-repos)
11. [Connecting dbt to Databricks](#11-connecting-dbt-to-databricks)
12. [Databricks Jobs (Orchestration)](#12-databricks-jobs-orchestration)
13. [Databricks Dashboards](#13-databricks-dashboards)
14. [Full Connection Map (Summary)](#14-full-connection-map-summary)

---

## 1. Prerequisites

- An active Azure subscription
- A GitHub account
- Python 3.10–3.12 installed locally (dbt does not yet support 3.13+/3.14 — use a virtual environment with a compatible version if your system default is newer)
- `psql` or pgAdmin installed locally (for connecting to Postgres directly)

---

## 2. Resource Group

Everything in this project lives inside one resource group, so it's easy to track cost and delete cleanly if needed.

1. Azure Portal → search **"Resource groups"** → **+ Create**
2. **Subscription**: your subscription
3. **Resource group name**: `rg-data-engg-project`
4. **Region**: pick one your subscription allows (this project used **Central India**; some resources like the Access Connector may need to match your Databricks workspace region)
5. **Review + Create**

---

## 3. Azure Database for PostgreSQL (Flexible Server)

This is the **batch/OLTP source system**.

### 3.1 Create the server

1. Azure Portal → search **"Azure Database for PostgreSQL"** → select the **Microsoft**-published, **Azure Service** tile (not third-party marketplace listings) → **Create** → **Flexible server**
2. **Basics**:
   - Resource group: `rg-data-engg-project`
   - Server name: `retailflow-postgres` (must be globally unique)
   - Region: match your resource group
   - PostgreSQL version: **16**
   - Workload type: **Development**
3. **Compute + storage** → **Configure server**:
   - Cluster options: **Server** (not Elastic cluster)
   - Compute tier: **Burstable**
   - Compute size: **Standard_B1ms (1 vCore, 2 GiB)**
   - Storage size: **32 GiB**
   - Storage autogrow: **off**
   - Zonal resiliency: **Disabled**
   - Save
4. **Authentication**:
   - Authentication method: **PostgreSQL authentication only**
   - Admin login: `source_admin`
   - Password: set a strong password (you'll need this later)
5. **Review + Create**

### 3.2 Configure networking

1. Once deployed, go to the resource → left sidebar → **Settings → Networking**
2. Check **"Allow public access from any Azure service within Azure to this server"** — this is what lets Databricks reach it
3. Click **"+ Add current client IP address"** — lets you connect from your own machine
4. **Save**

> Your IP may change over time (dynamic ISP addressing). If you later get connection timeouts from your local machine, return here and re-add your current IP.

### 3.3 Create the database and schema

1. Connect via `psql` or pgAdmin:
   ```
   Host: retailflow-postgres.postgres.database.azure.com
   Port: 5432
   User: source_admin
   Password: <your password>
   SSL mode: Require
   ```
2. Create the database:
   ```sql
   CREATE DATABASE ecommerce_source;
   ```
3. Connect to `ecommerce_source` specifically, then run the full schema from `Batch-source-system/sql/01_schema.sql` — this creates the `retail` schema with `customers`, `products`, `orders`, `order_items`, `payments` tables, indexes, and `updated_at` triggers.

### 3.4 Point local scripts at it

In `Batch-source-system/.env`:
```
PGHOST=retailflow-postgres.postgres.database.azure.com
PGPORT=5432
PGDATABASE=ecommerce_source
PGUSER=source_admin
PGPASSWORD=<your password>
PGSSLMODE=require
```

Then seed and simulate:
```bash
python seed.py --customers 500 --products 200
python simulate_traffic.py --interval 2
```

---

## 4. Azure Event Hubs

This is the **streaming source system** (clickstream events), consumed by Databricks via its Kafka-compatible endpoint.

### 4.1 Create the namespace

1. Azure Portal → search **"Event Hubs"** → **+ Create**
2. **Basics**:
   - Resource group: `rg-data-engg-project`
   - Namespace name: `clickstream-events` (globally unique)
   - Region: match your resource group
   - Pricing tier: **Standard** — **required**; Basic tier does *not* support the Kafka protocol, which Databricks Structured Streaming needs
3. **Review + Create**

### 4.2 Create the Event Hub (entity) inside the namespace

1. Go into the namespace → **Entities → Event Hubs** → **+ Event Hub**
2. Name: `clickstream-events-hub`
3. Partition count: **3**
4. Cleanup policy: **Delete**
5. Retention time: **24 hours** (max on Standard's default tier config used here)
6. **Create**

### 4.3 Get the connection string

1. Back at the **namespace** level → **Settings → Shared access policies**
2. Click **`RootManageSharedAccessKey`**
3. Copy the **Primary connection string** — looks like:
   ```
   Endpoint=sb://clickstream-events.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=xxxxxxxxxxxx
   ```

### 4.4 Point the local producer at it

In `Streaming-system/.env`:
```
STREAM_BACKEND=eventhub
EVENTHUB_CONNECTION_STR=Endpoint=sb://clickstream-events.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=<your key>
EVENTHUB_NAME=clickstream-events-hub
```

Run:
```bash
pip install azure-eventhub
python produce_clickstream.py --rate 5
```

Verify events are arriving: Event Hub resource → **Overview** tab → watch the **Incoming Messages** graph tick up.

---

## 5. Azure Data Lake Storage Gen2

This is where **all** Bronze/Silver/Gold Delta tables and raw Landing files physically live — a dedicated storage account you own, not Databricks' internal managed storage.

### 5.1 Create the storage account

1. Azure Portal → search **"Storage accounts"** → **+ Create**
2. Resource group: `rg-data-engg-project`
3. Storage account name: `datalakealli` (globally unique, lowercase, no hyphens)
4. Region: match your resource group
5. Performance: **Standard**
6. Redundancy: **LRS** (cheapest, fine for a dev project)
7. **Advanced** tab → check **"Enable hierarchical namespace"** — this is what makes it ADLS Gen2 rather than plain Blob Storage; required for Unity Catalog
8. **Review + Create**

### 5.2 Create the container

1. Go into the storage account → left sidebar → **Data storage → Containers**
2. **+ Add container**
3. Name: `retailflow-lake`
4. Public access level: **Private**
5. **Create**

At this point the container is empty — Unity Catalog will populate its internal folder structure once linked (Section 8).

---

## 6. Azure Databricks Workspace

1. Azure Portal → search **"Azure Databricks"** → **+ Create**
2. Resource group: `rg-data-engg-project`
3. Workspace name: e.g. `db-retailflow-project`
4. Region: match your other resources
5. Pricing tier: **Premium** (required for Unity Catalog features)
6. **Review + Create**
7. Once deployed, click **Launch Workspace** to open it in browser

> Creating the workspace **auto-creates** an Access Connector for Databricks (used later for Unity Catalog storage auth) and a default Unity Catalog metastore, depending on your subscription/region setup — check under **Catalog → gear icon → Credentials/External Locations** to see what already exists before creating new ones.

---

## 7. Databricks Compute (Clusters)

### 7.1 Create a dev/interactive cluster

1. Databricks workspace → left sidebar → **Compute** → **Create compute**
2. **Compute name**: `dev-retailflow-cluster`
3. **Policy**: Unrestricted
4. **Access mode**: Single user / Dedicated
5. **Databricks runtime**: latest LTS, non-ML (e.g. `17.3 LTS`)
6. **Photon acceleration**: on
7. **Node type**: smallest available (e.g. `Standard_DC4as_v5`)
8. Check **"Single node"** — no separate workers needed at this data scale
9. **Terminate after**: 20–30 minutes of inactivity
10. **Create compute**

> **Azure subscription core quota**: if you hit `AZURE_QUOTA_EXCEEDED_EXCEPTION` when creating additional (e.g. Job) clusters, it means your subscription's regional vCore limit is already used up by this cluster. Either request a quota increase from Azure, or reuse this same cluster for Job tasks instead of provisioning a separate Job Cluster (this project does the latter, for simplicity and zero extra cost).

---

## 8. Unity Catalog: Connecting Databricks to ADLS Gen2

This is the most involved integration — it establishes a secure, keyless trust chain from Databricks compute to your storage account.

### 8.1 Confirm/create the Access Connector for Databricks

1. Azure Portal → search **"Access Connector for Databricks"**
2. If one already exists (often auto-created with the workspace, e.g. named `unity-catalog-access-connector`), you can reuse it. Otherwise, **+ Create**:
   - Resource group: `rg-data-engg-project`
   - Name: `ac-retailflow-databricks`
   - Region: **must match** your Databricks workspace and storage account region
3. **Review + Create**

### 8.2 Grant the Access Connector's Managed Identity access to your storage account

1. Go to the Access Connector resource → **Overview** (or JSON view) → copy its **Resource ID**
2. Go to your `datalakealli` storage account → **Access control (IAM)** → **+ Add role assignment**
3. Role: **Storage Blob Data Contributor**
4. Assign access to: **Managed identity**
5. Select members → Managed identity type: **Access Connector for Databricks** → select your connector
6. **Review + assign**

### 8.3 Register the Storage Credential in Unity Catalog

1. In Databricks: **Catalog** → gear icon (or **⋮** menu) → **Credentials** → **Create credential**
2. Credential Type: **Azure Managed Identity**
3. **Access connector ID**: paste the Resource ID from step 8.1/8.2 — looks like:
   ```
   /subscriptions/<sub-id>/resourceGroups/rg-data-engg-project/providers/Microsoft.Databricks/accessConnectors/<connector-name>
   ```
4. Name: `retailflow-managed-identity`
5. **Create**

### 8.4 Create the External Location

1. Databricks: **Catalog** → gear icon → **External locations** → **Create location**
2. Name: `retailflow-lake-location`
3. URL:
   ```
   abfss://retailflow-lake@datalakealli.dfs.core.windows.net/
   ```
4. Storage credential: `retailflow-managed-identity`
5. **Test connection** — should show green checks for Read/List/Write/Delete/Path Exists
   - If "File Events" checks fail, that's fine to ignore — click **"Force create the location"**. File Events are an optional performance optimization (Event Grid-based change notification), not required.
6. **Create**

### 8.5 Create the Catalog, Schemas, and Volume (via notebook)

Run this once, from a Databricks notebook attached to your cluster:

```python
STORAGE_ACCOUNT = "datalakealli"
CONTAINER = "retailflow-lake"
BASE_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"

spark.sql(f"""
CREATE CATALOG IF NOT EXISTS retailflow
MANAGED LOCATION '{BASE_PATH}/catalog-root'
""")

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS retailflow.landing
MANAGED LOCATION '{BASE_PATH}/landing'
""")

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS retailflow.bronze
MANAGED LOCATION '{BASE_PATH}/bronze'
""")

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS retailflow.silver
MANAGED LOCATION '{BASE_PATH}/silver'
""")

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS retailflow.gold
MANAGED LOCATION '{BASE_PATH}/gold'
""")

spark.sql("""
CREATE VOLUME IF NOT EXISTS retailflow.landing.raw_files
""")
```

> **Idempotency note**: re-running this on a fresh cluster can hit a `LOCATION_OVERLAP` error if the catalog/schemas already exist from a prior run. Wrap each `CREATE` in a try/except that tolerates `LOCATION_OVERLAP`/"already exists" errors — see `databricks-notebooks/01_bronze/01_bronze_ingestion_postgres.py`'s `safe_create()` helper for the pattern used in this project.

### 8.6 Verify

Browse to your `datalakealli` storage account → **Containers** → `retailflow-lake` → you should see Unity Catalog's internal folder structure (GUID-named folders under `tables/`) once tables are actually created and written to.

---

## 9. Azure Key Vault + Databricks Secret Scope

Used to store all credentials (Postgres, Event Hubs) securely — no secrets are ever hardcoded in notebooks or committed to Git.

### 9.1 Create the Key Vault

1. Azure Portal → search **"Key vaults"** → **+ Create**
2. Resource group: `rg-data-engg-project`
3. Key vault name: `retailflowkeyvault`
4. Region: match your other resources
5. Pricing tier: Standard
6. **Review + Create**

### 9.2 Add secrets

Go into the Key Vault → **Objects → Secrets** → **+ Generate/Import** for each:

| Name | Value |
|---|---|
| `pg-host` | `retailflow-postgres.postgres.database.azure.com` |
| `pg-port` | `5432` |
| `pg-database` | `ecommerce_source` |
| `pg-user` | `source_admin` |
| `pg-password` | *(your Postgres password)* |
| `eventhub-connection-string` | *(full connection string from Section 4.3)* |
| `eventhub-name` | `clickstream-events-hub` |
| `client-id` | *(Service Principal Application ID, if using SP-based auth elsewhere)* |
| `client-secret` | *(Service Principal secret)* |
| `tenant-id` | *(Azure AD Tenant ID)* |

### 9.3 Link the Key Vault to Databricks as a secret scope

Databricks Secrets backed by Key Vault can **only** be created via a special URL-based flow (not the CLI, for Key Vault-backed scopes specifically):

1. Go to:
   ```
   https://<your-databricks-workspace-url>#secrets/createScope
   ```
   (e.g. `https://adb-7405609492314235.15.azuredatabricks.net#secrets/createScope`)
2. **Scope Name**: `retailflow_scope`
3. **Manage Principal**: `All Users` (or restrict as needed)
4. **DNS Name**: your Key Vault's **Vault URI** (found on the Key Vault's Overview page, e.g. `https://retailflowkeyvault.vault.azure.net/`)
5. **Resource ID**: your Key Vault's full Azure Resource ID (found via the Key Vault's **Properties** page, or JSON view)
6. **Create**

### 9.4 Use secrets in notebooks

```python
PG_HOST = dbutils.secrets.get(scope="retailflow_scope", key="pg-host")
PG_PASSWORD = dbutils.secrets.get(scope="retailflow_scope", key="pg-password")
```

Values retrieved this way are automatically redacted (`[REDACTED]`) if ever printed to notebook output — protecting against accidental leaks via screenshots or logs.

---

## 10. Connecting Databricks to GitHub (Repos)

### 10.1 Generate a GitHub Personal Access Token

1. GitHub → profile icon → **Settings**
2. Scroll to bottom → **Developer settings**
3. **Personal access tokens → Tokens (classic)** → **Generate new token (classic)**
4. Note: `databricks-integration`
5. Expiration: 90 days (or your preference)
6. Scopes: check **`repo`**
7. **Generate token** → copy immediately (shown once)

### 10.2 Link the token in Databricks

1. Databricks → profile icon (top right) → **Settings**
2. Left sidebar → **Linked accounts** (or **User → Git integration**, UI varies slightly)
3. Git provider: **GitHub**
4. Git provider username: your GitHub username
5. Personal access token: paste the token from 10.1
6. **Save**

### 10.3 Connect the repository

1. Databricks → **Workspace → Repos** (or the newer **"Create Git Folder"** flow accessible from Home)
2. **Add Repo** / **Create Git Folder**
3. Git repository URL: `https://github.com/<your-username>/RetailFlow.git`
4. Git provider: GitHub (should auto-detect)
5. **Create**

This clones the repo into your workspace, typically under:
```
/Workspace/Repos/<your-email>/RetailFlow/
```
or the simplified Git-folder path, depending on your Databricks version.

### 10.4 Commit/push from Databricks

Any Git-linked folder has a **Git panel** (branch name + status icon near the folder breadcrumb). Editing notebooks inside this folder and using **Commit & Push** syncs changes back to GitHub directly from the Databricks UI — no local clone required for notebook-only changes.

---

## 11. Connecting dbt to Databricks

dbt runs from your **local machine** (or CI), connecting to Databricks via a **SQL Warehouse** — separate from the all-purpose cluster used for notebooks.

### 11.1 Install dbt

> **Python version note**: dbt (as of the versions used in this project) does not support Python 3.13+/3.14. If your system's default Python is newer, create a virtual environment with an older, supported version (this project used 3.12):
> ```bash
> py -3.12 -m venv venv
> venv\Scripts\activate      # Windows
> ```

```bash
pip install dbt-databricks
```

### 11.2 Get SQL Warehouse connection details

1. Databricks → left sidebar → **SQL Warehouses**
2. Click your warehouse (e.g. "Serverless Starter Warehouse")
3. **Connection details** tab → copy:
   - **Server hostname** (e.g. `adb-7405609492314235.15.azuredatabricks.net`)
   - **HTTP path** (e.g. `/sql/1.0/warehouses/dab71c35b64aad8a`)

### 11.3 Generate a Databricks Personal Access Token for dbt

Same process as 10.1 but within Databricks: profile icon → **Settings → Developer → Access tokens → Generate new token**.

### 11.4 Create `~/.dbt/profiles.yml`

This file lives **outside** the project repo (in your home directory), and should **never** be committed to Git since it contains your access token.

- Windows: `C:\Users\<you>\.dbt\profiles.yml`
- Mac/Linux: `~/.dbt/profiles.yml`

```yaml
retailflow:
  target: dev
  outputs:
    dev:
      type: databricks
      catalog: retailflow
      schema: silver
      host: "adb-7405609492314235.15.azuredatabricks.net"
      http_path: "/sql/1.0/warehouses/<your-warehouse-id>"
      token: "<your-databricks-personal-access-token>"
      threads: 4
```

> If you already use dbt for other projects, this file can hold multiple named profiles side by side — just don't overwrite an existing one; append `retailflow:` as an additional top-level key.

### 11.5 Test the connection

```bash
cd dbt
dbt debug
```
Should end with **"All checks passed!"**

### 11.6 Run the project

```bash
dbt snapshot   # SCD2 history (customers, products)
dbt run        # build all Silver + Gold models
dbt test       # run data quality tests
dbt docs generate && dbt docs serve   # lineage graph + docs site
```

---

## 12. Databricks Jobs (Orchestration)

1. Databricks → left sidebar → **Jobs & Pipelines** → **Create** → **Job**
2. Rename the job: `retailflow-bronze-ingestion`
3. **Task 1** — `setup_catalog`:
   - Type: Notebook
   - Path: `databricks-notebooks/00_setup/00_setup_catalog_adls`
   - Cluster: `dev-retailflow-cluster` (reused to avoid quota issues)
4. **Task 2** — `bronze_postgres`:
   - Type: Notebook
   - Path: `databricks-notebooks/01_bronze/01_bronze_ingestion_postgres`
   - Depends on: `setup_catalog`
5. **Task 3** — `bronze_clickstream`:
   - Type: Notebook
   - Path: `databricks-notebooks/01_bronze/02_bronze_ingestion_clickstream`
   - Depends on: `setup_catalog`
   (Tasks 2 and 3 both depend only on Task 1, so they run in parallel.)
6. **Task 4** — `dbt_transform`:
   - Type: **dbt**
   - Source: Workspace
   - Project directory: your synced `dbt/` folder
   - dbt commands:
     ```
     dbt deps
     dbt seed
     dbt snapshot
     dbt run
     dbt test
     ```
   - SQL warehouse: your SQL Warehouse
   - Warehouse catalog: `retailflow`
   - Warehouse schema: `silver`
   - Depends on: **both** `bronze_postgres` and `bronze_clickstream`
7. **Schedule**: **Add trigger** → Scheduled → Interval → every 15 minutes
8. **Create** the job, then **Run now** to validate end to end

---

## 13. Databricks Dashboards

1. Databricks → left sidebar → **Dashboards** → **Create Dashboard**
2. Name: `RetailFlow - Business Overview`
3. Add visualizations by writing SQL against the Gold tables:
   ```sql
   SELECT * FROM retailflow.gold.gold_daily_sales_summary ORDER BY order_day;
   SELECT * FROM retailflow.gold.gold_customer_ltv ORDER BY lifetime_revenue DESC;
   SELECT * FROM retailflow.gold.gold_clickstream_funnel;
   ```
4. Organize into tabs (Sales Overview / Customer Insights / Clickstream Funnel), add filters, KPI cards, charts as needed. Databricks' AI assistant ("Genie") can refine chart types/labels via natural-language prompts directly in the dashboard editor.

---

## 14. Full Connection Map (Summary)

```
GitHub (RetailFlow repo)
   │  (Personal Access Token)
   ▼
Databricks Repos ── syncs notebooks + dbt project

Azure Postgres ──────────┐   (JDBC, via Key Vault secrets)
                           ├──► Databricks Cluster ──► Unity Catalog ──► ADLS Gen2 (retailflow-lake)
Azure Event Hubs ─────────┘   (Kafka protocol, via Key Vault secrets)         │
                                                                                ▼
                                                                    Access Connector (Managed Identity)
                                                                       + IAM role on storage account

Local machine (dbt) ──(SQL Warehouse, PAT token)──► Databricks ──► Unity Catalog tables

Azure Key Vault ──(linked secret scope)──► Databricks Secrets ──► used in all notebooks
```

Every credential in this system flows through either **Azure Key Vault** (Postgres, Event Hubs) or a **Managed Identity** (storage access) — no plaintext secrets in code, notebooks, or Git history at any point.
