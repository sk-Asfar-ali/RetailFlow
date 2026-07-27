# RetailFlow dbt Project

Transforms Bronze Delta tables (landed by the Databricks notebooks in
`databricks-notebooks/`) into Silver (cleaned/conformed) and Gold
(business aggregate) tables, using dbt running directly against Databricks.

## Setup

1. Install dbt with the Databricks adapter:
   ```bash
   pip install dbt-databricks
   ```

2. Copy `profiles_TEMPLATE.yml` to your local dbt profiles directory:
   - **Windows**: `C:\Users\<you>\.dbt\profiles.yml`
   - **Mac/Linux**: `~/.dbt/profiles.yml`

   Fill in your actual `host`, `http_path` (from your SQL Warehouse's
   Connection Details tab), and a Databricks Personal Access Token.

   **Never commit your real `profiles.yml` to Git** -- it contains your
   access token. It lives outside this repo entirely (in your home
   directory's `.dbt/` folder), which is the standard convention.

3. Test the connection:
   ```bash
   cd dbt
   dbt debug
   ```

4. Run all models:
   ```bash
   dbt run
   ```

5. Run tests (data quality checks defined in `schema.yml` files):
   ```bash
   dbt test
   ```

6. Generate and view documentation:
   ```bash
   dbt docs generate
   dbt docs serve
   ```

## Project structure

```
dbt/
├── dbt_project.yml
├── profiles_TEMPLATE.yml    (template only -- real profiles.yml lives outside the repo)
├── models/
│   ├── silver/              -- cleaned, conformed, deduplicated tables
│   └── gold/                -- business-level aggregates for reporting
├── seeds/                   -- static reference/lookup data, if needed
├── macros/                  -- reusable Jinja SQL snippets
└── tests/                   -- custom data tests beyond schema.yml built-ins
```

## Sources

Bronze tables are declared as dbt **sources** (see `models/silver/sources.yml`),
pointing at the `retailflow.bronze.*` tables created by the ingestion
notebooks -- dbt treats these as read-only inputs to build Silver from.
