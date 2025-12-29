# Soda Bundle — Databricks Asset Bundle

A small, cost-friendly setup to ingest **Soda Cloud Reporting API** data into **Unity Catalog** and keep a simple “latest per check” snapshot for monitoring.

---

## What’s included

- **Job**: `soda_monitoring_60m`
  - **Task 1** – `soda_extract.py`  
    Pulls the last N minutes of `check_results` from Soda and MERGEs into  
    `<catalog>.raw.check_results`.
  - **Task 2** – `last_check.py`  
    Writes today’s **latest per check_id** into  
    `<catalog>.managed.latest_checks`.

> The extractor also supports `coverage_checks` and `datasets` (RAW targets:  
> `<catalog>.raw.coverage_checks`, `<catalog>.raw.coverage_datasets`) if you add jobs for them.

---

## Repo layout

soda_bundle
├─ databricks.yml
├─ resources
│  └─ soda_monitoring.yml
├─ src
│  └─ soda_monitoring
│     ├─ soda_extract.py
│     └─ last_check.py
└─ README.md

- **databricks.yml** -> bundle config (targets, variables, includes)
- **soda_monitoring.yml** -> job definition (hourly window + aggregate)
- **soda_extract.py** -> ingestion from Soda Reporting API
- **last_check.py** -> latest-per-check aggregator

---

## Prerequisites

1. **Unity Catalog** catalogs exist (e.g., `soda_dev`, `soda_prod`) and your user has permissions on:
   - `<catalog>.raw`
   - `<catalog>.managed`
2. **Soda API secrets** in a Databricks secret scope (default: `soda-secrets`):
   - `prod_api_key_id`  → your Soda **X-API-KEY-ID**
   - `prod_api_key_secret` → your Soda **X-API-KEY-SECRET**
3. **Databricks CLI** installed and authenticated.

---

## Configure (quick)

Edit variables in `databricks.yml`:

- **Targets**  
  - `soda_catalog` → UC catalog to write to (e.g., `soda_dev`, `soda_prod`)  
  - `soda_env` → logical Soda env passed to scripts (`dev|stg|prod`)

- **API**  
  - `soda_secret_scope`, `soda_key_id_key`, `soda_key_secret_key`  
  - `soda_base_url` (default: `https://reporting.cloud.us.soda.io/v1`)

- **Window & paging (check_results)**  
  - `window_minutes` (default `60`)  
  - `page_size` (default `400`)  
  - `max_pages` (default `500`)

- **Cluster (cheap single-node)**  
  - `driver_node_type_id` (default `m5d.large`)  
  - `num_workers` = `0`  
  - `spark_conf.spark.master = local[*]` (set in job YAML)

Schedules are controlled by `resources/soda_monitoring.yml` via `pause_status`.

---

## Deploy & run

Validate & deploy to **dev**:

```bash
databricks bundle validate -t dev
databricks bundle deploy   -t dev
