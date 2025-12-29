# Mobile Master ID

This module contains identity stitching workflows for deriving stable `master_id`s using GraphFrames on user identifiers (`acct_id`, `adid`, `device_id`). It includes a WWESC mobile variant of the pipeline.

## Structure

* **master\_id/**

  * `nba2km_mobile_master_id.py`: GraphFrames-based identity resolution pipeline
  * `wwesc_mobile_master_id.py`: GraphFrames-based identity resolution pipeline for WWESC mobile
* **resources/**

  * `nba2km_master_id_config.yml`: Configuration for nba2km master id pipeline pipeline parameters
  * `wwesc_master_id_config.yml`: Configuration for wwesc master id pipeline pipeline parameters
  * Job config for `wwesc_master_id_weekly` (Databricks Jobs bundle)
* **databricks.yml**: Databricks bundle configuration

## Purpose

This module builds a unified identity map by linking identifiers across sources (e.g. Adjust installs and session summary data) using graph processing. It identifies connected components and assigns a stable `master_id` to each group of related identifiers. The WWESC pipeline applies the same approach to WWESC mobile data and includes a follow-up refresh of downstream Iceberg-backed tables.

## Deployment

```bash
databricks bundle deploy --target dev -p twok-dataeng
```

## Schedule

* **Frequency:** Weekly
* **Cron:** `quartz_cron_expression: "0 0 0 ? * MON"`  # Every Monday at 00:00 UTC
* **WWESC job:** `wwesc_master_id_weekly` — `quartz_cron_expression: "0 0 1 ? * MON"`  # Every Monday at 01:00 UTC