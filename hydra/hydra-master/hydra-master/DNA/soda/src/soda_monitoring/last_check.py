# Databricks notebook source
# Purpose:
#   - Read RAW.check_results (3-part name)
#   - For current UTC date, select the most recent scan per check_id
#   - Append a snapshot into MANAGED.latest_checks with strict column order
#   - Idempotent write using MERGE on (report_ts, check_id)
#
# Notes:
#   - A single report_ts (UTC) is stamped once per run so all rows share it.
#   - Table schema/order is enforced via AGG_ORDER.
#   - If the target table exists without `report_ts`, we add it (ALTER TABLE) or
#     evolve schema via a zero-row append; if it still doesn't exist, we fail fast.

import argparse
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W

# ---------------------------
# Param helpers
# ---------------------------

def _args_from_widgets_or_env():
    """Read JSON from dbutils.widgets.get('inputParam') or env var INPUT_PARAM."""
    try:
        spark = SparkSession.builder.getOrCreate()
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
        try:
            raw = dbutils.widgets.get("inputParam")
        except Exception:
            raw = os.getenv("INPUT_PARAM", "")
        if raw:
            return json.loads(raw)
    except Exception:
        pass
    return None

def get_args():
    d = _args_from_widgets_or_env()
    if d is not None:
        def pick(k, dv=None):
            return d.get(k, dv)
        ns = argparse.Namespace(
            environment          = pick("environment"),
            raw_checks_table     = pick("raw_checks_table"),
            managed_latest_table = pick("managed_latest_table"),
        )
        if ns.environment not in ("dev", "stg", "prod"):
            raise SystemExit("inputParam.environment must be one of: dev, stg, prod")
        return ns

    p = argparse.ArgumentParser(description="Aggregate today's latest-per-check into MANAGED.latest_checks.")
    p.add_argument("--environment", required=True, choices=["dev", "stg", "prod"])
    p.add_argument("--raw_checks_table", default=None)
    p.add_argument("--managed_latest_table", default=None)
    return p.parse_args()

def split_3part(name: str):
    parts = name.split(".")
    if len(parts) != 3:
        raise ValueError(f"Expected 3-part table name, got: {name}")
    return parts[0], parts[1], parts[2]

# ---------------------------
# Schema / column order
# ---------------------------

# Strict final column order for aggregated table (report_ts first)
AGG_ORDER = [
    "report_ts",
    "soda_link",
    "check_id",
    "business_unit",
    "title_name",
    "scan_time",
    "level",
    "table_name",
    "check_type",
    "column_check",
    "metric_value",
    "data_layer",
    "pii",
    "data_owner",
    "priority",
    "scan_date_utc",
    "is_fail",
    "is_warn",
]

# Minimal type hints for columns we may need to synthesize
TYPE_HINTS = {
    "report_ts": "timestamp",
    "scan_time": "timestamp",
    "metric_value": "double",
    "is_fail": "boolean",
    "is_warn": "boolean",
}

def ensure_cols(df, cols):
    for c in cols:
        if c not in df.columns:
            dt = TYPE_HINTS.get(c, "string")
            df = df.withColumn(c, F.lit(None).cast(dt))
    return df

# ---------------------------
# Main
# ---------------------------

def main():
    args = get_args()
    spark = SparkSession.builder.getOrCreate()

    # Ensure session time zone is UTC so current_date()/current_timestamp() are UTC
    spark.sql("SET spark.sql.session.timeZone=UTC")

    default_catalog = f"soda_{args.environment}"
    raw_tbl = args.raw_checks_table or f"{default_catalog}.raw.check_results"
    mgd_tbl = args.managed_latest_table or f"{default_catalog}.managed.latest_checks"

    mgd_cat, mgd_schema, _ = split_3part(mgd_tbl)

    # DDL in MANAGED catalog
    spark.sql(f"USE CATALOG `{mgd_cat}`")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{mgd_schema}`")

    # Read RAW
    df = spark.table(raw_tbl)

    # Ensure helper columns
    if "scan_date_utc" not in df.columns:
        df = df.withColumn("scan_date_utc", F.to_date("scan_time"))

    # Filter to "today" in UTC
    today_df = df.filter(F.col("scan_date_utc") == F.current_date())
    if today_df.rdd.isEmpty():
        print("No rows for today in RAW; nothing to aggregate.")
        return

    if not {"check_id", "scan_time"}.issubset(set(today_df.columns)):
        raise RuntimeError("RAW check_results missing required columns: check_id, scan_time")

    # Latest row per check_id for today
    w = W.partitionBy("check_id").orderBy(F.col("scan_time").desc())
    latest = (today_df
              .withColumn("rn", F.row_number().over(w))
              .filter(F.col("rn") == 1)
              .drop("rn"))

    # Stamp a single report timestamp for this run (UTC) so all rows share it
    run_ts = spark.sql("SELECT current_timestamp() AS ts").first()["ts"]
    latest = latest.withColumn("report_ts", F.lit(run_ts).cast("timestamp"))

    # Ensure strict output layout & only those columns
    latest = ensure_cols(latest, AGG_ORDER)
    latest_out = latest.select(*AGG_ORDER)

    # Just in case, drop duplicates within the batch on (report_ts, check_id)
    latest_out = latest_out.dropDuplicates(["report_ts", "check_id"])

    # Try to drop any extra columns on existing table to keep a clean schema
    if spark.catalog.tableExists(mgd_tbl):
        try:
            existing_cols = set(spark.table(mgd_tbl).columns)
            to_drop = [c for c in existing_cols if c not in set(AGG_ORDER)]
            for c in to_drop:
                spark.sql(f"ALTER TABLE {mgd_tbl} DROP COLUMN IF EXISTS {c}")
        except Exception:
            pass

    # Create table if not exists (strict schema). If it already exists it may be old (without report_ts).
    if not spark.catalog.tableExists(mgd_tbl):
        (latest_out.limit(0)
            .write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(mgd_tbl))
    else:
        # Ensure the MERGE key column report_ts exists on the target (handles pre-existing tables).
        try:
            spark.sql(f"ALTER TABLE {mgd_tbl} ADD COLUMN IF NOT EXISTS report_ts TIMESTAMP")
        except Exception:
            pass

    # ------- Robust schema check & evolution for report_ts (This is important so we can make sure we have a sequence of reports)-------
    # Re-read target columns after potential create/alter
    target_cols = spark.table(mgd_tbl).columns

    # If report_ts is still missing, try schema evolution via a zero-row append
    if "report_ts" not in target_cols:
        print(f"[schema] 'report_ts' missing on target {mgd_tbl}; attempting schema evolution write.")
        try:
            (latest_out.limit(0)
                .write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(mgd_tbl))
        except Exception as e:
            print(f"[schema] Zero-row schema evolution failed: {e}")

        # Re-check and fail fast if still missing
        target_cols = spark.table(mgd_tbl).columns
        if "report_ts" not in target_cols:
            raise RuntimeError(
                f"[schema] Target {mgd_tbl} is still missing 'report_ts'. "
                "Possible causes: table is a VIEW or non-Delta, or you lack ALTER privileges."
            )

    # Stage and MERGE (idempotent on (report_ts, check_id))
    latest_out.createOrReplaceTempView("staged_latest")

    # Align insert columns with target (preserve AGG_ORDER where possible)
    insert_cols = [c for c in AGG_ORDER if c in target_cols and c in latest_out.columns]
    cols_sql = ", ".join(f"`{c}`" for c in insert_cols)
    vals_sql = ", ".join(f"s.`{c}`" for c in insert_cols)

    spark.sql(f"""
      MERGE INTO {mgd_tbl} t
      USING staged_latest s
        ON t.report_ts = s.report_ts AND t.check_id = s.check_id
      WHEN NOT MATCHED THEN
        INSERT ({cols_sql}) VALUES ({vals_sql})
    """)

    num_rows = latest_out.count()
    print(f"[OK] Upserted snapshot of {num_rows} latest checks into {mgd_tbl} for report_ts={run_ts}.")

if __name__ == "__main__":
    main()
