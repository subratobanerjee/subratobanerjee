# Databricks notebook source
# Purpose:
#   - Ingest Soda Reporting API endpoints into Unity Catalog RAW Delta tables:
#       * check_results      -> soda_{env}.raw.check_results         (MERGE on result_id)
#       * coverage_checks    -> soda_{env}.raw.coverage_checks       (MERGE on (check_id, dw_insert_ts))
#       * datasets           -> soda_{env}.raw.coverage_datasets     (MERGE on (dataset_id, dw_insert_ts))
#   - Time selection modes for check_results: window | range | backfill
#   - Endpoint-aware flattening and idempotent MERGE
#   - Secrets via dbutils.secrets, with env var fallback
#
# Notes:
#   * Provide optional table override via table_name.
#   * For check_results only:
#       - Drop dataset_name
#       - Enforce specific leading column order (RAW_PRIMARY_ORDER)
#       - Explicit MERGE insert list (no INSERT *)
#       - Post-MERGE de-dup in TARGET on (result_id) keeping latest scan_time
#   * For ALL endpoints:
#       - Drop owner_email, owner_first_name, owner_last_name (legacy) [none emitted here]
#   * For SNAPSHOT endpoints (datasets, coverage_checks):
#       - Add dw_insert_ts (single run UTC timestamp) and MERGE on (id, dw_insert_ts)
#   * All timestamps treated as UTC.

import argparse
import json
import os
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List
from urllib.parse import urljoin

import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# ---------------------------
# Arg / param parsing
# ---------------------------

def _args_from_widgets_or_env():
    """Try to read JSON from dbutils.widgets.get('inputParam').
       Fallback to env var INPUT_PARAM. Return dict or None."""
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
    # 1) Try notebook-style inputParam (JSON)
    d = _args_from_widgets_or_env()
    if d is not None:
        def pick(k, dv=None):
            v = d.get(k, dv)
            return v
        ns = argparse.Namespace(
            environment           = pick("environment"),
            base_url              = (pick("base_url") or os.getenv("SODA_BASE_URL", "https://reporting.cloud.us.soda.io/v1")).rstrip("/"),
            endpoint              = pick("endpoint"),
            table_name            = pick("table_name"),
            mode                  = pick("mode", "window"),
            window_minutes        = int(pick("window_minutes", 15)) if pick("window_minutes") is not None else 15,
            from_datetime         = pick("from_datetime"),
            to_datetime           = pick("to_datetime"),
            dataset_ids           = pick("dataset_ids", ""),
            check_ids             = pick("check_ids", ""),
            page_size             = int(pick("page_size", 400)) if pick("page_size") is not None else 400,
            max_pages             = int(pick("max_pages", 2000)) if pick("max_pages") is not None else 2000,
            secret_scope          = pick("secret_scope", os.getenv("SODA_SECRET_SCOPE", "")),
            key_id_key            = pick("key_id_key", os.getenv("SODA_KEY_ID_KEY", "prod_api_key_id")),
            key_secret_key        = pick("key_secret_key", os.getenv("SODA_KEY_SECRET_KEY", "prod_api_key_secret")),
            diagnose              = bool(pick("diagnose", False)),
        )
        # Minimal validation like argparse would do:
        if ns.environment not in ("dev", "stg", "prod"):
            raise SystemExit("inputParam.environment must be one of: dev, stg, prod")
        if ns.endpoint not in ("check_results", "coverage_checks", "datasets"):
            raise SystemExit("inputParam.endpoint must be one of: check_results, coverage_checks, datasets")
        return ns

    # 2) Fallback to CLI flags for local testing
    p = argparse.ArgumentParser(description="Ingest Soda Reporting API into Delta RAW tables (endpoint-aware).")

    # Infra/env
    p.add_argument("--environment", required=True, choices=["dev", "stg", "prod"])
    p.add_argument("--base-url", dest="base_url",
                   default=os.getenv("SODA_BASE_URL", "https://reporting.cloud.us.soda.io/v1").rstrip("/"))

    # Endpoint & routing
    p.add_argument("--endpoint", required=True, choices=["check_results", "coverage_checks", "datasets"])
    p.add_argument("--table-name", dest="table_name", default=None)

    # Time selection (check_results only)
    p.add_argument("--mode", choices=["window", "range", "backfill"], default="window")
    p.add_argument("--window-minutes", dest="window_minutes", type=int, default=15)
    p.add_argument("--from-datetime", dest="from_datetime")
    p.add_argument("--to-datetime", dest="to_datetime")

    # Filters/pagination
    p.add_argument("--dataset-ids", dest="dataset_ids", default=os.getenv("DATASET_IDS", ""))
    p.add_argument("--check-ids", dest="check_ids", default=os.getenv("CHECK_IDS", ""))
    p.add_argument("--page-size", dest="page_size", type=int, default=400)
    p.add_argument("--max-pages", dest="max_pages", type=int, default=2000)

    # Secrets (dbutils.secrets) + env fallback
    p.add_argument("--secret-scope", dest="secret_scope", default=os.getenv("SODA_SECRET_SCOPE", ""))
    p.add_argument("--key-id-key", dest="key_id_key", default=os.getenv("SODA_KEY_ID_KEY", "prod_api_key_id"))
    p.add_argument("--key-secret-key", dest="key_secret_key", default=os.getenv("SODA_KEY_SECRET_KEY", "prod_api_key_secret"))

    # Diagnostics
    p.add_argument("--diagnose", action="store_true")
    return p.parse_args()

# ---------------------------
# Utilities
# ---------------------------

def get_dbutils(spark):
    try:
        from pyspark.dbutils import DBUtils
        return DBUtils(spark)
    except Exception:
        return None

def get_auth_headers(args, spark) -> Dict[str, str]:
    """Prefer env vars; if absent and a secret scope is provided, use dbutils.secrets."""
    key_id = (os.getenv("SODA_API_KEY_ID") or "").strip()
    key_secret = (os.getenv("SODA_API_KEY_SECRET") or "").strip()

    if (not key_id or not key_secret) and args.secret_scope:
        dbutils = get_dbutils(spark)
        if dbutils:
            try:
                if not key_id:
                    key_id = dbutils.secrets.get(scope=args.secret_scope, key=args.key_id_key).strip()
                if not key_secret:
                    key_secret = dbutils.secrets.get(scope=args.secret_scope, key=args.key_secret_key).strip()
            except Exception:
                pass

    if not key_id or not key_secret:
        raise RuntimeError("Soda API credentials not found. Provide env vars or dbutils.secrets scope/keys.")

    return {
        "X-API-KEY-ID": key_id,
        "X-API-KEY-SECRET": key_secret,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

RPT_ENDPOINTS = {
    "check_results":   "/quality/check_results",
    "datasets":        "/coverage/datasets",
    "coverage_checks": "/coverage/checks",
}

def first_present(d: Dict[str, Any], keys: List[str]) -> Any:
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None

def build_body(args) -> Dict[str, Any]:
    body = {"page": 1, "size": args.page_size}

    if args.endpoint == "check_results":
        if args.mode == "window":
            now_utc = datetime.now(timezone.utc)
            start = now_utc - timedelta(minutes=int(args.window_minutes))
            body["from_datetime"] = start.strftime("%Y-%m-%dT%H:%M:%SZ")
            body["to_datetime"]   = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        elif args.mode == "range":
            if not args.from_datetime or not args.to_datetime:
                raise RuntimeError("--mode=range requires --from-datetime and --to-datetime")
            body["from_datetime"] = args.from_datetime
            body["to_datetime"]   = args.to_datetime
        elif args.mode == "backfill":
            start = args.from_datetime or "1970-01-01T00:00:00Z"
            now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            body["from_datetime"] = start
            body["to_datetime"]   = now_utc

    # Optional filters
    if args.dataset_ids:
        body["dataset_ids"] = [s.strip() for s in str(args.dataset_ids).split(",") if s.strip()]
    if args.check_ids and args.endpoint == "check_results":
        body["check_ids"] = [s.strip() for s in str(args.check_ids).split(",") if s.strip()]

    return body

def fetch_reporting_paginated(base_url: str, path: str, body: Dict[str, Any],
                              headers: Dict[str, str], max_pages: int) -> List[Dict[str, Any]]:
    all_items: List[Dict[str, Any]] = []
    page = int(body.get("page") or 1)
    size = int(body.get("size") or 400)

    while True:
        if max_pages and page > int(max_pages):
            break
        body["page"] = page
        body["size"] = size
        url = urljoin(base_url if base_url.endswith("/") else base_url + "/", path.lstrip("/"))
        resp = requests.post(url, headers=headers, json=body, timeout=60)
        if resp.status_code >= 400:
            raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:800]}")

        payload = resp.json()
        data = payload.get("data") if isinstance(payload, dict) else (payload if isinstance(payload, list) else [])
        if not data:
            break

        all_items.extend(data)
        if len(data) < size:
            break
        page += 1
    return all_items

def diagnose_once(base_url, path, body, headers):
    import textwrap
    url = urljoin(base_url if base_url.endswith("/") else base_url + "/", path.lstrip("/"))
    probe = dict(body); probe["page"] = 1; probe["size"] = 1
    resp = requests.post(url, headers=headers, json=probe, timeout=60)
    print(f"[diag] HTTP {resp.status_code} {resp.reason} | CT={resp.headers.get('Content-Type')}")
    txt = resp.text
    snippet = (txt[:1200] + "...") if (len(txt) > 1200) else txt
    print("[diag] body preview:\n" + textwrap.indent(snippet, "  "))
    try:
        payload = resp.json()
        if isinstance(payload, dict):
            print(f"[diag] keys={list(payload.keys())} data_len={len(payload.get('data') or [])} page={payload.get('page')} size={payload.get('size')} total={payload.get('total')}")
    except Exception:
        pass

# ---------------------------
# Endpoint-aware flatten
# ---------------------------

def to_scalar(v):
    if isinstance(v, list):
        return v[0] if len(v) == 1 else "|".join(map(str, v))
    return v

def flatten_items(items: List[Dict[str, Any]], endpoint: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for it in items:
        attrs = it.get("attributes") or {}

        row = {
            "organization_id": it.get("organization_id"),
            "dataset_id":      it.get("dataset_id"),
            # dataset_name intentionally omitted for check_results schemas
            "check_id":        first_present(it, ["check_id", "id"]),
            "check_name":      first_present(it, ["check_name", "name"]),
            "metric_value":    first_present(it, ["metric_value", "value", "observedValue"]),
            "level":           first_present(it, ["level", "status", "evaluationStatus"]),
            "number_of_failed_checks": it.get("number_of_failed_checks"),
            # attributes
            "business_unit": to_scalar(attrs.get("business_unit")),
            "data_layer":    to_scalar(attrs.get("data_layer")),
            "data_owner":    to_scalar(attrs.get("data_owner")),
            "priority":      to_scalar(attrs.get("priority")),
            "pii":           to_scalar(attrs.get("pii")),
            "title_name":    to_scalar(first_present(attrs, ["title_name", "titleName", "title"])),
        }

        if endpoint == "check_results":
            row["result_id"] = first_present(it, ["result_id", "id"])
            row["scan_time"] = first_present(it, ["scan_time", "created_at", "timestamp"])

        if endpoint == "datasets":
            row["dataset_id"] = row.get("dataset_id") or first_present(it, ["id", "datasetId"])
            row["last_scan_time"] = it.get("last_scan_time")

        rows.append(row)
    return rows

@F.udf(returnType=T.StructType([
    T.StructField("table_name", T.StringType()),
    T.StructField("check_type", T.StringType()),
    T.StructField("column_check", T.StringType()),
]))
def parse_check_name(name: str):
    if not name:
        return (None, None, None)
    parts = [p.strip() for p in str(name).split("|")]
    if not parts:
        return (None, None, None)
    known = {"row_count": "row_count", "duplicate_check": "duplicate_check", "duplicate": "duplicate_check", "null_check": "null_check"}

    first = parts[0].lower()
    if first in known:
        ctype = known[first]
        if ctype in ("row_count", "duplicate_check"):
            table = parts[1] if len(parts) >= 2 else None
            return (table, ctype, None)
        if ctype == "null_check":
            table = parts[1] if len(parts) >= 2 else None
            col   = parts[2] if len(parts) >= 3 else None
            return (table, ctype, col)

    table = parts[0] if len(parts) >= 1 else None
    second = parts[1].lower() if len(parts) >= 2 else ""
    mapped = known.get(second)
    if mapped == "row_count":
        return (table, "row_count", None)
    if mapped in ("duplicate_check", "null_check"):
        col = parts[2] if len(parts) >= 3 else None
        return (table, mapped, col)
    return (None, None, None)

# ---------------------------
# Explicit schema to avoid NullType inference
# ---------------------------

def schema_for_endpoint(endpoint: str) -> T.StructType:
    common = [
        T.StructField("organization_id", T.StringType()),
        T.StructField("dataset_id", T.StringType()),
        T.StructField("check_id", T.StringType()),
        T.StructField("check_name", T.StringType()),
        T.StructField("metric_value", T.StringType()),
        T.StructField("level", T.StringType()),
        T.StructField("number_of_failed_checks", T.StringType()),
        T.StructField("business_unit", T.StringType()),
        T.StructField("data_layer", T.StringType()),
        T.StructField("data_owner", T.StringType()),
        T.StructField("priority", T.StringType()),
        T.StructField("pii", T.StringType()),
        T.StructField("title_name", T.StringType()),
    ]
    if endpoint == "check_results":
        return T.StructType(common + [
            T.StructField("result_id", T.StringType()),
            T.StructField("scan_time", T.StringType()),
        ])
    if endpoint == "datasets":
        return T.StructType(common + [
            T.StructField("last_scan_time", T.StringType()),
        ])
    if endpoint == "coverage_checks":
        return T.StructType(common)
    raise ValueError(f"Unsupported endpoint: {endpoint}")

def project_to_schema(rows: List[Dict[str, Any]], schema: T.StructType) -> List[Dict[str, Any]]:
    cols = [f.name for f in schema.fields]
    return [{c: r.get(c) for c in cols} for r in rows]

# ---------------------------
# Endpoint routing (default tables + MERGE keys/conditions)
# ---------------------------

def default_table_for_endpoint(env: str, endpoint: str) -> str:
    cat = f"soda_{env}"
    if endpoint == "check_results":
        return f"{cat}.raw.check_results"
    if endpoint == "coverage_checks":
        return f"{cat}.raw.coverage_checks"
    if endpoint == "datasets":
        return f"{cat}.raw.coverage_datasets"
    raise ValueError(f"Unsupported endpoint: {endpoint}")

def merge_condition_for_endpoint(endpoint: str) -> str:
    """Return the SQL ON condition for MERGE (supports composite keys)."""
    if endpoint == "check_results":
        return "t.result_id = s.result_id"
    if endpoint == "coverage_checks":
        return "t.check_id = s.check_id AND t.dw_insert_ts = s.dw_insert_ts"
    if endpoint == "datasets":
        return "t.dataset_id = s.dataset_id AND t.dw_insert_ts = s.dw_insert_ts"
    raise ValueError(f"Unsupported endpoint: {endpoint}")

def split_3part(full: str):
    parts = full.split(".")
    if len(parts) != 3:
        raise ValueError(f"Expected 3-part name catalog.schema.table, got: {full}")
    return parts[0], parts[1], parts[2]

# Desired primary order for the RAW table (check_results)
RAW_PRIMARY_ORDER = [
    "soda_link",
    "check_id",
    "business_unit",
    "scan_time",
    "level",
    "data_layer",
    "table_name",
    "check_type",
    "column_check",
    "metric_value",
    "title_name",
    "pii",
    "data_owner",
    "priority",
    "scan_date_utc",
    "is_fail",
    "is_warn",
]

def order_with_priority(df, wanted_first: List[str]):
    first = [c for c in wanted_first if c in df.columns]
    rest = [c for c in df.columns if c not in first]
    return df.select(*first, *rest)

# ---------------------------
# Main
# ---------------------------

def main():
    args = get_args()
    spark = SparkSession.builder.getOrCreate()

    headers = get_auth_headers(args, spark)
    path = RPT_ENDPOINTS[args.endpoint]
    body = build_body(args)

    fp = hashlib.sha256(headers["X-API-KEY-ID"].encode()).hexdigest()[:8]
    print(
        "[info] fetch config | "
        f"endpoint={args.endpoint} base={args.base_url} "
        f"from={body.get('from_datetime')} to={body.get('to_datetime')} "
        f"dataset_ids={body.get('dataset_ids')} check_ids={body.get('check_ids')} "
        f"size={args.page_size} max_pages={args.max_pages} key_fp={fp}"
    )

    if args.diagnose:
        diagnose_once(args.base_url, path, body, headers)
        return

    items = fetch_reporting_paginated(args.base_url, path, body, headers, max_pages=args.max_pages)
    if not items:
        print("No records returned from Soda API.")
        return

    # Flatten -> explicit schema (avoid NullType)
    rows = flatten_items(items, args.endpoint)
    schema = schema_for_endpoint(args.endpoint)
    df = spark.createDataFrame(project_to_schema(rows, schema), schema=schema)

    # Endpoint-specific enrichments
    if args.endpoint == "check_results":
        if "scan_time" in df.columns:
            df = df.withColumn("scan_time", F.to_timestamp("scan_time"))
            df = df.withColumn("scan_date_utc", F.to_date("scan_time"))
        if "metric_value" in df.columns:
            df = df.withColumn("metric_value", F.col("metric_value").cast("double"))
        if "level" in df.columns:
            lvl = F.lower(F.coalesce(F.col("level").cast("string"), F.lit("")))
            df = df.withColumn("is_fail", lvl.isin("fail", "failed", "critical", "error")) \
                   .withColumn("is_warn", lvl.isin("warn", "warning"))
        parsed = parse_check_name(F.col("check_name"))
        df = df.withColumn("table_name", parsed.getField("table_name")) \
               .withColumn("check_type", parsed.getField("check_type")) \
               .withColumn("column_check", parsed.getField("column_check"))
        df = df.withColumn(
            "soda_link",
            F.when(
                F.col("organization_id").isNotNull() &
                F.col("check_id").isNotNull() &
                F.col("result_id").isNotNull(),
                F.concat(
                    F.lit("https://cloud.us.soda.io/o/"), F.col("organization_id"),
                    F.lit("/checks/"), F.col("check_id"),
                    F.lit("/metric-result?testResultId="), F.col("result_id")
                )
            )
        )

    elif args.endpoint == "datasets":
        if "last_scan_time" in df.columns:
            df = df.withColumn("last_scan_time", F.to_timestamp("last_scan_time"))

    # --- Snapshot timestamp for datasets & coverage_checks ---
    run_ts = None
    if args.endpoint in ("datasets", "coverage_checks"):
        # Stamp a single run timestamp across all rows (UTC)
        run_ts = spark.sql("SELECT current_timestamp() AS ts").collect()[0]["ts"]
        df = df.withColumn("dw_insert_ts", F.lit(run_ts).cast("timestamp"))

    # numeric tidy-up
    if "number_of_failed_checks" in df.columns:
        df = df.withColumn("number_of_failed_checks", F.col("number_of_failed_checks").cast("long"))

    # For check_results only: drop dataset_name if present in DF and enforce ordering
    if args.endpoint == "check_results":
        if "dataset_name" in df.columns:
            df = df.drop("dataset_name")
        df = order_with_priority(df, RAW_PRIMARY_ORDER)

    # Resolve target table and MERGE condition
    target_table = args.table_name or default_table_for_endpoint(args.environment, args.endpoint)
    merge_condition = merge_condition_for_endpoint(args.endpoint)

    # Ensure schema exists
    cat, sch, tbl = split_3part(target_table)
    spark.sql(f"USE CATALOG `{cat}`")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{sch}`")
    full_name = f"{cat}.{sch}.{tbl}"

    # If table exists, proactively maintain schema (drop legacy, add snapshot col)
    if spark.catalog.tableExists(full_name):
        try:
            # Legacy drops (defensive)
            for c in ["owner_email", "owner_first_name", "owner_last_name"]:
                spark.sql(f"ALTER TABLE {full_name} DROP COLUMN IF EXISTS {c}")
            if args.endpoint == "check_results":
                spark.sql(f"ALTER TABLE {full_name} DROP COLUMN IF EXISTS dataset_name")
            # Ensure snapshot column exists for snapshot endpoints
            if args.endpoint in ("datasets", "coverage_checks"):
                spark.sql(f"ALTER TABLE {full_name} ADD COLUMN IF NOT EXISTS dw_insert_ts TIMESTAMP")
        except Exception:
            pass

    # Create table if not exists (zero-write preserves column order)
    if not spark.catalog.tableExists(full_name):
        (df.limit(0)
           .write
           .format("delta")
           .mode("append")
           .option("mergeSchema", "true")
           .saveAsTable(full_name))

    # Filter out null base keys (never insert those)
    if args.endpoint == "check_results":
        df = df.filter(F.col("result_id").isNotNull())
    elif args.endpoint == "datasets":
        df = df.filter(F.col("dataset_id").isNotNull())
    elif args.endpoint == "coverage_checks":
        df = df.filter(F.col("check_id").isNotNull())

    # Stage the batch as a temp view for MERGE
    df.createOrReplaceTempView("staged_soda_ingest")

    # Build explicit INSERT column list (avoid INSERT *)
    target_cols = spark.table(full_name).columns
    # Guard old/legacy columns just in case
    if args.endpoint == "check_results":
        target_cols = [c for c in target_cols if c != "dataset_name"]
    # Only insert columns present on BOTH sides
    insert_cols = [c for c in target_cols if c in df.columns]
    cols_sql = ", ".join([f"`{c}`" for c in insert_cols])
    vals_sql = ", ".join([f"s.`{c}`" for c in insert_cols])

    # Insert-only MERGE (idempotent for rolling windows / snapshot keys)
    spark.sql(f"""
        MERGE INTO {full_name} t
        USING staged_soda_ingest s
          ON {merge_condition}
        WHEN NOT MATCHED THEN INSERT ({cols_sql}) VALUES ({vals_sql})
    """)

    # --------- Post-MERGE target de-dup (check_results only) ----------
    # Keep the most recent scan per result_id based on scan_time.
    if args.endpoint == "check_results":
        dup_rows_before = spark.sql(f"""
            SELECT COALESCE(SUM(cnt-1),0) AS dup_rows
            FROM (SELECT result_id, COUNT(*) AS cnt FROM {full_name} GROUP BY result_id)
        """).first()[0]

        if dup_rows_before:
            print(f"[dedupe] Found {dup_rows_before} duplicate rows by result_id in {full_name}; cleaning up...")

            # Use correlated DELETE ... WHERE EXISTS (more widely accepted than DELETE USING subquery)
            spark.sql(f"""
                DELETE FROM {full_name} t
                WHERE EXISTS (
                  SELECT 1
                  FROM (
                    SELECT result_id, scan_time,
                           ROW_NUMBER() OVER (
                             PARTITION BY result_id
                             ORDER BY scan_time DESC, result_id
                           ) AS rn
                    FROM {full_name}
                  ) d
                  WHERE d.result_id = t.result_id
                    AND d.scan_time  = t.scan_time
                    AND d.rn > 1
                )
            """)

            dup_rows_after = spark.sql(f"""
                SELECT COALESCE(SUM(cnt-1),0) AS dup_rows
                FROM (SELECT result_id, COUNT(*) AS cnt FROM {full_name} GROUP BY result_id)
            """).first()[0]

            print(f"[dedupe] Remaining duplicate rows after cleanup: {dup_rows_after}")

    if args.endpoint in ("datasets", "coverage_checks"):
        print(f"[OK] Upserted snapshot for {full_name} at dw_insert_ts={run_ts}.")
    else:
        print(f"[OK] Upserted into {full_name} with rolling key.")

if __name__ == "__main__":
    main()
