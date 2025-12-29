# Databricks notebook source
# COMMAND ----------

# MAGIC %pip install graphframes>=0.6.0

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment', set_environment())
database = 'mobile'
print(f"environment = {environment}")
# Databricks notebook source

# COMMAND ----------

# === 1. Imports and Spark Setup ===
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from graphframes import GraphFrame

spark = SparkSession.builder.appName("wwe-sup-master-id").getOrCreate()
spark.sparkContext.setCheckpointDir("/tmp/sup_graph_checkpoint")

# Sources
WWESC_COMBOS = "sf_databricks_migration.sf_dbx.mixpanel_user_session_summary_combinations"  # acct_id, adid, device_id, first_seen_ts_utc
INSTALLS = "sf_databricks_migration.sf_dbx.wwesc_installs_clean"  # adid, gaid, idfa, install_ts_utc (filter game_id=1)
GAME_ID = 1

# output
OUTPUT_TABLE = f"{database}{environment}.managed.wwesc_master_id_map"

# === 1) Extract ===
def extract():
    combos = (
        spark.read.table(WWESC_COMBOS)
        .select("acct_id", "adid", "device_id", "first_seen_ts_utc")
    )
    installs = (
        spark.read.table(INSTALLS)
        .where(expr(f"game_id = {GAME_ID}"))
        .select("adid", "gaid", "idfa", "install_ts_utc")
    )
    return combos, installs


INVALID_DEV_LIST = ("unknown", "none", "undefined", "", "0000-0000", "00000000-0000-0000-0000-000000000000")


def norm(col_expr: str) -> str:
    return f"lower(trim(CAST({col_expr} AS STRING)))"


def valid_adid_pred(col_name: str) -> str:
    # 32 hex after stripping non-hex
    return f"REGEXP_LIKE(REGEXP_REPLACE({col_name}, '[^0-9a-f]', ''), '^[0-9a-f]{{32}}$')"


def valid_device_pred(col_name: str) -> str:
    invalid = ",".join([f"'{v}'" for v in INVALID_DEV_LIST])
    return f"{col_name} IS NOT NULL AND {col_name} NOT IN ({invalid})"


# === 2) Build vertices ===
def build_vertices_df(combos, installs):
    c = (
        combos.selectExpr(
            f"{norm('acct_id')}   AS acct_id",
            f"{norm('adid')}      AS adid",
            f"{norm('device_id')} AS device_id",
            "first_seen_ts_utc"
        )
    )
    i = (
        installs.selectExpr(
            f"{norm('adid')} AS adid",
            f"{norm('gaid')} AS gaid",
            f"{norm('idfa')} AS idfa",
            "install_ts_utc"
        )
    )

    vertices = (
        # acct_id
        c.where("acct_id IS NOT NULL")
        .selectExpr("acct_id AS id", "'acct_id' AS id_type", "first_seen_ts_utc AS ts")

        .unionAll(
            # adid from combos (32-hex)
            c.where(f"adid IS NOT NULL AND {valid_adid_pred('adid')}")
            .selectExpr("adid AS id", "'adid' AS id_type", "first_seen_ts_utc AS ts")
        )

        .unionAll(
            # device_id from combos (exclude junk)
            c.where(valid_device_pred("device_id"))
            .selectExpr("device_id AS id", "'device_id' AS id_type", "first_seen_ts_utc AS ts")
        )

        .unionAll(
            # adid from installs (32-hex)
            i.where(f"adid IS NOT NULL AND {valid_adid_pred('adid')}")
            .selectExpr("adid AS id", "'adid' AS id_type", "install_ts_utc AS ts")
        )

        .unionAll(
            # idfa/gaid → device_id (exclude junk)
            i.selectExpr("COALESCE(gaid, idfa) AS device_id", "install_ts_utc AS ts")
            .where(valid_device_pred("device_id"))
            .selectExpr("device_id AS id", "'device_id' AS id_type", "ts")
        )
        .where("id IS NOT NULL")
    )

    vertices_final = (
        vertices.groupBy("id", "id_type")
        .agg(expr("min(ts) AS first_seen"))
    )
    return vertices_final


# === 3) Build edges (bidirectional) ===
def build_edges_df(combos, installs):
    c = combos.selectExpr(
        f"{norm('acct_id')}   AS acct_id",
        f"{norm('adid')}      AS adid",
        f"{norm('device_id')} AS device_id"
    )
    i = installs.selectExpr(
        f"{norm('adid')} AS adid",
        f"{norm('gaid')} AS gaid",
        f"{norm('idfa')} AS idfa"
    ).selectExpr("adid", "COALESCE(gaid, idfa) AS device_id")

    # acct_id <-> adid
    e_acct_adid = (
        c.where(f"acct_id IS NOT NULL AND adid IS NOT NULL AND {valid_adid_pred('adid')}")
        .selectExpr("acct_id AS n1", "adid AS n2", "'acct_id' AS n1_name", "'adid' AS n2_name")
        .unionAll(
            c.where(f"acct_id IS NOT NULL AND adid IS NOT NULL AND {valid_adid_pred('adid')}")
            .selectExpr("adid AS n1", "acct_id AS n2", "'adid' AS n1_name", "'acct_id' AS n2_name")
        )
    )

    # acct_id <-> device_id
    e_acct_dev = (
        c.where(f"acct_id IS NOT NULL AND {valid_device_pred('device_id')}")
        .selectExpr("acct_id AS n1", "device_id AS n2", "'acct_id' AS n1_name", "'device_id' AS n2_name")
        .unionAll(
            c.where(f"acct_id IS NOT NULL AND {valid_device_pred('device_id')}")
            .selectExpr("device_id AS n1", "acct_id AS n2", "'device_id' AS n1_name", "'acct_id' AS n2_name")
        )
    )

    # adid <-> device_id (combos)
    e_adid_dev_c = (
        c.where(f"adid IS NOT NULL AND {valid_adid_pred('adid')} AND {valid_device_pred('device_id')}")
        .selectExpr("adid AS n1", "device_id AS n2", "'adid' AS n1_name", "'device_id' AS n2_name")
        .unionAll(
            c.where(f"adid IS NOT NULL AND {valid_adid_pred('adid')} AND {valid_device_pred('device_id')}")
            .selectExpr("device_id AS n1", "adid AS n2", "'device_id' AS n1_name", "'adid' AS n2_name")
        )
    )

    # adid <-> device_id (installs)
    e_adid_dev_i = (
        i.where(f"adid IS NOT NULL AND {valid_adid_pred('adid')} AND {valid_device_pred('device_id')}")
        .selectExpr("adid AS n1", "device_id AS n2", "'adid' AS n1_name", "'device_id' AS n2_name")
        .unionAll(
            i.where(f"adid IS NOT NULL AND {valid_adid_pred('adid')} AND {valid_device_pred('device_id')}")
            .selectExpr("device_id AS n1", "adid AS n2", "'device_id' AS n1_name", "'adid' AS n2_name")
        )
    )

    edges = (
        e_acct_adid.unionAll(e_acct_dev)
        .unionAll(e_adid_dev_c)
        .unionAll(e_adid_dev_i)
        .where("n1 IS NOT NULL AND n2 IS NOT NULL AND n1 <> n2")
        .dropDuplicates(["n1", "n2"])
        .selectExpr("n1 AS src", "n2 AS dst")  # GraphFrame needs src/dst
    )
    return edges


# === 4) Components + master_id preference ===
def build_identity_map(vertices_df, edges_df):
    g = GraphFrame(vertices_df.select("id"), edges_df)
    components = g.connectedComponents().checkpoint()  # id, component
    degrees = g.degrees

    # Map each component to a stable master ID (using the lexicographically smallest ID)
    stable_ids = (
        components
        .groupBy("component")
        .agg(expr("min(id) AS master_id"))
    )

    # Create the final identity map
    identity_map = (
        components
        .join(stable_ids, on="component", how="left")
        .join(vertices_df, on="id", how="left")
        .join(degrees, "id", "left")
        .select("id_type", "id", "master_id", "first_seen", "degree")
        .dropDuplicates()
    )
    return identity_map


# === 5) Load ===
def load(identity_map):
    """
    Writes the final identity map to Delta table with all required fields.
    """
    identity_map.write.mode("overwrite") \
        .option("overwriteSchema", "true") \
        .option("delta.columnMapping.mode", "name") \
        .option("delta.enableIcebergCompatV2", "true") \
        .option("delta.universalFormat.enabledFormats", "iceberg") \
        .option("delta.enableDeletionVectors", "false") \
        .option("delta.autoOptimize.autoCompact", "true") \
        .option("delta.autoOptimize.optimizeWrite", "true") \
        .option("delta.feature.allowColumnDefaults", "supported") \
        .option("delta.feature.appendOnly", "supported") \
        .option("delta.feature.columnMapping", "supported") \
        .option("delta.feature.icebergCompatV2", "supported") \
        .option("delta.feature.invariants", "supported") \
        .saveAsTable(OUTPUT_TABLE)

    record_count = identity_map.count()
    print(f"{record_count} records written into {OUTPUT_TABLE}")

    # Show schema
    print("\nOutput schema:")
    identity_map.printSchema()


# === 5. Run Pipeline ===
def run_batch():
    combos, installs = extract()
    vertices_df = build_vertices_df(combos, installs)
    edges_df = build_edges_df(combos, installs)
    identity_map = build_identity_map(vertices_df, edges_df)
    load(identity_map)


# COMMAND ----------
run_batch()
