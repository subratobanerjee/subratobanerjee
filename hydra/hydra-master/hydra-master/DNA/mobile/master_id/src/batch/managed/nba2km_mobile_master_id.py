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

# Initialize Spark session and set checkpoint directory
spark = SparkSession.builder.appName("master-id").getOrCreate()
spark.sparkContext.setCheckpointDir("/tmp/graph_checkpoint")


# COMMAND ----------

# === 2. Extract ===
def extract():
    """
    Reads source tables and filters only relevant rows.
    """
    df = (
        spark.read.table("sf_databricks_migration.sf_dbx.nba2km_user_session_summary_combinations")
        .select("acct_id", "adid", "device_id", "first_seen_ts_utc")
        .where(expr("acct_id IS NOT NULL OR adid IS NOT NULL OR device_id IS NOT NULL"))
    )

    adjust_df = (
        spark.read.table("sf_databricks_migration.sf_dbx.installs_clean")
        .where(expr("game_id = 2"))
        .select("adid", "gaid", "idfa", "install_ts_utc")
    )

    return df, adjust_df


# === 3. Transform ===
def transform(df, adjust_df):
    """
    Builds edges, constructs vertices with earliest timestamps,
    runs GraphFrame connectedComponents, and assigns master IDs.
    """

    # Cast columns early using selectExpr
    df = df.selectExpr(
        "CAST(acct_id AS STRING) AS acct_id_str",
        "CAST(adid AS STRING) AS adid_str",
        "CAST(device_id AS STRING) AS device_id_str",
        "first_seen_ts_utc"
    )

    adjust_df = adjust_df.selectExpr(
        "CAST(adid AS STRING) AS adid_str",
        "CAST(gaid AS STRING) AS gaid",
        "CAST(idfa AS STRING) AS idfa",
        "CAST(COALESCE(gaid, idfa) AS STRING) AS device_id_str",
        "install_ts_utc"
    )

    # Pre-filter conditions
    df_valid_adid = df.where(expr("length(adid_str) = 32 AND acct_id_str IS NOT NULL"))
    df_valid_device = df.where(expr("device_id_str IS NOT NULL AND device_id_str != 'Unknown' AND device_id_str != '0000-0000' AND acct_id_str IS NOT NULL"))
    df_adid_device = df.where(expr("length(adid_str) = 32 AND device_id_str IS NOT NULL AND device_id_str != 'Unknown' AND device_id_str != '0000-0000'"))
    adjust_valid = adjust_df.where(expr("device_id_str IS NOT NULL AND adid_str IS NOT NULL"))

    edges_df = (
        df_valid_adid.selectExpr("acct_id_str AS src", "adid_str AS dst")
        .union(df_valid_adid.selectExpr("adid_str AS src", "acct_id_str AS dst"))
        .union(df_valid_device.selectExpr("acct_id_str AS src", "device_id_str AS dst"))
        .union(df_valid_device.selectExpr("device_id_str AS src", "acct_id_str AS dst"))
        .union(df_adid_device.selectExpr("adid_str AS src", "device_id_str AS dst"))
        .union(df_adid_device.selectExpr("device_id_str AS src", "adid_str AS dst"))
        .union(adjust_valid.selectExpr("adid_str AS src", "device_id_str AS dst"))
        .union(adjust_valid.selectExpr("device_id_str AS src", "adid_str AS dst"))
    ).where(expr("src IS NOT NULL AND dst IS NOT NULL AND src != dst"))  # Only basic validation

    # Build vertices and compute the earliest timestamp per ID
    vertices_setup = (
        df.where(expr("acct_id_str IS NOT NULL")).selectExpr("acct_id_str AS id", "'acct_id' AS id_type", "first_seen_ts_utc AS ts")
        .union(df.where(expr("length(adid_str) = 32")).selectExpr("adid_str AS id", "'adid' AS id_type", "first_seen_ts_utc AS ts"))
        .union(df.where(expr("device_id_str IS NOT NULL AND device_id_str != 'Unknown' AND device_id_str != '0000-0000'")).selectExpr("device_id_str AS id", "'device_id' AS id_type", "first_seen_ts_utc AS ts"))
        .union(adjust_df.where(expr("adid_str IS NOT NULL")).selectExpr("adid_str AS id", "'adid' AS id_type", "install_ts_utc AS ts"))
        .union(adjust_df.where(expr("device_id_str IS NOT NULL")).selectExpr("device_id_str AS id", "'device_id' AS id_type", "install_ts_utc AS ts"))
        .where(expr("id IS NOT NULL"))
    )

    # Create vertices with the earliest timestamp (first_seen)
    vertices_df = (
        vertices_setup
        .groupBy("id", "id_type")
        .agg(expr("min(ts) AS first_seen"))
        .dropDuplicates()
    )

    # Run GraphFrames connectedComponents
    graph = GraphFrame(vertices_df.select("id"), edges_df)

    # Calculate degrees using GraphFrames built-in method
    degree_counts = graph.degrees
    # # Add degrees to vertices
    vertices_with_degrees = vertices_df.join(degree_counts, on="id", how="left")

    # Run connected components
    components = graph.connectedComponents().checkpoint()

    # Map each component to a stable master ID (using the lexicographically smallest ID)
    stable_ids = (
        components
        .groupBy("component")
        .agg(expr("min(id) AS master_id"))
    )

    # Create the final identity map with all required fields
    identity_map = (
        components
        .join(stable_ids, on="component", how="left")
        .join(vertices_with_degrees, on="id", how="left")
        .select("id_type", "id", "master_id", "first_seen", "degree")
        .dropDuplicates()
    )

    return identity_map


# === 4. Load ===
def load(identity_map):
    """
    Writes the final identity map to Delta table with all required fields.
    """
    output_table = f"{database}{environment}.managed.master_id_map"

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
        .saveAsTable(output_table)

    record_count = identity_map.count()
    print(f"{record_count} records written into {output_table}")

    # Show schema
    print("\nOutput schema:")
    identity_map.printSchema()


# === 5. Run Pipeline ===
def run_batch():
    df, adjust_df = extract()
    identity_map = transform(df, adjust_df)
    load(identity_map)


# COMMAND ----------

# Run the full ETL
run_batch()