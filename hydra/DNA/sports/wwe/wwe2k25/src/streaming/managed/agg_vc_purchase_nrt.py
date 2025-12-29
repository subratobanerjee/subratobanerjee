# Databricks notebook source
# MAGIC %run ../../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../../utils/ddl/table_functions

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import (
    expr, col, coalesce, when, countDistinct, greatest, current_timestamp, to_timestamp, window
)
from pyspark.sql import functions as F
from pyspark.sql.functions import expr, col, to_timestamp, current_timestamp, greatest, lit


# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment', set_environment())
database = 'wwe2k25'
title = "'WWE 2K25'"

# COMMAND ----------

def create_agg_vc_purchased_nrt(spark, database, environment):
    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.agg_vc_purchased_nrt (
        received_on STRING,
        platform STRING,
        service STRING,
        COUNTRY_CODE STRING,
        VC_SKU STRING,
        VC_PACKS_PURCHASED INT,
        TITLE STRING,
        dw_insert_ts TIMESTAMP,
        dw_update_ts TIMESTAMP,
        merge_key STRING
    )
    """

    properties = {
        "delta.feature.allowColumnDefaults": "supported"
    }

    create_table(spark, sql, properties)
    create_agg_vc_purchased_nrt_view(spark, database, environment)

    return f"dbfs:/tmp/{database}/managed/streaming/run{environment}/agg_vc_purchased_nrt"

# COMMAND ----------

def create_agg_vc_purchased_nrt_view(spark, database, environment):
    """
    Create the agg_vc_purchased_nrt view in the specified environment.
    """
    sql = f"""
    CREATE OR REPLACE VIEW {database}{environment}.managed_view.agg_vc_purchased_nrt AS (
        SELECT
            received_on,
            platform,
            service,
            country_code,
            VC_SKU,
            VC_PACKS_PURCHASED,
            TITLE,
            dw_insert_ts,
            dw_update_ts
        FROM {database}{environment}.managed.agg_vc_purchased_nrt
    )
    """
    spark.sql(sql)

# COMMAND ----------

def read_fact_player_transaction(spark, environment):
    """
    Reads fact player transaction data as a streaming DataFrame.
    """
    return (
        spark
        .readStream
        .table(f"wwe2k25{environment}.intermediate.fact_player_transaction")
        .selectExpr(
            "received_on",
            "platform",
            "service",
            "country_code",
            "transaction_id",
            "currency_amount as vc_sku",
            "action_type",
            "currency_type"
        )
        .where(
            (expr("action_type = 'Purchase' AND currency_type = 'VC' AND currency_amount <> '0'"))
        )
    )


# COMMAND ----------

def read_platform_territory_df(spark, environment):
    print("reading platform territory")

    return (
        spark
        .read
        .table(f"dataanalytics{environment}.standard_metrics.platform_territory_10min_ts")
        .select(
            "timestamp_10min_slice",
            "platform",
            "service",
            "country_code"
        ).where(expr("timestamp_10min_slice between current_timestamp() - interval '12 hour' and current_timestamp() + interval '10 minute' and platform in ('PS4', 'PS5', 'XBSX', 'XB1', 'Windows')"))
        .distinct()
    )

# COMMAND ----------

def extract(environment, database):
    df = read_fact_player_transaction(spark, environment)
    return df

# COMMAND ----------




def transform(df):
    """
    Transforms the raw transactions DataFrame into an aggregated form.
    """

    # Convert 'received_on' to timestamp
    df = df.withColumn("received_on_ts", to_timestamp(col("received_on")))

    # Apply the time slicing logic using the given example (rounding to nearest 10 minutes)
    df = df.withColumn(
        "received_on_10min_slice",
        expr("from_unixtime(round(floor(unix_timestamp(received_on_ts) / 600) * 600))").cast("timestamp")
    )

    # Load platform_service_10min_ts table
    plat_df = read_platform_territory_df(spark, environment)

    # Define time range for filtering (last 12 hours)
    time_slice_start = expr("current_timestamp() - interval 12 hours")
    time_slice_end = current_timestamp()

    # Perform the join between the dataframes based on the platform, service, and time window
    transformed_df = (
        plat_df.alias("s")
        .join(
            df.alias("p"),
            (col("s.timestamp_10min_slice") == col("p.received_on_10min_slice")) &
            (col("s.platform") == col("p.platform")) &
            (col("s.country_code") == col("p.country_code")) &
            (col("s.service") == col("p.service")),
            "left"
        )
        # .filter(
        #     (col("s.timestamp_10min_slice") >= time_slice_start) &
        #     (col("s.timestamp_10min_slice") <= time_slice_end)
        # )
        .groupby("s.platform", "s.service", "s.country_code", "vc_sku", "s.timestamp_10min_slice")
        .agg(
            # expr("IFNULL(SUM(p.currency_amount), 0)").alias("vc_sku"),
            expr("IFNULL(COUNT( p.transaction_id), 0)").alias("vc_packs_purchased")
        )
        .withColumn("received_on", greatest(to_timestamp(lit("2025-01-01 00:00:00")), col("s.timestamp_10min_slice")))
        .withColumn("title", expr("'WWE 2K25'"))
    )

    return transformed_df

# COMMAND ----------

def load_agg_vc_purchased_nrt(spark, df, database, environment):
    """
    Load transformed data into the target Delta table.
    """
    final_table = DeltaTable.forName(spark, f"{database}{environment}.managed.agg_vc_purchased_nrt")
    
    df = df.select(
        "*",
        expr("CURRENT_TIMESTAMP() as dw_insert_ts"),
        expr("CURRENT_TIMESTAMP() as dw_update_ts"),
        expr("sha2(concat_ws('|', platform, service, country_code, received_on, vc_sku), 256) as merge_key")  # Ensure `country_code` is included
    )

    (
        final_table.alias("old")
        .merge(df.alias("new"), "new.merge_key = old.merge_key")
        .whenMatchedUpdate(
            set={
                "vc_packs_purchased": col("old.vc_packs_purchased") + col("new.vc_packs_purchased"),   
                "dw_update_ts": expr("current_timestamp()") 
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

def proc_batch(df, environment):
    df = transform(df)
    load_agg_vc_purchased_nrt(spark, df, database, environment)

# COMMAND ----------

def run_stream():
    checkpoint_location = create_agg_vc_purchased_nrt(spark, database, environment)
    df = extract(environment, database)
    
    (
        df.writeStream
        .trigger(processingTime="10 minutes")
        .option("checkpointLocation", checkpoint_location)
        .foreachBatch(lambda df, batch_id: proc_batch(df, environment))
        .start()
    )

# COMMAND ----------

run_stream()

# COMMAND ---------- 

# dbutils.fs.rm("dbfs:/tmp/wwe2k25/managed/streaming/run_stg/agg_vc_purchased_nrt", True)
# spark.sql("drop table wwe2k25_stg.managed.agg_vc_purchased_nrt")