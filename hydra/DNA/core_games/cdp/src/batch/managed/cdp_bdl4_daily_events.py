# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/table_functions

# COMMAND ----------


import json
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
from delta.tables import DeltaTable
import argparse
from pyspark.sql import SparkSession

# COMMAND ----------


from pyspark.sql.functions import expr

def extract(spark, environment, prev_max):
    fact_player_ltd = (
        spark.table(f"dataanalytics{environment}.standard_metrics.fact_player_ltd")
        .where(expr(f"CAST(dw_update_ts AS DATE) >= to_date('{prev_max}') - INTERVAL 2 DAYS"))
        .where(expr("title = 'Borderlands 4'"))
        .filter(expr("platform<>'Unknown' and service <> 'Unknown'"))
    )

    services_df = spark.table("REFERENCE_CUSTOMER.PLATFORM.SERVICE")
    platform_df = spark.table("reference_customer.platform.platform")
    vw_fpid_all_df = spark.table("reference_customer.customer.vw_fpid_all")
    vw_bdl4_summary_df = spark.table(f"dataanalytics{environment}.cdp_ng.bdl4_summary")

    return fact_player_ltd, services_df, platform_df, vw_fpid_all_df, vw_bdl4_summary_df  
    
def transform(environment, spark, fact_player_ltd, services_df, platform_df, vw_fpid_all_df, vw_bdl4_summary_df):
    
    services = (
        services_df
        .selectExpr(
            "CASE WHEN SERVICE = 'xbl' THEN 'Xbox Live' ELSE SERVICE END AS JOIN_KEY",
            "SERVICE",
            "VENDOR"
        )
        .dropDuplicates(["JOIN_KEY", "SERVICE", "VENDOR"])
    )

    joined_df = (
        fact_player_ltd.alias("f")
        .join(
            services.alias("s"),
            expr("lower(s.JOIN_KEY) = lower(f.service)"),
            "inner"
        )
        .join(
            platform_df.alias("v"),
            expr("f.platform = v.src_platform"),
            "inner"
        )
        .where(expr("f.platform IS NOT NULL"))
        .selectExpr(
            "lower(f.player_id) AS player_id",
            "case when f.install_date < '2025-09-12' then '2025-09-12' else f.install_date end as install_date",
            "case when f.last_seen < '2025-09-12' then '2025-09-12' else f.last_seen end as last_seen",
            "v.vendor",
            "s.service"
        )
        .distinct()
    )

    window_spec = Window.partitionBy("player_id").orderBy(expr("cast(Install_date as timestamp)"))

    fact_players_ltd = (
        joined_df
        .withColumn("vendor", first("vendor").over(window_spec))
        .withColumn("service", first("service").over(window_spec))
        .selectExpr(
            "player_id",
            "vendor",
            "service",
            "cast(Install_date as date) as Install_date",
            "cast(last_seen as date) as last_seen"
        )
        .distinct()
    )

    install_df = (
        fact_players_ltd
        .groupBy("player_id", "vendor")
        .agg(expr("CAST(MIN(Install_date) AS DATE)").alias("EVENT_DATE"))
        .withColumn("GAME_TITLE", expr("CAST('bdl_bl4' AS STRING)"))
        .withColumn("SUB_GAME", expr("CAST('total' AS STRING)"))
        .withColumn("EVENT_NAME", expr("CAST('install' AS STRING)"))
        .withColumn("EVENT_ACTION", expr("CAST(NULL AS STRING)"))
        .withColumn("EVENT_RESULT", expr("CAST('1' AS STRING)"))
        .withColumn("EVENT_DESCRIPTION", expr("CAST('First date that a player appears in the game' AS STRING)"))
        .withColumn("CDP_FIELD_NAME", expr("CAST('bdl_bl4_total_install_daily' AS STRING)"))
    )

    base_reactivation_df = (
        fact_player_ltd.alias("s")
        .join(services_df.alias("v"), expr("s.service = v.service"), "inner")
        .selectExpr(
            "s.player_id",
            "s.last_seen",
            "CASE WHEN v.vendor = 'Valve' THEN 'PC' ELSE v.vendor END AS vendor"
        )
    )

    agg_reactivation_df = (
        base_reactivation_df
        .groupBy("player_id", "vendor")
        .agg(expr("CAST(LAST(last_seen) AS DATE) AS event_date"))
    )

    window_spec = Window.partitionBy("player_id").orderBy("event_date")

    reactivation_df = (
        agg_reactivation_df
        .withColumn("prev_event_date", lag("event_date").over(window_spec))
        .withColumn("days_since_last", expr("DATEDIFF(event_date, prev_event_date)"))
        .filter(expr("days_since_last >= 14"))
        .drop("prev_event_date", "days_since_last")
    )

    reactivation_df = (
        reactivation_df
        .selectExpr(
            "player_id",
            "vendor",
            "event_date AS EVENT_DATE",
            "CAST('bdl_bl4' AS STRING) AS GAME_TITLE",
            "CAST('total' AS STRING) AS SUB_GAME",
            "CAST('reactivation' AS STRING) AS EVENT_NAME",
            "CAST('' AS STRING) AS EVENT_ACTION",
            "CAST('' AS STRING) AS EVENT_RESULT",
            "CAST('This event is triggered when a user leaves the game for 14 days (has churned) and then returns (reactivates)' AS STRING) AS EVENT_DESCRIPTION",
            "CAST('bdl_bl4_total_reactivation_daily' AS STRING) AS CDP_FIELD_NAME"
        )
    )

    all_events_df = install_df.unionByName(reactivation_df)
    fpids_df = vw_fpid_all_df \
        .filter(expr("FPID IS NOT NULL")) \
        .selectExpr(
            "PLAYER_ID as platform_id",
            "FPID as puid"
        )
    final_df = (
        all_events_df.alias("a")
        .join(fpids_df.alias("fpd"), expr("LOWER(fpd.platform_id) = LOWER(a.PLAYER_ID)"))
        .join(vw_bdl4_summary_df.alias("sum"), expr("fpd.puid = sum.puid"))
        .selectExpr(
            "a.EVENT_DATE",
            f"VENDOR || ':' || dataanalytics{environment}.cdp_ng.salted_puid(fpd.puid) AS brand_firstpartyid",
            f"dataanalytics{environment}.cdp_ng.salted_puid(fpd.puid) AS salted_puid",
            "fpd.puid",
            "VENDOR",
            "GAME_TITLE",
            "SUB_GAME",
            "EVENT_NAME",
            "EVENT_ACTION",
            "CDP_FIELD_NAME",
            "EVENT_DESCRIPTION",
            "CAST(CAST(EVENT_RESULT AS DECIMAL(20,2)) AS STRING) AS EVENT_RESULT"
        )
        .distinct()
    )

    df = final_df.withColumn("dw_update_ts", expr("current_timestamp()")) \
                 .withColumn("dw_insert_ts", expr("current_timestamp()"))
    df = df.withColumn(
        "merge_key", 
        expr("sha2(concat_ws('|', CAST(EVENT_DATE AS STRING),VENDOR, PUID,EVENT_NAME), 256)")
    )
    return df

def create_bdl4_daily_events_table(environment, spark):
    sql = f"""
    CREATE TABLE IF NOT EXISTS dataanalytics{environment}.cdp_ng.bdl4_daily_events (
        EVENT_DATE date,
        BRAND_FIRSTPARTYID STRING,
        SALTED_PUID STRING,
        PUID STRING,
        VENDOR STRING,
        GAME_TITLE STRING,
        SUB_GAME STRING,
        EVENT_NAME STRING,
        EVENT_ACTION STRING,
        EVENT_RESULT STRING,
        EVENT_DESCRIPTION STRING,
        CDP_FIELD_NAME STRING,
        dw_update_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        dw_insert_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        merge_key STRING
    )
    """

    properties = {
        "delta.feature.allowColumnDefaults": "supported"  # this won't work until iceberg v3 is released
    }

    create_table(spark, sql, properties)
    return (
        f"dbfs:/tmp/dataanalytics/cdp_ng/streaming/run{environment}/bdl4_daily_events"
    )

def load(df, environment, spark):
    # Reference to the target Delta table
    target_df = DeltaTable.forName(spark, f"dataanalytics{environment}.cdp_ng.bdl4_daily_events")

    # Merge condition
    merger_condition = 'target.merge_key = source.merge_key'

    # Generate update conditions for numeric fields
    update_types = (LongType, DoubleType, IntegerType,TimestampType)
    update_fields = [
        field.name for field in df.schema.fields if isinstance(field.dataType, update_types)
    ]

    # Dynamically build set_fields dict
    set_fields = {
        field: f"greatest(target.{field}, source.{field})" for field in update_fields
    }
    set_fields["dw_update_ts"] = "source.dw_update_ts"

    # Optional: construct a condition string (can be improved or removed)
    update_condition = " OR ".join([f"target.{field} != source.{field}" for field in update_fields])

    merge_update_conditions = [
        {
            'condition': update_condition,
            'set_fields': set_fields
        }
    ]

    # Begin merge operation
    merge_df = target_df.alias("target").merge(df.alias("source"), merger_condition)
    merge_df = set_merge_update_condition(merge_df, merge_update_conditions)
    merge_df = set_merge_insert_condition(merge_df, df)

    merge_df.execute()

# COMMAND ----------

def run_batch():
    # Parse the environment argument
    input_param = dbutils_input_params()
    environment = input_param.get('environment', set_environment())
    # Create a Spark session
    spark = SparkSession.builder.appName("Hydra").getOrCreate()
    #create_table
    create_bdl4_daily_events_table(environment ,spark)

    prev_max = max_timestamp(spark, f"dataanalytics{environment}.cdp_ng.bdl4_daily_events", 'EVENT_DATE')
    if prev_max is None:
        prev_max = date(1999, 1, 1)
    print(prev_max)
    
    

    # Extract the data
    fact_player_ltd, services_df, platform_df, vw_fpid_all_df, vw_bdl4_summary_df=extract(spark, environment, prev_max)
    
    # Transform the data (if needed)
    df = transform(environment, spark, fact_player_ltd, services_df, platform_df, vw_fpid_all_df, vw_bdl4_summary_df)
    # df.display()

    # Load the data into a Delta table
    load(df, environment, spark)

# COMMAND ----------

if __name__ == "__main__":
    run_batch()
