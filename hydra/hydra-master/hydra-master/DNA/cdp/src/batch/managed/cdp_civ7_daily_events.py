# Databricks notebook source
# MAGIC %run ../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../utils/helpers

# COMMAND ----------
# MAGIC %run ../../../../../utils/ddl/table_functions

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window


#extract
def extract(environment, prev_max):
    civ7_raw_df = (
        spark.table(f"dataanalytics{environment}.cdp_ng.civ7_summary")
             .where(expr(f"CAST(civ_civ7_install_date AS DATE) >= to_date('{prev_max}') - INTERVAL 2 DAYS"))
    )

    fact_df = (
        spark.table(f"inverness{environment}.intermediate.fact_player_activity")
             .where(expr(f"CAST(dw_update_ts AS DATE) >= to_date('{prev_max}') - INTERVAL 2 DAYS"))
    )

    app_df = (
        spark.table(f"inverness{environment}.raw.applicationsessionstatus")
             .where(expr(f"CAST(insert_ts AS DATE) >= to_date('{prev_max}') - INTERVAL 2 DAYS"))
    )

    return civ7_raw_df, fact_df, app_df



#transformation
def transformation(civ7_raw_df, fact_df, app_df) :

    # ---------- INSTALL EVENTS ----------------------------------------
    summary_view = civ7_raw_df.select(
        "civ_civ7_install_date",
        "civ_civ7_last_seen_date",
        "brand_firstpartyid",
        "salted_puid",
        "puid",
        "platform_account_id",
        "civ_civ7_vendor"
    )   
    install_df = summary_view.selectExpr(
        "CAST(civ_civ7_install_date AS DATE) AS event_date",
        "platform_account_id AS player_id",
        "civ_civ7_vendor AS vendor"
    ).withColumn("game_title", lit("civ_civ7")) \
    .withColumn("sub_game", lit("total")) \
    .withColumn("event_name", lit("install")) \
    .withColumn("event_action", lit(None)) \
    .withColumn("event_result", lit(1)) \
    .withColumn("event_description", lit(None)) \
    .distinct()

    # ---------- REACTIVATION EVENTS -----------------------------------
    joined_df = fact_df.alias("F").join(
        summary_view.alias("sum"),
        col("F.player_id") == col("sum.platform_account_id"),
        how="inner"
    )

    # Define window spec
    window_spec = Window.partitionBy("F.player_id").orderBy(col("received_on").cast("date"))

    # Apply LAG function and compute date difference
    with_lag_df = joined_df.withColumn("prev_received_on", lag(col("received_on").cast("date")).over(window_spec)) \
        .withColumn("date_diff", datediff(col("received_on").cast("date"), col("prev_received_on"))) \
        .filter(col("date_diff") >= 14)

    # Select and transform final columns
    reactivation_df = with_lag_df.select(
        col("received_on").cast("date").alias("event_date"),
        col("F.player_id").alias("player_id"),
        col("sum.civ_civ7_vendor").alias("vendor"),
        lit("civ_civ7").alias("game_title"),
        lit("total").alias("sub_game"),
        lit("reactivation").alias("event_name"),
        lit(None).cast("string").alias("event_action"),
        lit(1).alias("event_result"),
        lit(None).cast("string").alias("event_description")
    )

    # -----------------------------------union both -------------------------

    all_events_df = install_df.unionByName(reactivation_df) 

    # -----------------------------------dlc_install------------------------
    filtered_app_df = app_df.filter(
        (col("playerpublicid").isNotNull()) & 
        (col("playerpublicid") != "anonymous")
    )

    exploded_df = filtered_app_df.select(
        col("playerpublicid").alias("player_id"),
        col("receivedon").cast("timestamp").alias("received_on"),
        explode(split(col("activedlc"), ",")).alias("entitlement_id")
    )

    dlc_df = exploded_df.withColumn("entitlement_id", trim(lower(col("entitlement_id"))))

    dlc_filtered_df = dlc_df.filter(
        col("entitlement_id").like("%shawnee-tecumseh%") |
        col("entitlement_id").like("%collection-1%") |
        col("entitlement_id").like("%collection-2%") |
        col("entitlement_id").like("%collection-3%") |
        col("entitlement_id").like("%collection-4%")
    )

    joined_dlc_df = dlc_filtered_df.join(
        summary_view,
        dlc_filtered_df.player_id == summary_view.platform_account_id,
        how="inner"
    )

    dlc_event_df = joined_dlc_df.groupBy(
        col("player_id"),
        col("civ_civ7_vendor").alias("vendor")
    ).agg(
        min(col("received_on").cast("date")).alias("event_date")
    )

    dlc_event_df = dlc_event_df.select(
        col("event_date"),
        col("player_id"),
        col("vendor"),
        lit("civ_civ7").alias("game_title"),
        lit("total").alias("sub_game"),
        lit("dlc_install").alias("event_name"),
        lit(None).cast("string").alias("event_action"),
        lit(1).alias("event_result"),
        lit(None).cast("string").alias("event_description")
    )

    # -----------------------------------union All-------------------------
    all_events = all_events_df.unionByName(dlc_event_df) 
    
    # -----------------------------------final-------------------------
    joined_df = all_events.alias("al").join(
    summary_view.alias("sum"),
    col("al.player_id") == col("sum.platform_account_id"),
    how="inner"
    )
    result_df = joined_df.selectExpr(
    "CAST(event_date AS DATE) AS event_date",
    "brand_firstpartyid",
    "salted_puid",
    "puid",
    "vendor",
    "game_title",
    "sub_game",
    "event_name",
    "CAST(NULL AS STRING) AS event_action",
    "CAST(CAST(event_result AS DECIMAL(20,2)) AS STRING) AS event_result",
    "CAST(NULL AS STRING) AS event_description",
    """
    dataanalytics_stg.cdp_ng.mk_cdp_field2(
        game_title,
        sub_game,
        event_name,
        CAST(NULL AS STRING)
    ) AS cdp_field_name
    """
    ).distinct()
    
    return result_df


def create_civ7_daily_events_table(environment, spark):
    sql = f"""
    CREATE TABLE IF NOT EXISTS dataanalytics{environment}.cdp_ng.civ7_daily_events (
        event_date DATE,
        brand_firstpartyid STRING,
        salted_puid STRING,
        puid STRING,
        vendor STRING,
        game_title STRING,
        sub_game STRING,
        event_name STRING,
        event_action STRING,
        event_result STRING,
        event_description STRING,
        cdp_field_name STRING,
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
        f"dbfs:/tmp/dataanalytics/cdp_ng/streaming/run{environment}/civ7_daily_events"
    )

def create_civ7_daily_events_view(environment):
    table_name = f"dataanalytics{environment}.cdp_ng.civ7_daily_events"
    df = spark.table(table_name)

    excluded_columns = {'dw_insert_ts', 'dw_update_ts', 'merge_key'}
    selected_columns = [col for col in df.columns if col not in excluded_columns]
    selected_columns_str = ",\n    ".join(selected_columns)

    sql = f"""
    CREATE OR REPLACE VIEW dataanalytics{environment}.cdp_ng.vw_civ7_daily_events AS (
        SELECT
            {selected_columns_str}
        FROM {table_name}
    )
    """

    spark.sql(sql)

def load(df, environment, spark):
    # Reference to the target Delta table
    target_df = DeltaTable.forName(spark, f"dataanalytics{environment}.cdp_ng.civ7_daily_events")

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

    print("Table has been created successfully.")
    print("Creating temporary view...")
    #create view
    create_civ7_daily_events_view(environment)
    print("view created successfully.")


def run_batch():
    # Parse the environment argument
    input_param = dbutils_input_params()
    environment = input_param.get('environment', set_environment())

    # Create a Spark session
    spark = SparkSession.builder.appName("Hydra").getOrCreate()

    create_civ7_daily_events_table(environment, spark)

    prev_max = max_timestamp(spark, f"dataanalytics{environment}.cdp_ng.civ7_daily_events", 'event_date')
    if prev_max is None:
            prev_max = date(1999, 1, 1)
            print(prev_max)

    civ7_raw_df, fact_df, app_df=extract(environment,prev_max)
    df=transformation(civ7_raw_df, fact_df, app_df)
    
    df = df.withColumn("dw_update_ts", expr("current_timestamp()")) \
                 .withColumn("dw_insert_ts", expr("current_timestamp()"))

    df = df.withColumn(
        "merge_key", 
        expr("sha2(concat_ws('|', CAST(event_date AS STRING),vendor,puid,event_name), 256)")
    )
    load(df, environment, spark)


if __name__ == "__main__":
    run_batch()