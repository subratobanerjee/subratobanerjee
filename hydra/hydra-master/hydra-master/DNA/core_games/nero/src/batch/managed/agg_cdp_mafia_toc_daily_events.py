# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/table_functions

# COMMAND ----------


from pyspark.sql.functions import expr
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, DoubleType, IntegerType,TimestampType


# COMMAND ----------

def extract(spark, environment, prev_max):
    vw_mafia_toc_summary = (
        spark.table(f"dataanalytics{environment}.cdp_ng.mafia_toc_summary")
        # .where(expr(f"CAST(maf_toc_install_date AS DATE) >= to_date('{prev_max}') - INTERVAL 2 DAYS"))
    )
    return vw_mafia_toc_summary

def transform(environment, spark, vw_mafia_toc_summary):
    
    summary_view = vw_mafia_toc_summary.selectExpr(
        "maf_toc_install_date",
        "brand_firstpartyid",
        "salted_puid",
        "puid",
        "platform_account_id",
        "maf_toc_vendor"
    )

    all_events = summary_view.selectExpr(
        "CAST(maf_toc_install_date AS DATE) AS event_date",
        "platform_account_id AS player_id",
        "maf_toc_vendor AS vendor",
        "'maf_toc' AS game_title",
        "'total' AS sub_game",
        "'install' AS event_name",
        "CAST(NULL AS STRING) AS event_action",
        "1 AS event_result",
        "CAST(NULL AS STRING) AS event_description"
    ).distinct()

    joined = all_events.alias("al").join(
        summary_view.alias("sum"),
        expr("al.player_id = sum.platform_account_id"),
        "inner"
    )

    final_df = joined.selectExpr(
        "CAST(al.event_date AS DATE) AS event_date",
        "sum.brand_firstpartyid",
        "sum.salted_puid",
        "sum.puid",
        "al.vendor",
        "al.game_title",
        "al.sub_game",
        "al.event_name",
        "CAST(NULL AS STRING) AS event_action",
        "CAST(CAST(al.event_result AS DECIMAL(20,2)) AS STRING) AS event_result",
        "CAST(NULL AS STRING) AS event_description",
        "dataanalytics_prod.cdp_ng.mk_cdp_field2(al.game_title, al.sub_game, al.event_name, NULL) AS cdp_field_name"
    )

    df = final_df.withColumn("dw_update_ts", expr("current_timestamp()")) \
                 .withColumn("dw_insert_ts", expr("current_timestamp()"))
    df = df.withColumn(
        "merge_key", 
        expr("sha2(concat_ws('|', CAST(EVENT_DATE AS STRING),VENDOR, PUID,EVENT_NAME), 256)")
    )
    return df

def create_mafia_toc_daily_events_table(environment, spark):
    sql = f"""
    CREATE TABLE IF NOT EXISTS dataanalytics{environment}.cdp_ng.mafia_toc_daily_events (
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
        f"dbfs:/tmp/dataanalytics/cdp_ng/streaming/run{environment}/mafia_toc_daily_events"
    )




def load(df, environment, spark):
    # Reference to the target Delta table
    target_df = DeltaTable.forName(spark, f"dataanalytics{environment}.cdp_ng.mafia_toc_daily_events")

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
    create_mafia_toc_daily_events_table(environment ,spark)

    prev_max = max_timestamp(spark, f"dataanalytics{environment}.cdp_ng.mafia_toc_daily_events", 'EVENT_DATE')
    if prev_max is None:
        prev_max = date(1999, 1, 1)
    print(prev_max)
    
    # Extract the data
    vw_mafia_toc_summary=extract(spark, environment,prev_max)
    
    # Transform the data (if needed)
    df = transform(environment, spark, vw_mafia_toc_summary)

    # Load the data into a Delta table
    load(df, environment, spark)

# COMMAND ----------

if __name__ == "__main__":
    run_batch()
