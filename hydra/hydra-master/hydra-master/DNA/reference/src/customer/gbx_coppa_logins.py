# Databricks notebook source
# MAGIC %run ../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../utils/ddl/table_functions

# COMMAND ----------

from pyspark.sql.functions import expr
from delta.tables import DeltaTable
from datetime import date

spark = create_spark_session(name="gbx_coppa_logins_etl")

input_param = dbutils_input_params()
environment = input_param.get('environment', set_environment())
target_table = f"reference{environment}.customer.gbx_coppa_logins"

# COMMAND ----------

def create_table_gbx_coppa_logins(spark, table_name):

    sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            maw_time_received TIMESTAMP,
            event STRING,
            title STRING,
            email STRING,
            dw_insert_ts TIMESTAMP,
            dw_update_ts TIMESTAMP
        )"""

    properties = {
        "delta.feature.allowColumnDefaults": "supported",
        "delta.enableIcebergCompatV2": "true",
        "delta.universalFormat.enabledFormats": "iceberg"
    }

    create_table(spark, sql, properties)

# COMMAND ----------

def extract(spark, environment, prev_max_ts):

    source_df = spark.table(f"gbx{environment}.raw.archway_kpi_events") \
        .filter(f"maw_time_received > '{prev_max_ts}'")

    return source_df

# COMMAND ----------

def transform(source_df):
    
    exploded_df = source_df.selectExpr(
        "maw_time_received",
        "explode(archway_kpi_events) as kpi_data"
    )

    final_df = exploded_df.filter("kpi_data.event_string = 'remove_email_from_marketing'") \
        .selectExpr(
            "maw_time_received",
            "kpi_data.event_string as event",
            "kpi_data.game_title_string as title",
            "kpi_data.email_string as email"
        )

    return final_df

# COMMAND ----------

def load(df, table_name):

    df_with_ts = df.withColumn("dw_insert_ts", expr("current_timestamp()")) \
                   .withColumn("dw_update_ts", expr("current_timestamp()"))
    
    #Dedup
    df_to_load = df_with_ts.withColumn("rn", expr("row_number() OVER (PARTITION BY email ORDER BY maw_time_received DESC)")) \
                            .filter("rn = 1") \
                            .drop("rn")
    
    merge_condition = "target.email = source.email"
    
    update_set = {
        "event": "source.event",
        "title": "source.title",
        "dw_update_ts": "source.dw_update_ts"
    }
    
    insert_set = {col: f"source.{col}" for col in df_to_load.columns}

    delta_table = DeltaTable.forName(spark, table_name)
    
    delta_table.alias("target").merge(
        source=df_to_load.alias("source"),
        condition=expr(merge_condition)
    ) \
    .whenMatchedUpdate(set=update_set) \
    .whenNotMatchedInsert(values=insert_set) \
    .execute()

# COMMAND ----------


def run_batch(environment, target_table):

    create_table_gbx_coppa_logins(spark, target_table)

    prev_max_ts = max_timestamp(spark, target_table, 'maw_time_received')
    if prev_max_ts is None:
        prev_max_ts = date(1999, 1, 1)

    source_df = extract(spark, environment, prev_max_ts)

    final_df = transform(source_df)

    load(final_df, target_table)

# COMMAND ----------

run_batch(environment, target_table)
