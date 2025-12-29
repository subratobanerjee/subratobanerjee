# Databricks notebook source
# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/table_functions

# COMMAND ----------

from datetime import date
from pyspark.sql.functions import expr, col
from delta.tables import DeltaTable
from pyspark.sql.types import LongType, DoubleType, IntegerType, TimestampType

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment', set_environment())
#environment = "_stg"
database = 'oak2'

# COMMAND ----------

def extract(spark, database, environment, prev_max_date):

    game_status_daily_df = spark.table(f"{database}{environment}.managed.fact_player_game_status_daily") \
        .filter(expr(f"date >= '{prev_max_date}'"))

    player_session_df = spark.table(f"{database}{environment}.intermediate.fact_player_session") \
        .filter(expr(f"to_date(session_start_ts) >= '{prev_max_date}'"))

    return game_status_daily_df, player_session_df

# COMMAND ----------

def transform_core_metrics(game_status_daily_df, player_session_df):

    player_session_df = player_session_df.withColumn("agg_1", col("agg_1").cast("double"))

    daily_metrics_df = (
        game_status_daily_df
        .groupBy(
            expr("date"),
            expr("player_platform_id")
        )
        .agg(
            expr("sum(hours_played * 60) as minutes_per_day")
        )
    )

    session_metrics_df = (
        player_session_df
        .groupBy(
            expr("DATE(session_start_ts) as date"),
            expr("platform"),
            expr("service"),
            expr("player_platform_id")
        )
        .agg(
            expr("sum(agg_1) as session_minutes")
        )
    )

    result_df = (
        session_metrics_df.alias("sm")
        .join(
            daily_metrics_df.alias("dm"),
            ["date", "player_platform_id"],
            "left"
        )
        .groupBy(
            expr("sm.date"),
            expr("sm.platform"),
            expr("sm.service")
        )
        .agg(
            expr("CAST(round(AVG(dm.minutes_per_day), 2) AS DOUBLE) as avg_minutes_per_day"),
            expr("CAST(round(PERCENTILE_APPROX(dm.minutes_per_day, 0.5), 2) AS DOUBLE) as median_minutes_per_day"),
            expr("CAST(round(AVG(sm.session_minutes), 2) AS DOUBLE) as avg_minutes_per_session"),
            expr("CAST(round(PERCENTILE_APPROX(sm.session_minutes, 0.5), 2) AS DOUBLE) as median_minutes_per_session")
        )
    )
    
    return result_df


# COMMAND ----------

def create_agg_core_metrics_daily(spark, database, environment, properties={}):

    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.agg_core_metrics_daily (
        date DATE,
        platform STRING,
        service STRING,
        avg_minutes_per_day DOUBLE,
        median_minutes_per_day DOUBLE,
        avg_minutes_per_session DOUBLE,
        median_minutes_per_session DOUBLE,
        merge_key STRING,
        dw_insert_ts TIMESTAMP,
        dw_update_ts TIMESTAMP
    )
    """

    default_properties = {
        "delta.feature.allowColumnDefaults": "supported"
    }
    
    final_properties = {**default_properties, **properties}

    create_table(spark, sql, final_properties)
    
    return f"dbfs:/tmp/{database}/managed/batch/run{environment}/agg_core_metrics_daily"


# COMMAND ----------

def create_agg_core_metrics_daily_view(spark, database):
    """
    Create the agg_core_metrics_daily view in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
        mapping (dict): A dictionary of column mappings.
    """

    sql = f"""
    CREATE VIEW IF NOT EXISTS {database}{environment}.managed_view.agg_core_metrics_daily AS (
        SELECT
        *
        from {database}{environment}.managed.agg_core_metrics_daily
    )
    """
    spark.sql(sql)

# COMMAND ----------

def load(df, database, environment, spark):
    """
    Merges the transformed DataFrame into the target summary table.
    """
    target_table_name = f"{database}{environment}.managed.agg_core_metrics_daily"
    target_df = DeltaTable.forName(spark, target_table_name)

    merger_condition = 'target.merge_key = source.merge_key'

    update_expr = {col: f"source.{col}" for col in df.columns if col not in ['dw_insert_ts', 'merge_key']}
    update_expr["dw_update_ts"] = "current_timestamp()"

    insert_expr = {col: f"source.{col}" for col in df.columns}

    (target_df.alias("target")
     .merge(df.alias("source"), merger_condition)
     .whenMatchedUpdate(set=update_expr)
     .whenNotMatchedInsert(values=insert_expr)
     .execute())
    





# COMMAND ----------
def run_batch(database, environment):
    """
    Orchestrates the ETL process from extraction to loading.
    """
    database = database.lower()
    spark = create_spark_session(name=f"{database}_core_metrics")
    
    target_table = f"{database}{environment}.managed.agg_core_metrics_daily"
    create_agg_core_metrics_daily(spark, database, environment)
    create_agg_core_metrics_daily_view(spark, database)
    
    prev_max_date = max_timestamp(spark, target_table)
    if prev_max_date is None:
        prev_max_date = date(1999, 1, 1)

    print("prev_max_date is ", prev_max_date)

    game_status_daily_df, player_session_df = extract(spark, database, environment, prev_max_date)

    result_df = transform_core_metrics(game_status_daily_df, player_session_df)                                      

    final_df = result_df.withColumn("dw_update_ts", expr("current_timestamp()")) \
                        .withColumn("dw_insert_ts", expr("current_timestamp()")) \
                        .withColumn(
                            "merge_key", 
                            expr("sha2(concat_ws('|', date, platform, service), 256)")
                        )

    load(final_df, database, environment, spark)


run_batch(database, environment)