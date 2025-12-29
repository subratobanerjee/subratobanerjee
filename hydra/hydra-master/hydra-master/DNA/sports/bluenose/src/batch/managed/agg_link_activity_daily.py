# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/managed/agg_link_activity_daily

# COMMAND ----------

from pyspark.sql.functions import (expr, current_timestamp, sha2, concat_ws)

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment', set_environment())
database = 'bluenose'
data_source = 'dna'
view_mapping = {
    'AGG_1': 'AGG_1 as AGG_LINK_COUNT',
    'AGG_2': 'AGG_2 as AGG_UNLINK_COUNT',
    'AGG_3': 'AGG_3 as AGG_ENTERING_SCREEN_COUNT',
    'AGG_4': 'AGG_4 as AGG_LEAVING_SCREEN_COUNT',
    'AGG_5': 'AGG_5 as AGG_ATTEMPTS_TO_LINK_COUNT',
    'AGG_6': 'AGG_6 as AGG_PLAYER_ID_COUNT'
    }   

def read_fact_player_link_activity(prev_max_ts):
    return (
        spark.read.table(f"{database}{environment}.managed.fact_player_link_activity")
        .where(f"date >= '{prev_max_ts}' - INTERVAL 2 DAY")
    )

def extract(prev_max_ts):
    fact_player_link_activity = read_fact_player_link_activity(prev_max_ts)
    return fact_player_link_activity

def transform(fact_player_link_activity):
    transformed_df = (
        fact_player_link_activity.groupBy(
            "DATE", 
            "PLATFORM", 
            "SERVICE",
            "COUNTRY_CODE"
        )
        .agg(
            expr("sum(AGG_1)::int as AGG_1"),
            expr("sum(AGG_2)::int as AGG_2"),
            expr("sum(AGG_3)::int as AGG_3"),
            expr("sum(AGG_4)::int as AGG_4"),
            expr("sum(AGG_5)::int as AGG_5"),
            expr("count(distinct player_id)::int as AGG_6")
        )
    ).select(
        "DATE", "PLATFORM", "SERVICE", "COUNTRY_CODE",
        "AGG_1", "AGG_2", "AGG_3", "AGG_4", "AGG_5", "AGG_6",
        expr("current_timestamp() as DW_INSERT_TS"),
        expr("current_timestamp() as DW_UPDATE_TS"), 
        expr("sha2(concat_ws('|', DATE,PLATFORM,SERVICE,COUNTRY_CODE), 256) as MERGE_KEY")
    )
    
    return transformed_df

def load_agg_link_activity_daily(spark,df, database, environment):
    """
    Load the data into the agg_link_activity_daily table in the specified environment.

    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        df (DataFrame): The DataFrame to load into the table.
        database (str): The database to load the table into.
        environment (str): The environment to load the table into.
    """
    final_table = DeltaTable.forName(spark, f"{database}{environment}.managed.agg_link_activity_daily")

    # get schema of the table
    table_schema = spark.read.table(f"{database}{environment}.managed.agg_link_activity_daily").schema

    # create empty dataframe using the schema
    out_df = spark.createDataFrame([], table_schema)
    out_df = out_df.unionByName(df, allowMissingColumns=True)
    agg_cols = [col_name for col_name in out_df.columns if 'AGG_' in col_name]
    merge_condition = " OR ".join(f"old.{col_name} <> new.{col_name}" for col_name in agg_cols)

    update_set = {}
    for col_name in agg_cols:
        update_set[f"old.{col_name}"] = f"greatest(new.{col_name}, old.{col_name})"
    update_set[f"old.dw_update_ts"] = "CURRENT_TIMESTAMP()"

    # merge the table
    (
    final_table.alias('old')
    .merge(
        out_df.alias('new'),
        "new.MERGE_KEY = old.MERGE_KEY"
    )
    .whenMatchedUpdate(condition=merge_condition,set=update_set)
    .whenNotMatchedInsertAll()
    .execute()
    )

def run_batch():
    spark = create_spark_session(name=f"{database}")
    create_agg_link_activity_daily(spark, database, environment)
    prev_max_ts = max_timestamp(spark, f"{database}{environment}.managed.fact_player_link_activity")
    fact_player_link_activity = extract(prev_max_ts)
    df=transform(fact_player_link_activity)
    load_agg_link_activity_daily(spark, df, database, environment)

# COMMAND ----------
run_batch()