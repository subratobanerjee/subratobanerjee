# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/managed/agg_subject_progression_daily

# COMMAND ----------

from pyspark.sql.functions import expr, countDistinct, when, sha2, concat_ws, broadcast



# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment', set_environment())
database = 'bluenose'
data_source = 'dna'
view_mapping = {
    'AGG_1': 'AGG_1 as DEMO_SUBJECT_COUNT',
    'AGG_2': 'AGG_2 as FULL_GAME_SUBJECT_COUNT',
    'AGG_3': 'AGG_3 as DEMO_PLAYER_COUNT',
    'AGG_4': 'AGG_4 as FULL_GAME_PLAYER_COUNT',
    'AGG_5': 'AGG_5 as CONVERTED_PLAYER_COUNT'
    }   

def read_fact_myplayer_subject_progression_daily(prev_max_ts):
    return (
        spark.read.table(f"{database}{environment}.managed.fact_myplayer_subject_progression_daily")
        .where(f"date >= '{prev_max_ts}' - INTERVAL 2 DAY")
    )

def read_fact_player_summary_ltd():
    return (
        spark.read.table(f"{database}{environment}.managed.fact_player_summary_ltd")
    )

def extract(prev_max_ts):
    fact_myplayer_subject_progression_daily = read_fact_myplayer_subject_progression_daily(prev_max_ts)
    fact_player_summary_ltd = read_fact_player_summary_ltd()
    return fact_myplayer_subject_progression_daily, fact_player_summary_ltd

def transform(fact_myplayer_subject_progression_daily, fact_player_summary_ltd, spark):

    agg_columns = {
            "AGG_1": expr("count(distinct case when ltd_string_1= 'demo' then fsp.subject_id when ltd_string_1= 'converted' and  ltd_ts_1::date > fsp.date then fsp.subject_id else null end) as AGG_1"),
            "AGG_2": expr("count(distinct case when ltd_string_1='full game' then fsp.subject_id else null end) as AGG_2"),
            "AGG_3": expr("count(distinct case when ltd_string_1= 'demo' then fsp.player_id when ltd_string_1= 'converted' and  ltd_ts_1::date > fsp.date then fsp.player_id else null end) as AGG_3"),
            "AGG_4": expr("count(distinct case when ltd_string_1='full game' then fsp.player_id else null end) as AGG_4"),
            "AGG_5": expr("count(distinct case when ltd_string_1= 'converted' and ltd_ts_1::date <= fsp.date then fsp.player_id else null end) as AGG_5")
                  }
    
    # Perform the main aggregation with a regular join
    transformed_df = (
        fact_myplayer_subject_progression_daily.alias("fsp")
        .join(
            fact_player_summary_ltd.alias("fpsl"),
            on=["player_id", "platform", "service"],
            how="left_outer"  # Use left_outer join to include all rows from the fact table
        )
        .groupBy(
            "DATE", "PLATFORM", "SERVICE", "SUBJECT_ID"
        )
        .agg(*agg_columns.values())
        .select(
            "DATE", "PLATFORM", "SERVICE","SUBJECT_ID",
            "AGG_1", "AGG_2",
            "AGG_3", "AGG_4", "AGG_5",
            expr("current_timestamp()").alias("DW_INSERT_TS"),
            expr("current_timestamp()").alias("DW_UPDATE_TS"),
            expr("sha2(concat_ws('|', DATE, PLATFORM, SERVICE, SUBJECT_ID), 256)").alias("MERGE_KEY")
        )
    )
    
    return transformed_df



def load_agg_subject_progression_daily(spark,df, database, environment):
    """
    Load the data into the agg_subject_progression_daily table in the specified environment.

    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        df (DataFrame): The DataFrame to load into the table.
        database (str): The database to load the table into.
        environment (str): The environment to load the table into.
    """
    final_table = DeltaTable.forName(spark, f"{database}{environment}.managed.agg_subject_progression_daily")

    # get schema of the table
    table_schema = spark.read.table(f"{database}{environment}.managed.agg_subject_progression_daily").schema

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
    create_agg_subject_progression_daily(spark, database, environment)
    prev_max_ts = max_timestamp(spark, f"{database}{environment}.managed.fact_myplayer_subject_progression_daily")
    fact_myplayer_subject_progression_daily, fact_player_summary_ltd = extract(prev_max_ts)
    df=transform(fact_myplayer_subject_progression_daily, fact_player_summary_ltd, spark)
    load_agg_subject_progression_daily(spark, df, database, environment)

# COMMAND ----------

run_batch()
