# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/managed/agg_game_status_daily

# COMMAND ----------

from pyspark.sql.functions import expr

# COMMAND ----------
input_param = dbutils_input_params()
environment = input_param.get('environment', set_environment())
database = 'bluenose'
data_source = 'dna'

view_mapping = {
    'extra_grain_1': 'extra_grain_1 as game_type',
    'agg_game_status_1': 'agg_game_status_1 as rounds_completed',
    'agg_game_status_2': 'agg_game_status_2 as rounds_started',
    'agg_gp_1': 'agg_gp_1 as gameplay_time_sec',
    'agg_gp_2': 'agg_gp_2 as holes_completed',
    'agg_gp_3': 'agg_gp_3 as demo_player_count',
    'agg_gp_4': 'agg_gp_4 as converted_player_count',
    'agg_gp_5': 'agg_gp_5 as full_game_player_count'
}

# COMMAND ----------

def read_fact_player_summary(prev_max_ts):
    return (
        spark.read.table(f"{database}{environment}.managed.fact_player_summary_ltd")
    )

def read_fact_player_game_status(prev_max_ts):
    return (
        spark.read.table(f"{database}{environment}.managed.fact_player_game_status_daily")
        .where(f"DATE::date >= to_date('{prev_max_ts}') - interval '2 day'")

    )

def extract(prev_max_ts):
    fact_player_summary = read_fact_player_summary(prev_max_ts)
    fact_player_game_status = read_fact_player_game_status(prev_max_ts)
    return fact_player_summary,fact_player_game_status


def transform(fact_player_summary, fact_player_game_status):

    joined_df = fact_player_game_status.join(
        fact_player_summary,
        on=['player_id', 'platform', 'service'],
        how='left_outer'
    )
    grouped_df = joined_df.groupBy(
        "date",
        "platform",
        "service",
        "game_mode",
        "sub_mode",
        "country_code",
        "extra_grain_1"
    ).agg(
        expr("coalesce(sum(agg_game_status_1), 0) as agg_game_status_1"),
        expr("coalesce(sum(agg_game_status_2), 0) as agg_game_status_2"),
        expr("coalesce(sum(agg_gp_1), 0) as agg_gp_1"),
        expr("coalesce(sum(agg_gp_2), 0) as agg_gp_2"),
        expr("count(DISTINCT CASE WHEN ltd_string_1 = 'demo' THEN player_id WHEN ltd_string_1 = 'converted' AND ltd_ts_1::date > DATE THEN player_id ELSE NULL END) as agg_gp_3"),
        expr("count(DISTINCT CASE WHEN ltd_string_1 = 'converted' AND ltd_ts_1::date <= DATE THEN player_id ELSE NULL END) as agg_gp_4"),
        expr("count(DISTINCT CASE WHEN ltd_string_1 = 'full game' THEN player_id END) as agg_gp_5"),
    )
    return grouped_df

def run_batch():
    spark = create_spark_session(name=f"{database}")
    checkpoint_location = create_agg_game_status_daily(spark, database, view_mapping)
    prev_max_ts = max_timestamp(spark, f"{database}{environment}.managed.agg_game_status_daily")
    fact_player_summary,fact_player_game_status = extract(prev_max_ts)
    df=transform(fact_player_summary,fact_player_game_status)
    load_agg_game_status_daily(spark, df, database, environment)


# COMMAND ----------

run_batch()