# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/managed/fact_player_game_status_daily

# COMMAND ----------

from pyspark.sql.functions import expr

# COMMAND ----------
input_param = dbutils_input_params()
environment = input_param.get('environment', set_environment())
database = 'bluenose'
data_source = 'dna'
view_mapping = {
    'extra_grain_1': 'extra_grain_1 as game_type',
    'extra_grain_2': 'extra_grain_2 as is_onboarding',
    'agg_game_status_1': 'agg_game_status_1 as rounds_completed',
    'agg_game_status_2': 'agg_game_status_2 as rounds_started',
    'agg_game_status_3': 'agg_game_status_3 as rounds_won',
    'agg_game_status_4': 'agg_game_status_4 as rounds_quit',
    'agg_gp_1': 'agg_gp_1 as gameplay_time_sec',
    'agg_gp_2': 'agg_gp_2 as holes_completed'
}

# COMMAND ----------

def read_player_activity(environment,min_received_on):
    return (
        spark
        .read
        .table(f"bluenose{environment}.intermediate.fact_player_activity")
        .where((expr(f"received_on::timestamp::date > to_date('{min_received_on}') - INTERVAL 2 DAY")) & 
            (expr("player_id NOT ILIKE '%Null%' AND player_id is not null AND player_id != 'anonymous' AND extra_info_2 is not Null"))
            )
    )

# COMMAND ----------

def extract(environment,database):
    prev_max_ts = max_timestamp(spark, f"{database}{environment}.managed.fact_player_game_status_daily", "date")
    min_received_on = spark.sql(f"select ifnull(min(received_on), '1970-10-01')::date as min_received_on from {database}{environment}.intermediate.fact_player_activity where dw_insert_ts >= current_date - interval '2' day").collect()[0]['min_received_on']
    min_received_on_inc = min(prev_max_ts, min_received_on)
    
    activity_df = read_player_activity(environment,min_received_on_inc)
    return activity_df

# COMMAND ----------

def transform(df):
    out_df = (
        df
        .groupBy(
            expr("received_on::date as date"),
            "player_id",
            "platform",
            "service",
            expr("extra_info_2 as game_mode"),
            expr("extra_info_3 as sub_mode"),
            "country_code",
            expr("extra_info_5 as extra_grain_1"),
            expr("extra_info_10 as extra_grain_2")
        )
        .agg(
            expr("COUNT(DISTINCT CASE WHEN EVENT_TRIGGER = 'RoundComplete' THEN extra_info_6 END) as agg_game_status_1"),
            expr("COUNT(DISTINCT CASE WHEN EVENT_TRIGGER = 'RoundStart' THEN extra_info_6 END) as agg_game_status_2"),
            expr("COUNT(DISTINCT CASE WHEN EVENT_TRIGGER = 'RoundWon' THEN extra_info_6 END) as agg_game_status_3"),
            expr("COUNT(DISTINCT CASE WHEN EVENT_TRIGGER = 'RoundQuit' THEN extra_info_6 END) as agg_game_status_4"),
            expr("SUM(CASE WHEN EVENT_TRIGGER in ( 'RoundComplete', 'RoundWon', 'RoundQuit' ) then extra_info_9 END) as agg_gp_1"),
            expr("COUNT(CASE WHEN EVENT_TRIGGER = 'HoleComplete' THEN TRUE END) as agg_gp_2"),

        )
    )

    return out_df

# COMMAND ----------

def run_batch():
    spark = create_spark_session(name=f"{database}")
    checkpoint_location = create_fact_player_game_status_daily(spark, database, view_mapping)

    df = extract(environment,database)
    df = transform(df)

    load_fact_player_game_status_daily(spark, df, database, environment)

# COMMAND ----------

run_batch()
