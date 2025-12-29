# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/managed/fact_player_session_daily


# COMMAND ----------
from pyspark.sql.functions import expr
from delta.tables import DeltaTable

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'bluenose'
data_source ='dna'
title = 'PGA Tour 2K25'

view_mapping = {
    'solo_instances': 'solo_instances',
    'local_multiplayer_instances': 'local_multiplayer_instances',
    'online_multiplayer_instances': 'online_multiplayer_instances',
    'session_count': 'session_count',
    'session_len_sec': 'session_len_sec',
    'session_avg_len_sec': 'session_avg_len_sec',
    'session_mode_len_sec': 'session_mode_len_sec',
    'agg_1': 'agg_1 as session_median_len_sec',
    'agg_2': 'agg_2 as num_hole_complete',
    'agg_3': 'agg_3 as num_round_complete',
    'agg_4': 'agg_4 as avg_hole_complete',
    'agg_5': 'agg_5 as avg_round_complete'
}
agg_columns = {
    "solo_instances": expr("sum(solo_instances) as solo_instances"),
    "local_multiplayer_instances": expr("sum(local_multiplayer_instances) as local_multiplayer_instances"),
    "online_multiplayer_instances": expr("sum(online_multiplayer_instances) as online_multiplayer_instances"),
    "session_count": expr("count(distinct application_session_id) as session_count"),
    "session_len_sec": expr( "sum(UNIX_TIMESTAMP(SESSION_END_TS) - UNIX_TIMESTAMP(SESSION_START_TS)) as session_len_sec"),
    "session_avg_len_sec": expr( "avg(UNIX_TIMESTAMP(SESSION_END_TS) - UNIX_TIMESTAMP(SESSION_START_TS)) as session_avg_len_sec"),
    "session_mode_len_sec": expr("mode(UNIX_TIMESTAMP(SESSION_END_TS) - UNIX_TIMESTAMP(SESSION_START_TS)) as session_mode_len_sec"),
    "agg_1": expr("median(UNIX_TIMESTAMP(SESSION_END_TS) - UNIX_TIMESTAMP(SESSION_START_TS)) as agg_1"),
    "agg_2": expr("sum(agg_1) as agg_2"),
    "agg_3": expr("sum(agg_2) as agg_3"),
    "agg_4": expr("avg(agg_1) as agg_4"),
    "agg_5": expr("avg(agg_2) as agg_5")
    }

# COMMAND ----------
def read_player_session(environment,min_received_on):
    
    player_session_df = (spark
                         .read
                         .table(f"bluenose{environment}.intermediate.fact_player_session")
                         .where((expr(f"dw_insert_ts::timestamp::date > to_date('{min_received_on}') - INTERVAL 2 DAY"))))

    return player_session_df

def extract(environment, database):
    prev_max_ts = max_timestamp(spark, f"{database}{environment}.managed.fact_player_session_daily", "date")
    min_received_on = spark.sql(f"select ifnull(min(received_on), '1970-10-01')::date as min_received_on from {database}{environment}.intermediate.fact_player_progression where dw_insert_ts >= current_date - interval '2' day").collect()[0]['min_received_on']
    min_received_on_inc = min(prev_max_ts, min_received_on)
    
    player_sesion_df = read_player_session(environment, min_received_on_inc)
    return player_sesion_df


# COMMAND ----------

def transform(df,agg_columns):
    out_df = (
        df
        .groupBy(
            expr("session_end_ts::date as date"),
            "player_id",
            "platform",
            "service",
            "country_code",
            "session_type"
        )
        .agg(*agg_columns.values())
    )

    return out_df

# COMMAND ----------

def run_batch():
    spark = create_spark_session(name=f"{database}")
    checkpoint_location = create_fact_player_session_daily(spark, database, view_mapping)

    df = extract(environment,database)
    df = transform(df,agg_columns)

    load_fact_player_session_daily(spark, df, agg_columns, database, environment)

# COMMAND ----------

run_batch()