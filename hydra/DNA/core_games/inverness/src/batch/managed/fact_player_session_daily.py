# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/table_functions

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/managed/fact_player_session_daily

# COMMAND ----------

from pyspark.sql.functions import expr

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'inverness'
data_source ='dna'
title = 'Civilization VII'
view_mapping = {
    'session_count': 'session_count',
    'session_len_sec': 'session_len_sec',
    'session_avg_len_sec': 'session_avg_len_sec',
    'session_mode_len_sec': 'session_mode_len_sec',
    'agg_1': 'agg_1 as session_median_len_sec',
    'agg_2': 'agg_2 as num_turns_played',
    'agg_3': 'agg_3 as num_app_suspends',
    'agg_4': 'agg_4 as avg_session_turns',
    'agg_5': 'agg_5 as median_session_turns'
}
agg_columns = {
    "session_count": expr("count(distinct application_session_id) as session_count"),
    "session_len_sec": expr("sum((UNIX_TIMESTAMP(session_end_ts) - UNIX_TIMESTAMP(session_start_ts)) ) as session_len_sec"),
    "session_avg_len_sec": expr("avg((UNIX_TIMESTAMP(session_end_ts) - UNIX_TIMESTAMP(session_start_ts)) ) as session_avg_len_sec"),
    "session_mode_len_sec": expr("mode((UNIX_TIMESTAMP(session_end_ts) - UNIX_TIMESTAMP(session_start_ts)) ) as session_mode_len_sec"),
    "agg_1": expr("median((UNIX_TIMESTAMP(session_end_ts) - UNIX_TIMESTAMP(session_start_ts)) ) as agg_1"),
    "agg_2": expr("sum(agg_1) as agg_2"),
    "agg_3": expr("sum(agg_2) as agg_3"),
    "agg_4": expr("avg(agg_1) as agg_4"),
    "agg_5": expr("median(agg_1) as agg_5")
    }

# COMMAND ----------

def extract(environment):

    prev_max = max_timestamp(spark, f"inverness{environment}.managed.fact_player_session_daily", 'dw_insert_ts')

    current_min = (
        spark.read
        .table(f"inverness{environment}.intermediate.fact_player_session")
        .where(expr("dw_insert_ts::date between current_date - 3 and current_date"))
        .select(expr(f"ifnull(min(session_start_ts::timestamp::date),'1999-01-01')::date as min_date"))
    ).collect()[0]['min_date']

    inc_min_date = min(prev_max,current_min)

    sesh_df =  (
        spark
        .read
        .table(f"inverness{environment}.intermediate.fact_player_session")
        .where(expr(f"player_id !='anonymous' and player_id is not null and player_id NOT LIKE '%NULL%' and session_start_ts::date >= to_date('{inc_min_date}')"))

    )

    return sesh_df

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

def load_inverness_fact_player_session_daily(spark, df, agg_columns, database, environment):

    # Perform the merge operation into the Delta table
    final_table = DeltaTable.forName(spark, f"{database}{environment}.managed.fact_player_session_daily")

    # get schema of the table
    table_schema = spark.read.table(f"{database}{environment}.managed.fact_player_session_daily").schema

    # create empty dataframe using the schema
    out_df = spark.createDataFrame([], table_schema)

    # union the incoming dataframe with the empty dataframe, we can set allowMissingColumns to False to be more strict
    df = df.selectExpr(
            "*",
            "CURRENT_TIMESTAMP() as dw_insert_ts",
            "CURRENT_TIMESTAMP() as dw_update_ts",
            "SHA2(CONCAT_WS('|', DATE, PLAYER_ID, PLATFORM, SERVICE, COUNTRY_CODE, SESSION_TYPE), 256) as merge_key"
        )
    out_df = out_df.unionByName(df, allowMissingColumns=True)

    # dynamically setup merge condition based on the aggregate columns in this database's fact_player_session_daily table
    agg_cols = [col_name for col_name in out_df.columns if 'agg_' in col_name]
    merge_condition = " OR ".join(f"old.{col_name} <> new.{col_name}" for col_name in agg_cols)

    # create the update column dict
    update_set = {}
    for col_name in agg_cols:
        update_set[f"old.{col_name}"] = f"new.{col_name}"
    
    update_set[f"old.dw_update_ts"] = "CURRENT_TIMESTAMP()"

    # merge the table
    (
        final_table.alias('old')
        .merge(
            out_df.alias('new'),
            "new.merge_key = old.merge_key"
        )
        .whenMatchedUpdate(condition=merge_condition, set=update_set)
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

def run_batch():
    spark = create_spark_session(name=f"{database}")
    checkpoint_location = create_fact_player_session_daily(spark, database, view_mapping)

    df = extract(environment)
    df = transform(df,agg_columns)

    load_inverness_fact_player_session_daily(spark, df, agg_columns, database, environment)

# COMMAND ----------

run_batch()
