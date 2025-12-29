# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/table_functions

# COMMAND ----------

from pyspark.sql.functions import expr
from delta.tables import DeltaTable

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'bluenose'
data_source ='dna'
title = 'PGA Tour 2K25'

# COMMAND ----------

def create_fact_myplayer_subject_progression_daily(spark, database):

    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.fact_myplayer_subject_progression_daily (
        date DATE,
        player_id STRING,
        platform STRING,
        service STRING,
        my_player_id STRING,
        subject_id STRING,
        game_type STRING,
        subject_count INT,
        ap_spent INT,
        sp_spent INT,
        min_subject_value DOUBLE,
        max_subject_value DOUBLE,
        dw_insert_ts TIMESTAMP,
        dw_update_ts TIMESTAMP,
        merge_key STRING
    )
        """

    properties = {
        "delta.feature.allowColumnDefaults": "supported" # this won't work until iceberg v3 is released
    }

    create_table(spark, sql, properties)
    create_fact_myplayer_subject_progression_daily_view(spark, database, environment)
    return f"Table fact_myplayer_progression_daily created"


# COMMAND ----------

def create_fact_myplayer_subject_progression_daily_view(spark, database, environment):
    """
    Create the fact_myplayer_subject_progression_daily view in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
        mapping (dict): A dictionary of column mappings.
    """


    sql = f"""
    CREATE OR REPLACE VIEW {database}{environment}.managed_view.fact_myplayer_subject_progression_daily AS (
        SELECT
        *
        from {database}{environment}.managed.fact_myplayer_subject_progression_daily
    )
    """

    spark.sql(sql)

# COMMAND ----------

def read_player_progression(environment, min_received_on):
    player_prog_df = (
    spark
    .read
    .table(f"bluenose{environment}.intermediate.fact_player_progression")
    .select(
        expr("received_on::date as date"),
        "player_id",
        "platform",
        "service",
        expr("primary_instance_id as my_player_id"),
        "subject_id",
        expr("extra_info_2 as current_subject_value"),
        expr("extra_info_6 as ap_spent"),
        expr("extra_info_7 as sp_spent"),
        expr("extra_info_8 as game_type"),
    ).where((expr(f"received_on::timestamp::date > to_date('{min_received_on}') - INTERVAL 2 DAY")) & 
            (expr("current_subject_value is not null")) &
            (expr("subject_id not in ('attributepointsspent', 'attributepointsremaining', 'skillpointsspent', 'skillpointsremaining', 'ovr', 'myplayerlevel', 'GreenSkills31')"))
            )
    )
    return player_prog_df

# COMMAND ----------

def extract(environment, database):
    prev_max_ts = max_timestamp(spark, f"{database}{environment}.managed.fact_myplayer_subject_progression_daily", "date")
    min_received_on = spark.sql(f"select ifnull(min(received_on), '1970-10-01')::date as min_received_on from {database}{environment}.intermediate.fact_player_progression where dw_insert_ts >= current_date - interval '2' day").collect()[0]['min_received_on']
    min_received_on_inc = min(prev_max_ts, min_received_on)
    print(min_received_on_inc)
    player_prog_df = read_player_progression(environment, min_received_on_inc)
    return player_prog_df

# COMMAND ----------

def transform(player_prog_df):
    transformed_df = player_prog_df.groupBy(
        "date",
        "player_id",
        "platform",
        "service",
        "my_player_id",
        "game_type",
        "subject_id",
    ).agg(
        expr("count(subject_id) as subject_count"),
        expr("coalesce(sum(ap_spent), 0) as ap_spent"),
        expr("coalesce(sum(sp_spent), 0) as sp_spent"),
        expr("coalesce(min(if(subject_id ilike '%attribute%', current_subject_value, null)), 0) as min_subject_value"),
        expr("coalesce(max(if(subject_id ilike '%attribute%', current_subject_value, null)), 0) as max_subject_value")
    )
    return transformed_df

# COMMAND ----------

def load_fact_myplayer_subject_progression_daily(spark, df, database, environment):
    """
    Load the data into the fact_player_game_status_daily table in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        df (DataFrame): The DataFrame to load into the table.
        database (str): The database to load the table into.
        environment (str): The environment to load the table into.
    """

    # Perform the merge operation into the Delta table
    final_table = DeltaTable.forName(spark, f"{database}{environment}.managed.fact_myplayer_subject_progression_daily")

    # get schema of the table
    table_schema = spark.read.table(f"{database}{environment}.managed.fact_myplayer_subject_progression_daily").schema

    # create empty dataframe using the schema
    out_df = spark.createDataFrame([], table_schema)

    # union the incoming dataframe with the empty dataframe, we can set allowMissingColumns to False to be more strict
    df = df.select(
            "*",
            expr("CURRENT_TIMESTAMP() as dw_insert_ts"),
            expr("CURRENT_TIMESTAMP() as dw_update_ts"),
            expr("sha2(concat_ws('|', date, player_id, platform, service, my_player_id, game_type, subject_id), 256) as merge_key")
        )
    out_df = out_df.unionByName(df, allowMissingColumns=True)

    # dynamically setup merge condition based on the aggregate columns in this database's fact_player_game_status_daily table
    agg_cols = ['subject_count', 'ap_spent', 'sp_spent', 'min_subject_value', 'max_subject_value']

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
    create_fact_myplayer_subject_progression_daily(spark, database)
    df = extract(environment, database)
    df = transform(df)
    load_fact_myplayer_subject_progression_daily(spark, df, database, environment)

# COMMAND ----------

run_batch()

# COMMAND ----------


