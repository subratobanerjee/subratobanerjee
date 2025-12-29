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

def create_agg_myplayer_progression_daily(spark, database):

    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.agg_myplayer_progression_daily (
        date DATE,
        platform STRING,
        service STRING,
        game_type STRING,
        total_ap_spent DOUBLE,
        avg_ap_spent DOUBLE,
        total_sp_spent DOUBLE,
        avg_sp_spent DOUBLE,
        total_ap_earned DOUBLE,
        total_sp_earned DOUBLE,
        max_level_reached INT,
        total_num_level_ups INT,
        dw_insert_ts TIMESTAMP,
        dw_update_ts TIMESTAMP,
        merge_key STRING
    )
        """

    properties = {
        "delta.feature.allowColumnDefaults": "supported" # this won't work until iceberg v3 is released
    }

    create_table(spark, sql, properties)
    create_agg_myplayer_progression_daily_view(spark, database, environment)
    return f"AGG_MYPLAYER_PROGRESSION_DAILY table created"

# COMMAND ----------

def create_agg_myplayer_progression_daily_view(spark, database, environment):
    """
    Create the agg_myplayer_progression_daily view in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
        mapping (dict): A dictionary of column mappings.
    """


    sql = f"""
    CREATE OR REPLACE VIEW {database}{environment}.managed_view.agg_myplayer_progression_daily AS (
        SELECT
        *
        from {database}{environment}.managed.agg_myplayer_progression_daily
    )
    """

    spark.sql(sql)

# COMMAND ----------

def read_player_progression(environment, min_date_inc):
    fact_player_prog_df = (
    spark
    .read
    .table(f"bluenose{environment}.managed.fact_myplayer_progression_daily")
    .select(
        'date',
        'platform',
        'service',
        'game_type',
        'total_ap_spent',
        'avg_ap_spent',
        'total_sp_spent',
        'avg_sp_spent',
        'ap_earned',
        'sp_earned',
        'max_level_reached',
        'num_level_ups',
    ).where((expr(f"date > to_date('{min_date_inc}') - INTERVAL 2 DAY"))
            )
    )
    return fact_player_prog_df

# COMMAND ----------

def extract(environment, database):
    prev_max_date = max_timestamp(spark, f"{database}{environment}.managed.agg_myplayer_progression_daily", "date")

    min_date = spark.sql(f"select ifnull(min(date), '1970-10-01')::date as min_date from {database}{environment}.managed.fact_myplayer_progression_daily where dw_insert_ts >= current_date - interval '2' day").collect()[0]['min_date']

    min_date_inc = min(prev_max_date, min_date)
    
    player_prog_df = read_player_progression(environment, min_date_inc)
    return player_prog_df

# COMMAND ----------

def transform(player_prog_df):
    transformed_df = player_prog_df.groupBy(
        "date",
        "platform",
        "service",
        "game_type"
    ).agg(
        expr("SUM(total_ap_spent) as total_ap_spent"),
        expr("AVG(avg_ap_spent) as avg_ap_spent"),
        expr("SUM(total_sp_spent) as total_sp_spent"),
        expr("AVG(avg_sp_spent) as avg_sp_spent"),
        expr("SUM(ap_earned) as total_ap_earned"),
        expr("SUM(sp_earned) as total_sp_earned"),
        expr("MAX(max_level_reached) as max_level_reached"),
        expr("SUM(num_level_ups) as total_num_level_ups")
    ).select(
        "date",
        "platform",
        "service",
        "game_type",
        "total_ap_spent",
        "avg_ap_spent",
        "total_sp_spent",
        "avg_sp_spent",
        "total_ap_earned",
        "total_sp_earned",
        "max_level_reached",
        "total_num_level_ups"
        )
    return transformed_df

# COMMAND ----------

def load_agg_myplayer_progression_daily(spark, df, database, environment):
    """
    Load the data into the fact_player_game_status_daily table in the specified environment.

    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        df (DataFrame): The DataFrame to load into the table.
        database (str): The database to load the table into.
        environment (str): The environment to load the table into.
    """

    # Perform the merge operation into the Delta table
    final_table = DeltaTable.forName(spark, f"{database}{environment}.managed.agg_myplayer_progression_daily")

    # get schema of the table
    table_schema = spark.read.table(f"{database}{environment}.managed.agg_myplayer_progression_daily").schema

    # create empty dataframe using the schema
    out_df = spark.createDataFrame([], table_schema)

    # union the incoming dataframe with the empty dataframe, we can set allowMissingColumns to False to be more strict
    df = df.select(
            "*",
            expr("CURRENT_TIMESTAMP() as dw_insert_ts"),
            expr("CURRENT_TIMESTAMP() as dw_update_ts"),
            expr("sha2(concat_ws('|', date, platform, service, game_type), 256) as merge_key")
        )
    out_df = out_df.unionByName(df, allowMissingColumns=True)

    # dynamically setup merge condition based on the aggregate columns in this database's fact_player_game_status_daily table
    agg_cols = [
    'total_ap_spent', 'avg_ap_spent', 'total_sp_spent', 'avg_sp_spent', 'total_ap_earned', 'total_sp_earned', 'max_level_reached', 'total_num_level_ups',
    ]
    
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
    create_agg_myplayer_progression_daily(spark, database)
    df = extract(environment, database)
    df = transform(df)
    load_agg_myplayer_progression_daily(spark, df, database, environment)

# COMMAND ----------

run_batch()
