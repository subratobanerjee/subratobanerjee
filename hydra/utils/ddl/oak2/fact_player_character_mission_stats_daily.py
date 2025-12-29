# Databricks notebook source
# MAGIC %run ../table_functions

# COMMAND ----------

from delta.tables import DeltaTable


# COMMAND ----------
def create_fact_player_character_mission_stats_daily(spark, database, environment, properties={}):
    """
    Create the fact_player_character_mission_stats_daily table in the specified environment.

    Parameters:
    - spark (SparkSession): Spark session.
    - database (str): Database name prefix (e.g., 'inverness').
    - environment (str): Suffix for environment (e.g., '_dev').
    - properties (dict, optional): Table properties.

    Returns:
    - None
    """

    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.fact_player_character_mission_stats_daily (
        player_id STRING,
        player_platform_id STRING,
        player_character_id STRING,
        date DATE,
        mission_name STRING,
        mission_set STRING,
        mission_type STRING,
        is_complete BOOLEAN,
        is_replay BOOLEAN,
        steps_completed INTEGER,
        percent_of_mission_completed DOUBLE,
        time_spent_at_start DOUBLE,
        time_spent_at_end DOUBLE,
        time_spent_in_mission DOUBLE,
        last_step_completed STRING,
        starting_player_level BIGINT,
        ending_player_level BIGINT,
        average_enemy_level DOUBLE,
        min_enemy_level BIGINT,
        max_enemy_level BIGINT,
        min_uvh_level BIGINT,
        max_uvh_level BIGINT,
        dw_insert_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        dw_update_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        merge_key STRING
    )
    """

    properties = {
        "delta.feature.allowColumnDefaults": "supported"
    }

    create_table(spark, sql, properties)

def create_fact_player_character_mission_stats_daily_view(spark, database, environment):
    """
    Create the fact_player_character_mission_stats_daily view in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
        mapping (dict): A dictionary of column mappings.
    """

    sql = f"""
    CREATE VIEW IF NOT EXISTS {database}{environment}.managed_view.fact_player_character_mission_stats_daily AS (
        SELECT
        *
        from {database}{environment}.managed.fact_player_character_mission_stats_daily
    )
    """
    spark.sql(sql)