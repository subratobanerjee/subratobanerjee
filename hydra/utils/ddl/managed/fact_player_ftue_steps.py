# Databricks notebook source
# MAGIC %run ../table_functions

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------
def create_fact_player_ftue_step(spark, database, environment, properties={}):
    """
    Create the fact_player_ftue_step table in the specified environment.

    Parameters:
    - spark (SparkSession): Spark session.
    - database (str): Database name prefix (e.g., 'inverness').
    - environment (str): Suffix for environment (e.g., '_dev').
    - properties (dict, optional): Table properties.

    Returns:
    - None
    """

    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.fact_player_ftue_step (
        player_id STRING,
        player_platform_id STRING,
        player_character_id STRING,
        player_character STRING,
        platform STRING,
        service STRING,
        completed_session_id STRING,
        max_session_players INT,
        avg_session_players DOUBLE,
        max_local_players BIGINT,
        avg_local_players DOUBLE,
        max_online_players BIGINT,
        avg_online_players DOUBLE,
        mission_name STRING,
        mission_objective_id STRING,
        ftue_step_num INT,
        start_ts TIMESTAMP,
        complete_ts TIMESTAMP,
        starting_level BIGINT,
        ending_level BIGINT,
        starting_time_played DOUBLE,
        ending_time_played DOUBLE,
        mission_step_completed BOOLEAN,
        mission_step_failed BOOLEAN,
        time_to_complete_mins DOUBLE,
        dw_update_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        dw_insert_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        merge_key STRING
    )
    """

    properties = {
        "delta.feature.allowColumnDefaults": "supported"
    }

    create_table(spark, sql, properties)

def create_fact_player_ftue_step_view(spark, database, environment):
    """
    Create the create_fact_player_ftue_step_view view in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
        mapping (dict): A dictionary of column mappings.
    """

    sql = f"""
    CREATE VIEW IF NOT EXISTS {database}{environment}.managed_view.fact_player_ftue_step AS (
        SELECT
        *
        from {database}{environment}.managed.fact_player_ftue_step
    )
    """
    spark.sql(sql)
