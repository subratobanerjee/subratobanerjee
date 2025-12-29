# Databricks notebook source
# MAGIC %run ../table_functions

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

def fact_player_game_status_daily(spark, database, environment, properties={}):
    """
    Create the fact_player_game_status_daily table in the specified environment.
    
    Parameters:
    - spark (SparkSession): Spark session.
    - database (str): Database name prefix (e.g., 'inverness').
    - environment (str): Suffix for environment (e.g., '_dev').
    - properties (dict, optional): Table properties.
    
    Returns:
    - None
    """

    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.fact_player_game_status_daily (
        player_platform_id STRING,
        player_id STRING,
        date DATE,
        max_player_level BIGINT,
        max_UVHM_level BIGINT,
        most_recent_mission STRING,
        main_story_completion DOUBLE,
        side_mission_completion DOUBLE,
        hours_played DOUBLE,
        multiplayer_hours_played DOUBLE,
        online_multiplayer_hours_played DOUBLE,
        split_screen_hours_played DOUBLE,
        poa_completion DOUBLE,
        times_beat_sol BIGINT,
        times_beat_liktor BIGINT,
        times_beat_callis_fortress BIGINT,
        times_beat_callis_elpis BIGINT,
        times_beat_tk_human BIGINT,
        times_beat_tk_guardian BIGINT,
        most_recent_boss STRING,
        most_recent_boss_outcome STRING,
        vehicle_use_count BIGINT,
        minutes_in_vehicle DOUBLE,
        meters_in_vehicle DOUBLE,
        meters_on_foot DOUBLE,
        times_downed BIGINT,
        times_deaths BIGINT,
        date_last_seen DATE,
        days_since_last_seen INT,
        days_since_install INT,
        max_difficulty_encountered BIGINT,
        player_difficulty_setting BIGINT,
        max_amon_level BIGINT,
        max_vex_level BIGINT,
        max_harlowe_level BIGINT,
        max_rafa_level BIGINT,
        dw_update_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        dw_insert_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        merge_key STRING
    )
    """

    # Ensure Delta Lake column default feature is enabled
    properties.update({
        "delta.feature.allowColumnDefaults": "supported"
    })

    create_table(spark, sql, properties)

def fact_player_game_status_daily_view(spark, database, environment):
    """
    Create the fact_player_game_status_daily_view view in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
        mapping (dict): A dictionary of column mappings.
    """

    sql = f"""
    CREATE VIEW IF NOT EXISTS {database}{environment}.managed_view.fact_player_game_status_daily AS (
        SELECT
        *
        from {database}{environment}.managed.fact_player_game_status_daily
    )
    """
    spark.sql(sql)