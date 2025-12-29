# Databricks notebook source
# MAGIC %run ../table_functions

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

def create_fact_player_character_game_status_daily(spark, database, environment, properties={}):
    """
    Create the fact_player_character_game_status_daily table in the specified environment.
    
    Parameters:
    - spark (SparkSession): Spark session.
    - database (str): Database name prefix (e.g., 'inverness').
    - environment (str): Suffix for environment (e.g., '_dev').
    - properties (dict, optional): Table properties.
    
    Returns:
    - None
    """
    
    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.fact_player_character_game_status_daily (
        player_platform_id STRING,
        player_id STRING,
        player_character_id STRING,
        date DATE,
        skill_tree_snapshot array<struct<skill_node_id:string,skill_node_points_allocated:bigint,branches:array<struct<skill_branch_id:string>>>>,
        skill_total_points_allocated BIGINT,
        vault_hunter STRING,
        max_player_level LONG,
        max_UVHM_level LONG,
        most_recent_mission STRING,
        main_story_completion DOUBLE,
        side_mission_completion DOUBLE,
        total_hours_played DOUBLE,
        multiplayer_hours_played DOUBLE,
        online_multiplayer_hours_played DOUBLE,
        split_screen_hours_played DOUBLE,
        poa_completion DOUBLE,
        times_beat_sol LONG,
        times_beat_liktor LONG,
        times_beat_callis_fortress LONG,
        times_beat_callis_elpis LONG,
        times_beat_tk_human LONG,
        times_beat_tk_guardian LONG,
        most_recent_boss STRING,
        most_recent_boss_outcome STRING,
        vehicle_use_count LONG,
        minutes_in_vehicle DOUBLE,
        meters_in_vehicle DOUBLE,
        meters_on_foot DOUBLE,
        times_downed LONG,
        times_deaths LONG,
        date_last_seen DATE,
        days_since_last_seen INT,
        days_since_install INT,
        max_difficulty_encountered LONG,
        player_difficulty_setting BIGINT,
        dw_insert_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        dw_update_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        merge_key STRING
    )
    """

    properties = {
        "delta.feature.allowColumnDefaults": "supported"
    }

    create_table(spark, sql, properties)

def create_fact_player_character_game_status_daily_view(spark, database, environment):
    """
    Create the create_fact_player_character_game_status_daily_view view in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
        mapping (dict): A dictionary of column mappings.
    """

    sql = f"""
    CREATE VIEW IF NOT EXISTS {database}{environment}.managed_view.fact_player_character_game_status_daily AS (
        SELECT
        *
        from {database}{environment}.managed.fact_player_character_game_status_daily
    )
    """
    spark.sql(sql)    