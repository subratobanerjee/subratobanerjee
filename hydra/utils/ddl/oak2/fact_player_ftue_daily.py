# Databricks notebook source
# MAGIC %run ../table_functions

# COMMAND ----------

def create_fact_player_ftue_daily(spark, database, environment,properties={}):
    """
    Creates the fact_player_ftue_daily table in the specified environment.

    This function constructs a SQL command to create the fact_player_ftue_daily
    table with predefined columns and executes it using the create_table function.
    The 'create_table' and 'environment' variables are initiated from the included 
    'table_functions' notebook.

    Parameters:
    Input:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The target database name (e.g., 'oak2').
        properties (dict, optional): A dictionary of properties to add to the table.
    Output:
        A string representing the checkpoint location for streaming operations.

    Example:
    fact_player_ftue_daily(spark, 'oak2')
    Output: "dbfs:/tmp/oak2/managed/streaming/run_dev/fact_player_ftue_daily"
    """

    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.fact_player_ftue_daily (
        player_id STRING,
        player_platform_id STRING,
        player_character_id STRING,
        player_character STRING,
        player_character_num INT,
        platform STRING,
        service STRING,
        minutes_in_prison DOUBLE,
        minutes_in_prison_and_beach DOUBLE,
        minutes_in_beach DOUBLE,
        is_complete_prison BOOLEAN,
        is_complete_beach BOOLEAN,
        player_character_create_ts TIMESTAMP,
        local_player_count_prison INT,
        online_player_count_prison INT,
        total_player_count_prison INT,
        local_player_count_beach INT,
        online_player_count_beach INT,
        total_player_count_beach INT,
        completed_intro_prison BOOLEAN,
        minutes_complete_intro_prison DECIMAL(18, 2),
        days_complete_intro_prison INT,
        sessions_complete_intro_prison INT,
        completed_beach BOOLEAN,
        minutes_complete_beach DECIMAL(18, 2),
        days_complete_beach INT,
        sessions_complete_beach INT,
        completed_first_silo BOOLEAN,
        minutes_claim_first_silo DECIMAL(18, 2),
        days_claim_first_silo INT,
        sessions_claim_first_silo INT,
        completed_unlock_vehicle BOOLEAN,
        minutes_unlock_vehicle DECIMAL(18, 2),
        days_unlock_vehicle INT,
        sessions_unlock_vehicle INT,
        action_skill_used_beach BOOLEAN,
        used_vehicle_same_day BOOLEAN,
        grenade_used_beach BOOLEAN,
        heavy_weapon_used_beach BOOLEAN,
        repkit_uses_prison INT,
        repkit_uses_beach INT,
        downs_prison INT,
        deaths_prison INT,
        downs_beach INT,
        deaths_beach INT,
        go_west_on_beach_to_camp BOOLEAN,
        player_character_main_trunk STRING,
        completed_FTUE BOOLEAN,
        minutes_complete_FTUE DECIMAL(18, 2),
        sessions_complete_FTUE INT,
        days_complete_FTUE INT,
        merge_key STRING,
        dw_insert_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        dw_update_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )
    """

    # Default properties for enabling column default values in Delta
    default_properties = {
        "delta.feature.allowColumnDefaults": "supported"
    }
    # Merge user-provided properties with defaults
    final_properties = {**default_properties, **properties}

    create_table(spark, sql, final_properties)
    
    # Return the conventional checkpoint location for this table
    return f"dbfs:/tmp/{database}/managed/batch/run{environment}/fact_player_ftue_daily"


# COMMAND ----------

def create_fact_player_ftue_daily_view(spark, database):
    """
    Create the fact_player_ftue_daily view in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
        mapping (dict): A dictionary of column mappings.
    """

    sql = f"""
    CREATE VIEW IF NOT EXISTS {database}{environment}.managed_view.fact_player_ftue_daily AS (
        SELECT
        *
        from {database}{environment}.managed.fact_player_ftue_daily
    )
    """
    spark.sql(sql)