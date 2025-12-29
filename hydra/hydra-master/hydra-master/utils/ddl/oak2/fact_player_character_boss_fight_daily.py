# Databricks notebook source
# MAGIC %run ../table_functions

# COMMAND ----------

def create_fact_boss_fight_daily_summary(spark, database, environment,properties={}):
    """
    Creates the fact_boss_fight_daily_summary table in the specified environment.

    This function constructs a SQL command to create the fact_boss_fight_daily_summary
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
    fact_boss_fight_daily_summary(spark, 'oak2')
    Output: "dbfs:/tmp/oak2/managed/streaming/run_dev/fact_player_character_boss_fight_daily"
    """

    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.fact_player_character_boss_fight_daily (
            player_id STRING,
            player_platform_id STRING,
            player_character_id STRING,
            date DATE,
            boss_name STRING,
            build_changelist BIGINT,
            boss_name_clean STRING,
            times_attempted BIGINT,
            times_succeeded BIGINT,
            times_failed BIGINT,
            is_featured BOOLEAN,
            is_world_boss BOOLEAN,
            time_in_boss_fight_mins DOUBLE,
            median_time_to_beat_mins DOUBLE,
            median_time_to_fail_mins DOUBLE,
            median_player_level_success INT,
            median_player_level_fail INT,
            percent_hp_remaining DOUBLE,
            dw_update_ts TIMESTAMP,
            dw_insert_ts TIMESTAMP,
            merge_key STRING
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
    return f"dbfs:/tmp/{database}/managed/batch/run{environment}/fact_boss_fight_daily_summaryy"


# COMMAND ----------

def create_fact_player_character_boss_fight_daily_view(spark, database, environment):
    """
    Create the fact_player_character_boss_fight_daily view in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
        mapping (dict): A dictionary of column mappings.
    """

    sql = f"""
    CREATE VIEW IF NOT EXISTS {database}{environment}.managed_view.fact_player_character_boss_fight_daily AS (
        SELECT
        *
        from {database}{environment}.managed.fact_player_character_boss_fight_daily
    )
    """
    spark.sql(sql)