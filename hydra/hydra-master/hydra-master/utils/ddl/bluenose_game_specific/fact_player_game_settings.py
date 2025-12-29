# Databricks notebook source
# MAGIC %run ../table_functions

# COMMAND ----------

# MAGIC %run ../../helpers

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

def create_fact_player_game_settings_daily(spark, database, view_mapping, properties={}):
    """
    Create the fact_player_game_settings_daily table in the specified environment.

    This function constructs a SQL command to create the fact_player_game_settings_daily
    table with predefined columns and executes it using the create_table function.
    create_table and environment are initiated as part of the table_functions

    Parameters:
    Input
        spark (SparkSession): The Spark session for executing the SQL command.
        title (str): The base title for the table (e.g., the database name).
        properties (dict, optional): A dictionary of properties to add to the table.
    Output
        A checkpoint location should be used for your checkpoint location.

    Example:
    Running it in a dev workspace
    create_fact_player_session(spark, 'wwe2k25', {'delta.enableIcebergCompatV2': 'true'})
    Output:  "dbfs:/tmp/wwe2k25/managed/streaming/run_dev/fact_player_game_status_daily"
    """

    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.fact_player_game_settings_daily (
        date DATE,
        player_id STRING,
        platform STRING,
        service STRING,
        game_type STRING,
        setting_instance_id STRING,
        setting_instance_first_seen_ts TIMESTAMP,
        setting_instance_last_seen_ts TIMESTAMP,
        setting_name STRING,
        country_code STRING,
        is_first_seen_player_setting BOOLEAN,
        player_current_setting_value STRING,
        player_previous_setting_value STRING,
        dw_insert_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        dw_update_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        merge_key STRING
    )   
    """

    properties = {
        "delta.feature.allowColumnDefaults": "supported" # this won't work until iceberg v3 is released
    }

    create_table(spark, sql, properties)
    create_agg_game_settings_daily_view(spark, database, view_mapping)
    return f"dbfs:/tmp/{database}/managed/batch/run{environment}/fact_player_game_settings_daily"

# COMMAND ----------

def create_agg_game_settings_daily_view(spark, database, mapping):
    """
    Create the agg_game_settings_daily view in the specified environment.

    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
        environment (str): The environment (e.g., 'dev', 'prod') where the view will be created.
        mapping (dict): A dictionary of column mappings.
    """
    sql = f"""
    CREATE OR REPLACE VIEW {database}{environment}.managed_view.fact_player_game_settings_daily AS (
        SELECT 
            date,
            player_id,
            platform,
            service,
            game_type,
            setting_instance_id,
            setting_instance_first_seen_ts,
            setting_instance_last_seen_ts,
            setting_name,
            country_code,
            is_first_seen_player_setting,
            player_current_setting_value,
            player_previous_setting_value,
            dw_insert_ts,
            dw_update_ts        
        FROM {database}{environment}.managed.fact_player_game_settings_daily
    )
    """

    spark.sql(sql)

# COMMAND ----------

def load_fact_player_game_settings_daily(spark, df, database, environment):
    """
    Load the data into the fact_player_game_settings_daily table in the specified environment.

    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        df (DataFrame): The DataFrame to load into the table.
        database (str): The database to load the table into.
        environment (str): The environment to load the table into.
    """

    target_table = DeltaTable.forName(spark, f"{database}{environment}.managed.fact_player_game_settings_daily")
    merge_key_logic = "sha2(concat_ws('|', date::string, player_id, platform, service, game_type, setting_instance_id, setting_name,country_code), 256)"
    merge_to_table(df, target_table, table_name=f"{database}{environment}.managed.fact_player_game_settings_daily", min_cols=["setting_instance_first_seen_ts", "is_first_seen_player_setting"], max_cols=["setting_instance_last_seen_ts"], exclude_cols=["date", "player_id", "platform", "service", "setting_instance_id", "setting_name"], merge_key_logic=merge_key_logic)