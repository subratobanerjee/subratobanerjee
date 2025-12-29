# Databricks notebook source
# MAGIC %run ../table_functions

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

def create_agg_game_settings_daily(spark, database, view_mapping, properties={}):
    """
    Create the agg_game_settings_daily table in the specified environment.
    This function constructs a SQL command to create the agg_game_settings_daily
    table with predefined columns and executes it using the create_table function.
    create_table and environment are initiated as part of the table_functions.
    
    Parameters:
    Input:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The base title for the table (e.g., the database name).
        environment (str): The environment in which the table will be created.
        properties (dict, optional): A dictionary of properties to add to the table.
    Output:
        A checkpoint location for your checkpoint.
    Example:
    Running it in a dev workspace:
    create_fact_player_session(spark, 'bluenose', {'delta.enableIcebergCompatV2': 'true'})
    Output:  "dbfs:/tmp/bluenose/managed/streaming/run_dev/agg_game_settings_daily"
    """

    # Define the SQL query to create the table
    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.agg_game_settings_daily (
        date DATE,
        platform STRING,
        service STRING,
        agg_settings_change_count BIGINT,
        agg_new_settings_count BIGINT,
        agg_new_swing_stick_settings_count BIGINT,
        agg_new_three_clicks_settings_count BIGINT,
        agg_swing_stick_settings_change_count BIGINT,
        agg_three_clicks_settings_change_count BIGINT,
        agg_swing_stick_settings_change_player_count BIGINT,
        agg_three_clicks_settings_change_player_count BIGINT,
        dw_insert_ts TIMESTAMP,
        dw_update_ts TIMESTAMP,
        merge_key STRING



    )
    """

    properties = {
        "delta.feature.allowColumnDefaults": "supported"  # Optional: Set table properties, specific to Delta
    }
    create_table(spark, sql, properties)  
    create_agg_game_settings_daily_view(spark, database, view_mapping)  
    return

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
    CREATE OR REPLACE VIEW {database}{environment}.managed_view.agg_game_settings_daily AS (
        SELECT 
            date,
            platform,
            service,
            agg_settings_change_count,
            agg_new_settings_count,
            agg_new_swing_stick_settings_count,
            agg_new_three_clicks_settings_count,
            agg_swing_stick_settings_change_count,
            agg_three_clicks_settings_change_count,
            agg_swing_stick_settings_change_player_count,
            agg_three_clicks_settings_change_player_count,
            dw_insert_ts,
            dw_update_ts
        FROM {database}{environment}.managed.agg_game_settings_daily
    )
    """

    spark.sql(sql) 
