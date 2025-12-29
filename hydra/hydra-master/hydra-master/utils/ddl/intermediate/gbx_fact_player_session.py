# Databricks notebook source
# MAGIC %run ../table_functions

# COMMAND ----------

def create_fact_player_session(spark, database, properties={}):
    """
    Create the fact_player_session table in the specified environment.

    This function constructs a SQL command to create the fact_player_session 
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
    Output:  "dbfs:/tmp/wwe2k25/intermediate/streaming/run_dev/fact_player_session"
    """
    
    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.intermediate.fact_player_session (
        application_session_id STRING,
        window STRUCT<start: TIMESTAMP, end: TIMESTAMP>,
        player_id STRING,
        player_platform_id STRING,
        platform STRING,
        service STRING,
        hardware STRING,
        session_type STRING DEFAULT 'application',
        session_start_ts TIMESTAMP,
        session_end_ts TIMESTAMP,
        local_multiplayer_instances INT DEFAULT -1,
        online_multiplayer_instances INT DEFAULT -1,
        solo_instances INT DEFAULT -1,
        agg_1 DECIMAL(10,2) DEFAULT -1,
        agg_2 DECIMAL(10,2) DEFAULT -1,
        agg_3 DECIMAL(10,2) DEFAULT -1,
        agg_4 DECIMAL(10,2) DEFAULT -1,
        agg_5 DECIMAL(10,2) DEFAULT -1,
        dw_insert_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        dw_update_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        merge_key STRING
    )
    """
    properties = {
        "delta.feature.allowColumnDefaults": "supported" # this won't work until iceberg v3 is released
    }

    create_table(spark, sql, properties)
    return f"dbfs:/tmp/{database}/intermediate/streaming/run{environment}/fact_player_session"