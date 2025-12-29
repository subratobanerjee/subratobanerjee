# Databricks notebook source
# MAGIC %run ../table_functions

# COMMAND ----------

def create_fact_player_activity(spark, database, properties={}):
    """
    Create the fact_player_activity table in the specified environment.

    This function constructs a SQL command to create the fact_player_activity 
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
    create_fact_player_activity(spark, 'wwe2k25', {'delta.enableIcebergCompatV2': 'true'})
    Output:  "dbfs:/tmp/wwe2k25/intermediate/streaming/run_dev/fact_player_activity"
    """
    
    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.intermediate.fact_player_activity (
        player_id STRING,
        parent_id STRING,
        platform STRING,
        service STRING,
        country_code STRING,
        received_on TIMESTAMP,
        source_table STRING,
        desc STRING,
        event_trigger STRING,
        session_id STRING,
        build_changelist STRING,
        language_setting STRING,
        extra_info_1 STRING,
        extra_info_2 STRING,
        extra_info_3 STRING,
        extra_info_4 STRING,
        extra_info_5 STRING,
        extra_info_6 STRING,
        extra_info_7 STRING,
        extra_info_8 STRING,
        extra_info_9 STRING,
        extra_info_10 STRING,
        dw_insert_date date,
        dw_insert_ts TIMESTAMP,
        dw_update_ts TIMESTAMP
    )
    comment 'the table will have the player activity from all or most of the events that includes ingame evetns and logins'
    partitioned by (platform,source_table,dw_insert_date)

    """

    create_table(spark, sql, properties)
    return f"dbfs:/tmp/{database}/intermediate/streaming/run{environment}/fact_player_activity"
