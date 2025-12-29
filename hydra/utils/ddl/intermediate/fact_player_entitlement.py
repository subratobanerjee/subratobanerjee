# Databricks notebook source
# MAGIC %run ../table_functions

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

def create_fact_player_entitlement(spark, database, environment, properties={}):
    """
    Create the fact_player_entitlement table in the specified environment.
    This function constructs a SQL command to create the fact_player_entitlement
    table with predefined columns and executes it using the create_table function.
    create_table and environment are initiated as part of the table_functions.
    
    Parameters:
    Input:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The base title for the table (e.g., the database name).
        properties (dict, optional): A dictionary of properties to add to the table.
    Output:
        A checkpoint location should be used for your checkpoint location.
    Example:
    Running it in a dev workspace:
    create_fact_player_session(spark, 'bluenose', {'delta.enableIcebergCompatV2': 'true'})
    Output:  "dbfs:/tmp/bluenose/intermediate/streaming/run_dev/fact_player_entitlement"
    """

    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.intermediate.fact_player_entitlement (
        player_id STRING,
        platform STRING,
        service STRING,
        game_type STRING,
        entitlement_id STRING,
        first_seen_country_code STRING,
        first_seen TIMESTAMP,
        last_seen TIMESTAMP,
        entitlement_name STRING,
        entitlement_type STRING,
        purchase_price DOUBLE,
        dw_insert_ts TIMESTAMP,
        dw_update_ts TIMESTAMP,
        merge_key STRING
    )
    """

    properties = {
        "delta.feature.allowColumnDefaults": "supported"  # this won't work until iceberg v3 is released
    }

    create_table(spark, sql, properties)
    return f"dbfs:/tmp/{database}/intermediate/streaming/run{environment}/fact_player_entitlement"
