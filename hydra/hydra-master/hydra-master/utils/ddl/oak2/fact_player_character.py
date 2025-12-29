# Databricks notebook source
# MAGIC %run ../table_functions

# COMMAND ----------

from delta.tables import DeltaTable


# COMMAND ----------
def create_fact_player_character(spark, database, environment, properties={}):
    """
    Create the fact_player_character table in the specified environment.
    Parameters:
    - spark (SparkSession): Spark session.
    - database (str): Database name prefix (e.g., 'inverness').
    - environment (str): Suffix for environment (e.g., '_dev').
    - properties (dict, optional): Table properties.
    Returns:
    - None
    """

    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.fact_player_character (
        player_id STRING,
        player_platform_id STRING,
        player_character_id STRING,
        player_character STRING,
        player_character_num INT,
        player_character_create_ts TIMESTAMP,
        player_level BIGINT,
        prison_complete_ts TIMESTAMP,
        beach_complete_ts TIMESTAMP,
        beach_max_ts TIMESTAMP,
        beach_complete_time_played DOUBLE,
        silo_claim_ts TIMESTAMP,
        vehicle_scan_ts TIMESTAMP,
        dw_insert_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        dw_update_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        merge_key STRING
    )
    """

    properties = {
        "delta.feature.allowColumnDefaults": "supported"
    }

    create_table(spark, sql, properties)

def create_fact_player_character_view(spark, database, environment):
    """
    Create the fact_player_character view in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
        mapping (dict): A dictionary of column mappings.
    """

    sql = f"""
    CREATE VIEW IF NOT EXISTS {database}{environment}.managed_view.fact_player_character AS (
        SELECT
        *
        from {database}{environment}.managed.fact_player_character
    )
    """
    spark.sql(sql)