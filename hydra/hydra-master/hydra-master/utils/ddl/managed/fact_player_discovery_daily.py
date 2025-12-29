# Databricks notebook source
# MAGIC %run ../table_functions

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------
def create_fact_player_discovery_daily(spark, database, environment, properties={}):
    """
    Create the fact_player_discovery_daily table in the specified environment.

    Parameters:
    - spark (SparkSession): Spark session.
    - database (str): Database name prefix (e.g., 'inverness').
    - environment (str): Suffix for environment (e.g., '_dev').
    - properties (dict, optional): Table properties.

    Returns:
    - None
    """

    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.fact_player_discovery_daily (
        player_id STRING,
        player_platform_id STRING,
        player_character_id STRING,
        date DATE,
        character_level BIGINT,
        world STRING,
        biome STRING,
        region STRING,
        time_in_region_mins DOUBLE,
        meters_on_foot DOUBLE,
        meters_in_vehicle DOUBLE,
        total_meters DOUBLE,
        dw_update_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        dw_insert_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        merge_key STRING
    )
    """

    properties = {
        "delta.feature.allowColumnDefaults": "supported"
    }

    create_table(spark, sql, properties)