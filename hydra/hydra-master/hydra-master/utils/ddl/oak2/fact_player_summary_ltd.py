# Databricks notebook source
# MAGIC %run ../table_functions

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

def fact_player_summary_ltd(spark, database, environment, properties={}):
    """
    Create the fact_player_summary_ltd table in the specified environment.
    
    Parameters:
    - spark (SparkSession): Spark session.
    - database (str): Database name prefix (e.g., 'inverness').
    - environment (str): Suffix for environment (e.g., '_dev').
    - properties (dict, optional): Table properties.
    
    Returns:
    - None
    """
    
    full_table_name = f"{database}{environment}.managed.fact_player_summary_ltd"

    sql = f"""
    CREATE TABLE IF NOT EXISTS {full_table_name} (
        player_id STRING,
        platform STRING,
        service STRING,
        first_seen TIMESTAMP,
        last_seen TIMESTAMP,
        first_seen_country_code STRING NOT NULL,
        last_seen_country_code STRING NOT NULL,
        dw_update_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        dw_insert_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        merge_key STRING
    )
    """

    # Ensure Delta Lake column default feature is enabled
    properties.update({
        "delta.feature.allowColumnDefaults": "supported"
    })

    # Execute table creation
    create_table(spark, sql, properties)



# COMMAND ----------

def create_fact_player_summary_ltd_view(spark, database, environment):
    """
    Create the fact_player_summary_ltd view in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
        mapping (dict): A dictionary of column mappings.
    """

    sql = f"""
    CREATE VIEW IF NOT EXISTS {database}{environment}.managed_view.fact_player_summary_ltd AS (
        SELECT
        *
        from {database}{environment}.managed.fact_player_summary_ltd
    )
    """
    spark.sql(sql)
