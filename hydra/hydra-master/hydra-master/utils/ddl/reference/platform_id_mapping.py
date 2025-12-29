# Databricks notebook source
# MAGIC %run ../table_functions

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------


def create_platform_id_mapping(spark, database, environment, properties={}):
    
    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.reference.platform_id_mapping (
        maw_user_platformid STRING,
        player_platform_id STRING,
        dw_insert_ts TIMESTAMP,
        dw_update_ts TIMESTAMP,
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
    return f"dbfs:/tmp/{database}/reference/batch/run{environment}/platform_id_mapping"

