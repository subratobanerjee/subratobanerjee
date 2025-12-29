# Databricks notebook source
# MAGIC %run ../table_functions

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------


def create_bundle_table(spark, database, environment, properties={}):
    
    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.reference.bundle_table (
        id INT,
        release_date date,
        size INT,
        qty INT,
        pack_id INT,
        name_loc_key STRING,
        dw_update_ts TIMESTAMP,
        merge_key STRING
    )
    """

    create_table(spark, sql, properties)

