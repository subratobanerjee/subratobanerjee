# Databricks notebook source
# MAGIC %run ../table_functions

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------


def create_card_table(spark, database, environment, properties={}):
    
    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.reference.card_table (
        card_id INT,
        name STRING,
        rarity STRING,
        series_id INT,
        gender STRING,
        roster_id INT,
        type STRING,
        dw_update_ts TIMESTAMP,
        merge_key STRING
    )
    """

    create_table(spark, sql, properties)

