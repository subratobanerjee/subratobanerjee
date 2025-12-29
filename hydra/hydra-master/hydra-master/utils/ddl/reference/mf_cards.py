# Databricks notebook source
# MAGIC %run ../table_functions

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

def create_mf_cards(spark, database, environment, properties={}):
    
    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.reference.mf_cards (
        card_id INT,
        name STRING,
        dw_update_ts TIMESTAMP,
        merge_key STRING
    )
    """

    create_table(spark, sql, properties)