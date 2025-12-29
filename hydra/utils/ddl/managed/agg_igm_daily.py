# Databricks notebook source
# MAGIC %run ../table_functions

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

def create_agg_igm_daily(spark, database,environment,properties={}):
    """
    Create the agg_igm_daily table in the specified environment.
    This function constructs a SQL command to create the agg_igm_daily
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
    create_fact_player_session(spark, 'bluenose', {'delta.enableIcebergCompatV2': 'true'})
    Output:  "dbfs:/tmp/bluenose/managed/streaming/run_dev/agg_igm_daily"
    """

    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.agg_igm_daily (
        DATE DATE,
        PLATFORM STRING,
        SERVICE STRING,
        COUNTRY_CODE STRING,
        IGM_ID STRING,
        CONFIG_1 STRING,
        CONFIG_2 STRING,
        STATUS STRING,
        AGG_1 BIGINT,
        AGG_2 BIGINT,
        AGG_3 BIGINT,
        AGG_4 BIGINT,
        AGG_5 BIGINT,
        AGG_6 BIGINT,
        AGG_7 BIGINT,
        AGG_8 BIGINT,
        AGG_9 BIGINT,
        AGG_10 BIGINT,
        DW_INSERT_TS TIMESTAMP,
        DW_UPDATE_TS TIMESTAMP,
        MERGE_KEY STRING
    )
    comment 'Daily IGM aggreated value'
    partitioned by (platform,date)
    """
    create_table(spark, sql, properties)
    create_agg_igm_daily_view(spark, database, view_mapping)
    return

def create_agg_igm_daily_view(spark, database, mapping):
    """
    Create the fact_player_game_status_daily view in the specified environment.

    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
        mapping (dict): A dictionary of column mappings.
    """
    

    sql = f"""
    CREATE OR REPLACE VIEW {database}{environment}.managed_view.agg_igm_daily AS (
        SELECT
            DATE,
            PLATFORM,
            SERVICE,
            COUNTRY_CODE,
            IGM_ID,
            {''.join(str(mapping[key])+',' for key in mapping if 'config' in key)}
            status,
            {','.join(str(mapping[key]) for key in mapping if 'config' not in key)},
            DW_INSERT_TS,
            DW_UPDATE_TS
        from {database}{environment}.managed.agg_igm_daily
    )
    """
    spark.sql(sql)
