# Databricks notebook source
# MAGIC %run ../table_functions

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

def create_fact_player_igo_daily(spark, database,environment,properties={}):
    """
    Create the fact_player_igo_daily table in the specified environment.
    This function constructs a SQL command to create the fact_player_igo_daily
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
    Output:  "dbfs:/tmp/bluenose/managed/streaming/run_dev/fact_player_igo_daily"
    """

    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.fact_player_igo_daily (
        DATE DATE,
        PLAYER_ID STRING,
        PRIMARY_INSTANCE_ID string,
        PLATFORM STRING,
        SERVICE STRING,
        COUNTRY_CODE STRING,
        IGO_ID_1 STRING,
        IGO_ID_2 STRING,
        IGO_ID_3 STRING,
        IGO_TYPE STRING,
        STATUS STRING,
        AGG_1 INT,
        AGG_2 INT,
        AGG_3 INT,
        AGG_4 INT,
        AGG_5 INT,
        AGG_6 INT,
        DW_INSERT_TS TIMESTAMP,
        DW_UPDATE_TS TIMESTAMP,
        MERGE_KEY STRING
    )
    comment 'player level igo table'
    partitioned by (platform,date)
    """
    create_table(spark, sql, properties)
    create_fact_player_igo_daily_view(spark, database, view_mapping)
    return f"dbfs:/tmp/{database}/managed/batch/run{environment}/fact_player_igo_daily"

def create_fact_player_igo_daily_view(spark, database, mapping):
    """
    Create the fact_player_game_status_daily view in the specified environment.

    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
        mapping (dict): A dictionary of column mappings.
    """
    

    sql = f"""
    CREATE OR REPLACE VIEW {database}{environment}.managed_view.fact_player_igo_daily AS (
        SELECT
            DATE ,
            PLAYER_ID ,
            {''.join(str(mapping[key])+',' for key in mapping if 'primary_instance_id' in key)}
            PLATFORM ,
            SERVICE ,
            COUNTRY_CODE,
            {''.join(str(mapping[key])+',' for key in mapping if 'igo_id' in key)}
            IGO_TYPE,
            STATUS ,
            {','.join(str(mapping[key]) for key in mapping if 'agg' in key)},
            DW_INSERT_TS,
            DW_UPDATE_TS
        from {database}{environment}.managed.fact_player_igo_daily
    )
    """
    
    spark.sql(sql)
