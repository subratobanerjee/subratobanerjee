# Databricks notebook source
# MAGIC %run ../table_functions

# COMMAND ----------

def create_fact_player_transaction(spark, database, properties={}):
    """
    Create the fact_player_transaction table in the specified environment.

    This function constructs a SQL command to create the fact_player_transaction 
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
    create_fact_player_transaction(spark, 'wwe2k25', {'delta.enableIcebergCompatV2': 'true'})
    Output:  "dbfs:/tmp/wwe2k25/intermediate/streaming/run_dev/fact_player_transaction"
    """
    
    sql = f"""
        CREATE TABLE IF NOT EXISTS {database}{environment}.intermediate.fact_player_transaction
            (
                transaction_id STRING,
                player_id STRING,
                platform STRING,
                service STRING,
                received_on TIMESTAMP,
                game_id STRING,
                game_mode STRING,
                sub_mode STRING,
                source_desc STRING,
                country_code STRING,
                currency_type STRING,
                currency_amount DECIMAL(38, 2),
                action_type STRING,
                item STRING,
                item_type STRING,
                item_price DECIMAL(38, 2),
                build_changelist STRING,
                extra_info_1 STRING,
                extra_info_2 STRING,
                extra_info_3 STRING,
                extra_info_4 STRING,
                extra_info_5 STRING,
                extra_info_6 STRING,
                extra_info_7 STRING,
                extra_info_8 STRING,
                dw_insert_date DATE,
                dw_insert_ts TIMESTAMP,
                dw_update_ts TIMESTAMP,
                merge_key STRING
            )
        COMMENT 'The table will have transactions related'
        PARTITIONED BY (platform,dw_insert_date)
    """
    create_table(spark, sql, properties)
    return f"dbfs:/tmp/{database}/intermediate/streaming/run{environment}/fact_player_transaction"
