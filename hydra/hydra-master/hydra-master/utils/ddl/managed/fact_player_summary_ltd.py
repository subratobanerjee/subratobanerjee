# Databricks notebook source
# MAGIC %run ../table_functions

# COMMAND ----------

def create_fact_player_summary_ltd(spark, database, view_mapping, properties={}):
    """
    Create the fact_player_summary_ltd table in the specified environment.

    This function constructs a SQL command to create the fact_player_summary_ltd 
    table with predefined columns and executes it using the create_table function.
    create_table and environment are initiated as part of the table_functions

    Parameters:
    Input
        spark (SparkSession): The Spark session for executing the SQL command.
        title (str): The base title for the table (e.g., the database name).
        properties (dict, optional): A dictionary of properties to add to the table.
    Output
        A checkpoint location should be used for your checkpoint location.
    """
    
    sql = f"""
            CREATE TABLE IF NOT EXISTS {database}{environment}.managed.fact_player_summary_ltd (
                    player_id string,
                    platform string,
                    service string,
                    config_1 string,
                    first_seen timestamp,
                    last_seen timestamp,
                    first_seen_lang string,
                    last_seen_lang string,
                    ltd_bool_1 boolean,
                    ltd_bool_2 boolean,
                    ltd_bool_3 boolean,
                    ltd_bool_4 boolean,
                    ltd_ts_1 timestamp,
                    ltd_ts_2 timestamp,
                    ltd_ts_3 timestamp,
                    ltd_ts_4 timestamp,
                    ltd_float_1 decimal(38,2),
                    ltd_float_2 decimal(38,2),
                    ltd_float_3 decimal(38,2),
                    ltd_float_4 decimal(38,2),
                    ltd_float_5 decimal(38,2),
                    ltd_float_6 decimal(38,2),
                    ltd_string_1 string,
                    ltd_string_2 string,
                    ltd_string_3 string,
                    ltd_string_4 string,
                    ltd_string_5 string,
                    ltd_string_6 string,
                    ltd_string_7 string,
                    ltd_string_8 string,
                    ltd_string_9 string,
                    ltd_string_10 string,
                    dw_insert_date date,
                    dw_insert_ts timestamp,
                    dw_update_ts timestamp,
                    merge_key string
            )
            partitioned by (PLATFORM,DW_INSERT_DATE)  
        """
    create_table(spark, sql, properties)
    create_fact_player_summary_ltd_view(spark, database, view_mapping)
    return f"dbfs:/tmp/{database}/managed/streaming/run{environment}/fact_player_summary_ltd"



def create_fact_player_summary_ltd_view(spark, database, mapping):
    """
    Create the fact_player_summary_ltd view in the specified environment.

    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
        mapping (dict): A dictionary of column mappings.
    """
    sql = f"""
    CREATE OR REPLACE VIEW {database}{environment}.managed_view.fact_player_summary_ltd AS (
        SELECT
            player_id,
            platform,
            service,
            {','.join(str(mapping[key])+',' for key in mapping if 'config' in key)}
            first_seen,
            last_seen,
            first_seen_lang,
            last_seen_lang,
            {','.join(str(mapping[key]) for key in mapping if 'config' not in key)},
            dw_insert_ts,
            dw_update_ts
        from {database}{environment}.managed.fact_player_summary_ltd
    )
    """

    spark.sql(sql)
