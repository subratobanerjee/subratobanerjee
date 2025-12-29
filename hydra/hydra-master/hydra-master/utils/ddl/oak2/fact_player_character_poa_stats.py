# Databricks notebook source
# MAGIC %run ../table_functions

# COMMAND ----------

def create_fact_player_character_poa_stats(spark, database, environment,properties={}):
    """
    Creates the fact_player_character_poa_summary table in the specified environment.

    This function constructs a SQL command to create the fact_player_character_poa_summary
    table with predefined columns and executes it using the create_table function.
    The 'create_table' and 'environment' variables are initiated from the included 
    'table_functions' notebook.

    Parameters:
    Input:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The target database name (e.g., 'oak2').
        properties (dict, optional): A dictionary of properties to add to the table.
    Output:
        A string representing the checkpoint location for streaming operations.

    Example:
    create_fact_player_character_poa_summary(spark, 'oak2')
    Output: "dbfs:/tmp/oak2/managed/batch/run_dev/fact_player_character_poa_stats"
    """

    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.fact_player_character_poa_stats (
        player_id STRING,
        player_platform_id STRING,
        player_character_id STRING,
        received_on TIMESTAMP,
        poa_name STRING,
        poa_name_clean STRING,
        poa_type_clean STRING,
        poa_status STRING,
        character_level BIGINT,
        poa_status_number INT,
        is_complete BOOLEAN,
        prerequisite_complete BOOLEAN,
        hours_played DOUBLE,
        minutes_played DOUBLE,
        minutes_since_first_step DOUBLE,
        minutes_since_discovered DOUBLE,
        minutes_since_previous_step DECIMAL(24,2),
        merge_key STRING,
        dw_insert_ts TIMESTAMP,
        dw_update_ts TIMESTAMP
    )
    """

    default_properties = {
        "delta.feature.allowColumnDefaults": "supported"
    }

    final_properties = {**default_properties, **properties}

    create_table(spark, sql, final_properties)
    
    return f"dbfs:/tmp/{database}/managed/batch/run{environment}/fact_player_character_poa_stats"


# COMMAND ----------

def create_fact_player_character_poa_stats_view(spark, database):
    """
    Create the fact_player_character_poa_stats view in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
        mapping (dict): A dictionary of column mappings.
    """

    sql = f"""
    CREATE VIEW IF NOT EXISTS {database}{environment}.managed_view.fact_player_character_poa_stats AS (
        SELECT
        *
        from {database}{environment}.managed.fact_player_character_poa_stats
    )
    """
    spark.sql(sql)