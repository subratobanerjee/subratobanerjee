# Databricks notebook source
# MAGIC %run ../table_functions

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

def create_fact_player_progression(spark, database):
    """
    Create the fact_player_progression table in the specified environment.

    This function constructs a SQL command to create the fact_player_progression
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
    CREATE TABLE IF NOT EXISTS {database}{environment}.intermediate.fact_player_progression (
        received_on timestamp,
        player_id STRING,
        platform STRING,
        service STRING,
        primary_instance_id STRING DEFAULT '-1',
        secondary_instance_id STRING,
        category STRING,
        subject_id STRING,
        action STRING,
        extra_info_1 STRING,
        extra_info_2 STRING,
        extra_info_3 STRING,
        extra_info_4 STRING,
        extra_info_5 STRING,
        extra_info_6 STRING,
        extra_info_7 STRING,
        extra_info_8 STRING,
        extra_info_9 STRING,
        extra_info_10 STRING,
        dw_insert_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        dw_update_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        merge_key STRING
    )
    """

    properties = {
        "delta.feature.allowColumnDefaults": "supported" # this won't work until iceberg v3 is released
    }

    create_table(spark, sql, properties)
    return f"dbfs:/tmp/{database}/intermediate/batch/run{environment}/fact_player_progression"


# COMMAND ----------

def load_fact_player_progression(spark, df, database, environment):
    """
    Load the data into the fact_player_progression table in the specified environment.

    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        df (DataFrame): The DataFrame to load into the table.
        database (str): The database to load the table into.
        environment (str): The environment to load the table into.
    """

    # Perform the merge operation into the Delta table
    final_table = DeltaTable.forName(spark, f"{database}{environment}.intermediate.fact_player_progression")

    # get schema of the table
    table_schema = spark.read.table(f"{database}{environment}.intermediate.fact_player_progression").schema

    # create empty dataframe using the schema
    out_df = spark.createDataFrame([], table_schema)

    # union the incoming dataframe with the empty dataframe, we can set allowMissingColumns to False to be more strict
    df = df.selectExpr(
            "*",
            "CURRENT_TIMESTAMP() as dw_insert_ts",
            "CURRENT_TIMESTAMP() as dw_update_ts",
            "SHA2(CONCAT_WS('|', PLAYER_ID, PLATFORM, SERVICE, PRIMARY_INSTANCE_ID, SECONDARY_INSTANCE_ID, CATEGORY, SUBJECT_ID, ACTION), 256) as merge_key"
        )
    out_df = out_df.unionByName(df, allowMissingColumns=True)

    # dynamically setup merge condition based on the aggregate columns in this database's fact_player_game_status_daily table
    extra_info_cols = [col_name for col_name in out_df.columns if 'extra_info' in col_name]

    # merge the table, do nothing when the key matches and inserts when for new keys
    (
        final_table.alias('old')
        .merge(
            out_df.alias('new'),
            "new.merge_key = old.merge_key"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
