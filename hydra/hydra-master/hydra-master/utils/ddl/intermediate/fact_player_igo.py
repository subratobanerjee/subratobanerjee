# Databricks notebook source
# MAGIC %run ../table_functions

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------


def create_fact_player_igo(spark, database, environment, properties={}):
    """
    Create the fact_player_igo table in the specified environment.

    This function constructs a SQL command to create the fact_player_igo
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
    Output:  "dbfs:/tmp/bluenose/intermediate/streaming/run_dev/fact_player_igo"
    """

    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.intermediate.fact_player_igo (
        received_on TIMESTAMP,
        player_id STRING,
        platform STRING,
        service STRING,
        country_code STRING,
        primary_instance_id STRING,
        igo_type STRING,
        igo_id STRING,
        status STRING,
        extra_info_1 STRING,
        extra_info_2 STRING,
        extra_info_3 STRING,
        extra_info_4 STRING,
        extra_info_5 STRING,
        dw_insert_ts TIMESTAMP,
        dw_update_ts TIMESTAMP
    )
    """

    properties = {
        "delta.feature.allowColumnDefaults": "supported"  # this won't work until iceberg v3 is released
    }

    create_table(spark, sql, properties)
    return (
        f"dbfs:/tmp/{database}/intermediate/streaming/run{environment}/fact_player_igo"
    )


# COMMAND ----------


def load_fact_player_igo(spark, df, database, environment, checkpoint_location,trigger = "streaming"):
    """
    Load the data into the fact_player_igo table in the specified environment.

    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        df (DataFrame): The DataFrame to load into the table.
        database (str): The database to load the table into.
        environment (str): The environment to load the table into.
    """

    # get schema of the table
    table_schema = spark.read.table(
        f"{database}{environment}.intermediate.fact_player_igo"
    ).schema

    # union the incoming dataframe with the empty dataframe, we can set allowMissingColumns to False to be more strict
    df = df.selectExpr(
        "*",
        "CURRENT_TIMESTAMP() as dw_insert_ts",
        "CURRENT_TIMESTAMP() as dw_update_ts",
    )
    # out_df = out_df.unionByName(df, allowMissingColumns=True)

    (
        df.writeStream.option("checkpointLocation", checkpoint_location)
        .outputMode("append")
        .trigger(processingTime="10 minutes")
        .toTable(f"{database}{environment}.intermediate.fact_player_igo")
    )

    if trigger == "streaming":
        (
            df.writeStream.option("checkpointLocation", checkpoint_location)
            .outputMode("append")
            .trigger(processingTime="10 minutes")
            .toTable(f"{database}{environment}.intermediate.fact_player_igo")
        )
    else:
        (
            df.writeStream.option("checkpointLocation", checkpoint_location)
            .outputMode("append")
            .trigger(availableNow=True)
            .toTable(f"{database}{environment}.intermediate.fact_player_igo")
        )