# Databricks notebook source
# MAGIC %run ../table_functions

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

def create_agg_character_creation_nrt(spark, database, view_mapping, properties={}):
    """
    Create the agg_character_creation_nrt table in the specified environment.

    This function constructs a SQL command to create the agg_character_creation_nrt
    table with predefined columns and executes it using the create_table function.
    create_table and environment are initiated as part of the table_functions

    Parameters:
    Input
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The base database name for the table (e.g., 'oak2').
        view_mapping (dict): A dictionary of column mappings for the view.
        properties (dict, optional): A dictionary of properties to add to the table.
    Output
        A checkpoint location should be used for your checkpoint location.

    Example:
    Running it in a dev workspace
    create_agg_character_creation_nrt(spark, 'oak2', {'agg_gp_1': 'characters_created', 'agg_gp_2': 'player_count'})
    Output:  "dbfs:/tmp/oak2/managed/streaming/run_dev/agg_character_creation_nrt"
    """
    
    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.agg_character_creation_nrt (
        timestamp_10min_slice TIMESTAMP,
        title STRING,
        platform STRING,
        vault_hunter STRING,
        agg_gp_1 INT DEFAULT 0,
        agg_gp_2 INT DEFAULT 0,
        dw_insert_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        dw_update_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        merge_key STRING
    )
    """

    properties = {
        "delta.feature.allowColumnDefaults": "supported" # this won't work until iceberg v3 is released
    }

    create_table(spark, sql, properties)
    create_agg_character_creation_nrt_view(spark, database, view_mapping)
    return f"dbfs:/tmp/{database}/managed/streaming/run{environment}/agg_character_creation_nrt"

def create_player_character_creation_ledger(spark, database, properties={}):
    """
    Create the player_character_creation_ledger table in the specified environment.

    This function constructs a SQL command to create the player_character_creation_ledger
    table with predefined columns and executes it using the create_table function.
    create_table and environment are initiated as part of the table_functions

    Parameters:
    Input
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The base database name for the table (e.g., 'oak2').
        properties (dict, optional): A dictionary of properties to add to the table.
    Output
        A checkpoint location should be used for your checkpoint location.

    Example:
    Running it in a dev workspace
    create_player_character_creation_ledger(spark, 'oak2', {'delta.enableIcebergCompatV2': 'true'})
    Output:  "dbfs:/tmp/oak2/intermediate/streaming/run_dev/player_character_creation_ledger"
    """
    
    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.intermediate.player_character_creation_ledger (
        player_id STRING,
        first_player_id STRING,
        player_character_id STRING,
        player_character STRING,
        title STRING,
        platform STRING,
        character_created BOOLEAN,
        character_creation_ts TIMESTAMP,
        timestamp_10min_slice TIMESTAMP,
        dw_insert_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        dw_update_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        merge_key STRING
    )
    """

    properties = {
        "delta.feature.allowColumnDefaults": "supported" # this won't work until iceberg v3 is released
    }

    create_table(spark, sql, properties)
    return f"dbfs:/tmp/{database}/intermediate/streaming/run{environment}/player_character_creation_ledger"

# COMMAND ----------

def create_agg_character_creation_nrt_view(spark, database, mapping):
    """
    Create the agg_character_creation_nrt view in the specified environment.

    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
        mapping (dict): A dictionary of column mappings.
    """
    
    sql = f"""
    CREATE VIEW IF NOT EXISTS {database}{environment}.managed_view.agg_character_creation_nrt AS (
        SELECT
            timestamp_10min_slice,
            title,
            platform,
            vault_hunter,
            {','.join(f'{key} as {mapping[key]}' for key in mapping)}
        FROM {database}{environment}.managed.agg_character_creation_nrt
    )
    """

    spark.sql(sql)

# COMMAND ----------

def load_agg_character_creation_nrt(spark, df, database, environment):
    """
    Load the data into the agg_character_creation_nrt table in the specified environment.

    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        df (DataFrame): The DataFrame to load into the table.
        database (str): The database to load the table into.
        environment (str): The environment to load the table into.
    """

    # Perform the merge operation into the Delta table
    final_table = DeltaTable.forName(spark, f"{database}{environment}.managed.agg_character_creation_nrt")

    # get schema of the table
    table_schema = spark.read.table(f"{database}{environment}.managed.agg_character_creation_nrt").schema

    # create empty dataframe using the schema
    out_df = spark.createDataFrame([], table_schema)

    # union the incoming dataframe with the empty dataframe, we can set allowMissingColumns to False to be more strict
    df = df.selectExpr(
            "*",
            "CURRENT_TIMESTAMP() as dw_insert_ts",
            "CURRENT_TIMESTAMP() as dw_update_ts",
            "SHA2(CONCAT_WS('|', timestamp_10min_slice, title, platform, vault_hunter), 256) as merge_key"
        )
    out_df = out_df.unionByName(df, allowMissingColumns=True)

    # dynamically setup merge condition based on the aggregate columns in this database's agg_character_creation_nrt table
    agg_cols = [col_name for col_name in out_df.columns if 'agg_' in col_name]
    merge_condition = " OR ".join(f"old.{col_name} <> new.{col_name}" for col_name in agg_cols)
    merge_condition = f"({merge_condition}) AND (new.agg_gp_1 > 0 OR new.agg_gp_2 > 0)"

    # create the update column dict
    update_set = {}
    for col_name in agg_cols:
        update_set[f"old.{col_name}"] = f"new.{col_name} + old.{col_name}"
    
    update_set[f"old.dw_update_ts"] = "CURRENT_TIMESTAMP()"

    # merge the table
    (
        final_table.alias('old')
        .merge(
            out_df.alias('new'),
            "new.merge_key = old.merge_key"
        )
        .whenMatchedUpdate(condition=merge_condition, set=update_set)
        .whenNotMatchedInsertAll()
        .execute()
    )
