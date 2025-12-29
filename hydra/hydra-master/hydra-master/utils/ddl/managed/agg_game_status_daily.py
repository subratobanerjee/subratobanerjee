# Databricks notebook source
# MAGIC %run ../table_functions

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

def create_agg_game_status_daily(spark, database, view_mapping, properties={}):
    """
    Create the agg_game_status_daily table in the specified environment.

    This function constructs a SQL command to create the agg_game_status_daily
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
    create_agg_game_status_daily(spark, 'bluenose', {'delta.enableIcebergCompatV2': 'true'})
    Output:  "dbfs:/tmp/bluenose/managed/streaming/run_dev/create_agg_game_status_daily"
    """
    
    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.agg_game_status_daily (
        date DATE,
        platform STRING,
        service STRING,
        game_mode STRING DEFAULT 'Unused',
        sub_mode STRING DEFAULT 'Unused',
        country_code STRING DEFAULT 'ZZ',
        extra_grain_1 STRING DEFAULT 'Unused',
        agg_game_status_1 INT DEFAULT -1,
        agg_game_status_2 INT DEFAULT -1,
        agg_game_status_3 INT DEFAULT -1,
        agg_game_status_4 INT DEFAULT -1,
        agg_game_status_5 INT DEFAULT -1,
        agg_gp_1 INT DEFAULT -1,
        agg_gp_2 INT DEFAULT -1,
        agg_gp_3 INT DEFAULT -1,
        agg_gp_4 INT DEFAULT -1,
        agg_gp_5 INT DEFAULT -1,
        agg_gp_6 INT DEFAULT -1,
        agg_gp_7 INT DEFAULT -1,
        agg_gp_8 INT DEFAULT -1,
        agg_gp_9 INT DEFAULT -1,
        agg_gp_10 INT DEFAULT -1,
        agg_gp_11 INT DEFAULT -1,
        agg_gp_12 INT DEFAULT -1,
        agg_gp_13 INT DEFAULT -1,
        agg_gp_14 INT DEFAULT -1,
        agg_gp_15 INT DEFAULT -1,
        dw_insert_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        dw_update_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        merge_key STRING
    )
    """

    properties = {
        "delta.feature.allowColumnDefaults": "supported" #this won't work until iceberg v3 is released
    }

    create_table(spark, sql, properties)
    create_agg_game_status_daily_view(spark, database, view_mapping)
    return f"dbfs:/tmp/{database}/managed/streaming/run{environment}/agg_game_status_daily"

# COMMAND ----------

def create_agg_game_status_daily_view(spark, database, mapping):
    """
    Create the fact_player_game_status_daily view in the specified environment.

    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
        mapping (dict): A dictionary of column mappings.
    """
    

    sql = f"""
    CREATE OR REPLACE VIEW {database}{environment}.managed_view.agg_game_status_daily AS (
        SELECT
            date,
            platform,
            service,
            game_mode,
            sub_mode,
            country_code,
            {','.join(str(mapping[key]) for key in mapping)},
            dw_insert_ts,
            dw_update_ts
        from {database}{environment}.managed.agg_game_status_daily
    )
    """

    spark.sql(sql)

# COMMAND ----------

def load_agg_game_status_daily(spark, df, database, environment):
    """
    Load the data into the fact_player_game_status_daily table in the specified environment.

    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        df (DataFrame): The DataFrame to load into the table.
        database (str): The database to load the table into.
        environment (str): The environment to load the table into.
    """

    # Perform the merge operation into the Delta table
    final_table = DeltaTable.forName(spark, f"{database}{environment}.managed.agg_game_status_daily")

    # get schema of the table
    table_schema = spark.read.table(f"{database}{environment}.managed.agg_game_status_daily").schema

    # create empty dataframe using the schema
    out_df = spark.createDataFrame([], table_schema)

    # union the incoming dataframe with the empty dataframe, we can set allowMissingColumns to False to be more strict
    df = df.selectExpr(
            "*",
            "CURRENT_TIMESTAMP() as dw_insert_ts",
            "CURRENT_TIMESTAMP() as dw_update_ts",
            "SHA2(CONCAT_WS('|', DATE, PLATFORM, SERVICE, GAME_MODE, SUB_MODE, COUNTRY_CODE, EXTRA_GRAIN_1), 256) as merge_key"
        )
    out_df = out_df.unionByName(df, allowMissingColumns=True)

    # dynamically setup merge condition based on the aggregate columns in this database's fact_player_game_status_daily table
    agg_cols = [col_name for col_name in out_df.columns if 'agg_' in col_name]
    merge_condition = " OR ".join(f"old.{col_name} <> new.{col_name}" for col_name in agg_cols)

    # create the update column dict
    update_set = {}
    for col_name in agg_cols:
        update_set[f"old.{col_name}"] = f"greatest(new.{col_name}, old.{col_name})"
    
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
