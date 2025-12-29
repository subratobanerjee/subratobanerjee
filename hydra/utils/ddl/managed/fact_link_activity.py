# Databricks notebook source
# MAGIC %run ../table_functions

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

def create_fact_player_link_activity(spark, database, view_mapping, properties={}):
    """
    Create the fact_player_link_activity table in the specified environment.

    This function constructs a SQL command to create the fact_player_link_activity
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
    create_fact_player_session(spark, 'wwe2k25', {'delta.enableIcebergCompatV2': 'true'})
    Output:  "dbfs:/tmp/wwe2k25/managed/streaming/run_dev/fact_player_link_activity"
    """
    
    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.fact_player_link_activity (
        DATE DATE,
        player_id STRING,
        platform STRING,
        service STRING,
        country_code STRING DEFAULT 'ZZ',
        agg_1 INT,
        agg_2 INT,
        agg_3 INT,
        agg_4 INT,
        agg_5 INT,
        dw_insert_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        dw_update_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        merge_key STRING
    )
    """

    properties = {
        "delta.feature.allowColumnDefaults": "supported" # this won't work until iceberg v3 is released
    }

    create_table(spark, sql, properties)
    create_fact_player_link_activity_view(spark, database, view_mapping)
    return f"dbfs:/tmp/{database}/managed/streaming/run{environment}/fact_player_link_activity"

# COMMAND ----------

def create_fact_player_link_activity_view(spark, database, mapping):
    """
    Create the fact_player_link_activity view in the specified environment.

    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
        environment (str): The environment suffix (e.g., '_dev', '_prod').
        mapping (dict): A dictionary of column mappings.
    """

    sql = f"""
    CREATE OR REPLACE VIEW {database}{environment}.managed_view.fact_player_link_activity AS
    SELECT 
        date,
        player_id,
        platform,
        service,
        country_code,
        {','.join(str(mapping[key]) for key in mapping)},
        dw_insert_ts,
        dw_update_ts
    FROM {database}{environment}.managed.fact_player_link_activity;
    """

    spark.sql(sql)


# COMMAND ----------

def load_fact_player_link_activity(spark, df, database, environment):
    """
    Load the data into the fact_player_link_activity table in the specified environment.

    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        df (DataFrame): The DataFrame to load into the table.
        database (str): The database to load the table into.
        environment (str): The environment to load the table into.
    """

    # Perform the merge operation into the Delta table
    final_table = DeltaTable.forName(spark, f"{database}{environment}.managed.fact_player_link_activity")


    # get schema of the table
    table_schema = spark.read.table(f"{database}{environment}.managed.fact_player_link_activity").schema


    # create empty dataframe using the schema
    out_df = spark.createDataFrame([], table_schema)
    
    # union the incoming dataframe with the empty dataframe, we can set allowMissingColumns to False to be more strict
    df = df.distinct()

    df = df.selectExpr(
            "*",
            "CURRENT_TIMESTAMP() as dw_insert_ts",
            "CURRENT_TIMESTAMP() as dw_update_ts",
            "SHA2(CONCAT_WS('|', date, PLAYER_ID, PLATFORM, SERVICE, COUNTRY_CODE), 256) as merge_key"
        )

    
    out_df = out_df.unionByName(df, allowMissingColumns=True).dropDuplicates(["merge_key"])

    # dynamically setup merge condition based on the aggregate columns in this database's fact_player_link_activity table
    agg_cols = [col_name for col_name in out_df.columns if 'agg_' in col_name]
    merge_condition = " OR ".join(f"old.{col_name} <> new.{col_name}" for col_name in agg_cols)
    # merge_condition = " OR ".join(
    # [f"old.{col_name} <> new.{col_name}" for col_name in agg_cols] + ["old.final_status <> new.final_status"])

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