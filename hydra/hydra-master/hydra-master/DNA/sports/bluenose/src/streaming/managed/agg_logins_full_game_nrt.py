# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/table_functions

# COMMAND ----------

from pyspark.sql.functions import expr, coalesce, window
from delta.tables import DeltaTable

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'bluenose'
data_source ='dna'
title = "'PGA Tour 2K25'"

# COMMAND ----------

def create_agg_logins_full_game_nrt(spark, database, environment):
    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.agg_logins_full_game_nrt (
        title STRING,
        timestamp_10min_slice TIMESTAMP,
        platform STRING,
        service STRING,
        logins INT,
        dw_insert_ts TIMESTAMP,
        dw_update_ts TIMESTAMP,
        merge_key STRING
    )
    """

    properties = {
        "delta.feature.allowColumnDefaults": "supported" # this won't work until iceberg v3 is released
    }

    create_table(spark,sql, properties)
    create_agg_logins_full_game_nrt_view(spark, database, environment)
    return f"dbfs:/tmp/{database}/managed/streaming/run{environment}/agg_logins_full_game_nrt"

# COMMAND ----------

def create_agg_logins_full_game_nrt_view(spark, database, environment):
    """
    Create the fact_player_game_status_daily view in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
        mapping (dict): A dictionary of column mappings.
    """


    sql = f"""
    CREATE VIEW IF NOT EXISTS {database}{environment}.managed_view.agg_logins_full_game_nrt AS (
        SELECT
        *
        from {database}{environment}.managed.agg_logins_full_game_nrt
    )
    """

    spark.sql(sql)

# COMMAND ----------

def read_logins(spark, environment):
    df_logins = (
        spark
        .readStream
        .option("maxFilesPerTrigger", 30000)
        .table(f"coretech{environment}.sso.loginevent")
        .where(expr("appgroupId = '317c0552032c4804bb10d81b89f4c37e'"))
    )

    return df_logins

# COMMAND ----------

def read_dim_title(spark, environment):
    df_dim_title = (
        spark
        .read
        .table(f"reference{environment}.title.dim_title")
    )

    return df_dim_title

# COMMAND ----------

    
def transform(df): 
    logins_df = (
        df.alias("le")
        .groupBy(
            expr("'PGA Tour 2K25' as title"),
            expr("window(received_on, '10 minutes').end as timestamp_10min_slice"),
            "le.platform",
            "le.service"
        )
        .agg(
            expr("count(player_id) AS logins")
        )
    )

    return logins_df

# COMMAND ----------

def extract(environment, database):
    logins_df = read_logins(spark, environment)
    dim_title = read_dim_title(spark, environment)

    out_df = (
        logins_df.alias("le")
        .join(dim_title.alias("dt"), on=expr("le.appPublicId = dt.app_id"), how="inner")
        .select(
            expr("le.occurredOn::timestamp as received_on"),
            expr("le.accountId as player_id"),
            expr("dt.display_platform as platform"),
            expr("dt.display_service as service")
        )
    )

    return out_df   

# COMMAND ----------

def load_agg_logins_full_game_nrt(spark, df, database, environment):
    """
    Load the data into the agg_player_funnel_nrt table in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        df (DataFrame): The DataFrame to load into the table.
        database (str): The database to load the table into.
        environment (str): The environment to load the table into.
    """

    # Perform the merge operation into the Delta table
    final_table = DeltaTable.forName(spark, f"{database}{environment}.managed.agg_logins_full_game_nrt")

    # Get schema of the table
    table_schema = spark.read.table(f"{database}{environment}.managed.agg_logins_full_game_nrt").schema

    # Create empty dataframe using the schema
    out_df = spark.createDataFrame([], table_schema)

    # Union the incoming dataframe with the empty dataframe, we can set allowMissingColumns to False to be more strict
    df = df.select(
        "*",
        expr("CURRENT_TIMESTAMP() as dw_insert_ts"),
        expr("CURRENT_TIMESTAMP() as dw_update_ts"),
        expr("sha2(concat_ws('|', title, timestamp_10min_slice, platform, service), 256) as merge_key")
    )
    out_df = out_df.unionByName(df, allowMissingColumns=True)


    merge_condition = """
    old.timestamp_10min_slice = new.timestamp_10min_slice AND
    old.platform = new.platform AND
    old.service = new.service
    """

    update_set = {
        "logins": "new.logins + old.logins"
    }

    # Merge the table
    (
        final_table.alias('old')
        .merge(
            out_df.alias('new'),
            "new.merge_key = old.merge_key"
        )
        .whenMatchedUpdate(
            condition=expr("new.logins > 0"),
            set=update_set)
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

def proc_batch(df, environment):
    df = transform(df)
    load_agg_logins_full_game_nrt(spark, df, database, environment)

# COMMAND ----------

def run_stream():
    spark = create_spark_session(name=f"{database}")
    checkpoint_location = create_agg_logins_full_game_nrt(spark, database, environment)
    combined_df = extract(environment, database)
    (
        combined_df
        .writeStream
        .trigger(availableNow=True)
        .outputMode("update")
        .foreachBatch(lambda df, batch_id: proc_batch(df, environment ))
        .option("checkpointLocation", checkpoint_location)
        .start()
    )

# COMMAND ----------

run_stream()

# COMMAND ----------

# dbutils.fs.rm("dbfs:/tmp/bluenose/managed/streaming/run_dev/agg_logins_full_game_nrt", True)
# spark.sql("drop table bluenose_dev.managed.agg_logins_full_game_nrt")
