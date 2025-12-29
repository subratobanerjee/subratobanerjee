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

def create_agg_gameplay_nrt(spark, database, environment):
    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.agg_gameplay_nrt (
        timestamp_10min_slice TIMESTAMP,
        platform STRING,
        service STRING,
        MINS_IN_ROUNDS INT,
        NUM_ROUNDS INT,
        NUM_UNIQUE_PLAYERS INT,
        dw_insert_ts TIMESTAMP,
        dw_update_ts TIMESTAMP,
        merge_key STRING
    )
    """

    properties = {
        "delta.feature.allowColumnDefaults": "supported" # this won't work until iceberg v3 is released
    }

    create_table(spark,sql, properties)
    create_agg_gameplay_nrt_view(spark, database, environment)
    return f"dbfs:/tmp/{database}/managed/streaming/run{environment}/agg_gameplay_nrt"

# COMMAND ----------

def create_agg_gameplay_nrt_view(spark, database, environment):
    """
    Create the fact_player_game_status_daily view in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
        mapping (dict): A dictionary of column mappings.
    """


    sql = f"""
    CREATE VIEW IF NOT EXISTS {database}{environment}.managed_view.agg_gameplay_nrt AS (
        SELECT
        *
        from {database}{environment}.managed.agg_gameplay_nrt
    )
    """

    spark.sql(sql)

# COMMAND ----------

def read_player_activity(spark, environment):
    df_player_activity = (
            spark
            .readStream
            .table(f"bluenose{environment}.intermediate.fact_player_activity")
            .select(
            expr("window(received_on::timestamp,'10 minutes').end as received_on_10min_slice"),
            "player_id",
            "platform",
            "service",
            expr("extra_info_6 as round_instanceid"),
            expr("extra_info_9 as gameplay_time_sec")
        ).where(
            (expr("extra_info_5 = 'full game'")) &
            (expr("player_id is not null and player_id != 'anonymous'")) &
            (expr("source_table = 'roundstatus' and event_trigger in ('RoundComplete', 'RoundQuit', 'RoundWon')")) &
            (expr("extra_info_2 not in ('Training', 'Boot') "))
        )
    )
    return df_player_activity

# COMMAND ----------

def read_platform_territory_df(environment):
    return (
        spark
        .read
        .table(f"dataanalytics{environment}.standard_metrics.platform_territory_10min_ts")
        .select(
            "timestamp_10min_slice",
            "platform",
            "service"
        )
        .where(
            expr("timestamp_10min_slice between current_timestamp() - interval '12 hour' and current_timestamp()") &
            # expr("timestamp_10min_slice >= '2024-10-01T00:00:00.000'") &
            # expr("timestamp_10min_slice < current_timestamp()") &
            (
                (expr("platform = 'Windows' AND service = 'Steam'")) |
                (expr("platform = 'XBSX' AND service = 'Xbox Live'")) |
                (expr("platform = 'PS5' AND service = 'SEN'"))
            )
               
               )
        .distinct()
    )

# COMMAND ----------

    
def transform(df): 
    platform_territory_df = read_platform_territory_df(environment).alias("dt")

    gameplay_df = (
        platform_territory_df.alias("pt")
        .join(df.alias('pa'),
            on=expr("""pt.timestamp_10min_slice = pa.received_on_10min_slice
            and pt.platform = pa.platform
            and pt.service = pa.service""")
        )
        .groupBy(
            "pt.timestamp_10min_slice",
            "pt.platform", 
            "pt.service"
        )
        .agg(
            expr("sum(gameplay_time_sec)/60 AS mins_in_rounds"),
            expr("count(distinct round_instanceid) AS num_rounds"),
            expr("count(distinct player_id) AS num_unique_players")
        )
    )
    return gameplay_df

# COMMAND ----------

def extract(environment, database):
    fact_player_activity = read_player_activity(spark, environment)
    return fact_player_activity   

# COMMAND ----------

def load_agg_gameplay_nrt(spark, df, database, environment):
    """
    Load the data into the agg_player_funnel_nrt table in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        df (DataFrame): The DataFrame to load into the table.
        database (str): The database to load the table into.
        environment (str): The environment to load the table into.
    """

    # Perform the merge operation into the Delta table
    final_table = DeltaTable.forName(spark, f"{database}{environment}.managed.agg_gameplay_nrt")

    # Get schema of the table
    table_schema = spark.read.table(f"{database}{environment}.managed.agg_gameplay_nrt").schema

    # Create empty dataframe using the schema
    out_df = spark.createDataFrame([], table_schema)

    # Union the incoming dataframe with the empty dataframe, we can set allowMissingColumns to False to be more strict
    df = df.select(
        "*",
        expr("CURRENT_TIMESTAMP() as dw_insert_ts"),
        expr("CURRENT_TIMESTAMP() as dw_update_ts"),
        expr("sha2(concat_ws('|', timestamp_10min_slice, platform, service), 256) as merge_key")
    )
    out_df = out_df.unionByName(df, allowMissingColumns=True)


    merge_condition = """
    old.timestamp_10min_slice = new.timestamp_10min_slice AND
    old.platform = new.platform AND
    old.service = new.service
    """

    update_set = {
        "mins_in_rounds": "GREATEST(new.mins_in_rounds, old.mins_in_rounds)",
        "num_rounds": "GREATEST(new.num_rounds, old.num_rounds)",
        "num_unique_players": "GREATEST(new.num_unique_players, old.num_unique_players)"
    }

    # Merge the table
    (
        final_table.alias('old')
        .merge(
            out_df.alias('new'),
            "new.merge_key = old.merge_key"
        )
        .whenMatchedUpdate(set=update_set)
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

def proc_batch(df, environment):
    df = transform(df)
    load_agg_gameplay_nrt(spark, df, database, environment)

# COMMAND ----------

def run_stream():
    spark = create_spark_session(name=f"{database}")
    checkpoint_location = create_agg_gameplay_nrt(spark, database, environment)
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



# COMMAND ----------


# dbutils.fs.rm("dbfs:/tmp/bluenose/managed/streaming/run_dev/agg_gameplay_nrt", True)
# spark.sql("drop table bluenose_dev.managed.agg_gameplay_nrt")