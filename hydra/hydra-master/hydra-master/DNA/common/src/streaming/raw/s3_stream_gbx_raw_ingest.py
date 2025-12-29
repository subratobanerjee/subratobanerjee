# Databricks notebook source
# from src.utils.helpers import arg_parser_gbx_s3, setup_spark, setup_logger
from pyspark.sql.functions import split, col, current_timestamp, concat, lit, substring, to_date, expr, explode, col
from datetime import datetime, timedelta
import os
import concurrent.futures
from delta.tables import DeltaTable
from pyspark.sql.types import ArrayType, StructType
from pyspark.errors import AnalysisException
import pyspark.sql.utils
import logging
import time

# COMMAND ----------

logger = logging.getLogger(__name__)

dbutils.widgets.text(name="checkpoint_location", defaultValue="dbfs:/tmp/core_games/checkpoint")
dbutils.widgets.text(name="title", defaultValue="oak2")
dbutils.widgets.text(name="environment", defaultValue="_dev")
dbutils.widgets.text(name="target_schema", defaultValue="raw")
dbutils.widgets.text(name="s3_bucket", defaultValue="2k-ana-dev-oak2")
dbutils.widgets.text(name="s3_prefix", defaultValue="*")

title = dbutils.widgets.get("title")
environment = dbutils.widgets.get("environment")
checkpoint_location = dbutils.widgets.get("checkpoint_location")
target_schema = dbutils.widgets.get("target_schema")
s3_bucket = dbutils.widgets.get("s3_bucket")
s3_prefix = dbutils.widgets.get("s3_prefix")

# COMMAND ----------

# Check if a field exists in a struct column
def field_exists_in_struct(df, struct_column_name, field_name):
    """Check if a field exists in a struct column"""
    if struct_column_name not in df.columns:
        return False
    
    struct_type = df.schema[struct_column_name].dataType
    if not isinstance(struct_type, StructType):
        return False
    
    return field_name in [field.name for field in struct_type.fields]

# COMMAND ----------

def create_s3_df_for_event(spark, title, s3_bucket, checkpoint_location, s3_prefix=None):
    """Load a DataFrame from S3 for a specific event name, filtering by date if provided."""
    
    today = datetime.today()
    s3_path = f"s3://{s3_bucket}/{s3_prefix}"
    read_checkpoint_location = os.path.join(checkpoint_location, title)
    logger.info(f"Checkpoint location: {read_checkpoint_location}")
    logger.info(f"Reading from S3 path: {s3_path}")

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.includeExistingFiles", "true") 
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", read_checkpoint_location)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("mergeSchema", "true")  # Enable schema merging in read
        .load(s3_path)
    )

    # Add ingestion timestamp and calculate latency
    
    df_with_timestamps = df.select(
        "*",
        current_timestamp().alias("insert_ts"),
        (current_timestamp().cast("long") - col("_metadata.file_modification_time").cast("long")).alias("ingestion_latency")
    )

    return df_with_timestamps

# COMMAND ----------


def parallel_write(df, event_name, batch_id, checkpoint_path,title,target_schema,environment,spark):
    """
    writes the flattened data to a uniform table
    """
    logger.info(f"Flattening Event: {event_name}")

    if event_name == 'context':
            df_flattened = df.select(
            col("context.*")
        )

    # change to incorporate DATADEP-3518
    elif event_name == "oaktelemetry_boss_fight":
        df_exploded = df.withColumn("event", explode(col(event_name)))
        
        df = df_exploded.select(
            concat(lit('2k_'), substring(col("context.maw_user_platformid").cast("string"), 4, 100)).alias("maw_user_platformid"),
            col("context.execution_guid_string").cast("string").alias("execution_guid"),
            col("context.maw_time_received_timestamp").cast("timestamp").alias("maw_time_received"),
            col("event.*")
        )

        columns_to_drop = [col for col in df.columns if col.startswith("remaining_hp_percentage_of") or col.startswith("total_remaining_hp_percentage_of") or col.startswith("hp_remaining_of__char")]
        df_flattened = df.drop(*columns_to_drop)        

    elif title != 'gbx':
        df_exploded = df.withColumn("event", explode(col(event_name)))
        
        df_flattened = df_exploded.select(
            concat(lit('2k_'), substring(col("context.maw_user_platformid").cast("string"), 4, 100)).alias("maw_user_platformid"),
            col("context.execution_guid_string").cast("string").alias("execution_guid"),
            col("context.maw_time_received_timestamp").cast("timestamp").alias("maw_time_received"),
            col("event.*")
        )

    elif title == 'gbx' and event_name != 'context':
        df_flattened = df.select(
            col("context.maw_time_received_timestamp").cast("timestamp").alias("maw_time_received"),
            event_name,
            "context"
        )
            
    # Add the additional timestamp and date columns
    flattened_df = df_flattened.select(
        "*",
        current_timestamp().alias("insert_ts"),
        to_date(current_timestamp()).alias("date")
    )

    # pull out required fields from event_context, ignore ozone events for further flattening
    if 'event_contexts' in flattened_df.columns and 'ozone' not in event_name.lower() and title == 'oak2':
        if field_exists_in_struct(flattened_df, 'event_context', 'player_primary_id_string'):
            player_id_column = 'player_primary_id_string'
        else:
            player_id_column = 'player_primary_id_platformid'
            
        flattened_df = (
            flattened_df
            .withColumn("event_context", explode(col("event_contexts")))
            .select(
                expr("game_session_primary_player_id_platformid as host_id"),
                expr(f"case when event_context::string = '' then null else coalesce(event_context.{player_id_column}, event_context.player_primary_id_platformid) end as player_id"),
                expr("case when event_context::string = '' then null else event_context.player_primary_id_platformid end as player_platform_id"),
                expr("case when event_context::string = '' then null else event_context.context_class_string end as player_character"),
                expr("case when event_context::string = '' then null else event_context.player_character_id_string end as player_character_id"),
                expr("case when event_context::string = '' then null else event_context.player_exp_level_long end as player_level"),
                expr("case when event_context::string = '' then null else event_context.player_time_played_double end as player_time_played"),
                expr("case when event_context::string = '' then null else event_context.location_x_double end as player_location_x"),
                expr("case when event_context::string = '' then null else event_context.location_y_double * -1 end as player_location_y"),
                expr("case when event_context::string = '' then null else event_context.location_z_double end as player_location_z"),
                expr("case when (case when event_context::string = '' then null else event_context.player_local_index_long end) = -1 then 'online' else 'local' end as player_connection"),
                expr("coalesce(array_size(event_contexts), 1) as session_players"),
                "*"
            )
            .drop("event_contexts")
        )

    (
        DeltaTable.createIfNotExists(spark)
        .tableName(f"{title}{environment}.{target_schema}.{event_name}")
        .addColumns(flattened_df.schema)
        .property('delta.enableIcebergCompatV2', 'true')
        .property('delta.universalFormat.enabledFormats', 'iceberg')
        .execute()
    )
    
    (
        flattened_df
        .write
        .option("mergeSchema", "true")
        .option("txnVersion", batch_id)
        .option("txnAppId", f"{checkpoint_path}/{event_name}")
        .mode("append")
        .saveAsTable(f"{title}{environment}.{target_schema}.{event_name}")
    )

# COMMAND ----------

def proc_batch_pre_split(df, id, checkpoint_path,title,target_schema,environment,spark):
    """
    1. pulls distinct list of topics out of this microbatch
    2. starts up a pool of workers based on number of driver CPU cores
    3. assigns 1 topic to each worker to parallely write to uniform tables
    """
    logger.info("Starting proc_batch_pre_split function")
    df.persist()
    event_list = [field.name for field in df.schema.fields if isinstance(field.dataType, ArrayType)]
    event_list.append("context")
    logger.info(f"Event List: {event_list}")
    with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        future_map = {}
        futures = []

        for event_name in event_list:
            event = executor.submit(parallel_write, df, event_name, id, checkpoint_path,title,target_schema,environment,spark)
            future_map[event] = event_name
            futures.append(event)

        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                logger.error(f"ERROR in event {future_map[future]}: {exc}")
                raise exc

    df.unpersist()

# COMMAND ----------

def stream_s3_gbx_raw_ingest():
    """Main function to load data and process events."""

    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")
    spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")

    # Print for debugging the environment setup
    logger.info(f"Running in environment: {environment}, S3 bucket: {s3_bucket}")
    
    # Process each event concurrently with limited workers
    df = create_s3_df_for_event(spark, title, s3_bucket, checkpoint_location,s3_prefix)

    (
        df
        .writeStream
        .queryName(title)
        .trigger(processingTime="10 seconds")
        .foreachBatch(lambda df, batch_id: proc_batch_pre_split(df, batch_id, checkpoint_location,title,target_schema,environment,spark))
        .option("checkpointLocation", checkpoint_location)
        .option("mergeSchema", "true")
        .start() 
    )

    logger.info(f"Finished writing data for events")

# COMMAND ----------

if __name__ == "__main__":
    stream_s3_gbx_raw_ingest()