from src.utils.helpers import arg_parser, setup_spark, setup_logger
from pyspark.sql.functions import split, col, current_timestamp
from datetime import datetime, timedelta
import os
import concurrent.futures
logger = setup_logger()


def create_s3_df_for_event(spark, event_name, s3_bucket, checkpoint_location, backfill_days=None):
    """Load a DataFrame from S3 for a specific event name, filtering by date if provided."""
    
    today = datetime.today()
    s3_path = f"s3://{s3_bucket}/eventName={event_name}/*"
    read_checkpoint_location = os.path.join(checkpoint_location, event_name)

    logger.info(f"Reading from S3 path: {s3_path}")

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.includeExistingFiles", "true")  # Include all files
        .option("cloudFiles.schemaLocation", read_checkpoint_location)
        .load(s3_path)
    )

    # If backfill_days is provided, filter the DataFrame for the initial load
    if backfill_days is not None:
        start_date = today - timedelta(days=backfill_days)
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = today.strftime('%Y-%m-%d')

        df_with_dates = df.withColumn("date", split(split(col("_metadata.file_path"), "date=")[1], "/")[0])
        df = df_with_dates.filter((col("date") >= start_date_str) & (col("date") <= end_date_str))
        logger.info(f"Filtered data for event {event_name} from {start_date_str} to {end_date_str}")
    else:
        logger.debug(f"No date filtering applied for event: {event_name}")

    # Add ingestion timestamp and calculate latency
    df_with_timestamps = df.withColumn("insert_ts", current_timestamp()) \
                            .withColumn("ingestion_latency", col("insert_ts").cast("long") - col("_metadata.file_modification_time").cast("long"))
    
    logger.info("Schema after adding timestamp and latency:")
    df_with_timestamps.printSchema()

    return df_with_timestamps


def write_to_delta(df, event_name, catalog, schema, checkpoint_location, is_backfill=False):
    """Write the DataFrame to a Delta table."""
    logger.info(f"Writing data to Delta table: {catalog}.{schema}.{event_name}")
    write_checkpoint_location = os.path.join(checkpoint_location, catalog, schema, event_name)

    # If it's a backfill, process everything in one batch without waiting
    if is_backfill:
        query = df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", write_checkpoint_location) \
            .option("mergeSchema", "true") \
            .trigger(availableNow=True) \
            .table(f"{catalog}.{schema}.{event_name}") # Process all historical data once
    else:
        # For incremental loads, use the 60-second processing time
        query = df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", write_checkpoint_location) \
            .trigger(processingTime='60 seconds') \
            .option("mergeSchema", "true") \
            .table(f"{catalog}.{schema}.{event_name}")

    query.awaitTermination()
    logger.info(f"Finished writing data for event: {event_name}")


def process_event(spark, event_name, catalog, schema, s3_bucket, backfill_days, checkpoint_location):
    """Process and write data for a single event."""
    logger.info(f"Processing event: {event_name}")

    # Check if Delta table exists
    delta_table_path = f"{catalog}.{schema}.{event_name}"
    if spark.catalog.tableExists(delta_table_path):
        logger.info(f"Data already exists for event: {event_name}. Running incremental load.")
        # For incremental load, do not filter by date
        df = create_s3_df_for_event(spark, event_name, s3_bucket, checkpoint_location)
        write_to_delta(df, event_name, catalog, schema, checkpoint_location)
    else:
        logger.info(f"No data exists for event: {event_name}. Performing initial backfill load for the past {backfill_days} days.")
        # For initial load, include date filtering for backfill
        df = create_s3_df_for_event(spark, event_name, s3_bucket, checkpoint_location, backfill_days)
        # First, perform the backfill load
        write_to_delta(df, event_name, catalog, schema, checkpoint_location, is_backfill=True)
        logger.info(f"Backfill complete for event: {event_name}. Starting incremental load.")
        # After backfill, start incremental load
        df_incremental = create_s3_df_for_event(spark, event_name, s3_bucket, checkpoint_location)
        write_to_delta(df_incremental, event_name, catalog, schema, checkpoint_location)


def s3_stream_raw_ingest():
    """Main function to load data and process events."""
    environment, checkpoint_location, s3_bucket, target_catalog, target_schema, event_list, backfill_days = arg_parser()

    # Initialize Spark session
    spark = setup_spark()
    
    # Print for debugging the environment setup
    print(f"Running in environment: {environment}, S3 bucket: {s3_bucket}")
    
    # Process each event concurrently with limited workers
    with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        logger.info("Starting processing for all S3 events concurrently.")
        future_to_event = {executor.submit(process_event, spark, event_name, target_catalog, target_schema, s3_bucket, backfill_days, checkpoint_location): event_name for event_name in event_list}

        for future in concurrent.futures.as_completed(future_to_event):
            event_name = future_to_event[future]
            try:
                future.result()  # This will raise any exceptions caught in process_event
                logger.info(f"Successfully processed S3 event: {event_name}")
            except Exception as exc:
                logger.error(f"Error processing S3 event {event_name}: {exc}")


if __name__ == "__main__":
    s3_stream_raw_ingest()