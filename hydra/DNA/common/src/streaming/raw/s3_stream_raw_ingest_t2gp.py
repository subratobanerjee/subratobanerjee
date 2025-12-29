from src.utils.helpers import arg_parser_t2gp, setup_spark, setup_logger
from pyspark.sql.functions import  col, current_timestamp
import os
logger = setup_logger()

def create_s3_df_for_product(spark, product_id, s3_bucket, checkpoint_location):
    """Load a DataFrame from S3 for a specific product name, filtering by date if provided."""
    
    s3_path = f"{s3_bucket}/{product_id}/"
    read_checkpoint_location = os.path.join(checkpoint_location, product_id)

    logger.info(f"Reading from S3 path: {s3_path}")

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.includeExistingFiles", "true")  
        .option("cloudFiles.schemaLocation", read_checkpoint_location)
        .load(s3_path)
    )

    df_with_timestamps = df.withColumn("insert_ts", current_timestamp()) \
                            .withColumn("ingestion_latency", col("insert_ts").cast("long") - col("_metadata.file_modification_time").cast("long"))
    
    logger.info("schema after adding timestamp and latency:")
    return df_with_timestamps


def write_to_delta(df, product_id, target_catalog, target_schema, checkpoint_location,target_product_id):
    """Write the DataFrame to a Delta table."""
    logger.info(f"Writing data to Delta table: {target_catalog}.{target_schema}.{target_product_id}")
    write_checkpoint_location = os.path.join(checkpoint_location, target_catalog, target_schema, target_product_id)
    query = df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", write_checkpoint_location) \
            .trigger(processingTime='60 seconds') \
            .option("mergeSchema", "true") \
            .table(f"{target_catalog}.{target_schema}.{target_product_id}")

    query.awaitTermination()
    logger.info(f"Finished writing data for product: {product_id}")


def process_product(spark, product_id, target_catalog, target_schema, s3_bucket, checkpoint_location,target_product_id):
    """Process and write data for a single product."""
    logger.info(f"Processing product: {product_id}")
    df = create_s3_df_for_product(spark, product_id, s3_bucket, checkpoint_location)
    logger.info(f"Data already exists for product: {product_id}. Running incremental load.")
    write_to_delta(df, product_id, target_catalog, target_schema, checkpoint_location,target_product_id)

def s3_stream_raw_ingest_t2gp():
    """Process a single product ID without concurrency."""  
    environment,bucket,bucket_folder,checkpoint_location, s3_bucket, target_catalog, target_schema, product_id, target_product_id = arg_parser_t2gp()
    product_id_list = product_id
    product_id = product_id_list[0]
    logger.info(f"Reading from S3 path: {product_id}")
    spark = setup_spark() 
    try:
        logger.info(f"Starting to process S3 product: {product_id}")
        process_product(spark, product_id, target_catalog, target_schema, s3_bucket, checkpoint_location,target_product_id)      
        logger.info(f"Successfully processed S3 product: {product_id}")  
    except Exception as exc:
        logger.error(f"Error processing S3 product {product_id}: {exc}")

if __name__ == "__main__":
    s3_stream_raw_ingest_t2gp() 