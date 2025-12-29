import argparse
import logging
import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, current_timestamp
from pyspark.sql.types import ArrayType, StructType


# Configure logging
def setup_logger():

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    return logger

def arg_parser():
    """Parse command-line arguments for the Spark job."""
    parser = argparse.ArgumentParser(description="Parameters for Databricks job")
    parser.add_argument("--environment", type=str, required=True, help="Environment description")
    parser.add_argument("--checkpoint_location", type=str, required=True, help="Checkpoint location")
    parser.add_argument("--s3_bucket", type=str, required=True, help="S3 bucket name")
    parser.add_argument("--target_catalog", type=str, required=True, help="Target catalog name")
    parser.add_argument("--target_schema", type=str, required=True, help="Target schema name")
    parser.add_argument("--event_list", required=True, help="Comma-separated list of events")
    parser.add_argument("--backfill_days", type=int, required=True, help="Number of days to look back for data (e.g., 90 for 3 months)")

    args = parser.parse_args()
    return (
        args.environment,
        args.checkpoint_location,
        args.s3_bucket,
        args.target_catalog,
        args.target_schema,
        args.event_list.split(','),
        args.backfill_days
    )

def arg_parser_t2gp():
    """Parse command-line arguments for the Spark job."""
    parser = argparse.ArgumentParser(description="Parameters for Databricks job")
    parser.add_argument("--environment", type=str, required=True, help="Environment description")
    parser.add_argument("--bucket", required=True, help="bucket s3")
    parser.add_argument("--bucket_folder", required=True, help="bucket folder")
    parser.add_argument("--checkpoint_location", type=str, required=True, help="Checkpoint location")
    parser.add_argument("--s3_bucket", type=str, required=True, help="S3 bucket name")
    parser.add_argument("--target_catalog", type=str, required=True, help="Target catalog name")
    parser.add_argument("--target_schema", type=str, required=True, help="Target schema name")
    parser.add_argument("--product_id", required=True, help="Comma-separated product list")
    parser.add_argument("--target_product_id", required=True, help="target product id") 



    args = parser.parse_args() 
    return (
        args.environment,
        args.bucket,
        args.bucket_folder,
        args.checkpoint_location,
        args.s3_bucket,
        args.target_catalog,
        args.target_schema,
        args.product_id.split(','),
        args.target_product_id,
    )

def setup_spark():

    spark = SparkSession.builder.appName("S3 AutoLoader Ingestion").getOrCreate()

    return spark

def get_dbutils(spark):
    try:
        from pyspark.dbutils import DBUtils
        return DBUtils(spark)
    except ImportError:
        import IPython
        return IPython.get_ipython().user_ns["dbutils"]
    
def arg_parser_api():

    parser = argparse.ArgumentParser(description="Consume parameters from Databricks job")
    parser.add_argument("--environment", type=str, required=True)
    parser.add_argument("--endpoint", type=str, required=True)
    
    args = parser.parse_args()

    environment = args.environment
    endpoint = args.endpoint

    return environment,endpoint


def get_api_response(spark,endpoint,api_key):
    # Define the API endpoint and your API key
    api_url = endpoint
    api_key = api_key

    # Make the API request
    response = requests.get(api_url, headers={f"sharedkey": api_key})

    # Check if the request was successful
    if response.status_code == 200:
        # Convert the response JSON to a DataFrame
        data = response.json()
        if isinstance(data, list):
            df = spark.createDataFrame(data)
        else:
            df = spark.read.json(spark.sparkContext.parallelize([json.dumps(data)])) # Sparkcontext json dumps are required for dynamic flatten
        return df
    else:
        print(f"Failed to fetch data: {response.status_code}")
        


# Function to recursively flatten the DataFrame
def flatten_df(nested_df):
    flat_cols = [col for col in nested_df.columns if not isinstance(nested_df.schema[col].dataType, (ArrayType, StructType))]
    nested_cols = [col for col in nested_df.columns if isinstance(nested_df.schema[col].dataType, (ArrayType, StructType))]
    for nested_col in nested_cols:
        if isinstance(nested_df.schema[nested_col].dataType, ArrayType):
            nested_df = nested_df.withColumn(nested_col, explode(col(nested_col)))
    flat_df = nested_df.select(flat_cols + [col(nested_col + '.' + field.name).alias(nested_col + '_' + field.name
                                                                                     ) for field in nested_df.schema[nested_col].dataType.fields])


    return flat_df