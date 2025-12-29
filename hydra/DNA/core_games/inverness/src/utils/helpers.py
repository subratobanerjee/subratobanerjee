import argparse
import logging
from pyspark.sql import SparkSession

# Configure logging
def setup_logger():

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    return logger

def arg_parser():

    parser = argparse.ArgumentParser(description="Consume parameters from Databricks job")
    parser.add_argument("--environment", type=str, required=True, help="Description for environment")
    parser.add_argument("--checkpoint_location", type=str, required=True, help="Description for checkpoint_location")
    
    args = parser.parse_args()

    environment = args.environment
    checkpoint_location = args.checkpoint_location

    return environment,checkpoint_location

def setup_spark(processing=None):

    spark = SparkSession.builder.appName("Hydra").getOrCreate()

    if processing == 'batch':
        spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
        spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
    
    elif processing == 'stream':
        spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "false")
        spark.conf.set("spark.databricks.delta.autoCompact.enabled", "false")


    return spark