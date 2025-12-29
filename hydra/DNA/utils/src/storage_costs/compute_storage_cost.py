import argparse
import logging
import json
from pyspark.sql import SparkSession


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

spark = SparkSession.builder.appName("StorageCostCalculator").getOrCreate()

def arg_parser():
    parser = argparse.ArgumentParser(description="Compute Storage Cost")
    parser.add_argument("--environment", required=True, help="Environment for which to compute storage cost")
    parser.add_argument("--catalog_list", required=True, help="Comma-separated list of catalogs")
    parser.add_argument("--schema_list", required=True, help="JSON string of schema lists per catalog")
    parser.add_argument("--target_catalog", required=True, help="Target catalog for storing cost data")
    parser.add_argument("--target_schema", required=True, help="Target schema for storing cost data")
    parser.add_argument("--target_table", required=True, help="Target table for storing cost data")
    parser.add_argument("--cost_per_gb", required=True, help="Cost per GB in USD")

    args = parser.parse_args()
    
    environment = args.environment.lstrip('_')
    catalogs = args.catalog_list.split(',')
    schema_mapping = json.loads(args.schema_list)
    target_catalog = args.target_catalog
    target_schema = args.target_schema
    target_table = args.target_table
    cost_per_gb = float(args.cost_per_gb)
        
    return environment, catalogs, schema_mapping, target_catalog, target_schema, target_table, cost_per_gb

def extract_metadata():
    try:
        timestamp = spark.sql("SELECT current_timestamp() as timestamp").collect()[0]['timestamp']
        return timestamp
    except Exception as e:
        logger.error(f"Error extracting metadata: {e}")
        return None

def compute_storage_cost():
    environment, catalogs, schema_mapping, target_catalog, target_schema, target_table, cost_per_gb = arg_parser()

    logger.info(f"Computing storage cost for environment {environment}, catalogs {catalogs}")

    # Create the target table if it does not exist
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {target_catalog}.{target_schema}.{target_table} (
            catalog STRING,
            schema STRING,
            table_name STRING,
            schema_table STRING,
            environment STRING,
            timestamp_day STRING,
            size_gb STRING,
            storage_cost STRING
        )
        USING DELTA
    """)

    for catalog in catalogs:
        catalog_name = f"{catalog}_{environment}"

        # Get schemas for the current catalog from the configuration
        schemas = schema_mapping.get(catalog, [])

        for schema in schemas:
            try:
                logger.info(f"Processing catalog: {catalog_name}, schema: {schema}")

                # Show tables in the specified schema
                tables_in_schema = spark.sql(f"SHOW TABLES IN {catalog_name}.{schema}")
                
                # Calculate size, cost, and gather metadata for each table
                table_info = []

                for table_row in tables_in_schema.collect():
                    table_name = table_row['tableName']
                    try:
                        size_info = spark.sql(f"DESCRIBE DETAIL {catalog_name}.{schema}.{table_name}").select("sizeInBytes").collect()[0][0]
                        size_gb = size_info / (1024 ** 3)
                        storage_cost = size_gb * cost_per_gb

                        # Extract metadata
                        timestamp = extract_metadata()
                        if timestamp:
                            timestamp_day = timestamp.strftime("%Y-%m-%d")
                            schema_table = f"{catalog_name}.{schema}.{table_name}"
                            table_info.append((catalog_name,schema,table_name,schema_table, environment, timestamp_day, f"{size_gb:.3f}", f"{storage_cost:.4f}"))
                        
                    except Exception as e:
                        logger.error(f"Error processing table {table_name}: {e}")

                if table_info:
                    # Create a DataFrame with additional columns for metadata
                    columns = ["catalog", "schema","table_name", "schema_table","environment", "timestamp_day", "size_gb", "storage_cost"]
                    table_info_df = spark.createDataFrame(table_info, columns)

                    # Register the DataFrame as a temporary view for the merge operation
                    table_info_df.createOrReplaceTempView("temp_table_info")

                    # Merge into the target table
                    spark.sql(f"""
                        MERGE INTO {target_catalog}.{target_schema}.{target_table} AS target
                        USING temp_table_info AS source
                        ON target.table_name = source.table_name 
                           AND target.timestamp_day = source.timestamp_day 
                           AND target.schema = source.schema
                           AND target.environment = source.environment
                        WHEN MATCHED THEN
                            UPDATE SET 
                                target.size_gb = source.size_gb, 
                                target.storage_cost = source.storage_cost
                        WHEN NOT MATCHED THEN
                            INSERT (catalog,schema,table_name,schema_table, environment,timestamp_day, size_gb, storage_cost)
                            VALUES (source.catalog, source.schema, source.table_name, source.schema_table , source.environment, source.timestamp_day, source.size_gb, source.storage_cost)
                    """)

                    # Drop the temp view after usage
                    try:
                        spark.sql("DROP VIEW IF EXISTS temp_table_info")
                    except Exception as e:
                        logger.error(f"Error dropping temp view: {e}")

                    # Display the contents of the target table to verify the merge
                    result_df = spark.sql(f"SELECT * FROM {target_catalog}.{target_schema}.{target_table}")
                    result_df.show()

            except Exception as e:
                logger.error(f"Error processing catalog {catalog_name} and schema {schema}: {e}")

if __name__ == "__main__":
    compute_storage_cost()