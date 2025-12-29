from pyspark.sql import SparkSession
import argparse

# COMMAND ----------

def arg_parser():
    parser = argparse.ArgumentParser(description="Script to consume parameters for Databricks job")
    parser.add_argument("--environment", type=str, required=True, help="Environment name, e.g. _stg or _prod")

    args = parser.parse_args()
    environment = args.environment
    return environment

# COMMAND ----------

def extract(environment, spark):
    # Define the SQL query based on the environment
    sql_query_events = f"""
    SELECT *
    FROM dataanalytics{environment}.cdp_ng.vw_eth_daily_events
    """
        # Execute the SQL query and return the result as a DataFrame
    df = spark.sql(sql_query_events)
    return df

# COMMAND ----------

def transform(df, environment, spark):
    # In this case, no transformations are specified, so we return the DataFrame as is.
    transformed_df = df
    
    return transformed_df

# COMMAND ----------

def load(df, environment, spark):
    # Write the DataFrame to a Delta table with Iceberg format options
    df.write \
      .mode("overwrite") \
      .option("delta.enableIcebergCompatV2", "true") \
      .option("delta.universalFormat.enabledFormats", "iceberg") \
      .saveAsTable(f"dataanalytics{environment}.cdp_ng.eth_daily_events")

# COMMAND ----------

def run_batch():
    # Parse the environment argument
    environment = arg_parser()
    
    # Create a Spark session
    spark = SparkSession.builder.appName("Hydra").getOrCreate()
    
    # Extract the data
    df = extract(environment, spark)
    
    # Transform the data (if needed)
    df = transform(df, environment, spark)
    
    # Load the data into a Delta table
    load(df, environment, spark)

# COMMAND ----------

if __name__ == "__main__":
    run_batch()
