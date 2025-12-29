# Databricks notebook source
# MAGIC %run ../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../managed/cdp_views/cdp_vw_pga_franchise

# COMMAND ----------


import json
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
from delta.tables import DeltaTable
import argparse
from pyspark.sql import SparkSession

# COMMAND ----------

def extract(environment, spark):
    # Define the SQL query based on the environment
    sql_query_franchise = f"""
    SELECT * FROM dataanalytics{environment}.cdp_ng.vw_pga_franchise
    """
    
    # Execute the SQL query and return the result as a DataFrame
    df = spark.sql(sql_query_franchise)
    
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
      .option("overwriteSchema", "true") \
      .saveAsTable(f"dataanalytics{environment}.cdp_ng.franchise_pga")

# COMMAND ----------

def run_batch():
    # Parse the environment argument
    input_param = dbutils_input_params()
    environment = input_param.get('environment', set_environment())
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
