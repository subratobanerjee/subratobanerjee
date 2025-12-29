# Databricks notebook source
# MAGIC %run ../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../utils/helpers



# COMMAND ----------


import json
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
from delta.tables import DeltaTable
import argparse

# COMMAND ----------

def extract(environment):
    # Define the SQL query based on the environment
    sql_query_summary = f"""
    SELECT * FROM dataanalytics{environment}.cdp_ng.vw_civ7_franchise
    """
    
    # Execute the SQL query and return the result as a DataFrame
    df = spark.sql(sql_query_summary)
    
    return df

# COMMAND ----------

def transform(df):
    # In this case, no transformations are specified, so we return the DataFrame as is.
    transformed_df = df
    
    return transformed_df

# COMMAND ----------

def load(df, environment):
    # Write the DataFrame to a Delta table with Iceberg format options
    df.write \
      .mode("overwrite") \
      .option("delta.enableIcebergCompatV2", "true") \
      .option("delta.universalFormat.enabledFormats", "iceberg") \
      .option("overwriteSchema", "true") \
      .saveAsTable(f"dataanalytics{environment}.cdp_ng.civ7_franchise")

# COMMAND ----------

def run_batch():
    # Parse the environment argument
    input_param = dbutils_input_params()
    environment = input_param.get('environment', set_environment())

    # Extract the data
    df = extract(environment)
    
    # Transform the data (if needed)
    df = transform(df)
    
    # Load the data into a Delta table
    load(df, environment)

# COMMAND ----------

if __name__ == "__main__":
    run_batch()