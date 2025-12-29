# Databricks notebook source
# MAGIC %md
# MAGIC # Table Compaction Job
# MAGIC
# MAGIC This job is run on a schedule to compact (`optimize`) tables.
# MAGIC
# MAGIC 1. Define a list of schemas containing tables to compact
# MAGIC 2. Get list of all tables in those schemas
# MAGIC 3. Run `OPTIMIZE` on all tables

# COMMAND ----------

# MAGIC %run ../../../../utils/helpers

# COMMAND ----------

from pyspark.sql.functions import concat, lit, col,array_contains
import argparse
import logging
import json
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# COMMAND ----------

def arg_parser():
    parser = argparse.ArgumentParser(description="Optimize Tables")
    parser.add_argument("--environment", required=True, help="Environment to optimize")
    parser.add_argument("--catalog", required=False, help="Catalog to run optimization on")

    args = parser.parse_args()
    
    environment = args.environment
    catalog = args.catalog # unused for now
        
    return environment, catalog

# COMMAND ----------

def get_table_list(environment, spark):
    schema_list = spark.sql(f"select catalog, schema, table_exclusion_list from reference{environment}.utils.table_optimize_cfg where isEnabled = True").collect()
    df = []

    for target in schema_list:
        catalog = target[0]
        schema = target[1]

        exclusion_list = target[2] if target[2] else []

        table_df = spark.sql(f"SHOW TABLES IN {catalog}{environment}.{schema}")

        table_df = table_df.withColumn("full_table_path", concat(lit(catalog + environment), lit("."), col("database"), lit("."), col("tableName")))

        if exclusion_list:
            logger.info(f"Excluding tables from optimization: {exclusion_list}")
            table_df = table_df.filter(~array_contains(lit(exclusion_list), col("full_table_path")))

        table_df = table_df.withColumn("schema_table", concat(lit(catalog + environment), lit("."), col("database"), lit("."), col("tableName")))

        if not df:
            df = table_df
        else:
            df = df.union(table_df)
    
    return df.select("schema_table").collect()


# COMMAND ----------

def run_optimization():
    input_param = dbutils_input_params()
    environment = input_param.get('environment', set_environment())
    tables = get_table_list(environment, spark)

    for table in tables:
        logger.info(f"Optimizing {table[0]}")
        spark.sql(f"OPTIMIZE {table[0]}")
        # spark.sql(f"VACUUM {table[0]}")
        logger.info(f"Finished optimizing {table[0]}")

# COMMAND ----------

run_optimization()
