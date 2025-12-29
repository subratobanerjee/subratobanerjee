# Databricks notebook source
# MAGIC %run ../../../../../utils/ddl/table_functions

# COMMAND ----------

# from src.utils.helpers import arg_parser_gbx_s3, setup_spark, setup_logger
from pyspark.sql.functions import split, col, current_timestamp, concat, lit, substring, to_date, expr, explode, col
from datetime import datetime, timedelta
import os
import concurrent.futures
from delta.tables import DeltaTable
from pyspark.sql.types import ArrayType
from pyspark.errors import AnalysisException
import pyspark.sql.utils
import logging
import time

# COMMAND ----------

logger = logging.getLogger(__name__)

dbutils.widgets.text(name="checkpoint_location", defaultValue="dbfs:/tmp/gbx/archway/raw")
dbutils.widgets.text(name="title", defaultValue="gbx")
dbutils.widgets.text(name="environment", defaultValue="_dev")
dbutils.widgets.text(name="target_schema", defaultValue="raw")

title = dbutils.widgets.get("title")
environment = dbutils.widgets.get("environment")
checkpoint_location = dbutils.widgets.get("checkpoint_location")
target_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------

def extract(spark, environment):
    """
    Read a stream of data from archway_kpi_events
    """
    df = (
        spark
        .readStream
        .option("maxBytesPerTrigger", 2147483648) # 2gb of data per trigger
        .table(f"gbx{environment}.raw.archway_kpi_events")
    )

    return df

# COMMAND ----------

def proc_batch(df, batch_id, checkpoint_path, target_schema, environment, spark):
    """
    Process a microbatch of data from archway, pulling out the login events
    """

    # explode the kpi_events column and filter out non-logins
    logins_df = (
            df
            .withColumn("kpis", explode(col("archway_kpi_events")))
            .select(
                "kpis.*",
                *[col(c) for c in df.columns if c != "archway_kpi_events"]
            )
            .where(expr(f"kpis.event_string = 'login'"))
        )
    
    create_table_raw(spark, f"gbx{environment}.{target_schema}.logins", logins_df.schema)
    
    (
        logins_df
        .write
        .option("mergeSchema", "true")
        .option("txnVersion", batch_id)
        .option("txnAppId", f"{checkpoint_path}/logins")
        .mode("append")
        .saveAsTable(f"gbx{environment}.{target_schema}.logins")
    )

# COMMAND ----------

def stream_archway_logins():
    """Main function to load data and process events."""

    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    # Print for debugging the environment setup
    logger.info(f"Running in environment: {environment}")
    
    df = extract(spark, environment)

    (
        df
        .writeStream
        .queryName(title)
        .trigger(processingTime="30 seconds")
        .foreachBatch(lambda df, batch_id: proc_batch(df, batch_id, checkpoint_location, target_schema, environment, spark))
        .option("checkpointLocation", checkpoint_location)
        .option("mergeSchema", "true")
        .start() 
    )

    logger.info(f"Finished writing data for events")

# COMMAND ----------

if __name__ == "__main__":
    stream_archway_logins()
