# Databricks notebook source
import json
from pyspark.sql.functions import (lit, col, when, get_json_object, current_timestamp, split,window)
from delta.tables import DeltaTable
from pyspark.sql.functions import *
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import Window

# COMMAND ----------

def arg_parser():
    parser = argparse.ArgumentParser(description="Example script to consume parameters from Databricks job")
    parser.add_argument("--environment", type=str, required=True, help="Description for param1")
    parser.add_argument("--checkpoint_location", type=str, required=True, help="Description for param2")
    args = parser.parse_args()
    environment = args.environment
    checkpoint_location = args.checkpoint_location

    return environment,checkpoint_location

# COMMAND ----------

def read_web_event_df(environment, spark):
    df = (
        spark
        .readStream
        .table(f"coretech{environment}.web.web_event")
        .withColumns({
            "received_time": col("receivedOn").cast("timestamp"),
            "country_code": lit("ZZ")
            })
        .alias("web")
    )

    return df


# COMMAND ----------

def extract(environment, spark):
    web_df = read_web_event_df(environment, spark)

    return web_df.alias("web_events")

# COMMAND ----------

def transform(df, environment, spark):
    df = (
        df
        .withColumns({
            "timestamp_10min_slice": from_unixtime(round(floor(unix_timestamp(col("received_time")) / 600) * 600)).cast("timestamp")
        })
        .withWatermark("received_time", "1 minute")
        .groupBy(
            window("received_time", "10 minutes"),
            col("timestamp_10min_slice"),
            col("web_events.country_code")
        )
        .agg(
            count(when((col("web_events.event_trigger") == "Registration CTA"), True)).alias("num_registration_cta_events"),
            count(when((col("web_events.event_trigger") == "Country/Age Check"), True)).alias("num_country_age_checks"),
            count(when((col("web_events.event_trigger") == "PC Spec and Newsletter Page"), True)).alias("pc_specs_newsletter_events"),
            count(when((col("web_events.event_trigger") == "Key Delivery Click"), True)).alias("num_key_delivery_clicks"),
            count(when((col("web_events.event_trigger") == "Portal Check Test"), True)).alias("num_portal_check_tests")
        )
        .withColumns({
            "dw_insert_ts": current_timestamp(),
            "dw_update_ts": current_timestamp(),
            "merge_key": sha2(concat_ws("|", col("timestamp_10min_slice"), col("country_code")), 256)
        })
        .alias("links")
    )

    return df

# COMMAND ----------

def start_stream():
    environment, checkpoint_location = arg_parser()
    # environment, checkpoint_location = "_dev", "dbfs:/tmp/sm/managed/streaming/run_dev/agg_account_link_10min"
    spark = SparkSession.builder.appName("Hydra").getOrCreate()

    spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "false")
    spark.conf.set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")
    spark.conf.set("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")

    df = extract(environment, spark)
    transformed_df = transform(df, environment, spark)

    (
        transformed_df
        .writeStream
        .trigger(processingTime="1 minute")
        .option("mergeSchema", "true")
        .option("checkpointLocation", checkpoint_location)
        .toTable(f"dataanalytics{environment}.standard_metrics.agg_web_activity_10min")
    )

# COMMAND ----------

if __name__ == "__main__":
    start_stream()