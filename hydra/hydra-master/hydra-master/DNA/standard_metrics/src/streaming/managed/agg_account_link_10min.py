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

def read_link_df(environment, spark):
    df = (
        spark
        .readStream
        .table(f"coretech{environment}.sso.linkevent")
        .withColumns({
            "received_on": col("occurredOn").cast("timestamp"),
            "country_code": ifnull(get_json_object(col("geoIp"), "$.countryCode"), lit("ZZ")).alias("country_code")
            })
        .alias("le")
    )

    return df

# COMMAND ----------

def read_title_df(environment, spark):
    df = (
        spark
        .read
        .table(f"reference{environment}.title.dim_title")
        .alias("dt")
    )

    return df

# COMMAND ----------

def extract(environment, spark):
    link_df = read_link_df(environment, spark)

    return link_df.alias("sso_events")

# COMMAND ----------

def transform(df, environment, spark):
    df = (
        df
        .withColumns({
            "timestamp_10min_slice": from_unixtime(round(floor(unix_timestamp(col("received_on")) / 600) * 600)).cast("timestamp")
        })
        .withWatermark("received_on", "1 minute")
        .groupBy(
            window("received_on", "10 minutes"),
            col("timestamp_10min_slice"),
            col("sso_events.country_code")
        )
        .agg(
            count(when((((col("sso_events.onlineservicename") == "discord")) | (col("sso_events.linktype") == "discord")) & (col("sso_events.name") == "LinkEvent"), True)).alias("num_discord_links"),
            count(when((((col("sso_events.onlineservicename") == "google")) | (col("sso_events.linktype") == "google")) & (col("sso_events.name") == "LinkEvent"), True)).alias("num_google_links"),
            count(when((((col("sso_events.onlineservicename") == "steam")) | (col("sso_events.linktype") == "steam")) & (col("sso_events.name") == "LinkEvent"), True)).alias("num_steam_links"),
            count(when((((col("sso_events.onlineservicename") == "twitch")) | (col("sso_events.linktype") == "twitch")) & (col("sso_events.name") == "LinkEvent"), True)).alias("num_twitch_links")
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
        .toTable(f"dataanalytics{environment}.standard_metrics.agg_account_link_10min")
    )

# COMMAND ----------

if __name__ == "__main__":
    start_stream()
