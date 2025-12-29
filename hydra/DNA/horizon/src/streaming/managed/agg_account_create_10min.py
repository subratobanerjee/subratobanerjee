# Databricks notebook source
import json
from pyspark.sql.functions import (lit, col, when, get_json_object, current_timestamp, split, floor, from_unixtime, unix_timestamp, ifnull, round, window, count, sha2, concat_ws)
from delta.tables import DeltaTable
import argparse
from pyspark.sql import SparkSession

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

def read_account_create_df(environment, spark):
    df = (
        spark
        .readStream
        .table(f"coretech{environment}.sso.accountcreateevent")
        .withColumns({
            "received_on": col("occurredOn").cast("timestamp"),
            "country_code": ifnull(get_json_object(col("eventGeoIp"), "$.countryCode"), lit("ZZ")).alias("country_code")
            })
        .alias("account_create")
    )

    return df

# COMMAND ----------

def extract(environment, spark):
    account_create_df = read_account_create_df(environment, spark)

    return account_create_df

# COMMAND ----------

def transform(df, environment, spark):
    df = (
        df
        .where(col("accountType")==3)
        .withColumns({
            "timestamp_10min_slice": from_unixtime(round(floor(unix_timestamp(col("received_on")) / 600) * 600)).cast("timestamp")
        })
        .withWatermark("received_on", "1 minute")
        .groupBy(
            window("received_on", "10 minutes"),
            col("timestamp_10min_slice"),
            col("country_code")
        )
        .agg(
            count(when(col("appId").isin("1f9a1706bd8444838b5eb44087589d64","3b90f3160348447db69d2176b39be97a"), col("accountId"))).alias("num_account_creates"),
        )
        .withColumns({
            "dw_insert_ts": current_timestamp(),
            "dw_update_ts": current_timestamp(),
            "merge_key": sha2(concat_ws("|", col("timestamp_10min_slice"), col("country_code")), 256)
        })
    )

    return df

# COMMAND ----------

def stream_agg_account_create():
    environment, checkpoint_location = arg_parser()
    #environment, checkpoint_location = "_dev", "dbfs:/tmp/horizon/managed/streaming/run_dev/agg_account_create_10min"
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
        .toTable(f"horizon{environment}.managed.agg_account_create_10min")
    )

# COMMAND ----------

if __name__ == "__main__":
    stream_agg_account_create()
