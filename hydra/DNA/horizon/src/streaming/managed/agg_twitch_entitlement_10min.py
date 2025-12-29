# Databricks notebook source
import json
from pyspark.sql.functions import (lit, col, when, get_json_object, current_timestamp, split, floor, from_unixtime, unix_timestamp, ifnull, round, window, count, sha2, concat_ws, expr, coalesce)
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

def read_entitlement_df(environment, spark):
    # not filtering on itemid, some rows might get stuck in state if we don't progress the stream
    df = (
        spark
        .readStream
        .table(f"coretech{environment}.ecommercev2.entitlementgrantedevent")
        .withColumns({
            "received_on": col("receivedOn").cast("int").cast("timestamp"),
            "country_code": ifnull(col("countryCode"), lit("ZZ")).alias("country_code")
            })
        .alias("eg")
    )

    return df

# COMMAND ----------

def read_acc_create_df(environment, spark):
    df = (
        spark
        .read
        .table(f"coretech{environment}.sso.accountcreateevent")
        .alias("ac")
    )

    return df

# COMMAND ----------

def extract(environment, spark):
    entitlement_df = read_entitlement_df(environment, spark)
    acc_create_df = read_acc_create_df(environment, spark)
    df = entitlement_df.join(acc_create_df, 
                             expr("get_json_object(eg.eventData, '$.accountId')::string = ac.accountId"), 
                             "left")

    return df

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
            coalesce(col("ac.userSelectedCountry"), col("ac.geoIpCountry"), lit("ZZ")).alias("country_code")
        )
        .agg(
            count(when(get_json_object(col("eventData"), "$.itemId").cast("string") == '123c34868f3b45d198663584e2a02313', True)).alias("num_twitch_drops")
        )
        .withColumns({
            "dw_insert_ts": current_timestamp(),
            "dw_update_ts": current_timestamp(),
            "merge_key": sha2(concat_ws("|", col("timestamp_10min_slice"), col("country_code")), 256)
        })
        .select(
            "timestamp_10min_slice",
            "country_code",
            "num_twitch_drops",
            "dw_insert_ts",
            "dw_update_ts",
            "merge_key"
        )
    )

    return df

# COMMAND ----------

def start_stream():
    environment, checkpoint_location = arg_parser()
    # environment, checkpoint_location = "_dev", "dbfs:/tmp/horizon/managed/streaming/run_dev/agg_twitch_entitlement_10min"
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
        .toTable(f"horizon{environment}.managed.agg_twitch_entitlement_10min")
    )

# COMMAND ----------

if __name__ == "__main__":
    start_stream()
