# Databricks notebook source
import json
from pyspark.sql.functions import (lit, col, when, get_json_object, current_timestamp, split, floor, from_unixtime, unix_timestamp, ifnull, round, window, count, sha2, concat_ws, from_json, expr, array_contains)
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

def read_account_ban_df(environment, spark):
    df = (
        spark
        .readStream
        .table(f"coretech{environment}.sso.accountbanevent")
        .withColumns({
            "received_on": col("occurredOn").cast("timestamp"),
            "bannedProducts": from_json(col("bannedProducts"), "array<struct<productId: string, expiresOn: long, reason: int>>")
            })
        .alias("ab")
    )

    return df

# COMMAND ----------

def read_player_df(environment, spark):
    df = (
        spark
        .read
        .table(f"horizon{environment}.managed.dim_player")
        .alias("player")
    )

    return df

# COMMAND ----------

def extract(environment, spark):
    acc_ban_df = read_account_ban_df(environment, spark)
    player_df = read_player_df(environment, spark)

    return (
        acc_ban_df
        .join(player_df, expr("ab.accountId = player.player_id"), "left")
    )

# COMMAND ----------

def transform(df, environment, spark):
    df = (
        df
        .withColumns({
            "timestamp_10min_slice": from_unixtime(round(floor(unix_timestamp(col("received_on")) / 600) * 600)).cast("timestamp"),
            "country_code": ifnull(col("first_seen_country_code"), lit("ZZ"))
        })
        .withWatermark("received_on", "1 minute")
        .groupBy(
            window("received_on", "10 minutes"),
            col("timestamp_10min_slice"),
            col("country_code")
        )
        .agg(
            count(when(col("globalBanReason") != lit("None"), True)).alias("global_bans"),
            count(when(array_contains(col("bannedProducts.productId"), "2a0041fa85174ca19d01803957f975d1"), True)).alias("horizon_bans"),
            count(when(~array_contains(col("bannedProducts.productId"), "2a0041fa85174ca19d01803957f975d1"), True)).alias("other_product_bans")
        )
        .withColumns({
            "dw_insert_ts": current_timestamp(),
            "dw_update_ts": current_timestamp(),
            "merge_key": sha2(concat_ws("|", col("timestamp_10min_slice"), col("country_code")), 256)
        })
    )

    return df

# COMMAND ----------

def start_stream():
    environment, checkpoint_location = arg_parser()
    # environment, checkpoint_location = "_dev", "dbfs:/tmp/horizon/managed/streaming/run_dev/agg_account_ban_10min"
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
        .toTable(f"horizon{environment}.managed.agg_account_ban_10min")
    )

# COMMAND ----------

if __name__ == "__main__":
    start_stream()
