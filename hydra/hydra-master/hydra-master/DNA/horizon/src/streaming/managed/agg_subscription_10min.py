# Databricks notebook source
import json
from pyspark.sql.functions import (lit, col, when, get_json_object, current_timestamp, split, floor, from_unixtime, unix_timestamp, ifnull, round, window, count,approx_count_distinct, sha2, concat_ws)
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

def read_subscription_df(environment, spark):
    df = (
        spark
        .readStream
        .table(f"coretech{environment}.sso.subscribeevent")
        .withColumns({
            "player_id": col("account.id"),
            "received_on": col("occurredOn").cast("timestamp"),
            "country_code": ifnull(get_json_object(col("geoIp"), "$.countryCode"), lit("ZZ")).alias("country_code")
            })
        .alias("eg")
    )

    return df

# COMMAND ----------

def extract(environment, spark):
    subscription_df = read_subscription_df(environment, spark)

    return subscription_df

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
            col("country_code")
        )
        .agg(
            approx_count_distinct(when(col("attribute.value").isin("gstest_01ja8n83c3az729a3snsyvz13e"), col("player_id"))).alias("eligible_eu_registrations"),
            approx_count_distinct(when(col("attribute.value").isin("gstest_01ja8n9z939ckwvp1hxg3s6wxx"), col("player_id"))).alias("eligible_na_registrations"),
            approx_count_distinct(when(col("attribute.value").isin("ethos"), col("player_id"))).alias("newsletter_subscriptions"),
            approx_count_distinct(when(col("attribute.value").isin("hzn_beta_key_delivered"), col("player_id"))).alias("keys_delivered"),
            approx_count_distinct(when(col("attribute.value").isin("hzn_beta_pc_spec"), col("player_id"))).alias("pc_specs_accepted"),
            approx_count_distinct(when(col("attribute.value").isin("hzn_beta_ineligible_registration"), col("player_id"))).alias("ineligible_registrations")
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
    # environment, checkpoint_location = "_dev", "dbfs:/tmp/horizon/managed/streaming/run_dev/agg_subscription_10min"
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
        .toTable(f"horizon{environment}.managed.agg_subscription_10min")
    )

# COMMAND ----------

if __name__ == "__main__":
    start_stream()