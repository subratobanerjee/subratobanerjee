# Databricks notebook source
#from utils.helpers import arg_parser,setup_spark
import json
from pyspark.sql.functions import (
    col, 
    sha2, 
    concat_ws, 
    current_timestamp, 
    first_value,
    last_value,
    coalesce,
    max
    )
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import Window
import argparse
import logging

# COMMAND ----------


def arg_parser():
    parser = argparse.ArgumentParser(description="consume parameters from Databricks job")
    parser.add_argument("--environment", type=str, required=True, help="Description for param1")
    parser.add_argument("--checkpoint_location", type=str, required=True, help="Description for param2")
    
    args = parser.parse_args()
    environment = args.environment
    checkpoint_location = args.checkpoint_location

    return environment,checkpoint_location

# COMMAND ----------

def extract(spark,environment):

    dim_player_df = (
        spark
        .read
        .table(f"horizon{environment}.intermediate.fact_player_activity")
        .where(col("source_table")=='loginevent')
        .where(col("platform").isNotNull())
        .where("dw_insert_ts::date >= current_date - interval '3 day'")
    )

    return dim_player_df

# COMMAND ----------

def transform(df, environment, spark):
    transformed_df = (
        df
        .alias("dim_player")
        .withColumns({
                    "player_id": col("player_id"),
                    "parent_id": col("parent_id"),
                    "platform": col("platform"),
                    "service": col("service"),
                    "received_on": col("received_on"),
                    "country_code": col("country_code"),
                    "language_setting": col("language_setting"),
                    "first_seen_time": first_value(col("received_on")).over(Window.partitionBy(col("player_id"),col("parent_id"), col("platform"), col("service"))),
                    "last_seen_time": last_value(col("received_on")).over(Window.partitionBy(col("player_id"),col("parent_id"), col("platform"), col("service"))),
                    "first_seen_cc": first_value(col("country_code")).over(Window.partitionBy(col("player_id"),col("parent_id"), col("platform"), col("service")).orderBy(col("received_on"),col("country_code"))),
                    "last_seen_cc": last_value(col("country_code")).over(Window.partitionBy(col("player_id"),col("parent_id"), col("platform"), col("service")).orderBy(col("received_on"),col("country_code"))),
                    "first_seen_language": first_value(col("language_setting")).over(Window.partitionBy(col("player_id"),col("parent_id"), col("platform"), col("service")).orderBy(col("received_on"),col("language_setting"))),
                    "last_seen_language": last_value(col("language_setting")).over(Window.partitionBy(col("player_id"),col("parent_id"), col("platform"), col("service")).orderBy(col("received_on"),col("language_setting")))
                })
        .where(col("player_id").isNotNull())
        .where(col("platform").isNotNull())
        
        .select
        ("player_id",
         "parent_id",
         "platform",
         "service",
         "first_seen_time",
         "last_seen_time",
         "first_seen_cc",
         "last_seen_cc",
         "first_seen_language",
         "last_seen_language")
    )

    final_df=(
        transformed_df
        .groupBy("player_id",
                    "parent_id",
                    "platform",
                    "service"
                )
        .agg(max(col("first_seen_time")).alias("first_seen"), 
             max(col("last_seen_time")).alias("last_seen"), 
             max(col("first_seen_cc")).alias("first_seen_country_code"), 
             max(col("last_seen_cc")).alias("last_seen_country_code"), 
             max(col("first_seen_language")).alias("first_seen_lang"), 
             max(col("last_seen_language")).alias("last_seen_lang")
        )
        .withColumn("merge_key", sha2(concat_ws("|", col("player_id"),col("parent_id"), col("platform"), col("service")), 256))
        .withColumn("dw_insert_ts",current_timestamp())
        .withColumn("dw_update_ts",current_timestamp())
        
    )
    return final_df


# COMMAND ----------

def load(df, environment, spark):
    (
        DeltaTable.createIfNotExists(spark)
        .tableName(f"horizon{environment}.managed.dim_player")
        .addColumn("player_id", "string")
        .addColumn("parent_id", "string")
        .addColumn("platform", "string")
        .addColumn("service", "string")
        .addColumn("first_seen", "timestamp")
        .addColumn("last_seen", "timestamp")
        .addColumn("first_seen_country_code", "string")
        .addColumn("last_seen_country_code", "string")
        .addColumn("first_seen_lang", "string")
        .addColumn("last_seen_lang", "string")
        .addColumn("dw_insert_ts", "timestamp")
        .addColumn("dw_update_ts", "timestamp")
        .addColumn("merge_key", "string")
        .property('delta.enableIcebergCompatV2', 'true')
        .property('delta.universalFormat.enabledFormats', 'iceberg')
        .execute()
    )

    target_table = DeltaTable.forName(spark, f"horizon{environment}.managed.dim_player")
    (
        target_table
        .alias("target")
        .merge(
            df.alias("source"),
            "target.merge_key = source.merge_key")
        .withSchemaEvolution()
        .whenMatchedUpdate(condition="source.first_seen < target.first_seen", set = 
                           {
                                "target.first_seen": "source.first_seen" ,
                                "target.first_seen_country_code": coalesce("source.first_seen_country_code", "target.first_seen_country_code"),
                                "target.first_seen_lang": coalesce("source.first_seen_lang", "target.first_seen_lang"),
                                "target.dw_update_ts": "current_timestamp()"
                           })
        .whenMatchedUpdate(condition="source.last_seen > target.last_seen", set ={
                                "target.last_seen": "source.last_seen" ,
                                "target.last_seen_country_code": coalesce("source.last_seen_country_code", "target.last_seen_country_code"),
                                "target.last_seen_lang": coalesce("source.last_seen_lang", "target.last_seen_lang"),
                                "target.dw_update_ts": "current_timestamp()"
                           })
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

def process_dim_player():
    environment, checkpoint_location = arg_parser()
    #environment, checkpoint_location = "_stg", "dbfs:/tmp/horizon/raw/run_stg/dim_player"
    spark = SparkSession.builder.appName("Hydra").getOrCreate()

    spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

    df = extract(spark,environment)
    
    df = transform(df, environment, spark)

    load(df, environment, spark)


# COMMAND ----------

if __name__ == "__main__":
    process_dim_player()
