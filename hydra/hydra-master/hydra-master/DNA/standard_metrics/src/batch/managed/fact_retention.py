# Databricks notebook source
import json
from pyspark.sql.functions import (lit, col, when, get_json_object, current_timestamp, split,date_trunc)
from delta.tables import DeltaTable
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
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

def read_players_df(environment, spark):

    
    
    players_df = (
        spark
        .read
        .table(f"dataanalytics{environment}.standard_metrics.fact_player_ltd")
        .alias("logins")
        .where(col("player_id").isNotNull())
        #.where("dw_update_ts::date >= current_date - interval '3 day'")
    )
    return players_df

# COMMAND ----------

def transform(df, environment,spark):
    transformed_df = (
        df
        .groupBy(
            "platform",
            "service",
            "country_code",
            "title",
            "install_date"
        )
        .agg(
            countDistinct(col("player_id")).alias("installs"), 
            sum(when(col("day_1") == 1, 1).otherwise(0)).alias("day_1_count"),
            sum(when(col("day_2") == 1, 1).otherwise(0)).alias("day_2_count"),
            sum(when(col("day_3") == 1, 1).otherwise(0)).alias("day_3_count"),
            sum(when(col("day_7") == 1, 1).otherwise(0)).alias("day_7_count"),
            sum(when(col("day_30") == 1, 1).otherwise(0)).alias("day_30_count"),
            sum(when(col("day_60") == 1, 1).otherwise(0)).alias("day_60_count"),
            sum(when(col("week_1") == 1, 1).otherwise(0)).alias("week_1_count"),
            sum(when(col("week_2") == 1, 1).otherwise(0)).alias("week_2_count"),
            sum(when(col("week_3") == 1, 1).otherwise(0)).alias("week_3_count"),
            sum(when(col("week_4") == 1, 1).otherwise(0)).alias("week_4_count"),
            current_timestamp().alias("dw_insert_ts"),
            current_timestamp().alias("dw_update_ts")
        )
        .withColumn("merge_key", sha2(concat_ws("|", col("platform"), col("service"),col("title"),col("install_date"),col("country_code")), 256))  
    )
    return transformed_df

    

# COMMAND ----------

def load(df, environment, spark):
    (
        DeltaTable.createIfNotExists(spark)
        .tableName(f"dataanalytics{environment}.standard_metrics.fact_retention")
        .addColumn("platform", "string")
        .addColumn("service", "string")
        .addColumn("country_code", "string")
        .addColumn("title", "string")
        .addColumn("install_date", "date")
        .addColumn("installs", "int")
        .addColumn("day_1_count", "int")
        .addColumn("day_2_count", "int")
        .addColumn("day_3_count", "int")
        .addColumn("day_7_count", "int")
        .addColumn("day_30_count", "int")
        .addColumn("day_60_count", "int")
        .addColumn("week_1_count", "int")
        .addColumn("week_2_count", "int")
        .addColumn("week_3_count", "int")
        .addColumn("week_4_count", "int")
        .addColumn("dw_insert_ts", "timestamp")
        .addColumn("dw_update_ts", "timestamp")
        .addColumn("merge_key", "string")
        .property('delta.enableIcebergCompatV2', 'true')
        .property('delta.universalFormat.enabledFormats', 'iceberg')
        .execute()
    )

    target_table = DeltaTable.forName(spark, f"dataanalytics{environment}.standard_metrics.fact_retention")
    (
        target_table
        .alias("target")
        .merge(
            df.alias("source"),
            "target.merge_key = source.merge_key")
        .withSchemaEvolution()
        .whenMatchedUpdate(condition="(source.installs != target.installs) or (source.day_1_count != target.day_1_count) or (source.day_2_count != target.day_2_count) or (source.day_3_count != target.day_3_count) or (source.day_7_count != target.day_7_count) or (source.day_30_count != target.day_30_count) or (source.day_60_count != target.day_60_count) or (source.week_1_count != target.week_1_count) or (source.week_2_count != target.week_2_count) or (source.week_3_count != target.week_3_count) or (source.week_4_count != target.week_4_count)" , set =
                           {
                                "target.installs":"source.installs",
                                "target.day_1_count":"source.day_1_count",
                                "target.day_2_count":"source.day_2_count",
                                "target.day_3_count":"source.day_3_count",
                                "target.day_7_count":"source.day_7_count",
                                "target.day_30_count":"source.day_30_count",
                                "target.day_60_count":"source.day_60_count",
                                "target.week_1_count":"source.week_1_count",
                                "target.week_2_count":"source.week_2_count",
                                "target.week_3_count":"source.week_3_count",
                                "target.week_4_count":"source.week_4_count",
                                "target.dw_update_ts" : current_timestamp()
                           })
        .whenNotMatchedInsertAll()
        .execute()
    )
    

# COMMAND ----------

def batch_fact_retention():
    environment = dbutils.widgets.get("environment")
    checkpoint_location = dbutils.widgets.get("checkpoint")
    #environment, checkpoint_location = "_dev", "dbfs:/tmp/standard_metrics/managed/batch/run_dev/fact_retention"
    spark = SparkSession.builder.appName("Hydra").getOrCreate()

    spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

    df = read_players_df(environment, spark)
    df = transform(df, environment, spark)

    load(df, environment, spark)


    

# COMMAND ----------

if __name__ == "__main__":
    batch_fact_retention()
