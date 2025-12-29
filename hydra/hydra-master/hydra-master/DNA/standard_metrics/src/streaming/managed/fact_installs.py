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

def create_fact_installs(spark,environment):
    (
    DeltaTable.createIfNotExists(spark)
    .tableName(f"dataanalytics{environment}.standard_metrics.agg_install_nrt")
    .addColumn("timestamp_10min_slice", "timestamp")
    .addColumn("platform", "string")
    .addColumn("service", "string")
    .addColumn("territory_name", "string")
    .addColumn("install_count", "Long")
    .addColumn("dw_update_ts", "timestamp")
    .addColumn("title", "string")
    .addColumn("merge_key", "string")
    .property('delta.enableIcebergCompatV2', 'true')
    .property('delta.universalFormat.enabledFormats', 'iceberg')
    .execute()
)

# COMMAND ----------

def read_players_df(environment, spark):
  players_df = (
                spark.
                readStream.
                table(f"dataanalytics{environment}.standard_metrics.fact_player_nrt")
                .withColumn("player_join_key", sha2(concat_ws("|", col("install_timestamp_10min_slice"), col("platform"), col("service"), col("first_territory_name")), 256))
                .alias("players")
              )
  return players_df

# COMMAND ----------

def proc_batch(df, batch_id, checkpoint_path,environment,spark):
    

    df1 = (
        spark
        .read
        .table(f"dataanalytics{environment}.standard_metrics.platform_territory_10min_ts")
        .alias("timeslice")
        .withColumn("timeslice_join_key", sha2(concat_ws("|", col("timestamp_10min_slice"), col("platform"), col("service"), col("territory_name")), 256))
        .join(df,  expr("timeslice_join_key = player_join_key"), "left") 
        .groupBy(
            "timeslice.timestamp_10min_slice",
            "timeslice.platform", 
            "timeslice.service",
            "timeslice.territory_name"
            )
        .agg(
            count(col("install_timestamp_10min_slice")).alias("install_count"),
            current_timestamp().alias("dw_update_ts")
            )
        .withColumn("title", expr("'Horizon Beta'"))
        .withColumn(
            "merge_key", 
            sha2(concat_ws("|", col("timestamp_10min_slice"), col("platform"), col("service"), col("territory_name"), col("title")), 256)
        )
        .select("timestamp_10min_slice", 
                "platform", 
                "service", 
                "territory_name", 
                "install_count",
                "dw_update_ts",
                "title",
                "merge_key")
    )
    (
        DeltaTable.createIfNotExists(spark)
        .tableName(f"dataanalytics{environment}.standard_metrics.agg_install_nrt")
        .addColumns(df1.schema)
        .property('delta.enableIcebergCompatV2', 'true')
        .property('delta.universalFormat.enabledFormats', 'iceberg')
        .execute()
    )
    target_table = DeltaTable.forName(spark, f"dataanalytics{environment}.standard_metrics.agg_install_nrt")
    (
        target_table
        .alias("target")
        .merge(
            df1.alias("source"),
            "target.merge_key = source.merge_key")
        .withSchemaEvolution()
        .whenMatchedUpdate(condition="source.install_count <> target.install_count", set = 
                           {
                                "target.install_count": "source.install_count" ,
                                "target.dw_update_ts": "current_timestamp()"
                           })
       
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

def stream_installs():
    environment, checkpoint_location = arg_parser()
    #environment, checkpoint_location = "_dev", "dbfs:/tmp/standard_metrics/managed/streaming/run_dev/fact_install"
    spark = SparkSession.builder.appName("Hydra").getOrCreate()

    spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "false")
    spark.conf.set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")
    spark.conf.set("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")

    create_fact_installs(spark,environment)

    transformed_df = read_players_df(environment, spark)



    (
        transformed_df
        .writeStream
        .trigger(processingTime = "10 minutes")
        .outputMode("update")
        .foreachBatch(lambda df, batch_id: proc_batch(df, batch_id, checkpoint_location,environment,spark))
        .option("checkpointLocation", checkpoint_location)
        .start()
    )

# COMMAND ----------

if __name__ == "__main__":
    stream_installs()
