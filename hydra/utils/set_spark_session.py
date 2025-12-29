# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

def create_spark_session(name='Hydra',local_config={}):

	spark = SparkSession.builder.appName(f"{name}").getOrCreate()

	spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
	spark.conf.set("spark.databricks.delta.autoCompact.enabled", "false")
	spark.conf.set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")
	spark.conf.set("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")
	spark.conf.set("spark.sql.shuffle.partitions", "auto")
	return spark
