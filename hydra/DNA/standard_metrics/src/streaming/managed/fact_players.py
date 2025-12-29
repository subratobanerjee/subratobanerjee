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

def create_fact_players(spark,environment):
     (
    DeltaTable.createIfNotExists(spark)
    .tableName(f"dataanalytics{environment}.standard_metrics.fact_player_nrt")
    .addColumn("player_id", "string")
    .addColumn("parent_id", "string")
    .addColumn("platform", "string")
    .addColumn("service", "string")
    .addColumn("first_country_code", "string")
    .addColumn("first_territory_name", "string")
    .addColumn("install_timestamp", "timestamp")
    .addColumn("install_timestamp_10min_slice", "timestamp")
    .addColumn("dw_insert_ts", "timestamp")
    .addColumn("dw_update_ts", "timestamp")
    .addColumn("merge_key", "string")
    .property('delta.enableIcebergCompatV2', 'true')
    .property('delta.universalFormat.enabledFormats', 'iceberg')
    .execute()
    )

 

# COMMAND ----------

def read_title_df(environment, spark):
    title_df= (
                spark.
                read.
                table(f"reference{environment}.title.dim_title")
                .alias("title")
                )
    return title_df

# COMMAND ----------

def read_country_df(environment, spark):
    country_df = (
                spark.
                read.
                table(f"reference{environment}.location.dim_country")
                .alias("country")
                )
    return country_df



# COMMAND ----------

def read_subscriptions_df(environment, spark):
    subscriptions_df =(
        spark
        .readStream
        .table(f"coretech{environment}.sso.subscribeevent")
        .withColumns({
                "account_id": col("account.id"),
                "app_id": col("app.id"),
                "app_group_id": col("app.groupId"),
                "receivedon": col("occurredOn").cast("timestamp"),
                "countrycode": get_json_object(col("geoip"), "$.countryCode"),
                "subscription_name" : col("attribute.name"),
                "attribute_type": col("attribute.type")
        })
    #.where(col("app.groupId").isin("6aeb63f4494c4dca89e6f71b79789a84", "6d29e4bf8e1f4aefabde5526f2c221dc"))
    .where(col("attribute_type").isin("Newsletter","intent"))
    .withColumn( "join_key", sha2(concat_ws("|", col("account_id"), col("app_id"), col("app_group_id")), 256))
    .select(
        "account_id",
        "receivedon",
        "countrycode",
        "subscription_name",
        "app_id",
        "app_group_id",
        "join_key"
    )
    .alias("subscriptions")
    )
    return subscriptions_df

# COMMAND ----------

def read_logins_df(environment, spark):
    title_df = read_title_df(environment, spark)
    country_df = read_country_df(environment, spark)

    logins_df = (
        spark
        .readStream
        .table(f"coretech{environment}.sso.loginevent")
        .alias("logins")
        .withColumns({
                        "player_id": col("accountid"),
                        "parent_id": col("parentAccountId"),
                        "appid": col("appId"),
                        "appgroupid": col("appGroupId"),
                        "received_on": col("occurredOn").cast("timestamp"),
                        "received_on_10min_slice": from_unixtime(round(floor(unix_timestamp(col('received_on')) / 600) * 600)).cast("timestamp"),
                        "countrycode": get_json_object(col("geoip"), "$.countryCode"),                              
                        "login_join_key": sha2(concat_ws("|", col("player_id"), col("appid"), col("appgroupid")), 256)
                    })
        .where(col("appgroupid").isin("6aeb63f4494c4dca89e6f71b79789a84", "6d29e4bf8e1f4aefabde5526f2c221dc"))
        .where(col("isParentAccountDOBVerified")== lit(True))
        .join(title_df, expr("appId = title.app_id"), "left")
        .join(country_df.withColumnRenamed("name", "country_name"), expr("countrycode=country.country_code"), "left")
        .select(
                "player_id",
                "parent_id",
                col("display_platform").alias("platform"),
                col("display_service").alias("service"),
                "received_on",
                "appid",
                "appgroupid",
                "received_on_10min_slice",
                "login_join_key",
                "countrycode",
                "territory_name"
        )
    )
    return logins_df



# COMMAND ----------

def proc_batch(df, batch_id, checkpoint_path,environment,spark):
    df = (
        df
        .withColumns({
                "first_country_code":ifnull(first_value(col("countrycode")).over(Window.partitionBy(col("player_id"), col("platform")).orderBy(col("received_on"))),lit("ZZ")),
                "first_territory_name": ifnull(first_value(col("territory_name")).over(Window.partitionBy(col("player_id"), col("platform")).orderBy(col("received_on"))),lit("ZZ"))
            })
        .groupBy(
                "player_id",
                "parent_id",
                "platform",
                "service",
                "first_country_code",
                "first_territory_name"
               # "subscription_name"
            )
        .agg(
                min(col("received_on")).alias("install_timestamp"),
                min(col("received_on_10min_slice")).alias("install_timestamp_10min_slice"),
                current_timestamp().alias("dw_insert_ts"),
                current_timestamp().alias("dw_update_ts")
            )
        .where(col("player_id").isNotNull())
        .withColumn("merge_key", sha2(concat_ws("|", col("player_id"),col("parent_id"), col("platform"), col("service")), 256))
        .select("player_id",
                "parent_id",
                "platform",
                "service",
                "first_country_code",
                "first_territory_name",
                "install_timestamp",
                "install_timestamp_10min_slice",
                #"subscriptions.subscription_name",
                "dw_insert_ts",
                "dw_update_ts",
                "merge_key")
    )
    (
        DeltaTable.createIfNotExists(spark)
        .tableName(f"dataanalytics{environment}.standard_metrics.fact_player_nrt")
        .addColumns(df.schema)
        .property('delta.enableIcebergCompatV2', 'true')
        .property('delta.universalFormat.enabledFormats', 'iceberg')
        .execute()
    )
    target_table = DeltaTable.forName(spark, f"dataanalytics{environment}.standard_metrics.fact_player_nrt")
    (
        target_table
        .alias("target")
        .merge(
            df.alias("source"),
            "target.merge_key = source.merge_key")
        .withSchemaEvolution()
        .whenMatchedUpdate(condition="target.install_timestamp > source.install_timestamp", set = 
                           {
                                "target.install_timestamp": "source.install_timestamp" ,
                                "target.install_timestamp_10min_slice": "source.install_timestamp_10min_slice" ,
                                "target.first_country_code": "source.first_country_code" ,
                                "target.first_territory_name": "source.first_territory_name" ,
                                "target.dw_update_ts": "current_timestamp()"
                           })
       
        .whenNotMatchedInsertAll()
        .execute()
    )



# COMMAND ----------

def stream_players():
    environment, checkpoint_location = arg_parser()
    #environment, checkpoint_location = "_stg", "dbfs:/tmp/standard_metrics/managed/streaming/run_stg/fact_player"
    spark = SparkSession.builder.appName("Hydra").getOrCreate()

    spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "false")
    spark.conf.set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")
    spark.conf.set("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")

  
    create_fact_players(spark,environment)

    transformed_df = read_logins_df(environment, spark)

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
    stream_players()
