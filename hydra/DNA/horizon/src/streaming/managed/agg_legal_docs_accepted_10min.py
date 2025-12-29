# Databricks notebook source
import json
from pyspark.sql.functions import (lit, col, when, get_json_object, current_timestamp, split, floor, from_unixtime, unix_timestamp, ifnull, round, window, count, sha2, concat_ws,get_json_object,from_json,array_join,expr, explode,sum,max)
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

def create_legal_docs(spark,environment):
    (
        DeltaTable.createIfNotExists(spark)
        .tableName(f"horizon{environment}.managed.agg_legal_docs_accepted_10min")
        .addColumn("timestamp_10min_slice", "timestamp")
        .addColumn("platform", "string")
        .addColumn("service", "string")
        .addColumn("country_code", "string")
        .addColumn("cnt_privacy_policy_accepted", "Long")
        .addColumn("cnt_terms_of_service_accepted", "Long")
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

def read_platform_territory_df(environment, spark):
    platform_territory_df= (
                        spark.
                        read.
                        table(f"dataanalytics{environment}.standard_metrics.platform_territory_10min_ts")
                        .where(col("timestamp_10min_slice")>='2024-10-01T00:00:00.000')
                        .where(col("timestamp_10min_slice")<'2024-11-01T00:00:00.000')
                        .alias("timeslice")
                        .persist()
    )
    return platform_territory_df

# COMMAND ----------

def read_update_df(environment, spark):
    title_df = read_title_df(environment, spark)
    update_df = (
        spark
        .readStream
        .table(f"coretech{environment}.sso.accountupdateevent ")
        .withColumns({
            "player_id": col("accountid"),
            "received_on": col("occurredOn").cast("int").cast("timestamp"),
            "country_code":  get_json_object(col("eventGeoIp"), "$.countryCode"),
            "appid": col("appId"),
            "appgroupid": col("appGroupId"),
            "documents_accepted": explode(split(concat_ws(',', from_json(col("newDocumentsAccepted"), 'array<string>')),",")),
            "timestamp_10min_slice": from_unixtime(round(floor(unix_timestamp(col('received_on')) / 600) * 600)).cast("timestamp")
            })
        .where(col("appgroupid").isin("6aeb63f4494c4dca89e6f71b79789a84", "6d29e4bf8e1f4aefabde5526f2c221dc"))
        .where(col("received_on") >= '2024-10-01T00:00:00.000')
        .join(title_df, expr("appId = title.app_id"), "left")
        .withColumn("update_join_key", sha2(concat_ws("|", col("timestamp_10min_slice"), col("display_platform"), col("display_service"), col("country_code")), 256))
        .select(
                "player_id",
                col("display_platform").alias("platform"),
                col("display_service").alias("service"),
                "received_on",
                "appid",
                "appgroupid",
                "timestamp_10min_slice",
                "documents_accepted",
                "update_join_key",
                "country_code"
        )
        .alias("updates")
    )

    return update_df

# COMMAND ----------

def transform(df, environment, spark):
    df= (
        df
        .withWatermark("received_on", "1 minute")
        .groupBy(
            window("received_on", "10 minutes"),
            "timestamp_10min_slice",
            ifnull(col("platform"), lit('Unknown')).alias("platform"),
            ifnull(col("service"), lit('Unknown')).alias("service"),
            ifnull(col("country_code"), lit('Unknown')).alias("country_code")
        )
        .agg(
            count(when(col("documents_accepted").isin('493226ed82cb48df8a4cc0c3b9b86408','1fcc96a527f845f9a41e216ab45afc4f'), True)).alias("cnt_privacy_policy_accepted"),
            count(when(col("documents_accepted").isin('70e567d7c9aa47ffabddb3504963f906','e96815b458344479bd82675d5cee6c43'), True)).alias("cnt_terms_of_service_accepted")
        )
        .withColumns({
            "merge_key": sha2(concat_ws("|", col("timestamp_10min_slice"), col("platform"), col("service"), col("country_code")), 256),
            "dw_insert_ts": current_timestamp(),
            "dw_update_ts": current_timestamp()
        })
        .alias("fact_legal")
    )
    
    # df1= (
    #     platform_territory_df
    #      .join(
    #      df,
    #      (col("timestamp_10min_slice") == col("fact_legal.received_on_10min_slice")) &
    #      (col("country_code") == col("fact_legal.countrycode")) &
    #      (col("platform") == col("fact_legal.legal_platform")) &
    #      (col("service") == col("fact_legal.legal_service")),
    #      "left"
    #  )
    #      .groupBy("timestamp_10min_slice",
    #             col("timeslice.platform"), 
    #             col("timeslice.service"),
    #             col("timeslice.country_code"),
    #             col("timeslice.territory_name"))
    #      .agg(
    #          ifnull(sum(col("fact_legal.cnt_pp_accepted")), lit(0)).alias("cnt_privacy_policy_accepted"),
    #          ifnull(sum(col("fact_legal.cnt_tos_accepted")), lit(0)).alias("cnt_terms_of_service_accepted")
    #      )
        #  .withColumns({
        #      "merge_key": sha2(concat_ws("|", col("timestamp_10min_slice"), col("timeslice.platform"), col("timeslice.service"), col("timeslice.country_code")), 256),
        #      "dw_update_ts": current_timestamp()

        #  })
    #      .select("timestamp_10min_slice",
    #              "platform",
    #              "service",
    #              "country_code",
    #              "cnt_privacy_policy_accepted",
    #              "cnt_terms_of_service_accepted",
    #              "dw_update_ts",
    #              "merge_key")
    #         )
    
    # target_table = DeltaTable.forName(spark, f"horizon{environment}.managed.agg_legal_docs_accepted_10min")
    # (
    #     target_table
    #     .alias("target")
    #     .merge(
    #         df.alias("source"),
    #         "target.merge_key = source.merge_key")
    #     .withSchemaEvolution()
    #     .whenMatchedUpdate(condition="(source.cnt_privacy_policy_accepted <> target.cnt_privacy_policy_accepted) or (source.cnt_terms_of_service_accepted <> target.cnt_terms_of_service_accepted)", set = 
    #                        {
    #                             "target.cnt_privacy_policy_accepted": "source.cnt_privacy_policy_accepted" ,
    #                             "target.cnt_terms_of_service_accepted": "source.cnt_terms_of_service_accepted" ,
    #                             "target.dw_update_ts": "current_timestamp()"
    #                        })
       
    #     .whenNotMatchedInsertAll()
    #     .execute()
    # )

    return df

# COMMAND ----------

def stream_agg_legal_docs():
    environment, checkpoint_location = arg_parser()
    # environment, checkpoint_location = "_dev", "dbfs:/tmp/horizon/managed/streaming/run_dev/agg_legal_docs_accepted_10min"
    spark = SparkSession.builder.appName("Hydra").getOrCreate()

    spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "false")
    spark.conf.set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")
    spark.conf.set("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")

    create_legal_docs(spark,environment)

    transformed_df = read_update_df(environment, spark)
    transformed_df = transform(transformed_df, environment, spark)
    # platform_territory_df = read_platform_territory_df(environment, spark)

    (
        transformed_df
        .writeStream
        .trigger(processingTime = "1 minute")
        .option("mergeSchema", "true")
        # .foreachBatch(lambda df, batch_id: proc_batch(df, batch_id, checkpoint_location,environment,spark))
        .option("checkpointLocation", checkpoint_location)
        .toTable(f"horizon{environment}.managed.agg_legal_docs_accepted_10min")
    )

# COMMAND ----------

if __name__ == "__main__":
    stream_agg_legal_docs()
