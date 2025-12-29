# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------
import json
from pyspark.sql.functions import (
    col, 
    sha2, 
    concat_ws, 
    current_timestamp, 
    first_value,
    last_value,
    coalesce,
    max,
    when,
    min,
    any_value,
    rank
    )
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import Window
import argparse
import logging

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

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
        .table(f"inverness{environment}.intermediate.fact_player_activity")
        .where("dw_insert_ts::date >= current_date - interval '3 day'")
    )

    return dim_player_df


# COMMAND ----------

def transform(df, environment, spark):
    transformed_campaign_df = (
        df
        .alias("dim_player")
        .where(col("source_table")=='campaignstatus')
        .where(~col("player_id").contains("NULL"))
        .where(col("platform").isNotNull())
        .where(col("player_id")!='anonymous')
        .withColumns({
                    "player_id": col("player_id"),
                    "platform": col("platform"),
                    "service": col("service"),
                    "received_on": col("received_on"),
                    "first_campaign": first_value(col("extra_info_5"), ignoreNulls=True).over(Window.partitionBy(col("player_id"), col("platform"), col        
                                     ("service")).orderBy(col("received_on").asc(),col("extra_info_5"))),
                    "last_campaign": first_value(col("extra_info_5"), ignoreNulls=True).over(Window.partitionBy(col("player_id"), col("platform"), col 
                                     ("service")).orderBy(col("received_on").desc(),col("extra_info_5"))),  
                    "first_age_nm": first_value(col("extra_info_1"), ignoreNulls=True).over(Window.partitionBy(col("player_id"), col("platform"), col("service")). 
                                     orderBy (col("received_on").asc(),col("extra_info_1"))),
                    "last_age_nm": first_value(col("extra_info_1"), ignoreNulls=True).over(Window.partitionBy(col("player_id"), col("platform"), col("service")).orderBy 
                                     (col("received_on").desc(),col("extra_info_1"))),
                    "first_turn_nmbr": min(col("extra_info_4").cast("int")).over(Window.partitionBy(col("player_id"), col("platform"), col 
                                     ("service")).orderBy(col("received_on").asc(),col("extra_info_4"))),
                    "last_turn_nmbr": max(col("extra_info_4").cast("int")).over(Window.partitionBy(col("player_id"), col("platform"), col 
                                     ("service")).orderBy(col("received_on").desc(),col("extra_info_4"))),
                    "first_leader_nm":first_value(col("extra_info_3"), ignoreNulls=True).over(Window.partitionBy(col("player_id"), col("platform"), col 
                                     ("service")).orderBy(col("received_on").asc(),col("extra_info_3"))),
                    "last_leader_nm":first_value(col("extra_info_3"), ignoreNulls=True).over(Window.partitionBy(col("player_id"), col("platform"), col 
                                     ("service")).orderBy(col("received_on").desc(),col("extra_info_3"))),
                    "first_civ_nm":first_value(col("extra_info_2"), ignoreNulls=True).over(Window.partitionBy(col("player_id"), col("platform"), col 
                                     ("service")).orderBy(col("received_on").asc(),col("extra_info_2"))),
                    "last_civ_nm":first_value(col("extra_info_2"), ignoreNulls=True).over(Window.partitionBy(col("player_id"), col("platform"), col 
                                     ("service")).orderBy(col("received_on").desc(),col("extra_info_2")))

                })
        
        .select
        ("player_id",
         "platform",
         "service",
         "first_campaign",
         "last_campaign",
         "first_age_nm",
         "last_age_nm",
         "first_turn_nmbr",
         "last_turn_nmbr",
         "first_leader_nm",
         "last_leader_nm",
         "first_civ_nm",
         "last_civ_nm")
        .alias("transform")
    )
    int_campaign_df=(
        transformed_campaign_df

        .groupBy("transform.player_id",
                 "transform.platform",
                 "transform.service"
                )
        .agg(
             max(col("transform.first_campaign")).alias("first_campaign_id"),
             max(col("transform.last_campaign")).alias("last_campaign_id"),
             max(col("transform.first_age_nm")).alias("first_age"),
             max(col("transform.last_age_nm")).alias("last_age"),
             first_value(col("transform.first_turn_nmbr")).alias("first_turn"),
             last_value(col("transform.last_turn_nmbr")).alias("last_turn"),
             max(col("transform.first_leader_nm")).alias("first_leader"),
             max(col("transform.last_leader_nm")).alias("last_leader"),
             max(col("transform.first_civ_nm")).alias("first_civ"),
             max(col("transform.last_civ_nm")).alias("last_civ")
        )
        .alias("campaign")

    )
    transformed_all_df=(
        df
        .alias("dim_player")
        .where(~col("player_id").contains("NULL"))
        .where(col("platform").isNotNull())
        .where(col("player_id")!='anonymous')
        .withColumns({
                    "player_id": col("player_id"),
                    "platform": col("platform"),
                    "service": col("service"),
                    "received_on": col("received_on"),
                    "country_code": col("country_code"),
                    "first_seen_time": first_value(col("received_on")).over(Window.partitionBy(col("player_id"), col("platform"), col 
                                      ("service")).orderBy(col("received_on").asc())),
                    "last_seen_time": last_value(col("received_on")).over(Window.partitionBy(col("player_id"), col("platform"), col 
                                      ("service")).orderBy(col("received_on").desc())),
                    "first_seen_cc": first_value(col("country_code")).over(Window.partitionBy(col("player_id"), col("platform"), 
                                      col("service")).orderBy(col("received_on").cast("date").asc(),col("country_code"))),
                    "last_seen_cc": last_value(col("country_code")).over(Window.partitionBy(col("player_id"), col("platform"), col 
                                      ("service")).orderBy(col("received_on").cast("date").desc(),col("country_code")))
                    })
        .select
        ("player_id",
         "platform",
         "service",
         "first_seen_time",
         "last_seen_time",
         "first_seen_cc",
         "last_seen_cc")
        .alias("transform_all")
    )

    int_all_df=(
        transformed_all_df

        .groupBy("transform_all.player_id",
                 "transform_all.platform",
                 "transform_all.service"
                )
        .agg( 
            max(col("transform_all.first_seen_time")).alias("first_seen_time"), 
            max(col("transform_all.last_seen_time")).alias("last_seen_time"), 
            max(col("transform_all.first_seen_cc")).alias("first_seen_country_code"), 
            max(col("transform_all.last_seen_cc")).alias("last_seen_country_code")

        )
        .alias("all")

    )


    final_df=(
        int_all_df
        .join(int_campaign_df, (col("all.player_id") == col("campaign.player_id")) &
         (col("all.platform") == col("campaign.platform")) &
         (col("all.service") == col("campaign.service")), how="left")
        .withColumn("merge_key", sha2(concat_ws("|", col("all.player_id"), col("all.platform"), col("all.service")), 256))
        .withColumn("dw_insert_ts",current_timestamp())
        .withColumn("dw_update_ts",current_timestamp())
        .where(col("all.player_id").isNotNull())
        .select
        (
            "all.player_id",
            "all.platform",
            "all.service",
            "campaign.first_campaign_id",
            "campaign.last_campaign_id",
            "campaign.first_age",
            "campaign.last_age",
            "campaign.first_turn",
            "campaign.last_turn",
            "campaign.first_leader",
            "campaign.last_leader",
            "campaign.first_civ",
            "campaign.last_civ",
            "all.first_seen_time",
            "all.last_seen_time",
            "all.first_seen_country_code",
            "all.last_seen_country_code",
            "merge_key",
            "dw_insert_ts",
            "dw_update_ts"
        )
        )
    return final_df


# COMMAND ----------

def load(df, environment, spark):
    (
        DeltaTable.createIfNotExists(spark)
        .tableName(f"inverness{environment}.managed.fact_player_summary_ltd")
        .addColumn("player_id", "string")
        .addColumn("platform", "string")
        .addColumn("service", "string")
        .addColumn("first_campaign_id", "string")
        .addColumn("last_campaign_id", "string")
        .addColumn("first_age", "string")
        .addColumn("last_age", "string")
        .addColumn("first_turn", "string")
        .addColumn("last_turn", "string")
        .addColumn("first_leader", "string")
        .addColumn("last_leader", "string")
        .addColumn("first_civ", "string")
        .addColumn("last_civ", "string")
        .addColumn("first_seen_time", "timestamp")
        .addColumn("last_seen_time", "timestamp")
        .addColumn("first_seen_country_code", "string")
        .addColumn("last_seen_country_code", "string")
        .addColumn("dw_insert_ts", "timestamp")
        .addColumn("dw_update_ts", "timestamp")
        .addColumn("merge_key", "string")
        .property('delta.enableIcebergCompatV2', 'true')
        .property('delta.universalFormat.enabledFormats', 'iceberg')
        .execute()
    )

    target_table = DeltaTable.forName(spark, f"inverness{environment}.managed.fact_player_summary_ltd")
    (
        target_table
        .alias("target")
        .merge(
            df.alias("source"),
            "target.merge_key = source.merge_key")
        .withSchemaEvolution()
        .whenMatchedUpdate(condition=(
                "(source.first_seen_time < target.first_seen_time) or (target.first_campaign_id is null) or (target.first_age is null) or (target.first_turn is null) or (target.first_leader is null) or (target.first_civ is null) or (target.first_seen_country_code is null)"
            ),
                           set = 
                           {
                                "target.first_campaign_id": coalesce("target.first_campaign_id","source.first_campaign_id"),
                                "target.first_age":coalesce("target.first_age","source.first_age"),
                                "target.first_turn":coalesce("target.first_turn","source.first_turn"),
                                "target.first_seen_time": coalesce("target.first_seen_time","source.first_seen_time") ,
                                "target.first_seen_country_code": coalesce("target.first_seen_country_code", "source.first_seen_country_code"),
                                "target.first_leader": coalesce("target.first_leader","source.first_leader"),
                                "target.first_civ": coalesce("target.first_civ","source.first_civ"),
                                "target.last_campaign_id": coalesce("target.last_campaign_id","source.last_campaign_id"),
                                "target.last_age":"source.last_age",
                                "target.last_turn":"source.last_turn",
                                "target.last_seen_time": "source.last_seen_time" ,
                                "target.last_seen_country_code": coalesce("source.last_seen_country_code", "target.last_seen_country_code"),
                                "target.last_leader":"source.last_leader",
                                "target.last_civ":"source.last_civ",
                                "target.dw_update_ts": "current_timestamp()"
                           })
        .whenMatchedUpdate(condition="source.last_seen_time > target.last_seen_time", set ={
                                "target.last_campaign_id": "source.last_campaign_id",
                                "target.last_age":"source.last_age",
                                "target.last_turn":"source.last_turn",
                                "target.last_seen_time": "source.last_seen_time" ,
                                "target.last_seen_country_code": coalesce("source.last_seen_country_code", "target.last_seen_country_code"),
                                "target.last_leader":"source.last_leader",
                                "target.last_civ":"source.last_civ",
                                "target.dw_update_ts": "current_timestamp()"
                           })
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

def process_fact_player():
    environment = dbutils.widgets.get("environment")
    checkpoint_location = dbutils.widgets.get("checkpoint")
    #environment, checkpoint_location = "_stg", "dbfs:/tmp/inverness/batch/managed/run_dev/fact_player_summary_ltd_1"
    spark = create_spark_session()

    spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

    df = extract(spark,environment)
    
    df = transform(df, environment, spark)

    load(df, environment, spark)


# COMMAND ----------

if __name__ == "__main__":
    process_fact_player()
