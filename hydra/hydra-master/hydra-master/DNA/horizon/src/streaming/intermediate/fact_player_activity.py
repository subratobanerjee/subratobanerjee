# Databricks notebook source
import json
from pyspark.sql.functions import (lit, col, when, get_json_object, current_timestamp, split)
from delta.tables import DeltaTable
from pyspark.sql.window import Window
import argparse
from pyspark.sql import SparkSession
import logging
from pyspark.sql.functions import *
from pyspark.sql.types import *

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

def create_player_activity(spark,environment): 
    (
        DeltaTable.createIfNotExists(spark)
        .tableName(f"horizon{environment}.intermediate.fact_player_activity")
        .addColumn("source_table", "string")
        .addColumn("player_id", "string")
        .addColumn("parent_id", "string")
        .addColumn("platform", "string")
        .addColumn("service", "string")
        .addColumn("title", "string")
        .addColumn("received_on", "timestamp")
        .addColumn("desc", "string")
        .addColumn("event_trigger", "string")
        .addColumn("country_code", "string")
        .addColumn("build_changelist", "string")
        .addColumn("language_setting", "string")
        .addColumn("extra_info_1", "string")
        .addColumn("extra_info_2", "string")
        .addColumn("extra_info_3", "string")
        .addColumn("extra_info_4", "string")
        .addColumn("extra_info_5", "string")
        .addColumn("extra_info_6", "string")
        .addColumn("extra_info_7", "string")
        .addColumn("extra_info_8", "string")
        .addColumn("extra_info_9", "string")
        .addColumn("extra_info_10", "string")
        .addColumn("dw_insert_ts", "timestamp")
        .addColumn("dw_update_ts", "timestamp")
        .property('delta.enableIcebergCompatV2', 'true')
        .property('delta.universalFormat.enabledFormats', 'iceberg')
        .execute()
    )

# COMMAND ----------

def read_ms_df(environment, spark):
    ms_df = (
        spark
        .readStream
        .table(f"horizon{environment}.raw.match_status_event")
        .withColumns({
                        "source_table": lit("match_status_event"),
                        "player_id": col("player.dna_account_id"),
                        "parent_id": lit("NA"),
                        "platform": lit("NA"),
                        "service": lit("NA"),
                        "title":lit("Horizon Beta"),
                        "received_on": col("receivedOn").cast("timestamp"),
                        "desc": col("extra_details.match_outcome_condition"),
                        "event_trigger": col("event_trigger"),
                        "country_code": lit("NA"),
                        "build_changelist": col("event_defaults.cl"),
                        "language_setting": lit("NA"),
                        "extra_info_1": when(col("event_defaults.match_type") == "Trials", col("group.id")).otherwise(col("event_defaults.match_id")),
                        "extra_info_2": col("event_defaults.match_type"),
                        "extra_info_3": col("group.id"),
                        "extra_info_4": col("event_defaults.server_id"),
                        "extra_info_5": col("hero.type"),
                        "extra_info_6": get_json_object(col("hero.position"), "$.X").cast("string"),
                        "extra_info_7": get_json_object(col("hero.position"), "$.Y").cast("string"),
                        "extra_info_8": get_json_object(col("hero.position"), "$.Z").cast("string"),
                        "extra_info_9": col("gmi.gameplay_instance_id"),
                        "extra_info_10": lower(col("player.session_id")),
                        "dw_insert_ts": current_timestamp(),
                        "dw_update_ts": current_timestamp()
                    })
        .where(col("player_id").isNotNull())
        .where(col("extra_info_1").isNotNull())
        .where(col("extra_info_3").isNotNull())
        .where(col("event_defaults.build_environment") != "development_editor")
        .select("source_table", 
                "player_id",
                "parent_id", 
                "platform", 
                "service", 
                "title",
                "received_on", 
                "desc", 
                "event_trigger", 
                "country_code", 
                "build_changelist", 
                "language_setting", 
                "extra_info_1", 
                "extra_info_2", 
                "extra_info_3", 
                "extra_info_4", 
                "extra_info_5", 
                "extra_info_6", 
                "extra_info_7", 
                "extra_info_8", 
                "extra_info_9", 
                "extra_info_10", 
                "dw_insert_ts", 
                "dw_update_ts")
    )
    return ms_df

# COMMAND ----------

def read_spa_df(environment, spark):
        spa_df = (
        spark
        .readStream
        .table(f"horizon{environment}.raw.player_action_event")
        .withColumns({
                        "source_table": lit("player_action_event"),
                        "player_id": col("player.dna_account_id"),
                        "parent_id": lit("NA"),
                        "platform": lit("NA"),
                        "service": lit("NA"),
                        "title":lit("Horizon Beta"),
                        "received_on": col("receivedOn").cast("timestamp"),
                        "desc": when(col("event_trigger") == "ChestOpened", split(col("extra_details.classname"), "_")[1])
                                .when(col("event_trigger") == "item_used", col("extra_details.item_name"))
                                .otherwise(lit("NA")),
                        "event_trigger": col("event_trigger"),
                        "country_code": lit("NA"),
                        "build_changelist": col("event_defaults.cl"),
                        "language_setting": lit("NA"),
                        "extra_info_1": when(col("event_defaults.match_type") == "Trials", col("group.id")).otherwise(col("event_defaults.match_id")),
                        "extra_info_2": col("event_defaults.match_type"),
                        "extra_info_3": col("group.id"),
                        "extra_info_4": col("event_defaults.server_id"),
                        "extra_info_5": col("hero.type"),
                        "extra_info_6": get_json_object(col("hero.position"), "$.X").cast("string"),
                        "extra_info_7": get_json_object(col("hero.position"), "$.Y").cast("string"),
                        "extra_info_8": get_json_object(col("hero.position"), "$.Z").cast("string"),
                        "extra_info_9": col("gmi.gameplay_instance_id"),
                        "extra_info_10": lower(col("player.session_id")),
                        "dw_insert_ts": current_timestamp(),
                        "dw_update_ts": current_timestamp()
                        })
        .where(col("player_id").isNotNull())
        .where(col("extra_info_1").isNotNull())
        .where(col("extra_info_3").isNotNull())
        .where(col("event_defaults.build_environment") != "development_editor")
        .select("source_table", 
                "player_id", 
                "parent_id",
                "platform", 
                "service", 
                "title",
                "received_on", 
                "desc", 
                "event_trigger", 
                "country_code", 
                "build_changelist", 
                "language_setting", 
                "extra_info_1", 
                "extra_info_2", 
                "extra_info_3", 
                "extra_info_4", 
                "extra_info_5", 
                "extra_info_6", 
                "extra_info_7", 
                "extra_info_8", 
                "extra_info_9", 
                "extra_info_10", 
                "dw_insert_ts", 
                "dw_update_ts")
        )
        return spa_df

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

def read_logins_df(environment, spark):
    title_df = read_title_df(environment, spark)
    logins_df = (
         spark
        .readStream
        .table(f"coretech{environment}.sso.loginevent")
        .join(title_df, expr("appId = title.app_id"), "left")
        .withColumns({
                        "source_table": lit("loginevent"),
                        "player_id": col("accountId"),
                        "parent_id":col("parentAccountId"),
                        "platform": col("title.display_platform"),
                        "service": col("title.display_service"),
                        "title": col("title.title"),
                        "received_on": col("occurredOn").cast("timestamp"),
                        "desc": lit("SSO2 Logins"),
                        "event_trigger": lit("login"),
                        "country_code": get_json_object(col("geoip"), "$.countryCode"),
                        "build_changelist": lit("NA"),
                        "language_setting": col("language"),
                        "extra_info_1": lit("unused"),
                        "extra_info_2": lit("unused"),
                        "extra_info_3": lit("unused"),
                        "extra_info_4": lit("unused"),
                        "extra_info_5": lit("unused"),
                        "extra_info_6": lit("unused"),
                        "extra_info_7": lit("unused"),
                        "extra_info_8": lit("unused"),
                        "extra_info_9": lit("unused"),
                        "extra_info_10": col("sessionId"),
                        "dw_insert_ts": current_timestamp(),
                        "dw_update_ts": current_timestamp()
                    })
        .where(col("player_id").isNotNull())
        .where(col("appGroupId").isin("6aeb63f4494c4dca89e6f71b79789a84", "6d29e4bf8e1f4aefabde5526f2c221dc"))
        .where(col("isParentAccountDOBVerified")== lit(True))
        .select("source_table", 
                "player_id", 
                "parent_id",
                "platform", 
                "service", 
                "title",
                "received_on", 
                "desc", 
                "event_trigger", 
                "country_code", 
                "build_changelist", 
                "language_setting", 
                "extra_info_1", 
                "extra_info_2", 
                "extra_info_3", 
                "extra_info_4", 
                "extra_info_5", 
                "extra_info_6", 
                "extra_info_7", 
                "extra_info_8", 
                "extra_info_9", 
                "extra_info_10", 
                "dw_insert_ts", 
                "dw_update_ts")
    )
    return logins_df

# COMMAND ----------

def read_app_session_df(environment, spark):
    title_df = read_title_df(environment, spark)
    app_sesh_df = (
        spark
        .readStream
        .table(f"horizon{environment}.raw.application_session_status")
        .where(col("playerPublicId").isNotNull())
        .where(col("build_environment") != "development_editor")
        .where(col("version").isNotNull())
        .where(col("sessionId").isNotNull())
        .join(title_df, expr("appPublicId = title.app_id"), "left")
        .withColumns({
                "source_table": lit("application_session_status"),
                "player_id": col("playerPublicId"),
                "parent_id": col("user_id"),
                "platform": col("title.display_platform"),
                "service": col("title.display_service"),
                "title": col("title.title"),
                "received_on": col("receivedOn").cast("timestamp"),
                "desc": lit("Application Session Status"),
                "event_trigger": col("event_trigger"),
                "country_code": col("countryCode"),
                "build_changelist": split(col("version"), '[.]')[3],
                "language_setting": col("language"),
                "extra_info_1": lit("unused"),
                "extra_info_2": lit("unused"),
                "extra_info_3": lit("unused"),
                "extra_info_4": lit("unused"),
                "extra_info_5": lit("unused"),
                "extra_info_6": lit("unused"),
                "extra_info_7": lit("unused"),
                "extra_info_8": lit("unused"),
                "extra_info_9": lit("unused"),
                "extra_info_10": col("sessionId"),
                "dw_insert_ts": current_timestamp(),
                "dw_update_ts": current_timestamp()
        })
        .select(
            "source_table",
            "player_id",
            "parent_id",
            "title.display_platform",
            "title.display_service",
            "title",
            "received_on",
            "desc",
            "event_trigger",
            "country_code",
            "build_changelist",
            "language_setting",
            "extra_info_1",
            "extra_info_2",
            "extra_info_3",
            "extra_info_4",
            "extra_info_5",
            "extra_info_6",
            "extra_info_7",
            "extra_info_8",
            "extra_info_9",
            "extra_info_10",
            "dw_insert_ts",
            "dw_update_ts"
        )
    )

    return app_sesh_df

# COMMAND ----------

def transform(environment, spark):
    ms_df = read_ms_df(environment, spark)
    spa_df = read_spa_df(environment, spark)
    logins_df = read_logins_df(environment, spark)
    app_sesh_df = read_app_session_df(environment, spark)

    all_activity_df = (
        ms_df
        .unionAll(spa_df)
        .unionAll(logins_df)
        .unionAll(app_sesh_df)
    )
    return all_activity_df

# COMMAND ----------

def stream_player_activity():
    environment, checkpoint_location = arg_parser()
    # environment, checkpoint_location = "_prod", "dbfs:/tmp/horizon/intermediate/streaming/run_dev/fact_player_activity"
    spark = SparkSession.builder.appName("Hydra").getOrCreate()

    spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "false")
    spark.conf.set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")
    spark.conf.set("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")

    create_player_activity(spark,environment)
    all_activity_df = transform(environment, spark)

    (
        all_activity_df
        .writeStream
        .option("checkpointLocation", checkpoint_location)
        .option("mergeSchema", "true")
        .toTable(f"horizon{environment}.intermediate.fact_player_activity")
    )


# COMMAND ----------

if __name__ == "__main__":
    stream_player_activity()
