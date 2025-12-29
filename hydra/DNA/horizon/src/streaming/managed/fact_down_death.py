# Databricks notebook source
import json
from pyspark.sql.functions import (col, when, get_json_object, explode, sha2, concat_ws, from_json, sum, count, lit, max, min, power, round, expr, coalesce, sqrt, current_timestamp)
from pyspark.sql.types import *
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from utils.helpers import arg_parser, setup_logger, setup_spark

# COMMAND ----------

def read_target_df(spark, environment):
    tgt_df = (
        spark
        .readStream
        .table(f"horizon{environment}.raw.player_action_event")
        .alias("tgt")
        .where(col("event_trigger").isin("Death", "Down_Victim"))
        .withColumns({
                        "match_id": when(col("event_defaults.match_type") == "Trials", col("group.id")).otherwise(col("event_defaults.match_id")),
                        "build_environment": col("event_defaults.build_environment"),
                        "build_changelist": col("event_defaults.cl"),
                        "match_type": col("event_defaults.match_type"),
                        "mm_environment": col("event_defaults.matchmaker_environment"),
                        "receivedOn": col("receivedOn").cast("timestamp"),
                        "occurredOn": col("occurredOn").cast("timestamp"),
                        "event_trigger": col("event_trigger"),
                        "target_player_id": col("player.dna_account_id"),
                        "target_group_id": col("group.id"),
                        "target_hero_class_name": col("hero.type"),
                        "source_player_id": col("damage_record.instigator_user_id"),
                        "source_group_id": when(col("damage_record.instigator_user_id") == col("player.dna_account_id"), col("group.id")).alias("source_group_id"),
                        "source_hero_class_name": when(col("damage_record.instigator_user_id") == col("player.dna_account_id"), col("hero.type")).alias("source_hero_class_name"),
                        "self_kill": col("damage_record.instigator_user_id") == col("player.dna_account_id"),
                        "damage_record": explode(from_json(col("damage.damage_records"), "array<struct<amount: double, prev_armor: double, prev_health: double, source_type: string, instigator_user_id: string>>")),
                        "hero_pos_x": get_json_object(col("hero.position"), "$.X").cast("float"),
                        "hero_pos_y": get_json_object(col("hero.position"), "$.Y").cast("float"),
                        "hero_pos_z": get_json_object(col("hero.position"), "$.Z").cast("float")
                    })
        .withWatermark("receivedOn", "1 minute")
        .where(col("target_player_id").isNotNull())
        .where(col("target_group_id").isNotNull())
        .where(col("match_id").isNotNull())
        .where(col("build_environment") != "development_editor")
        .withColumn("join_key", sha2(concat_ws("|", col("event_relationship_id"), col("source_player_id")), 256))
        .select("match_id",
                "build_environment",
                "build_changelist",
                "match_type",
                "mm_environment",
                "receivedOn",
                "occurredOn",
                "event_trigger",
                "target_player_id",
                "target_group_id",
                "target_hero_class_name",
                "source_player_id",
                "source_group_id",
                "source_hero_class_name",
                "self_kill",
                "damage_record",
                "hero_pos_x",
                "hero_pos_y",
                "hero_pos_z",
                "join_key")
    )

    return tgt_df

# COMMAND ----------

def read_source_df(spark, environment):
    src_df = (
        spark
        .readStream
        .table(f"horizon{environment}.raw.player_action_event")
        .alias("src")
        .withColumns({
                        "match_id": when(col("event_defaults.match_type") == "Trials", col("group.id")).otherwise(col("event_defaults.match_id")),
                        "build_environment": col("event_defaults.build_environment"),
                        "build_changelist": col("event_defaults.cl"),
                        "match_type": col("event_defaults.match_type"),
                        "mm_environment": col("event_defaults.matchmaker_environment"),
                        "receivedOn": col("receivedOn").cast("timestamp"),
                        "occurredOn": col("occurredOn").cast("timestamp"),
                        "event_trigger": col("event_trigger"),
                        "hero_pos_x": get_json_object(col("hero.position"), "$.X").cast("float"),
                        "hero_pos_y": get_json_object(col("hero.position"), "$.Y").cast("float"),
                        "hero_pos_z": get_json_object(col("hero.position"), "$.Z").cast("float"),
                        "source_player_id": col("player.dna_account_id"),
                        "source_group_id": col("group.id"),
                        "source_hero_class_name": col("hero.type")
                    })
        .withWatermark("receivedOn", "1 minute")
        .where(col("event_trigger").isin('Kill', 'Down_Killer'))
        .where(col("source_player_id").isNotNull())
        .where(col("source_group_id").isNotNull())
        .where(col("match_id").isNotNull())
        .where(col("build_environment") != "development_editor")
        .withColumn("join_key", sha2(concat_ws("|", col("event_relationship_id"), col("source_player_id")), 256))
        .select("match_id",
                "build_environment",
                "build_changelist",
                "match_type",
                "mm_environment",
                "receivedOn",
                "occurredOn",
                "event_trigger",
                "source_player_id",
                "source_group_id",
                "source_hero_class_name",
                "hero_pos_x",
                "hero_pos_y",
                "hero_pos_z",
                "join_key")
    )

    return src_df

# COMMAND ----------

def extract(spark, environment):
    tgt_df = read_target_df(spark, environment)
    src_df = read_source_df(spark, environment)

    return (
        tgt_df
        .alias("tgt")
        .join(src_df.alias("src"), 
            expr("""tgt.join_key = src.join_key 
                        and src.receivedOn between tgt.receivedOn - interval 1 minute and tgt.receivedOn + interval 1 minute"""), 
            "left")
    )

# COMMAND ----------

def transform(df):
    dd_df = (
        df
        .groupBy(
            "tgt.match_id",
            "tgt.build_environment",
            "tgt.build_changelist",
            "tgt.match_type",
            "tgt.mm_environment",
            "tgt.receivedOn",
            "tgt.occurredOn",
            "tgt.event_trigger",
            "tgt.target_player_id",
            "tgt.target_group_id",
            "tgt.target_hero_class_name",
            coalesce(col("src.source_player_id"), col("tgt.source_player_id")).alias("source_player_id"),
            coalesce(col("src.source_group_id"), col("tgt.source_group_id")).alias("source_group_id"),
            coalesce(col("src.source_hero_class_name"), col("tgt.source_hero_class_name")).alias("source_hero_class_name"),
            col("tgt.damage_record.source_type").alias("damage_source_type"))
        .agg(
            sum("tgt.damage_record.amount").cast("int").alias("damage_amount"),
            count("tgt.damage_record").alias("instance_count"),
            max("tgt.damage_record.prev_health").cast("int").alias("start_health"),
            max("tgt.damage_record.prev_armor").cast("int").alias("start_shield"),
            min("tgt.damage_record.prev_health").cast("int").alias("end_health"),
            min("tgt.damage_record.prev_armor").cast("int").alias("end_shield"),
            max(round(sqrt(power(col("tgt.hero_pos_x") - col("src.hero_pos_x"),2) + power(col("tgt.hero_pos_y") - ("src.hero_pos_y"),2) + power(col("tgt.hero_pos_z") - col("src.hero_pos_z"),2)),0)/100).alias("dist_meters"),
            max(col("tgt.self_kill")).alias("self_kill"))
        .withColumn("dw_insert_ts", current_timestamp())
        .withColumn("dw_update_ts", current_timestamp())
    )

    return dd_df

# COMMAND ----------

def start_stream():
    environment, checkpoint_location = arg_parser()
    # environment, checkpoint_location = "_dev", "dbfs:/tmp/horizon/managed/streaming/run_dev/fact_down_death"
    spark = SparkSession.builder.appName("Hydra").getOrCreate()

    spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "false")
    spark.conf.set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")
    spark.conf.set("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")

    df = extract(spark, environment)
    transformed_df = transform(df)

    (
        DeltaTable.createIfNotExists(spark)
        .tableName(f"horizon{environment}.managed.fact_down_death")
        .addColumns(transformed_df.schema)
        .property('delta.enableIcebergCompatV2', 'true')
        .property('delta.universalFormat.enabledFormats', 'iceberg')
        .execute()
    )

    (
        transformed_df
        .writeStream
        .trigger(processingTime="1 minute")
        .option("mergeSchema", "true")
        .option("checkpointLocation", checkpoint_location)
        .toTable(f"horizon{environment}.managed.fact_down_death")
    )

# COMMAND ----------

if __name__ == "__main__":
    start_stream()
