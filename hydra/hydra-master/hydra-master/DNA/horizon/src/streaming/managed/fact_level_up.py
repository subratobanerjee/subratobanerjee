# Databricks notebook source
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
from pyspark.sql.window import Window
from pyspark.sql.functions import (
col, from_json, max, count, when, sha2, concat_ws,row_number,unix_timestamp,expr, col, max
)
from pyspark.sql.types import StructType, StructField, FloatType
import argparse
from pyspark.sql import SparkSession
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

def read_ignore_cheats_df(environment, spark):
    ignore_cheats_df = (
        spark
        .readStream
        .table(f"horizon{environment}.raw.match_hero_progression")
        .withColumns({
                    "receivedOn":  col("receivedOn").cast("timestamp"),
                    "player_id": col("player.dna_account_id"),
                    "level_up_debug": get_json_object(col("EXTRA_DETAILS.LevelUpSources"), "$.Debug").cast("float"),
                    "match_instance_id": when(
                                            col("extra_details.type") == "Trials",
                                            col("group.id"))
        })
        .withWatermark("receivedOn", "1 minute")
        .groupBy(
            session_window("receivedOn", "3 minutes"),
            col("match_instance_id"),
            col("player_id")
        )
        .agg(
            sum(col("level_up_debug")).alias("debug_level")
            )
        .where(col("debug_level").isNull())
        .withColumn("ic_join_key", sha2(concat_ws("|", col("player_id"), col("match_instance_id")), 256))
        .alias("ignore_cheats")
        .select("player_id","match_instance_id","debug_level","ic_join_key")
    )
    return ignore_cheats_df


# COMMAND ----------

def read_upgrades_df(environment, spark):
    upgrades_df= (
        spark
        .read
        .table(f"horizon{environment}.reference.hero_upgrades")
        .alias("upgrades")
        .withColumn("join_key", sha2(concat_ws("|", col("hero_name"), col("evolution_name")), 256))
    )
    return upgrades_df


# COMMAND ----------

def read_upgrade_row_df(environment, spark):
    upgrades_df= read_upgrades_df(environment, spark)
    upgrade_row_df = (
        spark
        .readStream
        .table(f"horizon{environment}.raw.match_hero_progression")
        .withColumns({
                        "player_id": col("player.dna_account_id"),
                        "player_name": col("player.name"),
                        "upgrade_selected": get_json_object(col("EXTRA_DETAILS.UpgradePayload"), "$.Selected Upgrade").cast("string"),
                        "upgrade_choice_1": get_json_object(col("EXTRA_DETAILS.UpgradePayload"), "$.UpgradeChoices[0]").cast("string"),
                        "upgrade_choice_2": get_json_object(col("EXTRA_DETAILS.UpgradePayload"), "$.UpgradeChoices[1]").cast("string"),
                        "upgrade_choice_3": get_json_object(col("EXTRA_DETAILS.UpgradePayload"), "$.UpgradeChoices[2]").cast("string"),
                        "time_to_choose_upgrade": col("occurredon").cast("timestamp"),
                        "event_trigger": col("event_trigger"),
                        "build_environment": col("event_defaults.build_environment"),
                        "hero_type" : col("hero.type"),
                        "match_id": col("event_defaults.match_id"),
                        "match_instance_id": when(col("extra_details.type") == "Trials",col("group.id")).otherwise(col("match_id")),
                        "upgrade_row" : col("extra_details.HeroLevelCurrent")+1,#window function in original code
                        "join_upgrades_key_1": sha2(concat_ws("|", col("hero_type"), col("upgrade_choice_1")), 256),
                        "join_upgrades_key_2": sha2(concat_ws("|", col("hero_type"), col("upgrade_choice_2")), 256),
                        "join_upgrades_key_3": sha2(concat_ws("|", col("hero_type"), col("upgrade_choice_3")), 256),
                    
                    })
        .join(upgrades_df.alias("upgrades_0"), expr("upgrade_selected = upgrades_0.evolution_name"), "left")
        .join(upgrades_df.alias("upgrades_1"), expr("join_upgrades_key_1 = upgrades_1.join_key"), "left")
        .join(upgrades_df.alias("upgrades_2"), expr("join_upgrades_key_2 = upgrades_2.join_key"), "left")
        .join(upgrades_df.alias("upgrades_3"), expr("join_upgrades_key_3 = upgrades_3.join_key"), "left")
    
        .where(col("player_id").isNotNull())
        .where(col("player_name").ilike("%2kgsv4drone%") == False)
        .where(col("event_trigger") == "UpgradeSelection")

        .withColumn("selected_upgrade_rarity",col("upgrades_0.rarity"))
        .withColumn("evolution_des",col("upgrades_0.evolution_description"))
        .withColumn("max_stacks",col("upgrades_0.stacks"))
        .withColumn("upgrade_choice_1_rarity",col("upgrades_1.rarity"))
        .withColumn("upgrade_choice_2_rarity",col("upgrades_2.rarity"))
        .withColumn("upgrade_choice_3_rarity",col("upgrades_3.rarity"))
        .withColumn("up_join_key", sha2(concat_ws("|", col("player_id"), col("match_instance_id")), 256))
        .select(
                "player_id",
                "player_name",
                "upgrade_selected",
                "selected_upgrade_rarity",
                "evolution_des",
                "max_stacks",
                "upgrade_row",
                "upgrade_choice_1",
                "upgrade_choice_2",
                "upgrade_choice_3",
                "upgrade_choice_1_rarity",
                "upgrade_choice_2_rarity",
                "upgrade_choice_3_rarity",
                "time_to_choose_upgrade",
                "up_join_key"
                )
    )
    return upgrade_row_df



# COMMAND ----------

def read_find_match_info_df(environment, spark):
    find_match_info_df = (
        spark
        .readStream
        .table(f"horizon{environment}.raw.match_status_event")
        .withColumns({
                    "received_on":  col("receivedOn").cast("timestamp"),
                    "occurred_on":  col("occurredOn").cast("timestamp"),
                    "player_id": col("player.dna_account_id"),
                    "match_id": col("event_defaults.match_id"),
                    "match_instance_id": when(
                                            col("extra_details.type") == "Trials",
                                            col("group.id"))
                                            .otherwise(col("match_id"))
        })
        .withWatermark("received_on", "1 minute")
        .groupBy(
            session_window("received_on", "3 minutes"),
            col("match_instance_id"),
            col("player_id")
        )
        .agg(
            max(when(col("event_trigger") == "PlayerMatchStart",col("occurred_on"))).alias("match_start_ts"),
            max(when(col("event_trigger").isin("PlayerMatchEnd","PlayerManualQuit"),col("occurred_on"))).alias("match_end_ts"),
            )
        .withColumn("match_join_key", sha2(concat_ws("|", col("player_id"), col("match_instance_id")), 256))
        .where(col("player_id").isNotNull())
        .where(col("match_instance_id").isNotNull())
        .alias("match_info")
        .select("player_id","match_instance_id","match_start_ts","match_end_ts","match_join_key")
    )
    return find_match_info_df


# COMMAND ----------

def transform(environment, spark):
    ignore_cheats_df = read_ignore_cheats_df(environment, spark)
    upgrade_row_df = read_upgrade_row_df(environment, spark)
    find_match_info_df = read_find_match_info_df(environment, spark)

    final_df = (
        spark
        .readStream
        .table(f"horizon{environment}.raw.match_hero_progression")
        .withColumns({
            "player_id": col("player.dna_account_id"),
            "occurred_on": col("occurredOn").cast("timestamp"),
            "level_up_new": col("extra_details.HeroLevelCurrent")+1,
            "match_type": col("event_defaults.match_type"),
            "build_changelist": col("event_defaults.cl"),
            "hero_type": col("hero.type"),
            "gameplay_instance_id": col("gmi.gameplay_instance_id"),
            "build_environment": col("event_defaults.build_environment"),
            "level_up_ring_close": get_json_object(col("EXTRA_DETAILS.LevelUpSources"), "$.RingClose").cast("float"),
            "level_up_pick_up_common": get_json_object(col("EXTRA_DETAILS.LevelUpSources"), "$.PickupCommon").cast("float"),
            "level_up_pick_up_rare": get_json_object(col("EXTRA_DETAILS.LevelUpSources"), "$.PickupRare").cast("float"),
            "level_up_kill": get_json_object(col("EXTRA_DETAILS.LevelUpSources"), "$.Kill").cast("float"),
            "level_up_gauntlet": get_json_object(col("EXTRA_DETAILS.LevelUpSources"), "$.GauntletRoundBasic").cast("float"),
            "match_instance_id": when(col("extra_details.type") == "Trials",col("group.id")).otherwise(col("event_defaults.match_id")),
            "final_join_key": sha2(concat_ws("|", col("player_id"), col("match_instance_id")), 256),
            "dw_insert_ts": current_timestamp(),
            "dw_update_ts": current_timestamp()
        })
        .unionByName(ignore_cheats_df, True)
        .unionByName(upgrade_row_df, True)
        .unionByName(find_match_info_df, True)
        .withColumns({
                "time_to_level_sec": unix_timestamp(col("match_start_ts")) - unix_timestamp(col("occurred_on")),
                "time_to_level_min": (col("match_start_ts").cast(LongType()) - col("occurred_on").cast(LongType())) / 60,
                "time_to_choose_upgrade_sec": unix_timestamp(col("match_start_ts")) - unix_timestamp(col("time_to_choose_upgrade")),
                "time_to_choose_upgrade_min": (col("match_start_ts").cast(LongType()) - col("time_to_choose_upgrade").cast(LongType())) / 60
        })
        .where(col("player_id").isNotNull())
        .where(col("match_instance_id").isNotNull())
        .where(col("level_up_new").isNotNull())
        #.where(col("player_name").ilike("%2kgsv4drone%") == False)
        .where(col("occurred_on") >= '2024-07-01')
        .where(col("build_environment") != 'development_editor')
        .select(
            "match_instance_id",
            "player_id",
            "match_type",
            "build_changelist",
            "build_environment",
            "gameplay_instance_id",
            "hero_type",
            "date",
            "level_up_new",
            "time_to_level_sec",
            "time_to_level_min",
            "level_up_ring_close",
            "level_up_pick_up_common",
            "level_up_pick_up_rare",
            "level_up_kill",
            "level_up_gauntlet",
            "upgrade_selected",
            "selected_upgrade_rarity",
            "evolution_des",
            "max_stacks",
            "time_to_choose_upgrade_sec",
            "time_to_choose_upgrade_min",
            "upgrade_choice_1",
            "upgrade_choice_1_rarity",
            "upgrade_choice_2",
            "upgrade_choice_2_rarity",
            "upgrade_choice_3",
            "upgrade_choice_3_rarity",
            "dw_insert_ts",
            "dw_update_ts"
        )
    )
    return final_df

# COMMAND ----------

def stream_level_up():
    environment, checkpoint_location = arg_parser()
    #environment, checkpoint_location = "_dev", "dbfs:/tmp/horizon/managed/streaming/run_dev/fact_level_up" #to test in local
    spark = SparkSession.builder.appName("Hydra").getOrCreate()

    spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "false")
    spark.conf.set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")
    spark.conf.set("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")

    level_up_df = transform(environment, spark)

    (
        level_up_df
        .writeStream
        .option("checkpointLocation", checkpoint_location)
        .option("mergeSchema", "true")
        .toTable(f"horizon{environment}.managed.fact_level_up")
    )

# COMMAND ----------

if __name__ == "__main__":
    stream_level_up()
