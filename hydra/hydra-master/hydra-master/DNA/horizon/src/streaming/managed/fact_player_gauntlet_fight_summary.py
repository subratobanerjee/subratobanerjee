# Databricks notebook source
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
from pyspark.sql.window import Window
from pyspark.sql.functions import (
col, from_json, max, count, when, sha2, concat_ws
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

def read_fight_status_df(environment, spark):
     fight_status_df = (
          spark
          .readStream
          .table(f"horizon{environment}.raw.match_status_event")
          .withColumns({
          "extra_details_tiebreakcoord_x": get_json_object(col("extra_details.TiebreakCoord"), "$.x").cast("string"),
          "extra_details_tiebreakcoord_y": get_json_object(col("extra_details.TiebreakCoord"), "$.y").cast("string"),
          "extra_details_tiebreakcoord_z": get_json_object(col("extra_details.TiebreakCoord"), "$.z").cast("string"),
          "group_id": get_json_object(col("group.members"), "$.id").cast("string"),
          "group_name": get_json_object(col("group.members"), "$.name").cast("string"),
          "player_id": col("player.dna_account_id"),
          "player_name": col("player.name"),
          "platform": col("event_defaults.platform"),
          "ms_match_id": col("event_defaults.match_id"),
          "gauntlet_round_num": col("extra_details.gauntletroundnumber"),
          "gauntlet_instance_id": col("extra_details.gauntletfightinstanceid"),
          "ms_receivedOn": col("receivedOn").cast("timestamp"),
          "occurred_on": col("occurredOn").cast("timestamp"),
          "medal_earned" : col("extra_details.medal"),
          "build_environment": col("event_defaults.build_environment"),
          "mm_environment":col("event_defaults.matchmaker_environment"),
          "server_id" : col("event_defaults.server_id"),
          "cl": col("event_defaults.cl"),
          "match_type":col("extra_details.type"),
          "final_showdown_result" : when((col("event_trigger") == "FightComplete"), when((col("extra_details.gauntletroundnumber") == 4) & 
                                        (col("extra_details.gauntletteamwins") == 4), 'W') 
                                        .when((col("extra_details.gauntletroundnumber") == 4) & 
                                        (col("extra_details.gauntletteamwins") == 3) & 
                                        (col("extra_details.gauntletteamwins") == 3), 'L')
                                        .otherwise(None)),
          "prev_ts" :   when(col("event_trigger") == "FightComplete",col("date")), #prev_ts lag calculation to be done downstream view
          "wait_time" : when(col("event_trigger") == "RoundStart", 
                                   unix_timestamp(col("date")) - unix_timestamp(col("prev_ts"))),
          "merge_key": sha2(concat_ws("|", col("player_id"), col("gauntlet_instance_id"), col("ms_match_id")), 256)
     })
     .alias("ms")
          .where(col("player_id").isNotNull())
          .where(col("gauntlet_instance_id").isNotNull())
          .where(col("player_name").isNotNull())
          .where(col("player_name").ilike("%2kgsv4drone%") == False) 
          .where(~col("build_environment").isin(["development_editor"]))
          .where(col("match_type") == "Gauntlet")
          .where(col("cl") >= 173947)
     )
     return fight_status_df

# COMMAND ----------

def read_lvlup_fin_df(environment, spark):
    lvlup_fin_df = (
        spark
        .readStream
        .table(f"horizon{environment}.managed.fact_level_up")
        .withColumn("merge_key", sha2(concat_ws("|", col("player_id"), col("gameplay_instance_id"), col("match_instance_id")), 256))   
        .where(col("player_id").isNotNull())
        .where(col("match_type") == "Gauntlet")           
        .select(
                "*"
        )

    )
    return lvlup_fin_df


# COMMAND ----------

def transform(environment, spark):
    fight_status_df = read_fight_status_df(environment, spark)
    lvlup_fin_df = read_lvlup_fin_df(environment, spark)

    gauntlet_df = (
        spark
        .readStream
        .table(f"horizon{environment}.raw.player_action_event")
        .withColumns({
                    "player_name": col("player.name"),
                    "group_id"  : col("group.id"),
                    "player_id" : col("player.dna_account_id"),
                    "build_environment" : col("event_defaults.build_environment"),
                    "build_changelist":  col("event_defaults.cl"),
                    "platform" : col("event_defaults.platform"),
                    "match_id" : col("event_defaults.match_id"),
                    "sequencenumber" : col("sequencenumber"),
                    "received_on" : col("receivedOn").cast("timestamp"),
                    "occurred_on" : col("occurredOn").cast("timestamp"),
                    "event_trigger" : col("event_trigger"),
                    "match_type" : col("extra_details.type"),
                    "gauntlet_instance_id" : col("gmi.gameplay_instance_id"),
                    "hero_type" : col("hero.type"),
                    "hero_position_x" :get_json_object(col("hero.position"), "$.X").cast("string"),
                    "hero_position_y" :get_json_object(col("hero.position"), "$.Y").cast("string"),
                    "hero_position_z" :get_json_object(col("hero.position"), "$.Z").cast("string"),
                    
                    "merge_key" : sha2(concat_ws("|", col("player_id"), col("gauntlet_instance_id"), col("match_id"),col("group_id")), 256)
    })
        .unionByName(fight_status_df, True)
        .unionByName(lvlup_fin_df, True)
        .withWatermark("received_on", "1 minute")
        .groupBy(
            session_window("received_on", "2 minutes"),
            "player_id",
            "group_id",
            "match_id",
            "hero_type",
            "build_environment",
            col("gauntlet_instance_id").alias("fight_id"),
            "platform",
            "build_changelist",
            "mm_environment",
            "player_name",
            "match_type",
            "gauntlet_round_num"
            )
        .agg(
            max(col("received_on")).alias("latest_ts"),
            min(when(col("event_trigger") == "RoundStart", col("occurredOn").cast("timestamp")).otherwise(None)).alias("round_start_ts"),
            min(when(col("event_trigger") == "FightStart", col("occurredOn").cast("timestamp")).otherwise(None)).alias("fight_start_ts"),
            max(when(col("event_trigger") == "FightTiebreakerStart", col("occurredOn").cast("timestamp")).otherwise(None)).alias("tiebreak_start_ts"),
            max(when(col("event_trigger") == "FightCollapseStart", col("occurredOn").cast("timestamp")).otherwise(None)).alias("zone_collapse_start_ts"),
            max(when(col("event_trigger") == "FightComplete", col("occurredOn").cast("timestamp")).otherwise(None)).alias("fight_end_ts"),
            max(when(col("event_trigger") == "FightComplete", col("occurredOn").cast("timestamp").cast("string")).otherwise(None)).alias("fight_duration_sec"),
            max(col("wait_time")).alias("wait_time"),
            max(col("extra_details.gauntletteamwins")).alias("gauntlet_team_wins"),
            max(col("extra_details.gauntletteamlosses")).alias("gauntlet_team_losses"),
            max(col("extra_details_tiebreakcoord_x")).alias("gauntlet_tiebreak_coord_x"), 
            max(col("extra_details_tiebreakcoord_y")).alias("gauntlet_tiebreak_coord_y"),
            max(col("extra_details_tiebreakcoord_z")).alias("gauntlet_tiebreak_coord_z"),
            max(coalesce(when(col("event_trigger") == "FightComplete", col("extra_details.fightoutcomecondition")).otherwise(None), lit('Unknown'))).alias("gauntlet_fight_outcome"),
            max(coalesce(when(col("event_trigger") == "FightComplete", col("extra_details.didwinfight")).otherwise(None), lit('False'))).alias("gauntlet_did_win_fight"),
            max(coalesce(when(col("event_trigger").isin("FightStart", "FightComplete"), col("extra_details.gauntletfightarena")).otherwise(None), lit('Unknown'))).alias("gauntlet_arena_map"),
            max(col("medal_earned")).alias("medal_earned"),
            max(col("extra_details.gauntletteamwins")).alias("max_gauntletteamwins"),
            approx_count_distinct(col("group_name"), rsd=0.01).alias("group_size"),
            max(col("final_showdown_result")).alias("final_showdown_result"),            
            count(when(col("event_trigger") == "Kill", col("sequencenumber")).otherwise(None)).alias("kills"),
            count(when(col("event_trigger") == "Death", col("sequencenumber")).otherwise(None)).alias("deaths"),
            count(when(col("event_trigger") == "Assist", col("sequencenumber")).otherwise(None)).alias("assists"),
            count(when(col("event_trigger") == "ReviveFromDowned_Reviver", col("sequencenumber")).otherwise(None)).alias("revives"),
            count(when(col("event_trigger") == "HeroLevelUp", col("sequencenumber")).otherwise(None)).alias("levelups"),
            count(when(col("event_trigger") == "UpgradeSelection", col("sequencenumber")).otherwise(None)).alias("upgrades"),
            count(when(col("event_trigger") == "ChestOpened", col("sequencenumber")).otherwise(None)).alias("chests_opened"),
            max(when(col("event_trigger") == "Land", col("hero_position_x")).otherwise(None)).alias("spawn_x"),
            max(when(col("event_trigger") == "Land", col("hero_position_y")).otherwise(None)).alias("spawn_y"),
            max(when(col("event_trigger") == "Land", col("hero_position_z")).otherwise(None)).alias("spawn_z"),
            collect_list(col("server_id")).alias("server_ids"),
            collect_list(col("hero_type")).alias("hero_class_name"),
            
            #max(col("hero_combo")).alias("group_hero_combo"),
            #max(col("player_name_combo")).alias("group_player_name_combo"),
            collect_list(col("upgrade_selected")).alias("upgrade_list"),
            avg(coalesce(round(col("time_to_choose_upgrade_sec"), 2), lit(None))).alias("avg_time_upg_choice"),
            min(coalesce(round(col("time_to_choose_upgrade_sec"), 2), lit(None))).alias("min_time_upg_choice"),
            max(coalesce(round(col("time_to_choose_upgrade_sec"), 2), lit(None))).alias("max_time_upg_choice"),
            current_timestamp().alias("dw_insert_ts"),
            current_timestamp().alias("dw_update_ts"),
        )
        
        .withColumns ({
            "kda_cal": when(col("deaths") == 0, 1).otherwise(col("deaths")),
            "date" : coalesce(col("fight_start_ts"),col("latest_ts")).cast("date") 
        })
        .where(col("player_id").isNotNull())
        .where(col("player_name").isNotNull())
        .where(col("player_name").ilike("%2kgsv4drone%") == False)
        .where(~col("build_environment").isin(["development_editor"]))
        .select 
        (
            "player_id",
            "platform",
            "match_id",
            "fight_id",
            "group_id",
            "date",
            "build_environment",
            "build_changelist",
            "mm_environment",
            "player_name",
            "gauntlet_round_num",
            col("gauntlet_arena_map").alias("fight_arena"),
            "server_ids",
            "hero_class_name",
            "round_start_ts",
            "fight_start_ts",
            "tiebreak_start_ts",
            "zone_collapse_start_ts",
            "fight_end_ts",
            "fight_duration_sec",
            "wait_time",
            "gauntlet_fight_outcome",
            "gauntlet_did_win_fight",
            "gauntlet_team_wins",
            "gauntlet_team_losses",
            "final_showdown_result",
            "medal_earned",
            "gauntlet_tiebreak_coord_x",
            "gauntlet_tiebreak_coord_y",
            "gauntlet_tiebreak_coord_z",
            "group_size",
        # "group_hero_combo",
        # "group_player_name_combo",
            "kills",
            "deaths",
            "assists",
            col("kda_cal").alias("kda"),
            "revives",
            "levelups",
            "upgrades",
            "chests_opened",
            "upgrade_list",
            "avg_time_upg_choice",
            "min_time_upg_choice",
            "max_time_upg_choice",
            "spawn_x",
            "spawn_y",
            "spawn_z" ,
            "dw_insert_ts" ,
            "dw_update_ts"
        )
    
    )
    return gauntlet_df


# COMMAND ----------

def stream_gauntlet_fight_summary():
    environment, checkpoint_location = arg_parser()
    #environment, checkpoint_location = "_dev", "dbfs:/tmp/horizon/managed/streaming/run_dev/fact_player_gauntlet_fight_summary" #to test in local
    spark = SparkSession.builder.appName("Hydra").getOrCreate()

    spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "false")
    spark.conf.set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")
    spark.conf.set("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")

    gauntlet_df = transform(environment, spark)

    (
        gauntlet_df
        .writeStream
        .option("checkpointLocation", checkpoint_location)
        .option("mergeSchema", "true")
        .toTable(f"horizon{environment}.managed.fact_player_gauntlet_fight_summary")
    )

# COMMAND ----------

# COMMAND ----------

if __name__ == "__main__":
    stream_gauntlet_fight_summary()
