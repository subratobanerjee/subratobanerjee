# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

import json
from pyspark.sql.functions import (lit, col, when, get_json_object, current_timestamp, split,split_part)
from delta.tables import DeltaTable
from pyspark.sql.window import Window
import argparse
from pyspark.sql import SparkSession
import logging
from pyspark.sql.functions import *
from pyspark.sql.types import *

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

def read_title_df(environment, spark):
    title_df= (
                spark.
                read.
                table(f"reference{environment}.title.dim_title")
                .alias("title")
                )
    return title_df

# COMMAND ----------

def read_campaign_status_df(environment, spark):
    title_df = read_title_df(environment, spark)
    campaign_status_df = (
        spark
        .readStream
        .option("skipChangeCommits", "true")
        .table(f"inverness{environment}.raw.campaignstatus")
        .join(title_df, expr("appPublicId = title.app_id"), "left")
        .where(col("playertype")=='HUMAN')
        .withColumns({
                        "source_table": lit("campaignstatus"),
                        "player_id": col("playerPublicId"),
                        "app_public_id": col("appPublicId"),
                        "platform": col("title.display_platform"),
                        "service": col("title.display_service"),
                        "received_on": col("receivedOn").cast("timestamp"),
                        "event_trigger": col("campaignstatusevent"),
                        "country_code": col("countryCode"),
                        "build_version":col("buildVersion"),
                        "age": col("age"),
                        "civilization": col("civilizationSelected"),
                        "leader": col("leader"),
                        "turn_number": col("turnNumber"),
                        "campaign_instance_id": col("campaignInstanceId"),
                        "application_session_instance_id": col("applicationSessionInstanceId"),
                        "player_slot": col("playerSlot"),
                        "player_type": col("playerType"),
                        "campaign_setup_instance_id": col("campaignSetupInstanceId"),
                        "lobby_instance_id":col("lobbyInstanceId"),
                        "save_instance_id": col("saveInstanceId"),
                        "settlement_cap": col("settlementCap"),
                        "campaign_load_time": col("campaignLoadTime"),
                        "storylets_available": col("storyletsAvailable"), 
                        "campaign_exit_type": col("campaignExitType"),
                        "food_per_turn": col("foodPerTurn"),
                        "gold_per_turn": col("goldPerTurn"),
                        "happiness_per_turn": col("happinessPerTurn"),
                        "influence_per_turn": col("influencePerTurn"),
                        "production_per_turn": col("productionPerTurn"),
                        "science_per_turn": col("sciencePerTurn"),
                        "total_gold": col("totalGold"),
                        "total_influence": col("totalInfluence"),
                        "culture_per_turn": col("cultureperturn"),
                        "dw_insert_ts": current_timestamp(),
                        "dw_update_ts": current_timestamp()
                    })
        .where(col("player_id").isNotNull())
        
        .select("source_table", 
                "player_id",
                "app_public_id",
                "platform",
                "service",
                "received_on", 
                "event_trigger", 
                "country_code", 
                "build_version",
                "age",
                "civilization",
                "leader",
                "turn_number",
                "campaign_instance_id",
                "application_session_instance_id",
                "player_slot",
                "player_type",
                "campaign_setup_instance_id",
                "lobby_instance_id",
                "save_instance_id", 
                "settlement_cap", 
                "campaign_load_time", 
                "storylets_available", 
                "campaign_exit_type",
                "food_per_turn", 
                "gold_per_turn", 
                "happiness_per_turn", 
                "influence_per_turn", 
                "production_per_turn", 
                "science_per_turn", 
                "total_gold", 
                "total_influence", 
                "culture_per_turn",
                "dw_insert_ts", 
                "dw_update_ts")
    )
    
    return campaign_status_df

# COMMAND ----------

def read_settlement_status_df(environment, spark):
    title_df = read_title_df(environment, spark)
    settlement_status_df = (
        spark
        .readStream
        .table(f"inverness{environment}.raw.settlementstatus")
        .join(title_df, expr("appPublicId = title.app_id"), "left")
        .where(col("playertype")=='HUMAN')
        .withColumns({
                        "source_table": lit("settlementstatus"),
                        "player_id": col("playerPublicId"),
                        "app_public_id": col("appPublicId"),
                        "platform": col("title.display_platform"),
                        "service": col("title.display_service"),
                        "received_on": col("receivedOn").cast("timestamp"),
                        "event_trigger": col("settlementevent"),
                        "country_code": col("countryCode"),
                        "build_version":col("buildVersion"),
                        "age": col("age"),
                        "civilization": lit("NA"),
                        "leader": col("leader"),
                        "turn_number": col("turnNumber"),
                        "campaign_instance_id": col("campaignInstanceId"),
                        "application_session_instance_id": col("applicationSessionInstanceId"),
                        "player_slot": col("playerSlot"),
                        "player_type": col("playerType"),
                        "campaign_setup_instance_id": col("campaignSetupInstanceId"),
                        "lobby_instance_id":col("lobbyInstanceId"),
                        "save_instance_id": col("saveInstanceId"),
                        "settlement_id": col("settlementId"),
                        "settlement_name": col("settlementName"),
                        "production_item": col("productionItem"),
                        "distant_lands": col("distantLands"),
                        "current_population": col("currentPopulation"),
                        "city_transfer_type":col("cityTransferType"),
                        "dw_insert_ts": current_timestamp(),
                        "dw_update_ts": current_timestamp()
                    })
        .where(col("player_id").isNotNull())
        
        .select("source_table", 
                "player_id",
                "app_public_id",
                "platform",
                "service",
                "received_on", 
                "event_trigger", 
                "country_code", 
                "build_version",
                "age",
                "civilization",
                "leader",
                "turn_number",
                "campaign_instance_id",
                "application_session_instance_id",
                "player_slot",
                "player_type",
                "campaign_setup_instance_id",
                "lobby_instance_id",
                "save_instance_id",
                "settlement_id", 
                "settlement_name",
                "production_item", 
                "distant_lands", 
                "current_population",
                "city_transfer_type",
                "dw_insert_ts", 
                "dw_update_ts")
    )
    return settlement_status_df

# COMMAND ----------

def read_military_status_df(environment, spark):
    title_df = read_title_df(environment, spark)
    military_status_df = (
        spark
        .readStream
        .table(f"inverness{environment}.raw.militarystatus")
        .join(title_df, expr("appPublicId = title.app_id"), "left")
        .where(col("playertype")=='HUMAN')
        .withColumns({
                        "source_table": lit("militarystatus"),
                        "player_id": col("playerPublicId"),
                        "app_public_id": col("appPublicId"),
                        "platform": col("title.display_platform"),
                        "service": col("title.display_service"),
                        "received_on": col("receivedOn").cast("timestamp"),
                        "event_trigger": col("militaryevent"),
                        "country_code": col("countryCode"),
                        "build_version":col("buildVersion"),
                        "age": col("age"),
                        "civilization": lit("NA"),
                        "leader": col("leader"),
                        "turn_number": col("turnNumber"),
                        "campaign_instance_id": col("campaignInstanceId"),
                        "application_session_instance_id": col("applicationSessionInstanceId"),
                        "player_slot": col("playerSlot"),
                        "player_type": col("playerType"),
                        "campaign_setup_instance_id": col("campaignSetupInstanceId"),
                        "lobby_instance_id":col("lobbyInstanceId"),
                        "save_instance_id": col("saveInstanceId"),
                        "attacks_made": col("attacksMade"),
                        "units_lost": col("unitsLost"),
                        "enemies_defeated": col("enemiesDefeated"),
                        "war_weariness": col("warWeariness"),
                        "player_eliminated": col("playerEliminated"),
                        "independent_dispersed": col("independentDispersed"),
                        "dw_insert_ts": current_timestamp(),
                        "dw_update_ts": current_timestamp()
                    })
        .where(col("player_id").isNotNull())
        
        .select("source_table", 
                "player_id",
                "app_public_id",
                "platform",
                "service",
                "received_on", 
                "event_trigger", 
                "country_code", 
                "build_version",
                "age",
                "civilization",
                "leader",
                "turn_number",
                "campaign_instance_id",
                "application_session_instance_id",
                "player_slot",
                "player_type",
                "campaign_setup_instance_id",
                "lobby_instance_id",
                "save_instance_id", 
                "attacks_made", 
                "units_lost", 
                "enemies_defeated", 
                "war_weariness", 
                "player_eliminated", 
                "independent_dispersed",
                "dw_insert_ts", 
                "dw_update_ts"
                )
    )
    return military_status_df

# COMMAND ----------

def read_victory_status_df(environment, spark):
    title_df = read_title_df(environment, spark)
    victory_status_df = (
        spark
        .readStream
        .table(f"inverness{environment}.raw.victorystatus")
        .join(title_df, expr("appPublicId = title.app_id"), "left")
        .where(col("playertype")=='HUMAN')
        .withColumns({
                        "source_table": lit("victorystatus"),
                        "player_id": col("playerPublicId"),
                        "app_public_id": col("appPublicId"),
                        "platform": col("title.display_platform"),
                        "service": col("title.display_service"),
                        "received_on": col("receivedOn").cast("timestamp"),
                        "event_trigger": col("victoryevent"),
                        "country_code": col("countryCode"),
                        "build_version":col("buildVersion"),
                        "age": col("age"),
                        "civilization": lit("NA"),
                        "leader": col("leader"),
                        "turn_number": col("turnNumber"),
                        "campaign_instance_id": col("campaignInstanceId"),
                        "application_session_instance_id": col("applicationSessionInstanceId"),
                        "player_slot": col("playerSlot"),
                        "player_type": col("playerType"),
                        "campaign_setup_instance_id": col("campaignSetupInstanceId"),
                        "lobby_instance_id":col("lobbyInstanceId"),
                        "save_instance_id": col("saveInstanceId"),
                        "current_progress": col("currentProgress"),
                        "victory_type": col("victoryType"),
                        "milestone_type": col("milestoneType"),
                        "dw_insert_ts": current_timestamp(),
                        "dw_update_ts": current_timestamp()
                    })
        .where(col("player_id").isNotNull())
        
        .select("source_table", 
                "player_id",
                "app_public_id",
                "platform",
                "service",
                "received_on", 
                "event_trigger", 
                "country_code", 
                "build_version",
                "age",
                "civilization",
                "leader",
                "turn_number",
                "campaign_instance_id",
                "application_session_instance_id",
                "player_slot",
                "player_type",
                "campaign_setup_instance_id",
                "lobby_instance_id",
                "save_instance_id", 
                "current_progress", 
                "victory_type", 
                "milestone_type",
                "dw_insert_ts", 
                "dw_update_ts"
                )
    )
    return victory_status_df

# COMMAND ----------

def read_diplomacy_action_status_df(environment, spark):
    title_df = read_title_df(environment, spark)
    diplomacy_action_status_df = (
        spark
        .readStream
        .table(f"inverness{environment}.raw.diplomacyactionstatus")
        .join(title_df, expr("appPublicId = title.app_id"), "left")
        .where(col("playertype")=='HUMAN')
        .withColumns({
                        "source_table": lit("diplomacyactionstatus"),
                        "player_id": col("playerPublicId"),
                        "app_public_id": col("appPublicId"),
                        "platform": col("title.display_platform"),
                        "service": col("title.display_service"),
                        "received_on": col("receivedOn").cast("timestamp"),
                        "event_trigger": col("diplomacyactionevent"),
                        "country_code": col("countryCode"),
                        "build_version":col("buildVersion"),
                        "age": col("age"),
                        "civilization": lit("NA"),
                        "leader": col("leader"),
                        "turn_number": col("turnNumber"),
                        "campaign_instance_id": col("campaignInstanceId"),
                        "application_session_instance_id": col("applicationSessionInstanceId"),
                        "player_slot": col("playerSlot"),
                        "player_type": col("playerType"),
                        "campaign_setup_instance_id": col("campaignSetupInstanceId"),
                        "lobby_instance_id":col("lobbyInstanceId"),
                        "save_instance_id": col("saveInstanceId"),
                        "diplomacy_action_name": col("actionname"),
                        "diplomacy_action_initiator": col("diplomacyActionInitiator"),
                        "diplomacy_action_instance_id": col("diplomacyActionInstanceId"),
                        "diplomacy_action_response":col("diplomacyActionResponse"),
                        "diplomacy_action_target": col("diplomacyActionTarget"),
                        "dw_insert_ts": current_timestamp(),
                        "dw_update_ts": current_timestamp()

                    })
        .where(col("player_id").isNotNull())
        
        .select("source_table", 
                "player_id",
                "app_public_id",
                "platform",
                "service",
                "received_on", 
                "event_trigger", 
                "country_code", 
                "build_version",
                "age",
                "civilization",
                "leader",
                "turn_number",
                "campaign_instance_id",
                "application_session_instance_id",
                "player_slot",
                "player_type",
                "campaign_setup_instance_id",
                "lobby_instance_id",
                "save_instance_id", 
                "diplomacy_action_name", 
                "diplomacy_action_initiator", 
                "diplomacy_action_instance_id",
                "diplomacy_action_response",
                "diplomacy_action_target",
                "dw_insert_ts", 
                "dw_update_ts"
                )
    )
    return diplomacy_action_status_df

# COMMAND ----------

def read_resource_status_df(environment, spark):
    title_df = read_title_df(environment, spark)
    resource_status_df = (
        spark
        .readStream
        .table(f"inverness{environment}.raw.resourcestatus")
        .join(title_df, expr("appPublicId = title.app_id"), "left")
        .where(col("playertype")=='HUMAN')
        .withColumns({
                        "source_table": lit("resourcestatus"),
                        "player_id": col("playerPublicId"),
                        "app_public_id": col("appPublicId"),
                        "platform": col("title.display_platform"),
                        "service": col("title.display_service"),
                        "received_on": col("receivedOn").cast("timestamp"),
                        "event_trigger": col("resourceevent"),
                        "country_code": col("countryCode"),
                        "build_version":col("buildVersion"),
                        "age": col("age"),
                        "civilization": lit("NA"),
                        "leader": col("leader"),
                        "turn_number": col("turnNumber"),
                        "campaign_instance_id": col("campaignInstanceId"),
                        "application_session_instance_id": col("applicationSessionInstanceId"),
                        "player_slot": col("playerSlot"),
                        "player_type": col("playerType"),
                        "campaign_setup_instance_id": col("campaignSetupInstanceId"),
                        "lobby_instance_id":col("lobbyInstanceId"),
                        "save_instance_id": col("saveInstanceId"),
                        "resources_changed": col("resourcesChanged"),
                        "resources_assigned": col("resourcesAssigned"),
                        "target_settlement_id": col("targetSettlementId"),
                        "trade_route_range":col("tradeRouteRange"),
                        "trade_route_type": col("tradeRouteType"),
                        "resource_settlement_id": col("settlementId"),
                        #"trade_route_break_reason":col("tradeRouteBreakReason"),
                        "dw_insert_ts": current_timestamp(),
                        "dw_update_ts": current_timestamp()

                    })
        .where(col("player_id").isNotNull())
        
        .select("source_table", 
                "player_id",
                "app_public_id",
                "platform",
                "service",
                "received_on", 
                "event_trigger", 
                "country_code", 
                "build_version",
                "age",
                "civilization",
                "leader",
                "turn_number",
                "campaign_instance_id",
                "application_session_instance_id",
                "player_slot",
                "player_type",
                "campaign_setup_instance_id",
                "lobby_instance_id",
                "save_instance_id", 
                "resources_changed", 
                "resources_assigned", 
                "target_settlement_id",
                "trade_route_range",
                "trade_route_type",
                "resource_settlement_id",
               # "trade_route_break_reason",
                "dw_insert_ts", 
                "dw_update_ts"
                )
    )
    return resource_status_df

# COMMAND ----------

def read_commander_status_df(environment, spark):
    title_df = read_title_df(environment, spark)
    commander_status_df = (
        spark
        .readStream
        .table(f"inverness{environment}.raw.commanderstatus")
        .join(title_df, expr("appPublicId = title.app_id"), "left")
        .where(col("playertype")=='HUMAN')
        .withColumns({
                        "source_table": lit("commanderstatus"),
                        "player_id": col("playerPublicId"),
                        "app_public_id": col("appPublicId"),
                        "platform": col("title.display_platform"),
                        "service": col("title.display_service"),
                        "received_on": col("receivedOn").cast("timestamp"),
                        "event_trigger": col("commanderevent"),
                        "country_code": col("countryCode"),
                        "build_version":col("buildVersion"),
                        "age": col("age"),
                        "civilization": lit("NA"),
                        "leader": col("leader"),
                        "turn_number": col("turnNumber"),
                        "campaign_instance_id": col("campaignInstanceId"),
                        "application_session_instance_id": col("applicationSessionInstanceId"),
                        "player_slot": col("playerSlot"),
                        "player_type": col("playerType"),
                        "campaign_setup_instance_id": col("campaignSetupInstanceId"),
                        "lobby_instance_id":col("lobbyInstanceId"),
                        "save_instance_id": col("saveInstanceId"),
                        "commander_id": col("commanderId"),
                        "commander_level": col("commanderLevel"),
                        "promotion_node": col("promotionNode"),
                        "xp_gained":col("xpGained"),
                        #"xp_gained_source": col("xpGainedSource"),
                        "num_units_packed": col("numUnitsPacked"),
                        "commander_action": col("commanderAction"),
                        "dw_insert_ts": current_timestamp(),
                        "dw_update_ts": current_timestamp()

                    })
        .where(col("player_id").isNotNull())
        
        .select("source_table", 
                "player_id",
                "app_public_id",
                "platform",
                "service",
                "received_on", 
                "event_trigger", 
                "country_code", 
                "build_version",
                "age",
                "civilization",
                "leader",
                "turn_number",
                "campaign_instance_id",
                "application_session_instance_id",
                "player_slot",
                "player_type",
                "campaign_setup_instance_id",
                "lobby_instance_id",
                "save_instance_id", 
                "commander_id", 
                "commander_level", 
                "promotion_node",
                "xp_gained",
                #"xp_gained_source",
                "num_units_packed",
                "commander_action",
                "dw_insert_ts", 
                "dw_update_ts"
                )
    )
    return commander_status_df

# COMMAND ----------

def read_tech_tree_status_df(environment, spark):
    title_df = read_title_df(environment, spark)
    tech_tree_status_df = (
        spark
        .readStream
        .table(f"inverness{environment}.raw.techtreestatus")
        .join(title_df, expr("appPublicId = title.app_id"), "left")
        .where(col("playertype")=='HUMAN')
        .withColumns({
                        "source_table": lit("techtreestatus"),
                        "player_id": col("playerPublicId"),
                        "app_public_id": col("appPublicId"),
                        "platform": col("title.display_platform"),
                        "service": col("title.display_service"),
                        "received_on": col("receivedOn").cast("timestamp"),
                        "event_trigger": col("techtreeevent"),
                        "country_code": col("countryCode"),
                        "build_version":col("buildVersion"),
                        "age": col("age"),
                        "civilization": lit("NA"),
                        "leader": col("leader"),
                        "turn_number": col("turnNumber"),
                        "campaign_instance_id": col("campaignInstanceId"),
                        "application_session_instance_id": col("applicationSessionInstanceId"),
                        "player_slot": col("playerSlot"),
                        "player_type": col("playerType"),
                        "campaign_setup_instance_id": col("campaignSetupInstanceId"),
                        "lobby_instance_id":col("lobbyInstanceId"),
                        "save_instance_id": col("saveInstanceId"),
                        "node_depth": col("nodeDepth"),
                        "node_id": col("nodeId"),
                        "tree_id": col("treeId"),
                        "dw_insert_ts": current_timestamp(),
                        "dw_update_ts": current_timestamp()

                    })
        .where(col("player_id").isNotNull())
        
        .select("source_table", 
                "player_id",
                "app_public_id",
                "platform",
                "service",
                "received_on", 
                "event_trigger", 
                "country_code", 
                "build_version",
                "age",
                "civilization",
                "leader",
                "turn_number",
                "campaign_instance_id",
                "application_session_instance_id",
                "player_slot",
                "player_type",
                "campaign_setup_instance_id",
                "lobby_instance_id",
                "save_instance_id", 
                "node_depth", 
                "node_id", 
                "tree_id",
                "dw_insert_ts", 
                "dw_update_ts"
                )
    )
    return tech_tree_status_df

# COMMAND ----------

def read_civic_tree_status_df(environment, spark):
    title_df = read_title_df(environment, spark)
    civic_tree_status_df = (
        spark
        .readStream
        .table(f"inverness{environment}.raw.civictreestatus")
        .join(title_df, expr("appPublicId = title.app_id"), "left")
        .where(col("playertype")=='HUMAN')
        .withColumns({
                        "source_table": lit("civictreestatus"),
                        "player_id": col("playerPublicId"),
                        "app_public_id": col("appPublicId"),
                        "platform": col("title.display_platform"),
                        "service": col("title.display_service"),
                        "received_on": col("receivedOn").cast("timestamp"),
                        "event_trigger": col("civictreeevent"),
                        "country_code": col("countryCode"),
                        "build_version":col("buildVersion"),
                        "age": col("age"),
                        "civilization": lit("NA"),
                        "leader": col("leader"),
                        "turn_number": col("turnNumber"),
                        "campaign_instance_id": col("campaignInstanceId"),
                        "application_session_instance_id": col("applicationSessionInstanceId"),
                        "player_slot": col("playerSlot"),
                        "player_type": col("playerType"),
                        "campaign_setup_instance_id": col("campaignSetupInstanceId"),
                        "lobby_instance_id":col("lobbyInstanceId"),
                        "save_instance_id": col("saveInstanceId"),
                        "node_depth": col("nodeDepth"),
                        "node_id": col("nodeId"),
                        "tree_id": col("treeId"),
                        "dw_insert_ts": current_timestamp(),
                        "dw_update_ts": current_timestamp()
                        

                    })
        .where(col("player_id").isNotNull())
        
        .select("source_table", 
                "player_id",
                "app_public_id",
                "platform",
                "service",
                "received_on", 
                "event_trigger", 
                "country_code", 
                "build_version",
                "age",
                "civilization",
                "leader",
                "turn_number",
                "campaign_instance_id",
                "application_session_instance_id",
                "player_slot",
                "player_type",
                "campaign_setup_instance_id",
                "lobby_instance_id",
                "save_instance_id", 
                "node_depth", 
                "node_id", 
                "tree_id",
                "dw_insert_ts", 
                "dw_update_ts"
                
                )
    )
    return civic_tree_status_df

# COMMAND ----------

def read_policy_status_df(environment, spark):
    title_df = read_title_df(environment, spark)
    policy_status_df = (
        spark
        .readStream
        .table(f"inverness{environment}.raw.policystatus")
        .join(title_df, expr("appPublicId = title.app_id"), "left")
        .where(col("playertype")=='HUMAN')
        .withColumns({
                        "source_table": lit("policystatus"),
                        "player_id": col("playerPublicId"),
                        "app_public_id": col("appPublicId"),
                        "platform": col("title.display_platform"),
                        "service": col("title.display_service"),
                        "received_on": col("receivedOn").cast("timestamp"),
                        "event_trigger": col("policyevent"),
                        "country_code": col("countryCode"),
                        "build_version":col("buildVersion"),
                        "age": col("age"),
                        "civilization": lit("NA"),
                        "leader": col("leader"),
                        "turn_number": col("turnNumber"),
                        "campaign_instance_id": col("campaignInstanceId"),
                        "application_session_instance_id": col("applicationSessionInstanceId"),
                        "player_slot": col("playerSlot"),
                        "player_type": col("playerType"),
                        "campaign_setup_instance_id": col("campaignSetupInstanceId"),
                        "lobby_instance_id":col("lobbyInstanceId"),
                        "save_instance_id": col("saveInstanceId"),
                        "celebration_selected": col("celebrationSelected"),
                        "policy_cards": col("policyCards"),
                        "government_selected": col("governmentSelected"),
                        "dw_insert_ts": current_timestamp(),
                        "dw_update_ts": current_timestamp()
                    })
        .where(col("player_id").isNotNull())
        
        .select("source_table", 
                "player_id",
                "app_public_id",
                "platform",
                "service",
                "received_on", 
                "event_trigger", 
                "country_code", 
                "build_version",
                "age",
                "civilization",
                "leader",
                "turn_number",
                "campaign_instance_id",
                "application_session_instance_id",
                "player_slot",
                "player_type",
                "campaign_setup_instance_id",
                "lobby_instance_id",
                "save_instance_id", 
                "celebration_selected", 
                "policy_cards", 
                "government_selected",
                "dw_insert_ts", 
                "dw_update_ts"
                
                )
    )
    return policy_status_df

# COMMAND ----------

def read_world_status_df(environment, spark):
    title_df = read_title_df(environment, spark)
    world_status_df = (
        spark
        .readStream
        .table(f"inverness{environment}.raw.worldstatus")
        .join(title_df, expr("appPublicId = title.app_id"), "left")
        .where(col("playertype")=='HUMAN')
        .withColumns({
                        "source_table": lit("worldstatus"),
                        "player_id": col("playerPublicId"),
                        "app_public_id": col("appPublicId"),
                        "platform": col("title.display_platform"),
                        "service": col("title.display_service"),
                        "received_on": col("receivedOn").cast("timestamp"),
                        "event_trigger": col("worldevent"),
                        "country_code": col("countryCode"),
                        "build_version":col("buildVersion"),
                        "age": col("age"),
                        "civilization": lit("NA"),
                        "leader": col("leader"),
                        "turn_number": col("turnNumber"),
                        "campaign_instance_id": col("campaignInstanceId"),
                        "application_session_instance_id": col("applicationSessionInstanceId"),
                        "player_slot": col("playerSlot"),
                        "player_type": col("playerType"),
                        "campaign_setup_instance_id": col("campaignSetupInstanceId"),
                        "lobby_instance_id":col("lobbyInstanceId"),
                        "save_instance_id": col("saveInstanceId"),
                        "world_event_name": col("worldEventName"),
                        "damaged_tiles": col("damagedTiles"),
                        "affected_tiles": col("affectedTiles"),
                        #"boosted_tiles":col("boostedTiles"),
                        "dw_insert_ts": current_timestamp(),
                        "dw_update_ts": current_timestamp()
                    })
        .where(col("player_id").isNotNull())
        
        .select("source_table", 
                "player_id",
                "app_public_id",
                "platform",
                "service",
                "received_on", 
                "event_trigger", 
                "country_code", 
                "build_version",
                "age",
                "civilization",
                "leader",
                "turn_number",
                "campaign_instance_id",
                "application_session_instance_id",
                "player_slot",
                "player_type",
                "campaign_setup_instance_id",
                "lobby_instance_id",
                "save_instance_id", 
                "world_event_name", 
                "damaged_tiles", 
                "affected_tiles",
                #"boosted_tiles",
                "dw_insert_ts", 
                "dw_update_ts"
                
                )
    )
    return world_status_df

# COMMAND ----------

def read_leader_attribute_status_df(environment, spark):
    title_df = read_title_df(environment, spark)
    leader_attribute_status_df = (
        spark
        .readStream
        .table(f"inverness{environment}.raw.leaderattributestatus")
        .join(title_df, expr("appPublicId = title.app_id"), "left")
        .where(col("playertype")=='HUMAN')
        .withColumns({
                        "source_table": lit("leaderattributestatus"),
                        "player_id": col("playerPublicId"),
                        "app_public_id": col("appPublicId"),
                        "platform": col("title.display_platform"),
                        "service": col("title.display_service"),
                        "received_on": col("receivedOn").cast("timestamp"),
                        "event_trigger": col("leaderattributeevent"),
                        "country_code": col("countryCode"),
                        "build_version":col("buildVersion"),
                        "age": col("age"),
                        "civilization": lit("NA"),
                        "leader": col("leader"),
                        "turn_number": col("turnNumber"),
                        "campaign_instance_id": col("campaignInstanceId"),
                        "application_session_instance_id": col("applicationSessionInstanceId"),
                        "player_slot": col("playerSlot"),
                        "player_type": col("playerType"),
                        "campaign_setup_instance_id": col("campaignSetupInstanceId"),
                        "lobby_instance_id":col("lobbyInstanceId"),
                        "save_instance_id": col("saveInstanceId"),
                        "point_attribute": col("pointAttribute"),
                        "attribute_node_id": col("attributeNodeId"),
                        "dw_insert_ts": current_timestamp(),
                        "dw_update_ts": current_timestamp()
                    })
        .where(col("player_id").isNotNull())
        
        .select("source_table", 
                "player_id",
                "app_public_id",
                "platform",
                "service",
                "received_on", 
                "event_trigger", 
                "country_code", 
                "build_version",
                "age",
                "civilization",
                "leader",
                "turn_number",
                "campaign_instance_id",
                "application_session_instance_id",
                "player_slot",
                "player_type",
                "campaign_setup_instance_id",
                "lobby_instance_id",
                "save_instance_id", 
                "point_attribute", 
                "attribute_node_id", 
                "dw_insert_ts", 
                "dw_update_ts"
                )
    )
    return leader_attribute_status_df

# COMMAND ----------

def read_narrative_status_df(environment, spark):
    title_df = read_title_df(environment, spark)
    narrative_status_df = (
        spark
        .readStream
        .table(f"inverness{environment}.raw.narrativestatus")
        .join(title_df, expr("appPublicId = title.app_id"), "left")
        .where(col("playertype")=='HUMAN')
        .withColumns({
                        "source_table": lit("narrativestatus"),
                        "player_id": col("playerPublicId"),
                        "app_public_id": col("appPublicId"),
                        "platform": col("title.display_platform"),
                        "service": col("title.display_service"),
                        "received_on": col("receivedOn").cast("timestamp"),
                        "event_trigger": col("narrativeevent"),
                        "country_code": col("countryCode"),
                        "build_version":col("buildVersion"),
                        "age": col("age"),
                        "civilization": lit("NA"),
                        "leader": col("leader"),
                        "turn_number": col("turnNumber"),
                        "campaign_instance_id": col("campaignInstanceId"),
                        "application_session_instance_id": col("applicationSessionInstanceId"),
                        "player_slot": col("playerSlot"),
                        "player_type": col("playerType"),
                        "campaign_setup_instance_id": col("campaignSetupInstanceId"),
                        "lobby_instance_id":col("lobbyInstanceId"),
                        "save_instance_id": col("saveInstanceId"),
                        "player_tags": col("playerTags"),
                        "new_tags_strength": col("newtagstrength"),
                        "option_selected": col("optionSelected"),
                        "narrative_id": col("narrativeId"),
                        "dw_insert_ts": current_timestamp(),
                        "dw_update_ts": current_timestamp()
                    })
        .where(col("player_id").isNotNull())
        
        .select("source_table", 
                "player_id",
                "app_public_id",
                "platform",
                "service",
                "received_on", 
                "event_trigger", 
                "country_code", 
                "build_version",
                "age",
                "civilization",
                "leader",
                "turn_number",
                "campaign_instance_id",
                "application_session_instance_id",
                "player_slot",
                "player_type",
                "campaign_setup_instance_id",
                "lobby_instance_id",
                "save_instance_id", 
                "player_tags", 
                "new_tags_strength", 
                "option_selected",
                "narrative_id",
                "dw_insert_ts", 
                "dw_update_ts"
                )
    )
    return narrative_status_df

# COMMAND ----------

def read_independent_status_df(environment, spark):
    title_df = read_title_df(environment, spark)
    independent_status_df = (
        spark
        .readStream
        .table(f"inverness{environment}.raw.independentstatus")
        .join(title_df, expr("appPublicId = title.app_id"), "left")
        .where(col("playertype")=='HUMAN')
        .withColumns({
                        "source_table": lit("independentstatus"),
                        "player_id": col("playerPublicId"),
                        "app_public_id": col("appPublicId"),
                        "platform": col("title.display_platform"),
                        "service": col("title.display_service"),
                        "received_on": col("receivedOn").cast("timestamp"),
                        "event_trigger": col("independentevent"),
                        "country_code": col("countryCode"),
                        "build_version":col("buildVersion"),
                        "age": col("age"),
                        "civilization": lit("NA"),
                        "leader": col("leader"),
                        "turn_number": col("turnNumber"),
                        "campaign_instance_id": col("campaignInstanceId"),
                        "application_session_instance_id": col("applicationSessionInstanceId"),
                        "player_slot": col("playerSlot"),
                        "player_type": col("playerType"),
                        "campaign_setup_instance_id": col("campaignSetupInstanceId"),
                        "lobby_instance_id":col("lobbyInstanceId"),
                        "save_instance_id": col("saveInstanceId"),
                        "independent_id": col("independentid"),
                        "independent_attribute": col("independentattribute"),
                        "independent_name": col("independentname"),
                        "suzerain_bonus": col("suzerainbonus"),
                        "dw_insert_ts": current_timestamp(),
                        "dw_update_ts": current_timestamp()
                    })
        .where(col("player_id").isNotNull())
        
        .select("source_table", 
                "player_id",
                "app_public_id",
                "platform",
                "service",
                "received_on", 
                "event_trigger", 
                "country_code", 
                "build_version",
                "age",
                "civilization",
                "leader",
                "turn_number",
                "campaign_instance_id",
                "application_session_instance_id",
                "player_slot",
                "player_type",
                "campaign_setup_instance_id",
                "lobby_instance_id",
                "save_instance_id", 
                "independent_id",
                "independent_attribute", 
                "independent_name", 
                "suzerain_bonus",
                "dw_insert_ts", 
                "dw_update_ts"
                )
    )
    return independent_status_df

# COMMAND ----------

def read_lobby_status_df(environment, spark):
    title_df = read_title_df(environment, spark)
    lobby_status_df = (
        spark
        .readStream
        .table(f"inverness{environment}.raw.lobbystatus")
        .join(title_df, expr("appPublicId = title.app_id"), "left")

        .withColumns({
                        "source_table": lit("lobbystatus"),
                        "player_id": col("playerPublicId"),
                        "app_public_id": col("appPublicId"),
                        "platform": col("title.display_platform"),
                        "service": col("title.display_service"),
                        "received_on": col("receivedOn").cast("timestamp"),
                        "event_trigger": col("lobbyevent"),
                        "country_code": col("countryCode"),
                        "build_version":col("buildVersion"),
                        "age": col("startingAge"),
                        "civilization": lit("NA"),
                        "leader": lit("NA"),
                        "turn_number": lit(0),
                        "campaign_instance_id": col("campaignInstanceId"),
                        "application_session_instance_id": col("applicationSessionInstanceId"),
                        "player_slot": col("playerSlot"),
                        "player_type": lit("NA"),
                        "campaign_setup_instance_id": col("campaignSetupInstanceId"),
                        "lobby_instance_id":col("lobbyInstanceId"),
                        "save_instance_id": lit("NA"),
                        "crossplay_friend_relations": col("crossplayFriendRelations"),
                        "game_speed": col("gameSpeed"),
                        "map_type": col("mapType"),
                        "left_method":col("leftMethod"),
                        "standard_rules": col("standardRules"),
                        "dw_insert_ts": current_timestamp(),
                        "dw_update_ts": current_timestamp()
                    })
        .where(col("player_id").isNotNull())
        
        .select("source_table", 
                "player_id",
                "app_public_id",
                "platform",
                "service",
                "received_on", 
                "event_trigger", 
                "country_code", 
                "build_version",
                "age",
                "civilization",
                "leader",
                "turn_number",
                "campaign_instance_id",
                "application_session_instance_id",
                "player_slot",
                "player_type",
                "campaign_setup_instance_id",
                "lobby_instance_id",
                "save_instance_id", 
                "crossplay_friend_relations", 
                "game_speed", 
                "map_type",
                "left_method",
                "standard_rules",
                "dw_insert_ts", 
                "dw_update_ts"
                )
    )
    return lobby_status_df

# COMMAND ----------

def transform(environment, spark):
    campaign_status_df = read_campaign_status_df(environment, spark)
    settlement_status_df = read_settlement_status_df(environment, spark)
    military_status_df = read_military_status_df(environment, spark)
    victory_status_df = read_victory_status_df(environment, spark)
    diplomacy_action_status_df = read_diplomacy_action_status_df(environment, spark)
    resource_status_df = read_resource_status_df(environment, spark)
    commander_status_df = read_commander_status_df(environment, spark)
    tech_tree_status_df = read_tech_tree_status_df(environment, spark)
    civic_tree_status_df = read_civic_tree_status_df(environment, spark)
    policy_status_df = read_policy_status_df(environment, spark)
    world_status_df = read_world_status_df(environment, spark)
    leader_attribute_status_df = read_leader_attribute_status_df(environment, spark)
    narrative_status_df = read_narrative_status_df(environment, spark)
    independent_status_df = read_independent_status_df(environment, spark)
    lobby_status_df = read_lobby_status_df(environment, spark)

    all_activity_df = (
        campaign_status_df
        .unionByName(settlement_status_df, True)
        .unionByName(military_status_df, True)
        .unionByName(victory_status_df, True)
        .unionByName(diplomacy_action_status_df, True)
        .unionByName(resource_status_df, True)
        .unionByName(commander_status_df, True)
        .unionByName(tech_tree_status_df, True)
        .unionByName(civic_tree_status_df, True)
        .unionByName(policy_status_df, True)
        .unionByName(world_status_df, True)
        .unionByName(leader_attribute_status_df, True)
        .unionByName(narrative_status_df, True)
        .unionByName(independent_status_df, True)
        .unionByName(lobby_status_df, True)
    )
    return all_activity_df


# COMMAND ----------

def stream_player_campaign_activity():
    environment = dbutils.widgets.get("environment")
    checkpoint_location = dbutils.widgets.get("checkpoint")
    #environment, checkpoint_location = "_dev", "dbfs:/tmp/inverness/intermediate/streaming/run_dev/fact_player_campaign_activity"
    spark = create_spark_session()

    spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "false")
    spark.conf.set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")
    spark.conf.set("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")


    all_activity_df = transform(environment, spark)
    (
        DeltaTable.createIfNotExists(spark)
        .tableName(f"inverness{environment}.intermediate.fact_player_campaign_activity")
        .addColumns(all_activity_df.schema)
        .property('delta.enableIcebergCompatV2', 'true')
        .property('delta.universalFormat.enabledFormats', 'iceberg')
        .execute()
    )
   

    (
        all_activity_df
        .writeStream
        .trigger(availableNow=True)
        .option("checkpointLocation", checkpoint_location)
        .option("mergeSchema", "true")
        .toTable(f"inverness{environment}.intermediate.fact_player_campaign_activity")
    )


# COMMAND ----------

if __name__ == "__main__":
    stream_player_campaign_activity()