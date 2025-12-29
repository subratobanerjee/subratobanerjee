# Databricks notebook source
import json
from pyspark.sql.functions import (
    array_union,
    avg,
    coalesce,
    col,
    collect_list,
    collect_set,
    count,
    current_timestamp,
    explode,
    from_json,
    get_json_object,
    greatest,
    ifnull,
    lit,
    max,
    min,
    regexp_replace,
    round,
    sum,
    session_window,
    size,
    split,
    when
)
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

def read_ms_df(environment, spark):
    ms_df = (
        spark
        .readStream
        .table(f"horizon{environment}.raw.match_status_event")
        .withColumn("grp_arr", from_json("group.members", "array<struct<name: string, id: string>>"))
        .withColumns({
            "received_on": col("receivedOn").cast("timestamp"),
            "match_id": when(col("event_defaults.match_type") == "Trials", col("group.id")).otherwise(col("event_defaults.match_id")),
            "player_id": col("player.dna_account_id"),
            "full_account_id": col("player.user_id"),
            "group_id": col("group.id"),
            "group_size": size("grp_arr"),
            "match_type": col("event_defaults.match_type"),
            "build_environment": col("event_defaults.build_environment"),
            "build_changelist": col("event_defaults.cl").cast('int'),
            "mm_environment": col("event_defaults.matchmaker_environment"),
            "hero_class_name": col("hero.type"),
            "start_ts": when(col("event_trigger") == "PlayerMatchStart", col("received_on")),
            "end_ts": when((col("event_trigger") == "PlayerMatchEnd") | (col("event_trigger") == "PlayerManualQuit"), col("received_on")),
            "match_outcome": when(col("event_trigger") == "PlayerMatchEnd", col("extra_details.match_outcome_condition"))
                .when(col("event_trigger") == "PlayerManualQuit", lit("Quit"))
                .when(col("event_trigger") == "HardMode_TimeLimitReached", lit("TimeLimitReached")),
            "start_map": when(col("event_trigger") == "PlayerMatchStart", col("extra_details.map")),
            "end_map": when(col("event_trigger") == "PlayerMatchEnd", col("extra_details.map")),
            "start_level": when(col("event_trigger") == "PlayerMatchStart", col("hero.hero_level").cast("int")),
            "end_level": when(col("event_trigger") == "PlayerMatchEnd", col("hero.hero_level").cast("int")),
            "start_upgrades": when(col("event_trigger") == "PlayerMatchStart", split(regexp_replace("upgrades.active_upgrades", '\\{|\\}', ''), ',')),
            "extracted_upgrades": when((col("event_trigger") == "PlayerMatchEnd") & (col("extra_details.match_outcome_condition") == "extracted"), split(regexp_replace("upgrades.active_upgrades", '\\{|\\}', ''), ',')),
            "medal": when(col("event_trigger") == "PlayerMatchEnd", col("extra_details.medal")),
            "active_perks": when(col("event_trigger") == "PlayerMatchStart", from_json("extra_details.activeperks", "array<string>")),
            "final_showdown_result" : when((col("event_trigger") == "PlayerMatchEnd"), when((col("extra_details.gauntletroundnumber") == 4) & 
                                        (col("extra_details.medal") == lit('Diamond')), 'W') 
                                        .when((col("extra_details.gauntletroundnumber") == 4) & 
                                        (col("extra_details.medal") == lit('Platinum')), 'L')),
            "gauntlet_team_wins": col("extra_details.gauntletteamwins"),
            "gauntlet_team_losses": col("extra_details.gauntletteamlosses"),
            "gauntlet_fight_outcome": col("extra_details.fightoutcomecondition")
        })
        .where(col("player_id").isNotNull())
        .where(col("group_id").isNotNull())
        .where(col("match_id").isNotNull())
        .where(col("build_environment") != "development_editor")
        .where(col("event_trigger").isin(["PlayerMatchStart", "PlayerMatchEnd", "PlayerManualQuit", "FightComplete"]))
        .select("received_on", "match_id", "player_id", "full_account_id", "group_id", "group_size", "match_type", "build_environment", "build_changelist", "mm_environment", "hero_class_name", "start_ts", "end_ts", "match_outcome", "start_map", "end_map", "start_level", "end_level", "start_upgrades", "extracted_upgrades", "medal", "active_perks", "final_showdown_result", "gauntlet_team_wins", "gauntlet_team_losses", "gauntlet_fight_outcome")
    )

    return ms_df

# COMMAND ----------

def read_spa_df(environment, spark):
    spa_df = (
        spark
        .readStream
        .table(f"horizon{environment}.raw.player_action_event")
        .withColumns({
            "received_on": col("receivedOn").cast("timestamp"),
            "match_id": when(col("event_defaults.match_type") == "Trials", col("group.id")).otherwise(col("event_defaults.match_id")),
            "player_id": col("player.dna_account_id"),
            "full_account_id": col("player.user_id"),
            "group_id": col("group.id"),
            "match_type": col("event_defaults.match_type"),
            "build_environment": col("event_defaults.build_environment"),
            "build_changelist": col("event_defaults.cl").cast('int'),
            "mm_environment": col("event_defaults.matchmaker_environment"),
            "hero_class_name": col("hero.type")
        })
        .where(col("player_id").isNotNull())
        .where(col("group_id").isNotNull())
        .where(col("match_id").isNotNull())
        .where(col("event_defaults.build_environment") != "development_editor")
    ).alias("spa")

    return spa_df

# COMMAND ----------

def read_trials_event_df(environment, spark):
    trials_event_df = (
        spark
        .readStream
        .table(f"horizon{environment}.raw.trials_status_event")
        .where(col("event_trigger").isin(["extraction_complete", "extraction_start", "extraction_timeout", "major_event_loss", "major_event_win", "minor_event_loss", "minor_event_win", "enter_major_event", "leave_major_event"]))
        .withColumn("grp_expld", explode(from_json("group.members", "array<struct<name: string, id: string>>")))
        .withColumns({
            "received_on": col("receivedOn").cast("timestamp"),
            "match_id": col("group.id"),
            "player_id": col("player.dna_account_id"),
            "full_account_id": when(~col("event_trigger").isin(["enter_major_event", "leave_major_event"]), col("grp_expld.id")).otherwise(col("player.user_id")),
            "group_id": col("group.id"),
            "match_type": col("event_defaults.match_type"),
            "build_environment": col("event_defaults.build_environment"),
            "build_changelist": col("event_defaults.cl").cast('int'),
            "mm_environment": col("event_defaults.matchmaker_environment"),
            "hero_class_name": col("hero.type")
        })
        .where(col("full_account_id").isNotNull())
        .where(col("group_id").isNotNull())
        .where(col("match_id").isNotNull())
        .where(col("event_defaults.build_environment") != "development_editor")
        .select("received_on", "player_id", "full_account_id", "group_id", "match_id", "build_environment", "build_changelist", "mm_environment", "match_type", "event_trigger", "extra_details")
    )

    return trials_event_df

# COMMAND ----------

def read_mhp_df(environment, spark):
    mhp_df = (
        spark
        .readStream
        .table(f"horizon{environment}.raw.match_hero_progression")
        .withColumns({
            "received_on": col("receivedOn").cast("timestamp"),
            "match_id": when(col("event_defaults.match_type") == "Trials", col("group.id")).otherwise(col("event_defaults.match_id")),
            "player_id": col("player.dna_account_id"),
            "full_account_id": col("player.user_id"),
            "group_id": col("group.id"),
            "match_type": col("event_defaults.match_type"),
            "build_environment": col("event_defaults.build_environment"),
            "build_changelist": col("event_defaults.cl").cast('int'),
            "mm_environment": col("event_defaults.matchmaker_environment"),
            "hero_class_name": col("hero.type")
        })
        .where(col("player_id").isNotNull())
        .where(col("group_id").isNotNull())
        .where(col("match_id").isNotNull())
        .where(col("event_defaults.build_environment") != "development_editor")
        .select("received_on", "player_id", "full_account_id", "group_id", "match_id", "build_environment", "build_changelist", "mm_environment", "match_type", "event_trigger", "extra_details")
    )

    return mhp_df

# COMMAND ----------

def read_txn_df(environment, spark):
    txn_df = (
        spark
        .readStream
        .table(f"horizon{environment}.raw.transactions")
        .withColumn("grp_arr", explode(from_json("group.members", "array<struct<name: string, id: string>>")))
        .withColumns({
            "received_on": col("receivedOn").cast("timestamp"),
            "match_id": col("group.id"),
            "player_id": col("player.dna_account_id"),
            "full_account_id": when(get_json_object(col("extra_details.transactiontype"), "$[0].earn.ItemType") == lit("ORA_T"), col("grp_arr.id")).otherwise(col("player.user_id")),
            "group_id": col("group.id"),
            "match_type": col("event_defaults.match_type"),
            "build_environment": col("event_defaults.build_environment"),
            "build_changelist": col("event_defaults.cl").cast('int'),
            "mm_environment": col("event_defaults.matchmaker_environment"),
            "hero_class_name": col("hero.type"),
            "ora_t_earn": when(get_json_object(col("extra_details.transactiontype"), "$[0].earn.ItemType") == "ORA_T", get_json_object(col("extra_details.transactiontype"), "$[0].earn.ItemAmount").cast("int")).otherwise(lit(0)),
            "ora_group_wipe": when((get_json_object(col("extra_details.transactiontype"), "$[0].earn.ItemType") == "ORA_T") 
                                & (col("extra_details.transactiondescription") == "Group Wipe"), get_json_object(col("extra_details.transactiontype"), "$[0].earn.ItemAmount").cast("int")).otherwise(lit(0)),
            "ora_bounty_recovered": when((get_json_object(col("extra_details.transactiontype"), "$[0].earn.ItemType") == "ORA_T") 
                                        & (col("extra_details.transactiondescription") == "Bounty Recovered"), get_json_object(col("extra_details.transactiontype"), "$[0].earn.ItemAmount").cast("int")).otherwise(lit(0)),
            "ora_class_1_event_win": when((get_json_object(col("extra_details.transactiontype"), "$[0].earn.ItemType") == "ORA_T") 
                                        & (col("extra_details.transactiondescription") == "Minor Event"), get_json_object(col("extra_details.transactiontype"), "$[0].earn.ItemAmount").cast("int")).otherwise(lit(0)),
            "ora_class_2_event_win": when((get_json_object(col("extra_details.transactiontype"), "$[0].earn.ItemType") == "ORA_T") 
                                        & (col("extra_details.transactiondescription") == "Chest Reward"), get_json_object(col("extra_details.transactiontype"), "$[0].earn.ItemAmount").cast("int")).otherwise(lit(0)),
            "ora_class_3_event_participation": when((get_json_object(col("extra_details.transactiontype"), "$[0].earn.ItemType") == "ORA_T") 
                                                    & (col("extra_details.transactiondescription") == "Major Event Participation")
                                                    & (get_json_object(col("extra_details.transactiontype"), "$[0].earn.ItemAmount").cast("int") == 5), get_json_object(col("extra_details.transactiontype"), "$[0].earn.ItemAmount").cast("int")).otherwise(lit(0)),
            "ora_class_3_event_win": when((get_json_object(col("extra_details.transactiontype"), "$[0].earn.ItemType") == "ORA_T") 
                                        & (col("extra_details.transactiondescription") == "Major Event Participation") 
                                        & (get_json_object(col("extra_details.transactiontype"), "$[0].earn.ItemAmount").cast("int") == 15), get_json_object(col("extra_details.transactiontype"), "$[0].earn.ItemAmount").cast("int")).otherwise(lit(0)),
            "ora_perks": when((get_json_object(col("extra_details.transactiontype"), "$[0].earn.ItemType") == "ORA_T") 
                            & (col("extra_details.transactiondescription").ilike("%perk%")), get_json_object(col("extra_details.transactiontype"), "$[0].earn.ItemAmount").cast("int")).otherwise(lit(0)),
            "ora_p_earn": when((get_json_object(col("extra_details.transactiontype"), "$[0].earn.ItemType") == "ORA_P"), get_json_object(col("extra_details.transactiontype"), "$[0].earn.ItemAmount").cast("int")).otherwise(lit(0)),
            "bounties_claimed": when((get_json_object(col("extra_details.transactiontype"), "$[0].earn.ItemType") == "ORA_T") & (col("extra_details.transactiondescription").ilike("%Bounty%Pickup%")), lit(1)).otherwise(lit(0)),
            "bounties_t1_claimed": when((get_json_object(col("extra_details.transactiontype"), "$[0].earn.ItemType") == "ORA_T") & (col("extra_details.transactiondescription").ilike("%Bounty%T1%Pickup%")), lit(1)).otherwise(lit(0)),
            "bounties_t2_claimed": when((get_json_object(col("extra_details.transactiontype"), "$[0].earn.ItemType") == "ORA_T") & (col("extra_details.transactiondescription").ilike("%Bounty%T2%Pickup%")), lit(1)).otherwise(lit(0)),
            "bounties_t3_claimed": when((get_json_object(col("extra_details.transactiontype"), "$[0].earn.ItemType") == "ORA_T") & (col("extra_details.transactiondescription").ilike("%Bounty%T3%Pickup%")), lit(1)).otherwise(lit(0))
        })
        .withColumns({
            "did_class_3_participation": when(col("ora_class_3_event_participation") > 0, lit(1)).otherwise(lit(0))
        })
        .where(col("full_account_id").isNotNull())
        .where(col("group_id").isNotNull())
        .where(col("match_id").isNotNull())
        .where(col("event_defaults.build_environment") != "development_editor")
        .select("received_on", "player_id", "full_account_id", "group_id", "match_id", "build_environment", "build_changelist", "mm_environment", "match_type", "event_trigger", "extra_details", "ora_t_earn", "ora_group_wipe", "ora_bounty_recovered", "ora_class_1_event_win", "ora_class_2_event_win", "ora_class_3_event_participation", "did_class_3_participation", "ora_class_3_event_win", "ora_perks", "ora_p_earn", "bounties_claimed", "bounties_t1_claimed", "bounties_t2_claimed", "bounties_t3_claimed")
    )

    return txn_df

# COMMAND ----------

def transform(environment, spark):
    spa_df = read_spa_df(environment, spark)
    ms_df = read_ms_df(environment, spark)
    trials_event_df = read_trials_event_df(environment, spark)
    mhp_df = read_mhp_df(environment, spark)
    txn_df = read_txn_df(environment, spark)

    pms_df = (
        spa_df
        .unionByName(ms_df, True)
        .unionByName(trials_event_df, True)
        .unionByName(mhp_df, True)
        .unionByName(txn_df, True)
        .withWatermark("received_on", "1 minute")
        .groupBy(
            session_window("received_on", "2 minutes"),
            col("full_account_id"),
            col("group_id"),
            col("match_id"),
            col("build_environment"),
            col("build_changelist"),
            col("mm_environment"),
            col("match_type"))
        .agg(
            max("player_id").alias("player_id"),
            min(col("received_on").cast("date")).alias("date"),
            max(col("group_size")).alias("group_size"),
            max(col("hero_class_name")).alias("hero_class_name"),
            collect_set(col("event_defaults.server_id")).alias("server_ids"),
            max(col("start_ts")).alias("start_ts"),
            max(coalesce(col("end_ts"), col("received_on"))).alias("end_ts"),
            max(ifnull(col("match_outcome"), lit("Unknown"))).alias("match_outcome"),
            max(col("medal")).alias("medal"),
            max(col("start_map")).alias("start_map"),
            max(ifnull(col("end_map"), col("start_map"))).alias("end_map"),
            max(ifnull(col("start_level"), lit(0))).alias("start_level"),
            max(ifnull(col("end_level"), lit(0))).alias("end_level"),
            max(col("start_upgrades")).alias("start_upgrades"),
            max(greatest(size(col("start_upgrades")), lit(0))).alias("start_upgrades_cnt"),
            max(col("extracted_upgrades")).alias("extracted_upgrades"),
            max(greatest(size(col("extracted_upgrades")), lit(0))).alias("extracted_upgrades_cnt"),
            max(col("active_perks")).alias("active_perks"),
            count(when(col("event_trigger") == "Kill", True)).alias("kills"),
            count(when(col("event_trigger") == "Death", True)).alias("deaths"),
            count(when(col("event_trigger") == "Assist", True)).alias("assists"),
            count(when(col("event_trigger") == "Down_Killer", True)).alias("downs"),
            count(when(col("event_trigger") == "ReviveFromDowned_Reviver", True)).alias("revives"),
            count(when(col("event_trigger") == "ReviveFromDowned_Victim", True)).alias("times_revived"),
            count(when(col("event_trigger") == "ReviveFromKilled_Rescuer", True)).alias("rescues"),
            count(when(col("event_trigger") == "ReviveFromKilled_Revived", True)).alias("times_rescued"),
            count(when(col("event_trigger") == "ChestOpened", True)).alias("chests_opened"),
            count(when((col("event_trigger") == "item_used") & (col("extra_details.item_type") == "Healing"), True)).alias("healing_items_used"),
            count(when((col("event_trigger") == "item_used") & (col("extra_details.item_type") == "Gadget"), True)).alias("gadgets_used"),
            count(when(col("event_trigger") == "InteractablePickup_HealthLarge", True)).alias("large_health_picked_up"),
            count(when(col("event_trigger") == "InteractablePickup_HealthSmall", True)).alias("small_health_picked_up"),
            count(when(col("event_trigger") == "InteractablePickup_LargeArmorPack", True)).alias("large_armor_picked_up"),
            count(when(col("event_trigger") == "InteractablePickup_SmallArmorPack", True)).alias("small_armor_picked_up"),
            max(when(col("event_trigger") == "InteractablePickup_ArmorUpgrade", col("extra_details.item_level").cast('int')).otherwise(1)).alias("max_armor_tier"),
            max(when(col("event_trigger") == "PlayerOnStatsProcessed", get_json_object(col("extra_details.performance"), "$[3].value"))).alias("damage_done"),
            count(when(col("event_trigger") == "HeroLevelUp", True)).alias("levelups"),
            count(when(col("event_trigger") == "UpgradeSelection", True)).alias("upgrades"),
            collect_list(when(col("event_trigger") == "UpgradeSelection", get_json_object("extra_details.upgradepayload", "$.Selected Upgrade"))).alias("unlocked_upgrade_build"),
            count(when(col("event_trigger") == "extraction_start", True)).alias("extractions_started"),
            count(when(col("event_trigger") == "extraction_complete", True)).alias("extractions_completed"),
            count(when(col("event_trigger") == "extraction_timeout", True)).alias("extractions_abandoned"),
            collect_set(when(col("event_trigger") == "extraction_start", from_json("extra_details.event_coords", "struct<x: float, y: float, z: float>"))).alias("extraction_coord_set"),
            ifnull(sum("ora_t_earn"), lit(0)).alias("ora_t_earn"),
            ifnull(sum("ora_group_wipe"), lit(0)).alias("ora_group_wipe"),
            ifnull(sum("ora_bounty_recovered"), lit(0)).alias("ora_bounty_recovered"),
            ifnull(sum("ora_class_1_event_win"), lit(0)).alias("ora_class_1_event_win"),
            ifnull(sum("ora_class_2_event_win"), lit(0)).alias("ora_class_2_event_win"),
            ifnull(sum("ora_class_3_event_participation"), lit(0)).alias("ora_class_3_event_participation"),
            ifnull(sum("ora_class_3_event_win"), lit(0)).alias("ora_class_3_event_win"),
            ifnull(sum("ora_perks"), lit(0)).alias("ora_perks"),
            ifnull(max("ora_p_earn"), lit(0)).alias("ora_p_earn"),
            ifnull(sum("bounties_claimed"), lit(0)).alias("bounties_claimed"),
            ifnull(sum("bounties_t1_claimed"), lit(0)).alias("bounties_t1_claimed"),
            ifnull(sum("bounties_t2_claimed"), lit(0)).alias("bounties_t2_claimed"),
            ifnull(sum("bounties_t3_claimed"), lit(0)).alias("bounties_t3_claimed"),
            count(when((col("event_trigger") == "minor_event_win") & (col("extra_details.event_name").isin(["BeaconPuzzle", "BotCamp", "CorruptedChest", "DataCollect", "LootGoblin", "TargetPractice"])), True)).alias("class_1_event_wins"),
            count(when((col("event_trigger") == "minor_event_lost") & (col("extra_details.event_name").isin(["BeaconPuzzle", "BotCamp", "CorruptedChest", "DataCollect", "LootGoblin", "TargetPractice"])), True)).alias("class_1_event_losses"),
            count(when((col("event_trigger") == "minor_event_win") & (col("extra_details.event_name").isin(["HighValueTarget", "Rift"])), True)).alias("class_2_event_wins"),
            count(when((col("event_trigger") == "minor_event_lost") & (col("extra_details.event_name").isin(["HighValueTarget", "Rift"])), True)).alias("class_2_event_losses"),
            count(when((col("event_trigger") == "major_event_win"), True)).alias("class_3_event_wins"),
            count(when((col("event_trigger") == "major_event_loss"), True)).alias("class_3_event_losses"),
            collect_set(when(col("event_trigger") == "enter_major_event", col("extra_details.event_name"))).alias("class_3_unique_events"),
            max("gauntlet_team_wins").alias("gauntlet_team_wins"),
            max("gauntlet_team_losses").alias("gauntlet_team_losses"),
            max("final_showdown_result").alias("final_showdown_result"),
            count(when(col("gauntlet_fight_outcome") == "TeamEliminated", True)).alias("gauntlet_elims"),
            count(when(col("gauntlet_fight_outcome") == "TieBreakerCaptured", True)).alias("gauntlet_tiebreaks")
        )
        .withColumns({
            "match_duration": round((col("end_ts") - col("start_ts")).cast("int") / 60, 2),
            "server_count": size(col("server_ids")),
            "active_perk_cnt": greatest(size(col("active_perks")), lit(0)),
            "used_start_build": when(col("start_upgrades").isNotNull(), lit("True")).otherwise(lit("False")),
            "full_upgrade_build": array_union(col("start_upgrades"), col("unlocked_upgrade_build")),
            "ora_lost": col("ora_t_earn") - col("ora_p_earn"),
            "dw_insert_ts": current_timestamp(),
            "dw_update_ts": current_timestamp()
        })
        .where(col("hero_class_name").isNotNull())
        .select(
            "player_id",
            "full_account_id",
            "group_id",
            "match_id",
            "build_environment",
            "build_changelist",
            "mm_environment",
            "match_type",
            "server_ids",
            "server_count",
            "date",
            "hero_class_name",
            "group_size",
            "start_ts",
            "end_ts",
            "match_duration",
            "match_outcome",
            "medal",
            "start_map",
            "end_map",
            "start_level",
            "end_level",
            "start_upgrades",
            "start_upgrades_cnt",
            "used_start_build",
            "extracted_upgrades",
            "extracted_upgrades_cnt",
            "active_perks",
            "active_perk_cnt",
            "kills",
            "deaths",
            "assists",
            "downs",
            "revives",
            "times_revived",
            "rescues",
            "times_rescued",
            "chests_opened",
            "healing_items_used",
            "gadgets_used",
            "max_armor_tier",
            "damage_done",
            "levelups",
            "upgrades",
            "unlocked_upgrade_build",
            "full_upgrade_build",
            "extractions_started",
            "extractions_completed",
            "extractions_abandoned",
            "extraction_coord_set",
            "ora_t_earn",
            "ora_group_wipe",
            "ora_bounty_recovered",
            "ora_class_1_event_win",
            "ora_class_2_event_win",
            "ora_class_3_event_participation",
            "ora_class_3_event_win",
            "ora_perks",
            "ora_p_earn",
            "ora_lost",
            "bounties_claimed",
            "bounties_t1_claimed",
            "bounties_t2_claimed",
            "bounties_t3_claimed",
            "class_1_event_wins",
            "class_1_event_losses",
            "class_2_event_wins",
            "class_2_event_losses",
            "class_3_event_wins",
            "class_3_event_losses",
            "class_3_unique_events",
            "gauntlet_team_wins",
            "gauntlet_team_losses",
            "final_showdown_result",
            "gauntlet_elims",
            "gauntlet_tiebreaks",
            "dw_insert_ts",
            "dw_update_ts"
        )
    )

    return pms_df

# COMMAND ----------

def stream_player_match_summary():
    environment, checkpoint_location = arg_parser()
    # environment, checkpoint_location = "_dev", "dbfs:/tmp/horizon/managed/streaming/run_dev/fact_player_match_summary"
    spark = SparkSession.builder.appName("Hydra").getOrCreate()

    spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "false")
    spark.conf.set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")
    spark.conf.set("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")

    transformed_df = transform(environment, spark)
    
    (
        DeltaTable.createIfNotExists(spark)
        .tableName(f"horizon{environment}.managed.fact_player_match_summary")
        .addColumns(transformed_df.schema)
        .property('delta.enableIcebergCompatV2', 'true')
        .property('delta.universalFormat.enabledFormats', 'iceberg')
        .execute()
    )

    (
        transformed_df
        .writeStream
        .option("checkpointLocation", checkpoint_location)
        .option("mergeSchema", "true")
        .toTable(f"horizon{environment}.managed.fact_player_match_summary")
    )

# COMMAND ----------

if __name__ == "__main__":
    stream_player_match_summary()
