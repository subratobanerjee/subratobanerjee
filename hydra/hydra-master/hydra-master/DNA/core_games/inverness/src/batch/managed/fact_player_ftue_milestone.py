# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

import json
from pyspark.sql import Window
from pyspark.sql.functions import first, round, expr, lead, when, col, split
from delta.tables import DeltaTable
import datetime

# COMMAND ----------

def extract(spark, environment, prev_max_ts):

    fact_player_df = (
        spark
        .read
        .table(f"inverness{environment}.managed.fact_player_summary_ltd")
        .where(f"player_id is not null and player_id not in ('anonymous', 'NULL')")
        #  and dw_insert_ts::date >= to_date('{prev_max_ts}') - interval '2 day'")
    )

    campaign_status_df = (
        spark
        .read
        .table(f"inverness{environment}.raw.campaignsetupstatus")
        .where(f"insert_ts::date >= to_date('{prev_max_ts}') - interval '1 day'")
    )

    temp_campaign_activity_df = (
        spark
        .read
        .table(f"inverness{environment}.intermediate.fact_player_campaign_activity")
        .where(f"""player_type='HUMAN' 
               and dw_insert_ts::date >= to_date('{prev_max_ts}') - interval '1 day'
               and source_table not in ('yieldgain', 'narrativestatus')
               """)
    )

    independent_status_df = (
        spark
        .read
        .table(f"inverness{environment}.raw.independentstatus")
        .where(f"playertype='HUMAN' and insert_ts::date >= to_date('{prev_max_ts}') - interval '1 day' ")
    )

    ftue_status_df = (
        spark
        .read
        .table(f"inverness{environment}.raw.ftuestatus")
        .where(f"playertype='HUMAN' and insert_ts::date >= to_date('{prev_max_ts}') - interval '1 day'")
    )

    settings_status_df = (
        spark
        .read
        .table(f"inverness{environment}.raw.settingsstatus")
        .where(f"insert_ts::date >= to_date('{prev_max_ts}') - interval '1 day'")
    )

    greatworks_status_df = (
        spark
        .read
        .table(f"inverness{environment}.raw.greatworksstatus")
        .where(f"playertype='HUMAN' and insert_ts::date >= to_date('{prev_max_ts}') - interval '1 day'")
    )

    crisis_status_df = (
        spark
        .read
        .table(f"inverness{environment}.raw.crisisstatus")
        .where(f"playertype='HUMAN' and insert_ts::date >= to_date('{prev_max_ts}') - interval '1 day'")
    )

    strategy_status_df = (
        spark
        .read
        .table(f"inverness{environment}.raw.strategycardstatus")
        .where(f"playertype='HUMAN' and insert_ts::date >= to_date('{prev_max_ts}') - interval '1 day'")
    )

    raw_df = (
        independent_status_df
        .unionByName(ftue_status_df, True)
        .unionByName(settings_status_df, True)
        .unionByName(greatworks_status_df, True)
        .unionByName(crisis_status_df, True)
        .unionByName(strategy_status_df, True)
    )

    campaign_activity_df = (
        temp_campaign_activity_df.alias("tca")
        .join(fact_player_df.alias("fp"), 
            expr("tca.player_id = fp.player_id and tca.platform = fp.platform and tca.service = fp.service and tca.campaign_instance_id = fp.first_campaign_id"), 
            'inner')
        .select("tca.*")
    )

    session_df = (
        spark
        .read
        .table(f"inverness{environment}.intermediate.fact_player_session")
        .where(f"dw_insert_ts::date >= to_date('{prev_max_ts}') - interval '1 day' AND session_end_ts::date < '{prev_max_ts}'")
    )

    tutorial_level_df = (
        spark
        .read
        .table(f"inverness{environment}.raw.settingsstatus")
        .where(expr(f"insert_ts::date >= to_date('{prev_max_ts}') - interval '1 day' and playerPublicId is not null"))
        .groupBy("playerPublicId")
        .agg(expr("max(tutorialLevel) as tutorialLevel"))
    )

    return fact_player_df, campaign_status_df, campaign_activity_df, raw_df, session_df, tutorial_level_df


# COMMAND ----------

def transform(fact_player_df, campaign_status_df, campaign_activity_df, raw_df, session_df, tutorial_level_df, environment, spark):

    pca_group_by_columns = {
        "player_id": expr("pca.player_id as player_id"),
        "platform": expr("pca.platform as platform"),
        "service": expr("pca.service as service"),
    }

    window_spec_pca = Window.partitionBy("player_id", "campaign_instance_id", "platform", "service").orderBy("received_on")

    age_agg_columns = {
        "age_complete_count": expr("count(distinct CASE WHEN EVENT_TRIGGER = 'CampaignExit' AND CAMPAIGN_EXIT_TYPE = 'AgeEnd' and PLAYER_TYPE = 'HUMAN' THEN Age END) as age_complete_count"),
        "complete_next_age_ts": expr("max(CASE WHEN EVENT_TRIGGER = 'CampaignExit' AND CAMPAIGN_EXIT_TYPE = 'AgeEnd' AND PLAYER_TYPE = 'HUMAN' THEN received_on END) as complete_next_age_ts"),
    }

    prod_item_agg_columns = {
        "first_unit": expr("first(CASE WHEN pca.EVENT_TRIGGER = 'ProductionItemCompleted' THEN prod_item END, true) as first_unit"),
        "first_unit_ts": expr("first(CASE WHEN pca.EVENT_TRIGGER = 'ProductionItemCompleted' THEN prod_item_ts::timestamp END, true) as first_unit_ts")
    }

    pca_agg_columns = {
        "did_settle_capital": expr("max(CASE WHEN pca.EVENT_TRIGGER = 'SettlementFounded' THEN TRUE ELSE FALSE END) as did_settle_capital"),
        "settle_capital_ts": expr("min(CASE WHEN pca.EVENT_TRIGGER = 'SettlementFounded' THEN pca.RECEIVED_ON::timestamp END) as settle_capital_ts"),
        "did_campaign_start": expr("max(case when pca.event_trigger = 'CampaignEnter' then True else False end) as did_campaign_start"),
        "campaign_start_ts": expr("min(case when pca.event_trigger = 'CampaignEnter' then pca.received_on::timestamp end) as campaign_start_ts"),
        "did_settle_town": expr("max(case when pca.event_trigger = 'SettlementFounded' and pca.settlement_id NOT ILIKE '65536%1' then True else False end) as did_settle_town"),
        "settle_town_ts": expr("min(case when pca.event_trigger = 'SettlementFounded' and pca.settlement_id NOT ILIKE '65536%1' then pca.received_on::timestamp end) as settle_town_ts"),
        "did_convert_town_city": expr("max(case when pca.event_trigger = 'ConvertedToCity' then True else False end) as did_convert_town_city"),
        "convert_town_city_ts": expr("min(case when pca.event_trigger = 'ConvertedToCity' then pca.received_on::timestamp end) as convert_town_city_ts"),
        "did_encounter_civ": expr("max(case when pca.source_table = 'diplomacyactionstatus' AND pca.DIPLOMACY_ACTION_TARGET <> 'PLAYER_SLOT' and pca.DIPLOMACY_ACTION_TARGET::int < (CS.AIPLAYERCOUNT::int + CS.HUMANPLAYERCOUNT::int) THEN TRUE ELSE FALSE END) as did_encounter_civ"),
        "encounter_civ_ts":expr("min(CASE WHEN pca.SOURCE_TABLE = 'diplomacyactionstatus' AND pca.DIPLOMACY_ACTION_TARGET <> 'PLAYER_SLOT' AND pca.DIPLOMACY_ACTION_TARGET::int < (CS.AIPLAYERCOUNT::int + CS.HUMANPLAYERCOUNT::int)  THEN pca.RECEIVED_ON::timestamp END) as encounter_civ_ts"),
        "did_encounter_independent": expr("max(CASE WHEN pca.SOURCE_TABLE = 'diplomacyactionstatus' AND pca.DIPLOMACY_ACTION_TARGET <> 'PLAYER_SLOT' AND pca.DIPLOMACY_ACTION_TARGET::int >= (CS.AIPLAYERCOUNT::int + CS.HUMANPLAYERCOUNT::int)  THEN TRUE ELSE FALSE END) as did_encounter_independent"),
        "encounter_independent_ts": expr("min(CASE WHEN pca.SOURCE_TABLE = 'diplomacyactionstatus' AND pca.DIPLOMACY_ACTION_TARGET <> 'PLAYER_SLOT' AND pca.DIPLOMACY_ACTION_TARGET::int >= (CS.AIPLAYERCOUNT::int + CS.HUMANPLAYERCOUNT::int)  THEN pca.RECEIVED_ON::timestamp END) as encounter_independent_ts"),
        "did_engage_combat": expr("max(CASE WHEN pca.SOURCE_TABLE = 'militarystatus' AND pca.ATTACKS_MADE::int > 0 THEN TRUE ELSE FALSE END) as did_engage_combat"),
        "engage_combat_ts": expr("min(CASE WHEN pca.SOURCE_TABLE = 'militarystatus' AND pca.ATTACKS_MADE::int > 0 THEN pca.RECEIVED_ON::timestamp END) as engage_combat_ts"),
        "did_complete_milestone": expr("max(case when pca.event_trigger = 'Milestone Achieved' then True else False end) as did_complete_milestone"),
        "complete_milestone_ts": expr("min(case when pca.event_trigger = 'Milestone Achieved' then pca.received_on::timestamp end) as complete_milestone_ts"),
        "did_research_tech": expr("max(CASE WHEN pca.SOURCE_TABLE = 'techtreestatus' AND pca.EVENT_TRIGGER = 'Completed' THEN TRUE ELSE FALSE END) as did_research_tech"),
        "research_tech_ts": expr("min(CASE WHEN pca.SOURCE_TABLE = 'techtreestatus' AND pca.EVENT_TRIGGER = 'Completed' THEN pca.RECEIVED_ON::timestamp END) as research_tech_ts"),
        "did_research_civic": expr("max(CASE WHEN pca.SOURCE_TABLE = 'civictreestatus' AND pca.EVENT_TRIGGER = 'Completed' THEN TRUE ELSE FALSE END) as did_research_civic"),
        "research_civic_ts": expr("min(CASE WHEN pca.SOURCE_TABLE = 'civictreestatus' AND pca.EVENT_TRIGGER = 'Completed' THEN pca.RECEIVED_ON::timestamp END) as research_civic_ts"),
        "did_spend_attribute": expr("max(CASE WHEN pca.SOURCE_TABLE = 'leaderattributestatus' AND pca.EVENT_TRIGGER = 'Node Acquired' THEN TRUE ELSE FALSE END) as did_spend_attribute"),
        "spend_attribute_ts": expr("min(CASE WHEN pca.SOURCE_TABLE = 'leaderattributestatus' AND pca.EVENT_TRIGGER = 'Node Acquired' THEN pca.RECEIVED_ON::timestamp END) as spend_attribute_ts"),
        "did_intiate_dip_action": expr("max(CASE WHEN pca.SOURCE_TABLE = 'diplomacyactionstatus' AND pca.EVENT_TRIGGER = 'Initiate' THEN TRUE ELSE FALSE END) as did_intiate_dip_action"),
        "initiate_dip_action_ts": expr("min(CASE WHEN pca.SOURCE_TABLE = 'diplomacyactionstatus' AND pca.EVENT_TRIGGER = 'Initiate' THEN pca.RECEIVED_ON::timestamp END) as initiate_dip_action_ts"),
        "did_receive_commander": expr("max(case when pca.event_trigger = 'Spawned' then True else False end) as did_receive_commander"),
        "receive_commander_ts": expr("min(case when pca.event_trigger = 'Spawned'  then pca.received_on::timestamp end) as receive_commander_ts"),
        "did_promote_commander": expr("max(case when pca.event_trigger = 'Commander Action' and pca.commander_action = 'promote' then True else False end) as did_promote_commander"),
        "promote_commander_ts": expr("min(case when pca.event_trigger = 'Commander Action' and pca.commander_action = 'promote' then pca.received_on::timestamp end) as promote_commander_ts"),
        "did_conquer_settlement": expr("max(case when pca.event_trigger = 'SettlementAcquired' and city_transfer_type = 'BY_COMBAT' then True else False end) as did_conquer_settlement"),
        "conquer_settlement_ts": expr("min(case when pca.event_trigger = 'SettlementAcquired' then pca.received_on::timestamp end) as conquer_settlement_ts"),
        "did_start_trade_route": expr("max(case when pca.event_trigger = 'Trade Route Formed' then True else False end) as did_start_trade_route"),
        "start_trade_route_ts": expr("min(case when pca.event_trigger = 'Trade Route Formed' then pca.received_on::timestamp end) as start_trade_route_ts"),
        "did_assign_resource": expr("max(case when pca.event_trigger = 'Resources Added' then True else False end) as did_assign_resource"),
        "assign_resource_ts": expr("min(case when pca.event_trigger = 'Resources Added' then pca.received_on::timestamp end) as assign_resource_ts"),
        "did_complete_age": expr("min(case when pca.event_trigger = 'CampaignExit' and pca.campaign_exit_type = 'AgeEnd' then True else False end) as did_complete_age"),
        "complete_age_ts": expr("min(case when pca.event_trigger = 'CampaignExit' and pca.campaign_exit_type = 'AgeEnd' then pca.received_on::timestamp end) as complete_age_ts"),
        "did_complete_full_campaign": expr("max(case when pca.event_trigger = 'Victory Achieved' and pca.source_table = 'victorystatus' then True else False end) as did_complete_full_campaign"),
        "complete_full_campaign_ts": expr("min(case when pca.event_trigger = 'Victory Achieved' and pca.source_table = 'victorystatus' then pca.received_on::timestamp end) as complete_full_campaign_ts"),
        "num_sessions_played": expr("count(distinct pca.application_session_instance_id) as num_sessions_played"),
        "did_campaign_setup": expr("max(case when cs.campaignsetupevent = 'Complete' then True else False end) as did_campaign_setup"),
        "campaign_setup_ts": expr("min(case when cs.campaignsetupevent = 'Complete' then cs.receivedOn::int::timestamp end) as campaign_setup_ts"),
        "game_difficulty": expr("max(case when cs.campaignsetupevent = 'Complete' then cs.gamedifficulty end) as game_difficulty"),
    }

    age_agg_df = (
        campaign_activity_df.alias("pca")
        .where("source_table = 'campaignstatus'")
        .select("player_id", 
                "platform", 
                "service", 
                lead(expr("case when event_trigger = 'CampaignExit' and campaign_exit_type = 'AgeEnd' then true else false end")).over(window_spec_pca).alias("next_age_flag"),
                lead(expr("case when event_trigger = 'CampaignExit' and campaign_exit_type = 'AgeEnd' then received_on end")).over(window_spec_pca).alias("next_age_ts"),
                "event_trigger",
                "campaign_exit_type",
                "player_type",
                "age",
                "received_on")
        .groupBy(*pca_group_by_columns.values())
        .agg(*age_agg_columns.values())
    )
    
    campaign_activity_prod_df = (
        campaign_activity_df.alias("pca")
        .where(expr("source_table = 'settlementstatus' and production_item ilike '%UNIT_%'"))
        .select("player_id", 
                "platform", 
                "service",
                "event_trigger",
                first(split(col("production_item"), '_')[1]).over(window_spec_pca).alias("prod_item"),
                first(col("received_on")).over(window_spec_pca).alias("prod_item_ts"))
        .groupBy(*pca_group_by_columns.values())
        .agg(*prod_item_agg_columns.values())
    )

    pca_agg_df = (
        campaign_activity_df.alias("pca")
        .join(
        campaign_status_df.alias("cs"),
        expr(
            "lower(trim(cast(pca.player_id AS STRING))) = lower(trim(cast(cs.playerPublicId AS STRING))) "
            "AND lower(trim(cast(pca.campaign_setup_instance_id AS STRING))) = lower(trim(cast(cs.campaignsetupinstanceid AS STRING)))"
        ),
        'left')
        .groupBy(*pca_group_by_columns.values())
        .agg(*pca_agg_columns.values())
    )

    pca_agg_df = (
        pca_agg_df.alias("pca")
        .join(age_agg_df.alias("pcaa"), 
              expr("pca.player_id = pcaa.player_id and pca.platform = pcaa.platform and pca.service = pcaa.service"), 
              'left')
        .join(campaign_activity_prod_df.alias("pcap"), 
              expr("pca.player_id = pcap.player_id and pca.platform = pcap.platform and pca.service = pcap.service"), 
              'left')
        .select(
            expr("pca.*"),
            expr("CASE WHEN pcaa.age_complete_count > 1 THEN true ELSE false END AS did_complete_next_age"),
            expr("pcaa.complete_next_age_ts"),
            expr("pcap.first_unit"),
            expr("pcap.first_unit_ts")
        )
    )

    #added here change 1 :
    pca_agg_df = (
    pca_agg_df.alias("pcgg")
    .join(campaign_activity_df.alias("pcff"), 
          expr("pcgg.player_id = pcff.player_id and pcgg.platform = pcff.platform and pcgg.service = pcff.service"), 
          'left')
    .select(
        "pcgg.*",
        "pcff.application_session_instance_id"
    )
    .distinct()
    )

    window_spec = Window.partitionBy("player_id", "platform", "service").orderBy("session_start_ts")

    #added here change 2 :
    sessions_agg_df = (
    session_df
    .selectExpr(
        "player_id", 
        "platform", 
        "service",
        "application_session_id",
        "(CAST((unix_timestamp(session_end_ts) - unix_timestamp(session_start_ts)) / 60 AS INT)) AS num_mins",
        "agg_1 AS num_turns"
    )
    .distinct()
    )

    group_by_columns = {
        "player_id": expr("src.player_id as player_id"),
        "platform": expr("src.platform as platform"),
        "service": expr("src.service as service"),
    }

    agg_columns = {
        "install_date": expr("min(src.first_seen_time::date) as install_date"),
        "leader_selected": expr("max(src.first_leader) as leader_selected"),
        "civilization_selected": expr("max(src.first_civ) as civilization_selected"),
        "did_reject_legacy_path_quest": expr("null as did_reject_legacy_path_quest"),
        "reject_legacy_path_quest_ts": expr("null as reject_legacy_path_quest_ts"),
        "num_minutes_played": expr("sum(fps.num_mins) as num_minutes_played"),
        "num_turns_played": expr("sum(fps.num_turns) as num_turns_played")
    }
    
    agg_columns_raw = {
        "did_become_suzerain": expr("max(case when raw.independentevent = 'Became Suzerain' then True else False end) as did_become_suzerain"),
        "become_suzerain_ts": expr("min(case when raw.independentevent = 'Became Suzerain' then raw.receivedOn::timestamp end) as become_suzerain_ts"),
        "did_disperse_independent": expr("max(case when raw.independentevent = 'Independent Dispersed' then True else False end) as did_disperse_independent"),
        "disperse_independent_ts": expr("min(case when raw.independentevent = 'Independent Dispersed' then raw.receivedOn::timestamp end) as disperse_independent_ts"),
        "did_accept_legacy_path_quest": expr("max(CASE WHEN tutorialDefinitionId ilike '%accepted%' OR tutorialdefinitionid = 'military_victory_quest_1' THEN TRUE ELSE FALSE END) as did_accept_legacy_path_quest"),
        "accept_legacy_path_quest_ts": expr("min(CASE WHEN tutorialDefinitionId ilike '%accepted%' OR tutorialdefinitionid = 'military_victory_quest_1' THEN raw.RECEIVEDON::timestamp END) as accept_legacy_path_quest_ts"),
        "did_encounter_watch_out": expr("max(case when raw.ftueevent = 'Watch Out Triggered' then True else False end) as did_encounter_watch_out"),
        "encounter_watch_out_ts": expr("min(case when raw.ftueevent = 'Watch Out Triggered' then raw.receivedOn::timestamp end) as encounter_watch_out_ts"),
        "did_complete_legacy_path": expr("max(CASE WHEN TUTORIALDEFINITIONID ilike '%victory_quest_line_completed%' THEN TRUE ELSE FALSE END) as did_complete_legacy_path"),
        "complete_legacy_path_ts": expr("min(CASE WHEN TUTORIALDEFINITIONID ilike '%victory_quest_line_completed%' THEN raw.RECEIVEDON::timestamp END) as complete_legacy_path_ts"),
        "did_receive_great_work": expr("max(case when raw.greatworkevent = 'Great Work Obtained' then True else False end) as did_receive_great_work"),
        "receive_great_work_ts": expr("min(case when raw.greatworkevent = 'Great Work Obtained' then raw.receivedOn::timestamp end) as receive_great_work_ts"),
        "did_assign_great_work": expr("max(case when raw.greatworkevent = 'Great Work Displayed' then True else False end) as did_assign_great_work"),
        "assign_great_work_ts": expr("min(case when raw.greatworkevent = 'Great Work Displayed' then raw.receivedOn::timestamp end) as assign_great_work_ts"),
        "did_encounter_crisis": expr("max(case when raw.crisisevent = 'Crisis Triggered' then True else False end) as did_encounter_crisis"),
        "encounter_crisis_ts": expr("min(case when raw.crisisevent = 'Crisis Triggered' then raw.receivedOn::timestamp end) as encounter_crisis_ts"),
        "did_start_age_transition": expr("max(case when raw.strategycardevent = 'Selection Start' then True else False end) as did_start_age_transition"),
        "start_age_transition_ts": expr("min(case when raw.strategycardevent = 'Selection Start' then raw.receivedOn::timestamp end) as start_age_transition_ts"),
        "did_complete_age_transition": expr("max(case when raw.strategycardevent = 'Selection Finalized' then True else False end) as did_complete_age_transition"),
        "complete_age_transition_ts": expr("min(case when raw.strategycardevent = 'Selection Finalized' then raw.receivedOn::timestamp end) as complete_age_transition_ts"),
        "did_turn_off_tutorial": expr("max(case when tutorial.tutorialLevel = '0' then True else False end) as did_turn_off_tutorial"),
        "turn_off_tutorial_ts": expr("min(case when tutorial.tutorialLevel = '0' then raw.receivedOn::timestamp end) as turn_off_tutorial_ts")
    }

    #added here change 3 :
    raw_df = raw_df.alias("raw").join(tutorial_level_df.alias("tutorial"), expr("lower(raw.playerPublicId) = lower(tutorial.playerPublicId)"), 'left').groupBy('raw.playerPublicId', 'raw.campaigninstanceid').agg(*agg_columns_raw.values())
    #added here change 4 :
    final_df = (
    fact_player_df
    .alias("src")
    .join(raw_df.alias("raw_1"), expr("src.player_id = lower(raw_1.playerPublicId) and src.first_campaign_id = lower(raw_1.campaigninstanceid)"), 'left')
    .join(pca_agg_df
          .alias("pca"), expr("src.player_id = pca.player_id and src.platform = pca.platform and src.service = pca.service"), 'left')
    .join(sessions_agg_df
          .alias("fps"), expr("src.player_id = fps.player_id and src.platform = fps.platform and src.service = fps.service and pca.application_session_instance_id = fps.application_session_id"), 'left')      
    .drop('application_session_id')
    .drop('application_session_instance_id')
    .groupBy(*group_by_columns.values(), 'pca.did_complete_next_age', 'pca.complete_next_age_ts', *pca_agg_columns.keys(), *prod_item_agg_columns.keys(), *agg_columns_raw.keys())
    .agg(*agg_columns.values())
    )
    final_df = final_df.withColumn("num_minutes_played", expr("CASE WHEN num_minutes_played IS NULL THEN 0 ELSE num_minutes_played END"))
    final_df = final_df.withColumn("num_turns_played", expr("CASE WHEN num_turns_played IS NULL THEN 0 ELSE num_turns_played END"))
    agg_columns.update(agg_columns_raw)       
    agg_columns.update(pca_agg_columns)

    return final_df, agg_columns


# COMMAND ----------

def create_fact_player_ftue_milestone(spark, environment):
    (
        DeltaTable.createIfNotExists(spark)
        .tableName(f"inverness{environment}.managed.fact_player_ftue_milestone")
        .addColumn("player_id", "string")
        .addColumn("platform", "string")
        .addColumn("service", "string")
        .addColumn("install_date", "timestamp")
        .addColumn("did_campaign_setup", "boolean")
        .addColumn("campaign_setup_ts", "timestamp")
        .addColumn("did_campaign_start", "boolean")
        .addColumn("campaign_start_ts", "timestamp")
        .addColumn("leader_selected", "string")
        .addColumn("civilization_selected", "string")
        .addColumn("game_difficulty", "string")
        .addColumn("did_settle_capital", "boolean")
        .addColumn("settle_capital_ts", "timestamp")
        .addColumn("first_unit", "string")
        .addColumn("first_unit_ts", "timestamp")
        .addColumn("did_settle_town", "boolean")
        .addColumn("settle_town_ts", "timestamp")
        .addColumn("did_convert_town_city", "boolean")
        .addColumn("convert_town_city_ts", "timestamp")
        .addColumn("did_encounter_civ", "boolean")
        .addColumn("encounter_civ_ts", "timestamp")
        .addColumn("did_encounter_independent", "boolean")
        .addColumn("encounter_independent_ts", "timestamp")
        .addColumn("did_become_suzerain", "boolean")
        .addColumn("become_suzerain_ts", "timestamp")
        .addColumn("did_disperse_independent", "boolean")
        .addColumn("disperse_independent_ts", "timestamp")
        .addColumn("did_engage_combat", "boolean")
        .addColumn("engage_combat_ts", "timestamp")
        .addColumn("did_research_tech", "boolean")
        .addColumn("research_tech_ts", "timestamp")
        .addColumn("did_research_civic", "boolean")
        .addColumn("research_civic_ts", "timestamp")
        .addColumn("did_spend_attribute", "boolean")
        .addColumn("spend_attribute_ts", "timestamp")
        .addColumn("did_intiate_dip_action", "boolean")
        .addColumn("initiate_dip_action_ts", "timestamp")
        .addColumn("did_accept_legacy_path_quest", "boolean")
        .addColumn("accept_legacy_path_quest_ts", "timestamp")
        .addColumn("did_reject_legacy_path_quest", "boolean")
        .addColumn("reject_legacy_path_quest_ts", "timestamp")
        .addColumn("did_turn_off_tutorial", "boolean")
        .addColumn("turn_off_tutorial_ts", "timestamp")
        .addColumn("did_encounter_watch_out", "boolean")
        .addColumn("encounter_watch_out_ts", "timestamp")
        .addColumn("did_complete_milestone", "boolean")
        .addColumn("complete_milestone_ts", "timestamp")
        .addColumn("did_complete_legacy_path", "boolean")
        .addColumn("complete_legacy_path_ts", "timestamp")
        .addColumn("did_receive_commander", "boolean")
        .addColumn("receive_commander_ts", "timestamp")
        .addColumn("did_promote_commander", "boolean")
        .addColumn("promote_commander_ts", "timestamp")
        .addColumn("did_conquer_settlement", "boolean")
        .addColumn("conquer_settlement_ts", "timestamp")
        .addColumn("did_start_trade_route", "boolean")
        .addColumn("start_trade_route_ts", "timestamp")
        .addColumn("did_assign_resource", "boolean")
        .addColumn("assign_resource_ts", "timestamp")
        .addColumn("did_receive_great_work", "boolean")
        .addColumn("receive_great_work_ts", "timestamp")
        .addColumn("did_assign_great_work", "boolean")
        .addColumn("assign_great_work_ts", "timestamp")
        .addColumn("did_encounter_crisis", "boolean")
        .addColumn("encounter_crisis_ts", "timestamp")
        .addColumn("did_complete_age", "boolean")
        .addColumn("complete_age_ts", "timestamp")
        .addColumn("did_start_age_transition", "boolean")
        .addColumn("start_age_transition_ts", "timestamp")
        .addColumn("did_complete_age_transition", "boolean")
        .addColumn("complete_age_transition_ts", "timestamp")
        .addColumn("did_complete_next_age", "boolean")
        .addColumn("complete_next_age_ts", "timestamp")
        .addColumn("did_complete_full_campaign", "boolean")
        .addColumn("complete_full_campaign_ts", "timestamp")
        .addColumn("num_sessions_played", "int")
        .addColumn("num_minutes_played", "int")
        .addColumn("num_turns_played", "int")
        .addColumn("dw_insert_ts", "timestamp")
        .addColumn("dw_update_ts", "timestamp")
        .addColumn("merge_key", "string")
        .property('delta.enableIcebergCompatV2', 'true')
        .property('delta.universalFormat.enabledFormats', 'iceberg')
        .execute()
    )

# COMMAND ----------

def load(df, agg_columns, environment, spark):
    target_table = DeltaTable.forName(spark, f"inverness{environment}.managed.fact_player_ftue_milestone")

    # get schema of the table
    table_schema = spark.read.table(f"inverness{environment}.managed.fact_player_ftue_milestone").schema

    # create empty dataframe using the schema
    out_df = spark.createDataFrame([], table_schema)

    # union the incoming dataframe with the empty dataframe, we can set allowMissingColumns to False to be more strict
    df = df.selectExpr(
            "*",
            "current_timestamp() as dw_insert_ts",
            "current_timestamp() as dw_update_ts",
            "sha2(concat_ws('|', player_id, platform, service), 256) as merge_key"
        )
    out_df = out_df.unionByName(df, allowMissingColumns=True)

    # setup merge condition
    agg_cols = [*agg_columns.keys()]
    # only compare on the flag columns, not the timestamp columns to reduce the number of comps
    comp_cols = [col_name for col_name in agg_cols if "_ts" not in col_name]
    merge_condition = " OR ".join(f"old.{col_name} <> new.{col_name}" for col_name in comp_cols)

    # create the update column dict
    update_set = {}
    for col_name in agg_cols:
        # Access the PySpark column object
        col_value = agg_columns[f"{col_name}"]
        if str(col_value).lower().startswith("column<'max(") | str(col_value).lower().startswith("column<'count("):
            update_set[f"old.{col_name}"] = f"greatest(new.{col_name}, old.{col_name})"
        elif str(col_value).lower().startswith("column<'min("):
            update_set[f"old.{col_name}"] = f"least(new.{col_name}, old.{col_name})"
        else:
            update_set[f"old.{col_name}"] = f"new.{col_name}"

    update_set[f"old.dw_update_ts"] = "CURRENT_TIMESTAMP()"

    # merge the table
    (
        target_table.alias('old')
        .merge(
            out_df.alias('new'),
            "new.merge_key = old.merge_key"
        )
        .whenMatchedUpdate(condition=merge_condition, set=update_set)
        .whenNotMatchedInsertAll()
        .execute()
    )


# COMMAND ----------

def process_ftue_milestone():
    input_param = dbutils_input_params()
    environment = input_param.get('environment',set_environment())
    # environment, checkpoint_location = "_dev", "dbfs:/tmp/inverness/batch/managed/run_dev/fact_player_ftue_milestone"
    spark = create_spark_session()
    create_fact_player_ftue_milestone(spark, environment)
    prev_max_ts = max_timestamp(spark, f"inverness{environment}.managed.fact_player_ftue_milestone")
    print(prev_max_ts)

    fact_player_df, campaign_status_df, campaign_activity_df, raw_df, session_df, tutorial_level_df = extract(spark, environment, prev_max_ts)
    
    df, agg_columns = transform(fact_player_df, campaign_status_df, campaign_activity_df, raw_df, session_df, tutorial_level_df, environment, spark)
    
    load(df, agg_columns, environment, spark)


# COMMAND ----------

if __name__ == "__main__":
    process_ftue_milestone()