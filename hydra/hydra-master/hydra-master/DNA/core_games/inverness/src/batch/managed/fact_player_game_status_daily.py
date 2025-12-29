# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/managed/fact_player_game_status_daily

# COMMAND ----------

from pyspark.sql.functions import expr

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'inverness'
data_source ='dna'
title = 'Civilization VII'
view_mapping = {
    'agg_game_status_1': 'agg_game_status_1 as num_turns_played',
    'agg_game_status_2': 'agg_game_status_2 as num_games_started',
    'agg_game_status_3': 'agg_game_status_3 as num_games_completed',
    'agg_gp_1': 'agg_gp_1 as num_games',
    'agg_gp_2': 'agg_gp_2 as game_duration_sec',
    'agg_gp_3': 'agg_gp_3 as num_antiquity_turns',
    'agg_gp_4': 'agg_gp_4 as num_exploration_turns',
    'agg_gp_5': 'agg_gp_5 as num_modern_turns',
    'agg_gp_6': 'agg_gp_6 as num_atomic_turns',
    'agg_gp_7': 'agg_gp_7 as num_antiquity_age_starts',
    'agg_gp_8': 'agg_gp_8 as num_exploration_age_starts',
    'agg_gp_9': 'agg_gp_9 as num_modern_age_starts',
    'agg_gp_10': 'agg_gp_10 as num_atomic_age_starts',
    'agg_gp_11': 'agg_gp_11 as num_antiquity_age_completes',
    'agg_gp_12': 'agg_gp_12 as num_exploration_age_completes',
    'agg_gp_13': 'agg_gp_13 as num_modern_age_completes',
    'agg_gp_14': 'agg_gp_14 as num_atomic_age_completes'
}

# COMMAND ----------

def read_campaign_activity(environment):
    return (
        spark
        .read
        .table(f"inverness{environment}.intermediate.fact_player_campaign_activity")
        .where(expr("received_on::date > CURRENT_DATE() - INTERVAL 2 DAY and player_type = 'HUMAN'"))
    )

# COMMAND ----------

def read_player_session(environment):
    """
    Read player session data and aggregate it to grab the session duration for the entire day.
    """
    return (
        spark
        .read
        .table(f"inverness{environment}.intermediate.fact_player_session")
        .where(expr("session_start_ts::date > CURRENT_DATE() - INTERVAL 2 DAY"))
        .groupBy(
            expr("session_start_ts::date as date"),
            "player_id"
        )
        .agg(
            expr("SUM(TIMESTAMPDIFF(SECOND, SESSION_START_TS, SESSION_END_TS)) as session_duration")
        )
    )

# COMMAND ----------

def read_victory_status(environment):
    return (
        spark
        .read
        .table(f"inverness{environment}.raw.victorystatus")
        .where(expr("insert_ts::timestamp::date > CURRENT_DATE() - INTERVAL 2 DAY and victoryevent = 'Victory Achieved'"))
        .groupBy(
            "campaigninstanceid"
        )
        .agg(
            expr("max(case when victoryevent = 'Victory Achieved' THEN 1 ELSE 0 END) as campaign_complete")
        )
    )

# COMMAND ----------

def extract(environment):
    camp_df = read_campaign_activity(environment).alias("pca")
    sesh_df = read_player_session(environment).alias("sesh")
    comp_df = read_victory_status(environment).alias("victory")

    joined_df = (
        camp_df
        .join(sesh_df, on=expr("pca.player_id = sesh.player_id and pca.received_on::date = sesh.date"), how="left")
        .join(comp_df, on=expr("pca.campaign_instance_id = victory.campaigninstanceid"), how="left")
    )

    return joined_df

# COMMAND ----------

def transform(df):
    out_df = (
        df
        .groupBy(
            expr("received_on::date as date"),
            "pca.player_id",
            "platform",
            "service",
            expr("'Unused' as game_mode"),
            expr("'Unused' as sub_mode"),
            "country_code",
            expr("'Unused' as extra_grain_1"),
            expr("'Unused' as extra_grain_2")
        )
        .agg(
            expr("COUNT(CASE WHEN EVENT_TRIGGER = 'CampaignTurnEnd' THEN TRUE END) as agg_game_status_1"),
            expr("COUNT(DISTINCT CASE WHEN EVENT_TRIGGER = 'CampaignEnter' THEN campaign_instance_id END) as agg_game_status_2"),
            expr("COUNT(DISTINCT CASE WHEN CAMPAIGN_COMPLETE = 1 THEN campaign_instance_id END) as agg_game_status_3"),
            expr("-1::int as agg_game_status_4"),
            expr("-1::int as agg_game_status_5"),
            expr("COUNT(DISTINCT CAMPAIGN_INSTANCE_ID) as agg_gp_1"),
            expr("MAX(SESSION_DURATION) as agg_gp_2"),
            expr("COUNT(CASE WHEN EVENT_TRIGGER = 'CampaignTurnEnd' AND AGE = 'AGE_ANTIQUITY' THEN TRUE END) as agg_gp_3"),
            expr("COUNT(CASE WHEN EVENT_TRIGGER = 'CampaignTurnEnd' AND AGE = 'AGE_EXPLORATION' THEN TRUE END) as agg_gp_4"),
            expr("COUNT(CASE WHEN EVENT_TRIGGER = 'CampaignTurnEnd' AND AGE = 'AGE_MODERN' THEN TRUE END) as agg_gp_5"),
            expr("COUNT(CASE WHEN EVENT_TRIGGER = 'CampaignTurnEnd' AND AGE = 'AGE_ATOMIC' THEN TRUE END) as agg_gp_6"),
            expr("COUNT(CASE WHEN EVENT_TRIGGER = 'CampaignEnter' AND TURN_NUMBER = '1' AND AGE = 'AGE_ANTIQUITY' THEN TRUE END) as agg_gp_7"),
            expr("COUNT(CASE WHEN EVENT_TRIGGER = 'CampaignEnter' AND TURN_NUMBER = '1' AND AGE = 'AGE_EXPLORATION' THEN TRUE END) as agg_gp_8"),
            expr("COUNT(CASE WHEN EVENT_TRIGGER = 'CampaignEnter' AND TURN_NUMBER = '1' AND AGE = 'AGE_MODERN' THEN TRUE END) as agg_gp_9"),
            expr("COUNT(CASE WHEN EVENT_TRIGGER = 'CampaignEnter' AND TURN_NUMBER = '1' AND AGE = 'AGE_ATOMIC' THEN TRUE END) as agg_gp_10"),
            expr("COUNT(CASE WHEN EVENT_TRIGGER = 'CampaignExit' AND CAMPAIGN_EXIT_TYPE = 'AgeEnd' AND AGE = 'AGE_ANTIQUITY' THEN TRUE END) as agg_gp_11"),
            expr("COUNT(CASE WHEN EVENT_TRIGGER = 'CampaignExit' AND CAMPAIGN_EXIT_TYPE = 'AgeEnd' AND AGE = 'AGE_EXPLORATION' THEN TRUE END) as agg_gp_12"),
            expr("COUNT(CASE WHEN EVENT_TRIGGER = 'CampaignExit' AND CAMPAIGN_EXIT_TYPE = 'AgeEnd' AND AGE = 'AGE_MODERN' THEN TRUE END) as agg_gp_13"),
            expr("COUNT(CASE WHEN EVENT_TRIGGER = 'CampaignExit' AND CAMPAIGN_EXIT_TYPE = 'AgeEnd' AND AGE = 'AGE_ATOMIC' THEN TRUE END) as agg_gp_14"),
            expr("-1::int as agg_gp_15")
        )
    )

    return out_df

# COMMAND ----------

def run_batch():
    spark = create_spark_session(name=f"{database}")
    checkpoint_location = create_fact_player_game_status_daily(spark, database, view_mapping)

    df = extract(environment)
    df = transform(df)

    load_fact_player_game_status_daily(spark, df, database, environment)

# COMMAND ----------

run_batch()
