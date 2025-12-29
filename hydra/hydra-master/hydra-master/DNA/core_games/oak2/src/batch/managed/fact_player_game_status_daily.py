# Databricks notebook source
# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/oak2/fact_player_game_status_daily_oak2

# COMMAND ----------

# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, row_number, last, lower, when, isnotnull, 
    to_timestamp, to_date, desc, asc, expr, collect_list,
    countDistinct, current_timestamp, sha2, concat_ws,
    datediff, current_date, coalesce, max as pyspark_max,
    round as pyspark_round, min as pyspark_min ,lit,struct
)
from pyspark.sql.window import Window
from pyspark.sql.types import LongType, DoubleType, IntegerType,TimestampType
from delta.tables import DeltaTable
from datetime import date
from pyspark.sql import functions as F

input_param = dbutils_input_params()
environment = input_param.get('environment', set_environment())
database = 'oak2'
#environment='_stg' # Note : stg has latest data with latest new columns added which might not present on dev
data_source = 'gearbox'
title = 'Borderlands 4'


def extract(spark, database, environment, prev_max):
    filter_expr = f"CAST(maw_time_received AS DATE) >= to_date('{prev_max}') - INTERVAL 2 DAYS"
    buildconfig_filter = "buildconfig_string = 'Shipping Game'"

    # Progression - only columns needed for skill calculations
    progression_df = spark.table(f"{database}{environment}.raw.oaktelemetry_oak_player_progression_sk") \
        .where(expr(filter_expr)) \
        .where(expr(buildconfig_filter)) \
        .where(expr("player_platform_id IS NOT NULL AND points_allocated_long > 0")) \
        .select("player_platform_id", "maw_time_received", "skill_node_id_string", 
                "skill_branch_id_string", "points_allocated_long", "player_character", "player_id")
    
    # Logins - only columns needed for days_since calculation
    logins = spark.table(f"gbx_prod.raw.logins") \
        .where(col("game_title_string") == "oak2") \
        .select("player_id_platformid", "date", "game_title_string")
    
    # Context - main data source, select only required columns and apply null filters
    context_df = spark.table(f"{database}{environment}.raw.oaktelemetry_context_data") \
        .where(expr(filter_expr)) \
        .where(expr(buildconfig_filter)) \
        .where(expr("player_primary_id_platformid IS NOT NULL")) \
        .select("player_primary_id_platformid", 
                "player_character_id_string",
                "player_primary_id_string",
                "maw_time_received", "player_exp_level_long", "player_time_played_double", 
                "player_local_index_long", "game_session_guid_string", "player_profile_difficulty_long",
                "context_class_string")
    
    # Mission - apply all downstream filters early
    mission_df = spark.table(f"{database}{environment}.raw.oaktelemetry_mission_state") \
        .where(expr(filter_expr)) \
        .where(expr(buildconfig_filter)) \
        .where(expr("player_platform_id IS NOT NULL AND event_value_string IS NOT NULL AND event_value_string <> '' and event_value_string not in ('Mission_GbxMoment_FactsLevelSequence', 'displaytestmission', 'DisplayTextMission')")) \
        .select("player_platform_id", "maw_time_received", "event_value_string", 
                "mission_set_string", "game_session_uvh_level_long", "mission_objective_id_string", "player_id")
    
    # POA - select only required columns and apply null filters
    poa_df = spark.table(f"{database}{environment}.raw.oaktelemetry_poa_state") \
        .where(expr(filter_expr)) \
        .where(expr(buildconfig_filter)) \
        .where(expr("player_platform_id IS NOT NULL")) \
        .select("player_platform_id", "maw_time_received", "event_value_string", 
                "poa_status_string", "poa_type_string", "player_id")
    
    # Boss - select only required columns and apply null filters
    boss_df = spark.table(f"{database}{environment}.raw.oaktelemetry_boss_fight") \
        .where(expr(filter_expr)) \
        .where(expr(buildconfig_filter)) \
        .where(expr("player_platform_id IS NOT NULL")) \
        .where(expr("boss_fight_name_string IS NOT NULL AND boss_fight_name_string <> ''")) \
        .select("player_platform_id", "maw_time_received", 
                "boss_fight_result_string", "boss_fight_name_string", "player_id")
    
    # Vehicle - select only required columns
    vehicle_df = spark.table(f"{database}{environment}.raw.oaktelemetry_oak_vehicle") \
        .where(expr(filter_expr)) \
        .where(expr(buildconfig_filter)) \
        .where(expr("player_platform_id IS NOT NULL")) \
        .select("player_platform_id", "maw_time_received", 
                "event_key_string", "event_guid_string", "time_since_vehicle_enter_long", "player_id")
    
    # GBX Discovery - select only required columns
    gbx_df = spark.table(f"{database}{environment}.raw.oaktelemetry_gbxdiscovery") \
        .where(expr(filter_expr)) \
        .where(expr(buildconfig_filter)) \
        .where(expr("player_platform_id IS NOT NULL")) \
        .select("player_platform_id", "maw_time_received", 
                "distance_traveled_in_vehicle_long", "distance_traveled_on_foot_long", "player_id")
    
    # FFYL - select only required columns
    ffyl_df = spark.table(f"{database}{environment}.raw.oaktelemetry_ffyl") \
        .where(expr(filter_expr)) \
        .where(expr(buildconfig_filter)) \
        .where(expr("player_platform_id IS NOT NULL")) \
        .select("player_platform_id", "maw_time_received", "event_value_string", "player_id")
    
    # Analytics - select only required columns  
    analytics_df = spark.table(f"{database}{environment}.raw.oaktelemetry_player_analytics") \
        .where(expr(filter_expr)) \
        .where(expr(buildconfig_filter)) \
        .where(expr("player_platform_id IS NOT NULL")) \
        .select("player_platform_id", "maw_time_received", "event_context", "player_id")
    
    mission_info_df = spark.table(f"{database}{environment}.reference.mission_info") \
        .where(expr("is_dlc <> true AND mission_step_num IS NOT NULL")) \
        .select("mission_name", "mission_set", "mission_type", "is_dlc", "mission_step_num", "num_missions")
    
    poa_info_df = spark.table(f"{database}{environment}.reference.poa_info") \
        .select("poa_name", "final_status")
    
    character_mission_stats_df = spark.table(f"{database}{environment}.managed.fact_player_character_mission_stats_daily") \
        .where(expr(f"dw_update_ts >= to_date('{prev_max}') - INTERVAL 5 DAYS")) \
        .select("player_platform_id", "mission_name", "mission_type", "date")
    
    platform_id_mapping_df = spark.table(f"oak2{environment}.reference.platform_id_mapping") \
        .select("maw_user_platformid", "player_platform_id")
    
    session_players = (
        spark.table(f"{database}{environment}.raw.oaktelemetry_game_session")
        .where(expr(filter_expr))
        .where(expr(buildconfig_filter))
        .withColumn("dt", expr("CAST(maw_time_received AS DATE)"))
        .withColumn(
            "session_time_mins",
            expr(
                "(player_time_played - LAG(player_time_played, 1) OVER (PARTITION BY player_id, player_character_id, game_session_guid_string "
                "ORDER BY maw_time_received, time_ms_timeoffset)) / 60.0"
            )
        )
        .withColumn(
            "session_type",
            expr("""CASE 
                WHEN game_session_num_local_players_long > 1 AND game_session_num_remote_players_long = 0 THEN 'Local Co-Op Session'
                WHEN game_session_num_local_players_long > 1 AND game_session_num_remote_players_long > 1 THEN 'Mixed Co-Op Session'
                WHEN game_session_num_local_players_long <= 1 AND game_session_num_remote_players_long > 1 THEN 'Online Co-Op Session'
                WHEN game_session_num_local_players_long <= 1 AND game_session_num_remote_players_long <= 1 THEN 'Single Player Session'
                ELSE 'ERROR - FILTER OUT' END""")
        )
        .groupBy(
            expr("player_id"),
            expr("session_type"),
            expr("dt"),
            expr("buildchangelist_long AS build_number"),
            expr("platform_string AS platform")
            ,expr("player_platform_id AS player_platform_id")
        )
        .agg(
            expr("COUNT(DISTINCT game_session_guid_string) AS num_sessions"),
            expr("SUM(session_time_mins) / 60 AS total_session_time_hrs"),
            expr("LAST(game_session_guid_string) AS game_session_guid_string")
        )
    )

    return progression_df, context_df, mission_df, mission_info_df, poa_df, poa_info_df, boss_df, vehicle_df, gbx_df, ffyl_df, analytics_df,logins,character_mission_stats_df,platform_id_mapping_df, session_players


def transform(spark, database, environment, progression_df, context_df, mission_df, mission_info_df, poa_df,
              poa_info_df, boss_df, vehicle_df, gbx_df, ffyl_df, analytics_df, logins, character_mission_stats_df,
              platform_id_mapping_df, session_players):
    # Days Since
    days_since = (
        logins.alias("logins")
        .join(
            platform_id_mapping_df.alias("plat"),
            expr("logins.player_id_platformid = plat.maw_user_platformid"),
            "inner"
        )
        .filter(col("game_title_string") == "oak2")
        .select("plat.player_platform_id", "logins.date")
        .withColumnRenamed("plat.player_platform_id", "player_platform_id")
        .withColumnRenamed("date", "date_from_logins")
    ).distinct()

    ctx = (
        context_df.alias("ctx")
        .join(
            session_players.alias("session_players"),
            expr("ctx.game_session_guid_string = session_players.game_session_guid_string") &
            expr("ctx.player_primary_id_platformid = session_players.player_platform_id"),
            "left"
        )
        .join(
            days_since.alias("days_since"),
            expr(
                "ctx.player_primary_id_platformid = days_since.player_platform_id AND CAST(ctx.maw_time_received AS DATE) = days_since.date_from_logins"),
            "left"
        )
        .groupBy(
            expr("ctx.player_primary_id_platformid AS player_platform_id"),
            coalesce(col("ctx.player_primary_id_string"), col("ctx.player_primary_id_platformid"),
                     lit("Unknown")).alias("player_id"),
            expr("CAST(ctx.maw_time_received AS DATE) AS date"),
            expr("CAST(days_since.date_from_logins AS DATE) AS date_from_logins")
        )
        .agg(
            expr("MAX(ctx.player_exp_level_long) AS max_player_level"),
            expr(
                "CAST(MAX(CASE WHEN ctx.context_class_string = 'Char_Paladin' THEN ctx.player_exp_level_long END) AS BIGINT) AS max_amon_level"),
            expr(
                "CAST(MAX(CASE WHEN ctx.context_class_string = 'Char_DarkSiren' THEN ctx.player_exp_level_long END) AS BIGINT) AS max_vex_level"),
            expr(
                "CAST(MAX(CASE WHEN ctx.context_class_string = 'Char_Gravitar' THEN ctx.player_exp_level_long END) AS BIGINT) AS max_harlowe_level"),
            expr(
                "CAST(MAX(CASE WHEN ctx.context_class_string = 'Char_ExoSoldier' THEN ctx.player_exp_level_long END) AS BIGINT) AS max_rafa_level"),
            expr("last(player_profile_difficulty_long) as player_difficulty_setting")
        )
    )

    ctx = ctx.withColumn("date_from_logins", expr("COALESCE(date_from_logins, date)"))

    ctx = ctx.withColumn(
        "date_last_seen",
        expr("LAG(date_from_logins) OVER (PARTITION BY player_platform_id ORDER BY date_from_logins)")
    ).withColumn(
        "days_since_last_seen",
        expr(
            "CASE WHEN LAG(CAST(date_from_logins AS DATE)) OVER (PARTITION BY player_platform_id ORDER BY CAST(date_from_logins AS DATE)) IS NULL "
            "THEN 0 ELSE DATEDIFF(CAST(date_from_logins AS DATE), LAG(CAST(date_from_logins AS DATE)) OVER (PARTITION BY player_platform_id ORDER BY CAST(date_from_logins AS DATE))) END"
        )
    ).withColumn(
        "days_since_install",
        expr(
            "DATEDIFF(CAST(date_from_logins AS DATE), FIRST_VALUE(CAST(date_from_logins AS DATE)) OVER (PARTITION BY player_platform_id ORDER BY CAST(date_from_logins AS DATE)))"
        )
    )
    ctx = ctx.drop('date_from_logins')

    # SUM of MAX at character level
    ctx_hours_character_agg = (
        context_df.alias("ctx")
        .join(
            session_players.alias("session_players"),
            expr("ctx.game_session_guid_string = session_players.game_session_guid_string") &
            expr("ctx.player_primary_id_platformid = session_players.player_platform_id"),
            "left"
        )
        .groupBy(
            expr("ctx.player_primary_id_platformid AS player_platform_id"),
            expr("ctx.player_character_id_string AS player_character_id"),
            expr("CAST(ctx.maw_time_received AS DATE) AS date")
        )
        .agg(
            expr("ROUND(MAX(session_players.total_session_time_hrs), 2) AS hours_played"),
            expr(
                "ROUND(MAX(CASE WHEN session_players.session_type LIKE '%Co-Op%' THEN session_players.total_session_time_hrs ELSE 0 END), 2) AS multiplayer_hours_played"),
            expr(
                "ROUND(MAX(CASE WHEN session_players.session_type = 'Online Co-Op Session' THEN session_players.total_session_time_hrs ELSE 0 END), 2) AS online_multiplayer_hours_played"),
            expr(
                "ROUND(MAX(CASE WHEN session_players.session_type = 'Local Co-Op Session' THEN session_players.total_session_time_hrs ELSE 0 END), 2) AS split_screen_hours_played")
        )
    )

    # Sum character-level hours at player level
    ctx_hours_agg = (
        ctx_hours_character_agg
        .groupBy(
            expr("player_platform_id"),
            expr("date")
        )
        .agg(
            expr("ROUND(SUM(hours_played), 2) AS hours_played"),
            expr("ROUND(SUM(multiplayer_hours_played), 2) AS multiplayer_hours_played"),
            expr("ROUND(SUM(online_multiplayer_hours_played), 2) AS online_multiplayer_hours_played"),
            expr("ROUND(SUM(split_screen_hours_played), 2) AS split_screen_hours_played")
        )
    )

    ctx = (
        ctx.alias("ctx")
        .join(
            ctx_hours_agg.alias("hours"),
            (col("ctx.player_platform_id") == col("hours.player_platform_id")) &
            (col("ctx.date") == col("hours.date")),
            "left"
        )
        .select(
            col("ctx.player_platform_id"),
            col("ctx.player_id"),
            col("ctx.date"),
            col("ctx.max_player_level"),
            col("ctx.max_amon_level"),
            col("ctx.max_vex_level"),
            col("ctx.max_harlowe_level"),
            col("ctx.max_rafa_level"),
            col("hours.hours_played"),
            col("hours.multiplayer_hours_played"),
            col("hours.online_multiplayer_hours_played"),
            col("hours.split_screen_hours_played"),
            col("ctx.player_difficulty_setting"),
            col("ctx.date_last_seen"),
            col("ctx.days_since_last_seen"),
            col("ctx.days_since_install")
        )
    )

    # player_difficulty
    filtered_df = context_df \
        .filter(expr("player_primary_id_platformid IS NOT NULL AND player_profile_difficulty_long IS NOT NULL")) \
        .withColumn("ts", expr("CAST(maw_time_received AS TIMESTAMP)"))

    agg_df = filtered_df.groupBy(
        expr("player_primary_id_platformid"),
        expr("ts")
    ).agg(
        expr("last(player_profile_difficulty_long) AS player_profile_difficulty")
    )

    window_spec = Window.partitionBy(
        expr("player_primary_id_platformid"),
        expr("CAST(ts AS DATE)")
    ).orderBy(
        expr("CAST(player_profile_difficulty AS INT) ASC"),
        expr("ts DESC")
    )

    ranked_df = agg_df.withColumn("row_num", row_number().over(window_spec))

    player_difficulty = ranked_df \
        .filter(expr("row_num = 1")) \
        .selectExpr(
        "player_primary_id_platformid AS player_platform_id",
        "CAST(ts AS DATE) AS date",
        "player_profile_difficulty AS player_difficulty_setting"
    )

    # recent_mission_df
    all_missions = mission_df.alias("mission") \
        .join(
        mission_info_df.alias("mlkp"),
        on=(
                expr("lower(mission.event_value_string) = lower(mlkp.mission_name)") &
                expr("lower(mission.mission_set_string) = lower(mlkp.mission_set)") &
                expr("""
                CASE
                    WHEN LOWER(mission.event_value_string) LIKE '%uvh%' THEN 'uvhunlock'
                    WHEN LOWER(mlkp.mission_name) LIKE '%side_micronations%' THEN 'sidemission'
                    WHEN LOWER(mission.event_value_string) LIKE '%micro%' AND LOWER(mission.event_value_string) NOT LIKE '%uvh%' THEN 'micromission'
                    WHEN LOWER(mission.event_value_string) LIKE '%main%' THEN 'mainmission'
                    WHEN LOWER(mission.event_value_string) LIKE '%side%' THEN 'sidemission'
                    WHEN LOWER(mission.event_value_string) LIKE '%contract%' THEN 'contract'
                    WHEN LOWER(mission.event_value_string) LIKE '%dynamicevent%' THEN 'dynamicevent'
                    ELSE 'uncategorized'
                END = lower(mlkp.mission_type)
            """)
        ),
        how="left"
    ) \
        .filter(
        expr("mission.player_platform_id IS NOT NULL") &
        expr("mission.event_value_string IS NOT NULL") &
        expr("mission.event_value_string <> ''") &
        expr("mlkp.is_dlc <> true") &
        expr("mlkp.mission_step_num IS NOT NULL")
    ) \
        .groupBy(
        expr("mission.player_platform_id").alias("player_platform_id"),
        expr("cast(mission.maw_time_received as timestamp)").alias("ts"),
        expr("mlkp.mission_step_num").alias("mission_step_num")
    ) \
        .agg(
        expr("last(mission.event_value_string)").alias("mission")
    ) \
        .withColumn(
        "row_num",
        expr(
            "row_number() over (partition by player_platform_id, cast(cast(ts as timestamp) as date) order by cast(ts as timestamp) desc, mission_step_num desc)")
    ) \
        .orderBy(
        expr("ts desc"),
        expr("mission_step_num desc")
    )

    recent_mission = all_missions \
        .filter(expr("row_num = 1")) \
        .select(
        expr("player_platform_id"),
        expr("cast(ts as date)").alias("date"),
        expr("mission").alias("most_recent_mission")
    ) \
        .distinct()

    # missiondf
    # agg_df
    mission = mission_df.alias("mission").groupBy(
        expr("mission.player_platform_id"),
        expr("CAST(mission.maw_time_received AS DATE) AS date")
    ).agg(
        pyspark_max(expr("mission.game_session_uvh_level_long")).alias("max_UVHM_level")
    )

    # newly added the below logic to calculate the ltd values for main story completion and side mission completion #DATADEP-4598


    int_mission = character_mission_stats_df.alias("int_mission") \
        .join(
        mission_info_df.alias("mlkp"),
        on=(
            expr("lower(int_mission.mission_name) = lower(mlkp.mission_name)")
        ),
        how="left"
    ) \
        .groupBy(
        expr("player_platform_id"),
        expr("int_mission.mission_type"),
        expr("int_mission.mission_name")
    ).agg(
        expr("MIN(date) AS first_completion_date"),
        expr("MAX(num_missions) AS num_missions")
    )

    # Calculate cumulative distinct missions per day
    window_spec_cum = Window.partitionBy("player_platform_id", "mission_type").orderBy(
        "first_completion_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

    int_mission_cumulative = int_mission.withColumn(
        "total_missions_completed",
        F.count("mission_name").over(window_spec_cum)
    ).select(
        col("player_platform_id"),
        col("mission_type"),
        col("first_completion_date").alias("date"),
        col("total_missions_completed"),
        col("num_missions")
    )

    mission_updated_df = (
        int_mission_cumulative
        .select(
            expr("player_platform_id"),
            expr("date"),
            expr('''Round(
            CASE
                WHEN lower(mission_type) = 'mainmission' THEN total_missions_completed
                ELSE null
            END
            / (num_missions), 2) AS main_story_completion'''),
            expr(''' Round(
            CASE
                WHEN lower(mission_type) = 'sidemission' THEN total_missions_completed
                ELSE null
            END
            / (num_missions),
            2
            ) AS side_mission_completion''')
        )
        .distinct()
    )

    mission_ltd = (
        mission_updated_df
        .select(
            expr("player_platform_id"),
            expr("date"),
            expr('''
             max(main_story_completion) OVER (
                PARTITION BY player_platform_id
                ORDER BY date
            ) as main_story_completion
            '''),
            expr('''
             max(side_mission_completion) OVER (
                PARTITION BY player_platform_id
                ORDER BY date
            ) as side_mission_completion
            ''')

        )
    )


    # poa_completion_df
    poa_completion_df = poa_df.alias("poa") \
        .join(
        poa_info_df.alias("plkp"),
        expr("lower(poa.event_value_string) = lower(plkp.poa_name)"),
        "left"
    ) \
        .groupBy(
        expr("poa.player_platform_id"),
        expr("CAST(poa.maw_time_received AS DATE) as date")
    ) \
        .agg(
        pyspark_round(
            expr("""
                    (
                    COUNT(DISTINCT CASE WHEN poa.poa_status_string = plkp.final_status THEN lower(poa.event_value_string) ELSE NULL END)
                    /
                    COUNT(DISTINCT lower(poa.event_value_string))
                    ) * 100
                """),
            2
        ).alias("poa_completion")
    )

    #poa ltd
    poa_ltd = (
        poa_completion_df
        .select(
            expr("player_platform_id"),
            expr("date"),
            expr("""
                MAX(poa_completion) OVER (
                    PARTITION BY player_platform_id
                    ORDER BY date
                ) as poa_completion
            """)
        )
    )

    # boss_agg_df
    boss = boss_df.withColumn("date", expr("date(cast(maw_time_received as timestamp))")) \
        .groupBy("player_platform_id", "date") \
        .agg(
        expr(
            "SUM(CASE WHEN LOWER(boss_fight_result_string) = 'success' AND boss_fight_name_string = 'Boss_IdolatorSol' THEN 1 ELSE 0 END)").alias(
            "times_beat_sol"),
        expr(
            "SUM(CASE WHEN LOWER(boss_fight_result_string) = 'success' AND boss_fight_name_string = 'Boss_VileLictor' THEN 1 ELSE 0 END)").alias(
            "times_beat_liktor"),
        expr(
            "SUM(CASE WHEN LOWER(boss_fight_result_string) = 'success' AND boss_fight_name_string = 'Boss_Callis_Fortress' THEN 1 ELSE 0 END)").alias(
            "times_beat_callis_fortress"),
        expr(
            "SUM(CASE WHEN LOWER(boss_fight_result_string) = 'success' AND boss_fight_name_string = 'Boss_Callis_Elpis' THEN 1 ELSE 0 END)").alias(
            "times_beat_callis_elpis"),
        expr(
            "SUM(CASE WHEN LOWER(boss_fight_result_string) = 'success' AND boss_fight_name_string = 'Boss_Timekeeper_Human' THEN 1 ELSE 0 END)").alias(
            "times_beat_tk_human"),
        expr(
            "SUM(CASE WHEN LOWER(boss_fight_result_string) = 'success' AND boss_fight_name_string = 'Boss_Timekeeper_Guardian' THEN 1 ELSE 0 END)").alias(
            "times_beat_tk_guardian")
    )

    # recent_boss_df
    recent_boss = (boss_df
                   .filter(
        (col("player_platform_id").isNotNull()) &
        (col("boss_fight_name_string").isNotNull()) &
        (col("boss_fight_name_string") != "")
    )
                   .withColumn("ts", col("maw_time_received").cast("timestamp"))
                   .groupBy("player_platform_id", "ts")
                   .agg(
        last("boss_fight_name_string").alias("most_recent_boss"),
        last("boss_fight_result_string").alias("most_recent_boss_outcome")
    )
                   .withColumn(
        "row_num",
        row_number().over(
            Window.partitionBy("player_platform_id", to_date("ts"))
            .orderBy(desc("ts"))
        )
    )
                   .orderBy(desc("ts"))
                   )

    recent_boss = (recent_boss
    .filter(col("row_num") == 1)
    .select(
        "player_platform_id",
        to_date("ts").alias("date"),
        "most_recent_boss",
        "most_recent_boss_outcome"
    )
    )

    # vehicle_agg_df
    vehicle = vehicle_df.groupBy(
        expr("player_platform_id"),
        expr("CAST(maw_time_received AS DATE) as date")
    ).agg(
        expr("COUNT(DISTINCT CASE WHEN event_key_string = 'Begin' THEN event_guid_string ELSE NULL END)").alias(
            "vehicle_use_count"),
        expr(
            "ROUND(SUM(CASE WHEN event_key_string = 'End' THEN time_since_vehicle_enter_long / 60 ELSE 0 END), 2)").alias(
            "minutes_in_vehicle")
    )

    # gbx_agg_df
    gbx = gbx_df.groupBy(
        expr("player_platform_id"),
        expr("CAST(maw_time_received AS DATE) as date")
    ).agg(
        expr("ROUND(SUM(distance_traveled_in_vehicle_long / 100), 2)").alias("meters_in_vehicle"),
        expr("ROUND(SUM(distance_traveled_on_foot_long / 100), 2)").alias("meters_on_foot")
    )

    # fyyl_agg_df
    ffyl = ffyl_df.groupBy(
        expr("player_platform_id"),
        expr("CAST(maw_time_received AS DATE) as date")
    ).agg(
        expr("SUM(CASE WHEN event_value_string = 'Start' THEN 1 ELSE NULL END)").alias("times_downed"),
        expr("SUM(CASE WHEN event_value_string IN ('Instant', 'TimerExpired', 'GiveUp') THEN 1 ELSE NULL END)").alias(
            "times_deaths")
    )

    # analytics_agg_df
    analytics = analytics_df.groupBy(
        expr("player_platform_id"),
        expr("CAST(maw_time_received AS DATE) as date")
    ).agg(
        expr("MAX(event_context.player_profile_difficulty_long)").alias("max_difficulty_encountered")
    )

    # Aggregated dataframes are already renamed above


    mission_join_condition = (
            (ctx["player_platform_id"] == mission["player_platform_id"]) &
            (ctx["date"] == mission["date"])
    )

    player_difficulty_join_condition = (
            (ctx["player_platform_id"] == player_difficulty["player_platform_id"]) &
            (ctx["date"] == player_difficulty["date"])
    )

    recent_mission_join_condition = (
            (ctx["player_platform_id"] == recent_mission["player_platform_id"]) &
            (ctx["date"] == recent_mission["date"])
    )

    boss_join_condition = (
            (ctx["player_platform_id"] == boss["player_platform_id"]) &
            (ctx["date"] == boss["date"])
    )

    recent_boss_join_condition = (
            (ctx["player_platform_id"] == recent_boss["player_platform_id"]) &
            (ctx["date"] == recent_boss["date"])
    )

    vehicle_join_condition = (
            (ctx["player_platform_id"] == vehicle["player_platform_id"]) &
            (ctx["date"] == vehicle["date"])
    )

    gbx_join_condition = (
            (ctx["player_platform_id"] == gbx["player_platform_id"]) &
            (ctx["date"] == gbx["date"])
    )

    ffyl_join_condition = (
            (ctx["player_platform_id"] == ffyl["player_platform_id"]) &
            (ctx["date"] == ffyl["date"])
    )

    analytics_join_condition = (
            (ctx["player_platform_id"] == analytics["player_platform_id"]) &
            (ctx["date"] == analytics["date"])
    )

    mission_ltd_join_condition = (
            (ctx["player_platform_id"] == mission_ltd["player_platform_id"]) &
            (ctx["date"] == mission_ltd["date"]))

    poa_ltd_join_condition = (
            (ctx["player_platform_id"] == poa_ltd["player_platform_id"]) &
            (ctx["date"] == poa_ltd["date"])
    )

    # Perform all the LEFT JOINs
    joined_df = ctx \
        .join(player_difficulty, player_difficulty_join_condition, "left") \
        .join(mission, mission_join_condition, "left") \
        .join(recent_mission, recent_mission_join_condition, "left") \
        .join(boss, boss_join_condition, "left") \
        .join(recent_boss, recent_boss_join_condition, "left") \
        .join(vehicle, vehicle_join_condition, "left") \
        .join(gbx, gbx_join_condition, "left") \
        .join(ffyl, ffyl_join_condition, "left") \
        .join(analytics, analytics_join_condition, "left") \
        .join(mission_ltd, mission_ltd_join_condition, "left") \
        .join(poa_ltd, poa_ltd_join_condition, "left")

    # Apply WHERE clause filters
    filtered_df = joined_df.filter(
        ctx["player_platform_id"].isNotNull() &
        ctx["date"].isNotNull()
    )

    # Define window specification for the QUALIFY clause (ROW_NUMBER)
    window_spec = Window.partitionBy(
        ctx["player_platform_id"],
        ctx["date"]
    ).orderBy(
        col("hours_played").desc(),
        col("online_multiplayer_hours_played").desc(),
        col("split_screen_hours_played").desc(),
        col("max_amon_level").desc(),
        col("max_vex_level").desc(),
        col("max_harlowe_level").desc(),
        col("max_rafa_level").desc()
    )

    # Add row number and filter to get only row_number = 1
    windowed_df = filtered_df.withColumn("row_num", row_number().over(window_spec))
    windowed_df = windowed_df.filter(col("row_num") == 1)

    fill_window = Window.partitionBy(ctx["player_platform_id"]).orderBy(ctx["date"]).rowsBetween(
        Window.unboundedPreceding, Window.currentRow)

    windowed_df = windowed_df.withColumn(
        "main_story_filled",
        last(col("main_story_completion"), ignorenulls=True).over(fill_window)
    ).withColumn(
        "side_mission_filled",
        last(col("side_mission_completion"), ignorenulls=True).over(fill_window)
    ).withColumn(
        "poa_filled",
        last(col("poa_completion"), ignorenulls=True).over(fill_window)
    )

    final_df = windowed_df.select(
        ctx["player_platform_id"],
        coalesce(ctx["player_id"], ctx["player_platform_id"], lit("Unknown")).alias("player_id"),
        ctx["date"],
        ctx["max_player_level"],
        mission["max_UVHM_level"],
        recent_mission["most_recent_mission"],
        when(col("main_story_filled") > 100, lit(100))
        .otherwise(coalesce(col("main_story_filled"), lit(0)))
        .alias("main_story_completion"),
        when(col("side_mission_filled") > 100, lit(100))
        .otherwise(coalesce(col("side_mission_filled"), lit(0)))
        .alias("side_mission_completion"),
        coalesce(ctx["hours_played"], lit(0)).alias("hours_played"),
        coalesce(ctx["multiplayer_hours_played"], lit(0)).alias("multiplayer_hours_played"),
        coalesce(ctx["online_multiplayer_hours_played"], lit(0)).alias("online_multiplayer_hours_played"),
        coalesce(ctx["split_screen_hours_played"], lit(0)).alias("split_screen_hours_played"),
        when(col("poa_filled") > 100, lit(100))
        .otherwise(coalesce(col("poa_filled"), lit(0)))
        .alias("poa_completion"),
        coalesce(boss["times_beat_sol"], lit(0)).alias("times_beat_sol"),
        coalesce(boss["times_beat_liktor"], lit(0)).alias("times_beat_liktor"),
        coalesce(boss["times_beat_callis_fortress"], lit(0)).alias("times_beat_callis_fortress"),
        coalesce(boss["times_beat_callis_elpis"], lit(0)).alias("times_beat_callis_elpis"),
        coalesce(boss["times_beat_tk_human"], lit(0)).alias("times_beat_tk_human"),
        coalesce(boss["times_beat_tk_guardian"], lit(0)).alias("times_beat_tk_guardian"),
        recent_boss["most_recent_boss"],
        recent_boss["most_recent_boss_outcome"],
        coalesce(vehicle["vehicle_use_count"], lit(0)).alias("vehicle_use_count"),
        coalesce(vehicle["minutes_in_vehicle"], lit(0)).alias("minutes_in_vehicle"),
        when(coalesce(gbx["meters_in_vehicle"], lit(0)) < 0, lit(0)).otherwise(
            coalesce(gbx["meters_in_vehicle"], lit(0))).alias("meters_in_vehicle"),
        when(coalesce(gbx["meters_on_foot"], lit(0)) < 0, lit(0)).otherwise(
            coalesce(gbx["meters_on_foot"], lit(0))).alias("meters_on_foot"),
        coalesce(ffyl["times_downed"], lit(0)).alias("times_downed"),
        coalesce(ffyl["times_deaths"], lit(0)).alias("times_deaths"),
        ctx["date_last_seen"],
        coalesce(ctx["days_since_last_seen"], lit(0)).alias("days_since_last_seen"),
        coalesce(ctx["days_since_install"], lit(0)).alias("days_since_install"),
        analytics["max_difficulty_encountered"],
        player_difficulty["player_difficulty_setting"],
        ctx["max_amon_level"],
        ctx["max_vex_level"],
        ctx["max_harlowe_level"],
        ctx["max_rafa_level"]).distinct()

    if "row_num" in final_df.columns:
        final_df = final_df.drop("row_num")

    final_df = final_df.withColumn("dw_update_ts", expr("current_timestamp()")) \
        .withColumn("dw_insert_ts", expr("current_timestamp()"))
    final_df = final_df.withColumn(
        "merge_key",
        expr("sha2(concat_ws('|', CAST(date AS STRING),player_id, player_platform_id), 256)")
    )

    return final_df

# COMMAND ----------

def load(df, database, environment, spark):
    # Reference to the target Delta table
    target_df = DeltaTable.forName(spark, f"{database}{environment}.managed.fact_player_game_status_daily")

    # Merge condition
    merger_condition = 'target.merge_key = source.merge_key'

    # Define field categories for update logic
    no_update_fields = ['dw_insert_ts', 'merge_key', 'player_platform_id', 'player_id', 'date']
    
    # Fields that should use least() for earliest timestamps
    least_fields = ['received_on', 'first_session_start_on']
    
    # Fields that should use source values (most recent data)
    source_update_fields = [
        'most_recent_mission', 'most_recent_boss', 'most_recent_boss_outcome',
        'days_since_last_seen'
    ]
    
    # Fields that should use coalesce() to fill nulls
    coalesce_fields = ['player_difficulty_setting']
    
    # Build update expression
    update_expr = {}
    
    # Apply specific update logic for categorized fields
    for field in least_fields:
        if field in df.columns:
            update_expr[field] = f"least(coalesce(target.{field}, source.{field}), coalesce(source.{field}, target.{field}))"
    
    for field in source_update_fields:
        if field in df.columns:
            update_expr[field] = f"source.{field}"
    
    for field in coalesce_fields:
        if field in df.columns:
            update_expr[field] = f"coalesce(source.{field}, target.{field})"
    
    # Default to greatest() for all other fields (cumulative metrics)
    for field in df.columns:
        if field not in no_update_fields and field not in update_expr:
            update_expr[field] = f"greatest(target.{field}, source.{field})"
    
    # Always update timestamp
    update_expr["dw_update_ts"] = "current_timestamp()"

    insert_expr = {col: f"source.{col}" for col in df.columns}

    (target_df.alias("target")
     .merge(df.alias("source"), merger_condition)
     .whenMatchedUpdate(set=update_expr)
     .whenNotMatchedInsert(values=insert_expr)
     .execute())

# COMMAND ----------

def run_batch(database, environment):
    database = database.lower()

    # Setting the spark session
    spark = SparkSession.builder.appName(f"{database}").getOrCreate()
    
    # Creating the table structure
    fact_player_game_status_daily(spark, database, environment)
    fact_player_game_status_daily_view(spark, database, environment)

    print('Extracting the data')

    prev_max = max_timestamp(spark, f"{database}{environment}.managed.fact_player_game_status_daily", 'date')
    if prev_max is None:
        prev_max = date(1999, 1, 1)
    print(prev_max)

    #extract
    progression_df, context_df, mission_df, mission_info_df, poa_df, poa_info_df, boss_df, vehicle_df, gbx_df, ffyl_df, analytics_df,logins, character_mission_stats_df, platform_id_mapping_df, session_players = extract(spark, database, environment, prev_max)
    #transform
    df=transform(spark, database, environment,progression_df, context_df, mission_df, mission_info_df, poa_df, poa_info_df, boss_df, vehicle_df, gbx_df, ffyl_df, analytics_df,logins, character_mission_stats_df, platform_id_mapping_df, session_players)

    print('Merge Data')
    load(df, database, environment, spark)

# COMMAND ----------

run_batch(database, environment)
