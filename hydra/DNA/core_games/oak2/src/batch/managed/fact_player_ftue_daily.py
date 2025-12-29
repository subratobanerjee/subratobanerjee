# Databricks notebook source
# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/oak2/fact_player_ftue_daily

# COMMAND ----------

# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

from pyspark.sql.functions import sha2, concat_ws, expr
from delta.tables import DeltaTable
from datetime import date

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment', set_environment())
database = 'oak2'
# environment='_stg'
data_source = 'gearbox'
title = 'Borderlands 4'


# COMMAND ----------

def extract(spark, database, environment, prev_max):
    """
    Loads all necessary source tables from the data warehouse.
    """
    buildconfig_filter_expr = "buildconfig_string = 'Shipping Game'"
    date_filter = f"to_date(maw_time_received) >= to_date('{prev_max}') - interval 2 days"

    fact_player_ftue_steps = spark.table(f"{database}{environment}.managed.fact_player_ftue_step")
    fact_player_character = spark.table(f"{database}{environment}.managed.fact_player_character")
    oaktelemetry_gadget = spark.table(f"{database}{environment}.raw.oaktelemetry_gadget").where(
        expr(buildconfig_filter_expr)).where(expr(date_filter))
    oaktelemetry_health = spark.table(f"{database}{environment}.raw.oaktelemetry_health").where(
        expr(buildconfig_filter_expr)).where(expr(date_filter))
    oaktelemetry_context_data = spark.table(f"{database}{environment}.raw.oaktelemetry_context_data").where(
        expr(buildconfig_filter_expr)).where(expr(date_filter))
    oak_player_progression_sk = spark.table(
        f"{database}{environment}.raw.oaktelemetry_oak_player_progression_sk").where(
        expr(buildconfig_filter_expr)).where(expr(date_filter))
    oak_vehicle = spark.table(f"{database}{environment}.raw.oaktelemetry_oak_vehicle").where(
        expr(buildconfig_filter_expr)).where(expr(date_filter))
    oak_player_analytics = spark.table(f"{database}{environment}.raw.oaktelemetry_player_analytics").where(
        expr(buildconfig_filter_expr)).where(expr(date_filter))
    skill_tree_info = spark.table(f"{database}{environment}.reference.skill_tree_info")  # newly added

    return (
        fact_player_ftue_steps,
        fact_player_character,
        oaktelemetry_gadget,
        oaktelemetry_health,
        oaktelemetry_context_data,
        oak_player_progression_sk,
        oak_vehicle,
        oak_player_analytics,
        skill_tree_info
    )


# COMMAND ----------

def transformation_ftue_steps(ftue_steps_df, fact_player_char_df):
    first_platform = (
        ftue_steps_df
        .filter(expr("player_character <> 'OakCharacter' AND platform IS NOT NULL AND service IS NOT NULL"))
        .selectExpr(
            "coalesce(player_id,player_platform_id) as player_id",
            "player_platform_id",
            "player_character_id",
            "first_value(platform, true) over (partition by coalesce(player_id,player_platform_id), player_platform_id, player_character_id order by complete_ts rows between unbounded preceding and unbounded following) as platform",
            "first_value(service, true) over (partition by coalesce(player_id,player_platform_id), player_platform_id, player_character_id order by complete_ts rows between unbounded preceding and unbounded following) as service",
            "first_value(player_character, true) over (partition by coalesce(player_id,player_platform_id), player_platform_id, player_character_id order by complete_ts rows between unbounded preceding and unbounded following) as player_character"
        )
        .dropDuplicates(["player_id", "player_platform_id", "player_character_id"])  # one row per key
    )

    # Create cte_prison
    cte_prison = (
        ftue_steps_df
        .filter(expr("mission_name = 'mission_main_prisonprologue'"))
        .groupBy("player_character_id", "mission_name")
        .agg(
            expr("max(starting_time_played)").alias("max_prison_start_time"),
            expr("max(ending_time_played)").alias("max_prison_end_time"),
            expr("CASE WHEN max(ending_time_played) IS NULL THEN FALSE ELSE TRUE END").alias("is_complete_prison")
        )
    )

    # Create cte_beach
    cte_beach = (
        ftue_steps_df
        .filter(expr("mission_name = 'mission_main_beach'"))
        .groupBy("player_character_id", "mission_name")
        .agg(
            expr("max(starting_time_played)").alias("max_beach_start_time"),
            expr("max(ending_time_played)").alias("max_beach_end_time"),
            expr("CASE WHEN max(ending_time_played) IS NULL THEN FALSE ELSE TRUE END").alias("is_complete_beach")
        )
    )

    # Step 4: Final join of cte_prison and cte_beach
    play_cte = (
        cte_prison.alias("cp")
        .join(
            cte_beach.alias("cb"),
            expr("cp.player_character_id = cb.player_character_id")
        )
        .select(
            expr("cp.player_character_id"),
            expr("ROUND(COALESCE(cp.max_prison_end_time, cp.max_prison_start_time, 0) / 60.0, 2)").alias(
                "minutes_in_prison"),
            expr("ROUND(COALESCE(cb.max_beach_end_time, cb.max_beach_start_time, 0) / 60.0, 2)").alias(
                "minutes_in_prison_and_beach"),
            expr(
                "ROUND((COALESCE(cb.max_beach_end_time, cb.max_beach_start_time, 0) - COALESCE(cp.max_prison_end_time, cp.max_prison_start_time, 0)) / 60.0, 2)").alias(
                "minutes_in_beach"),
            expr("cp.is_complete_prison"),
            expr("cb.is_complete_beach")
        )
    )

    ftue_steps = (
        ftue_steps_df.alias("ftue")
        .filter(expr("ftue.player_character <> 'OakCharacter'"))
        .join(
            fact_player_char_df.alias("fpc"),
            expr(
                "ftue.player_platform_id = fpc.player_platform_id AND ftue.player_character_id = fpc.player_character_id"),
            "left"
        )

        .join(
            first_platform.alias("fp"),
            ["player_id", "player_platform_id", "player_character_id"],
            "left"
        )
        .join(
            play_cte.alias("pc"),
            on="player_character_id",
            how="left"
        )
        .groupBy(
            expr("coalesce(ftue.player_id,ftue.player_platform_id) as player_id"),
            "ftue.player_platform_id",
            "ftue.player_character_id",
            "fp.player_character",
            "fp.platform",
            "fp.service"
        )
        .agg(
            expr("max(pc.minutes_in_prison)").alias("minutes_in_prison"),
            expr("max(pc.minutes_in_prison_and_beach)").alias("minutes_in_prison_and_beach"),
            expr("max(pc.minutes_in_beach)").alias("minutes_in_beach"),
            expr("max(pc.is_complete_prison)").alias("is_complete_prison"),
            expr("max(pc.is_complete_beach)").alias("is_complete_beach"),
            expr("min(fpc.player_character_num)").alias("player_character_num"),
            expr("min(fpc.player_character_create_ts)").alias("player_character_create_ts"),
            expr(
                "max(case when ftue.mission_name = 'mission_main_prisonprologue' then ftue.avg_local_players end)").alias(
                "local_player_count_prison"),
            expr(
                "max(case when ftue.mission_name = 'mission_main_prisonprologue' then ftue.avg_online_players end)").alias(
                "online_player_count_prison"),
            expr(
                "max(case when ftue.mission_name = 'mission_main_prisonprologue' then ftue.max_session_players end)").alias(
                "total_player_count_prison"),
            expr("max(case when ftue.mission_name = 'mission_main_beach' then ftue.avg_local_players end)").alias(
                "local_player_count_beach"),
            expr("max(case when ftue.mission_name = 'mission_main_beach' then ftue.avg_online_players end)").alias(
                "online_player_count_beach"),
            expr("max(case when ftue.mission_name = 'mission_main_beach' then ftue.max_session_players end)").alias(
                "total_player_count_beach"),
            expr(
                "max(case when ftue.mission_name = 'mission_main_prisonprologue' and ftue.mission_objective_id in ('none', 'revivearjay') and ftue.mission_step_completed then true else false end)").alias(
                "completed_intro_prison"),
            expr(
                "round(max(case when ftue.mission_name = 'mission_main_prisonprologue' and ftue.mission_objective_id in ('none', 'revivearjay') then (ftue.ending_time_played - ftue.starting_time_played) / 60 else 0 end), 2)").alias(
                "minutes_complete_intro_prison"),
            # Fixed days_complete_intro_prison to match view logic - use date range between player_character_create_ts and prison_complete_ts
            expr(
                "count(distinct case when ftue.complete_ts between fpc.player_character_create_ts and fpc.prison_complete_ts then cast(ftue.complete_ts as date) end)").alias(
                "days_complete_intro_prison"),
            expr(
                "count(distinct case when ftue.mission_name = 'mission_main_prisonprologue' then ftue.completed_session_id end)").alias(
                "sessions_complete_intro_prison"),
            expr(
                "max(case when ftue.mission_name = 'mission_main_beach' and ftue.mission_objective_id in ('none', 'start_broadcast') and ftue.mission_step_completed then true else false end)").alias(
                "completed_beach"),
            expr(
                "round(max(case when ftue.mission_name = 'mission_main_beach' and ftue.mission_objective_id in ('none', 'start_broadcast') then (ftue.ending_time_played - ftue.starting_time_played) / 60 else 0 end), 2)").alias(
                "minutes_complete_beach"),
            # Fixed days_complete_beach to match view logic - use date range between player_character_create_ts and beach_complete_ts
            expr(
                "count(distinct case when ftue.complete_ts between fpc.player_character_create_ts and fpc.beach_complete_ts then cast(ftue.complete_ts as date) end)").alias(
                "days_complete_beach"),
            expr(
                "count(distinct case when ftue.mission_name = 'mission_main_beach' then ftue.completed_session_id end)").alias(
                "sessions_complete_beach"),
            expr(
                "max(case when ftue.mission_name = 'first_silo' and ftue.mission_step_completed then true else false end)").alias(
                "completed_first_silo"),
            expr(
                "max(case when ftue.mission_name = 'first_silo' then (ftue.ending_time_played - fpc.beach_complete_time_played) / 60 end)").alias(
                "minutes_claim_first_silo"),
            # Fixed days_claim_first_silo to match view logic - use date range between player_character_create_ts and silo_claim_ts
            expr(
                "count(distinct case when ftue.complete_ts between fpc.player_character_create_ts and fpc.silo_claim_ts then cast(ftue.complete_ts as date) end)").alias(
                "days_claim_first_silo"),
            # Rename to match view (sessions_claim_first_silo)
            expr("count(distinct case when ftue.mission_name = 'first_silo' then ftue.completed_session_id end)").alias(
                "sessions_claim_first_silo"),
            expr(
                "max(case when ftue.mission_objective_id = 'scan_vehicle' and ftue.mission_step_completed then true else false end)").alias(
                "completed_unlock_vehicle"),
            expr(
                "max(case when ftue.mission_objective_id = 'scan_vehicle' and ftue.mission_step_completed then (ftue.ending_time_played - fpc.beach_complete_time_played) / 60 end)").alias(
                "minutes_unlock_vehicle"),
            # Fixed days_unlock_vehicle to match view logic - use date range between player_character_create_ts and vehicle_scan_ts
            expr(
                "count(distinct case when ftue.complete_ts between fpc.player_character_create_ts and fpc.vehicle_scan_ts then cast(ftue.complete_ts as date) end)").alias(
                "days_unlock_vehicle"),
            expr(
                "count(distinct case when ftue.mission_objective_id = 'scan_vehicle' then ftue.completed_session_id end)").alias(
                "sessions_unlock_vehicle"),
            # --- FTUE 4 columns ---
            expr("""
                CASE
                    WHEN
                        max(case when ftue.mission_name = 'mission_main_prisonprologue' and ftue.mission_objective_id = 'none' and ftue.mission_step_completed then 1 end) = 1
                        AND max(case when ftue.mission_name = 'mission_main_beach' and ftue.mission_objective_id = 'none' and ftue.mission_step_completed then 1 end) = 1
                        AND max(case when ftue.mission_name = 'first_silo' and ftue.mission_step_completed then 1 end) = 1
                        AND max(case when ftue.mission_objective_id = 'scan_vehicle' and ftue.mission_step_completed then 1 end) = 1
                    THEN true
                    ELSE null
                END
            """).alias("completed_FTUE"),
            expr("""
                CASE
                    WHEN
                        max(case when ftue.mission_name = 'mission_main_prisonprologue' and ftue.mission_objective_id = 'none' and ftue.mission_step_completed then 1 end) = 1
                        AND max(case when ftue.mission_name = 'mission_main_beach' and ftue.mission_objective_id = 'none' and ftue.mission_step_completed then 1 end) = 1
                        AND max(case when ftue.mission_name = 'first_silo' and ftue.mission_step_completed then 1 end) = 1
                        AND max(case when ftue.mission_objective_id = 'scan_vehicle' and ftue.mission_step_completed then 1 end) = 1
                        AND (
                            greatest(
                                max(case when ftue.mission_name = 'mission_main_prisonprologue' and ftue.mission_objective_id = 'none' and ftue.mission_step_completed then ftue.ending_time_played end),
                                max(case when ftue.mission_name = 'mission_main_beach' and ftue.mission_objective_id = 'none' and ftue.mission_step_completed then ftue.ending_time_played end),
                                max(case when ftue.mission_name = 'first_silo' and ftue.mission_step_completed then ftue.ending_time_played end),
                                max(case when ftue.mission_objective_id = 'scan_vehicle' and ftue.mission_step_completed then ftue.ending_time_played end)
                            ) - min(ftue.starting_time_played)
                        ) > 0
                    THEN round(
                        (
                            greatest(
                                max(case when ftue.mission_name = 'mission_main_prisonprologue' and ftue.mission_objective_id = 'none' and ftue.mission_step_completed then ftue.ending_time_played end),
                                max(case when ftue.mission_name = 'mission_main_beach' and ftue.mission_objective_id = 'none' and ftue.mission_step_completed then ftue.ending_time_played end),
                                max(case when ftue.mission_name = 'first_silo' and ftue.mission_step_completed then ftue.ending_time_played end),
                                max(case when ftue.mission_objective_id = 'scan_vehicle' and ftue.mission_step_completed then ftue.ending_time_played end)
                            ) - min(ftue.starting_time_played)
                        ) / 60, 2)
                    ELSE null
                END
            """).alias("minutes_complete_FTUE"),
            expr("""
                CASE
                    WHEN
                        max(case when ftue.mission_name = 'mission_main_prisonprologue' and ftue.mission_objective_id = 'none' and ftue.mission_step_completed then 1 end) = 1
                        AND max(case when ftue.mission_name = 'mission_main_beach' and ftue.mission_objective_id = 'none' and ftue.mission_step_completed then 1 end) = 1
                        AND max(case when ftue.mission_name = 'first_silo' and ftue.mission_step_completed then 1 end) = 1
                        AND max(case when ftue.mission_objective_id = 'scan_vehicle' and ftue.mission_step_completed then 1 end) = 1
                    THEN count(distinct ftue.completed_session_id)
                    ELSE null
                END
            """).alias("sessions_complete_FTUE"),
            expr("""
                CASE
                    WHEN
                        max(case when ftue.mission_name = 'mission_main_prisonprologue' and ftue.mission_objective_id = 'none' and ftue.mission_step_completed then 1 end) = 1
                        AND max(case when ftue.mission_name = 'mission_main_beach' and ftue.mission_objective_id = 'none' and ftue.mission_step_completed then 1 end) = 1
                        AND max(case when ftue.mission_name = 'first_silo' and ftue.mission_step_completed then 1 end) = 1
                        AND max(case when ftue.mission_objective_id = 'scan_vehicle' and ftue.mission_step_completed then 1 end) = 1
                    THEN count(distinct cast(ftue.complete_ts as date))
                    ELSE null
                END
            """).alias("days_complete_FTUE")
        )
    )
    return ftue_steps


# COMMAND ----------

def transformation_gadgets(gadget_df):
    gadgets = (
        gadget_df
        .where(
            expr(
                "player_platform_id is not null and player_character_id is not null and lower(event_context.active_tracked_mission_string) = 'mission_main_beach'")
        )
        .groupBy(expr("coalesce(player_id,player_platform_id) as player_id"),"player_platform_id", "player_character_id")
        .agg(
            expr("max(case when lower(event_key_string) = 'throw_grenade' then true else false end)").alias(
                "grenade_used_beach"),
            expr("max(case when lower(event_key_string) = 'heavy_weapon' then true else false end)").alias(
                "heavy_weapon_used_beach")
        )
    )
    return gadgets


# COMMAND ----------

def transformation_health(health_df):
    health = (
        health_df
        .where(
            expr(
                "player_platform_id is not null and player_character_id is not null and lower(event_context.active_tracked_mission_string) in ('mission_main_beach', 'mission_main_prisonprologue')")
        )
        .groupBy(expr("coalesce(player_id,player_platform_id) as player_id"), "player_platform_id", "player_character_id")
        .agg(
            expr(
                "count(case when lower(event_key_string) = 'start_repair' and lower(event_context.active_tracked_mission_string) = 'mission_main_prisonprologue' then true end)").alias(
                "repkit_uses_prison"),
            expr(
                "count(case when lower(event_key_string) = 'start_repair' and lower(event_context.active_tracked_mission_string) = 'mission_main_beach' then true end)").alias(
                "repkit_uses_beach")
        )
    )
    return health


# COMMAND ----------

def transformation_context(context_df, fact_player_char_df):
    context = (
        context_df.alias("cd")
        .join(
            fact_player_char_df.alias("fpc"),
            expr(
                "cd.player_primary_id_platformid = fpc.player_platform_id AND cd.player_character_id_string = fpc.player_character_id"),
            "left"
        )
        .where(
            expr(
                "cd.maw_time_received <= coalesce(fpc.vehicle_scan_ts, cast('9999-12-31' as timestamp)) and cd.player_primary_id_platformid is not null and cd.player_character_id_string is not null")
        )
        .groupBy(expr("coalesce(cd.player_primary_id_string,cd.player_primary_id_platformid) as player_primary_id_string")  , "cd.player_primary_id_platformid", "cd.player_character_id_string")
        .agg(
            expr(
                "count(case when cd.event_name_string = 'FFYL' and cd.context_key_string = 'this' and cd.event_key_string = 'Stop' and lower(cd.active_tracked_mission_string) = 'mission_main_prisonprologue' then true end)").alias(
                "downs_prison"),
            expr(
                "count(case when cd.event_name_string = 'FFYL' and cd.context_key_string = 'this' and cd.event_value_string in ('Instant', 'TimerExpired', 'GiveUp') and lower(cd.active_tracked_mission_string) = 'mission_main_prisonprologue' then true end)").alias(
                "deaths_prison"),
            expr(
                "count(case when cd.event_name_string = 'FFYL' and cd.context_key_string = 'this' and cd.event_key_string = 'Stop' and lower(cd.active_tracked_mission_string) = 'mission_main_beach' then true end)").alias(
                "downs_beach"),
            expr(
                "count(case when cd.event_name_string = 'FFYL' and cd.context_key_string = 'this' and cd.event_value_string in ('Instant', 'TimerExpired', 'GiveUp') and lower(cd.active_tracked_mission_string) = 'mission_main_beach' then true end)").alias(
                "deaths_beach"),
            expr(
                "max(case when cd.location_x_double < -210000 and lower(cd.active_tracked_mission_string) = 'mission_main_beach' then true else false end)").alias(
                "go_west_on_beach_to_camp")
        )
    )
    return context


# COMMAND ----------


def transformation_skill_trunk(skill_trunk_df, fact_player_char_df, skill_tree_info_df):

    skill_trunk = (
        skill_trunk_df.alias("psk")
        .join(
            fact_player_char_df.alias("fpc"),
            expr(
                "psk.player_platform_id = fpc.player_platform_id AND psk.player_character_id = fpc.player_character_id AND from_unixtime(psk.timestamp_timestamp) between fpc.player_character_create_ts and fpc.beach_complete_ts"),
            "left"
        )

        .join(
            skill_tree_info_df.alias("sti"),
            expr("psk.skill_tree_id_string = sti.skill_tree_id_string"),
            "left"
        )
        .where(
            expr(
                "psk.player_character_id is not null and psk.player_platform_id is not null")
        )
        .groupBy("psk.player_character_id", "psk.player_platform_id", expr("coalesce(psk.player_id,psk.player_platform_id) as player_id"))
        .agg(

            expr("max_by(sti.skill_tree_name, psk.player_time_played)").alias("vault_hunter_main_trunk")
        )
    )
    return skill_trunk


# COMMAND ----------

def transformation_vehicle_usage(vehicle_df, fact_player_char_df):
    vehicle_usage = (
        vehicle_df.alias("v")
        .join(
            fact_player_char_df.alias("fpc"),
            expr("v.player_platform_id = fpc.player_platform_id AND v.player_character_id = fpc.player_character_id"),
            "left"
        )
        .where(
            expr("v.player_platform_id is not null and v.player_character_id is not null ")
        )
        .groupBy(expr("coalesce(v.player_id,v.player_platform_id) as player_id"), "v.player_platform_id", "v.player_character_id")
        .agg(
            expr(
                "max(case when cast(v.maw_time_received as date) = cast(fpc.vehicle_scan_ts as date) then true else false end)").alias(
                "used_vehicle_same_day")
        )
    )
    return vehicle_usage


def transformation_action_usage(oak_player_analytics):
    action_usage = (
        oak_player_analytics
        .filter(
            (expr("cast(aggregates as string) ILIKE '%DmgSrc_Skill_ActionSkill_%'")) &
            (expr("lower(event_context.active_tracked_mission_string) = 'mission_main_beach'")) &
            (expr("player_id is not null")) &
            (expr("player_platform_id is not null")) &
            (expr("player_character_id is not null"))
        )
        .select(
            expr("coalesce(player_id,player_platform_id) as player_id"),
            expr("player_platform_id"),
            expr("player_character_id"),
            expr("true as used_action_skill")
        )
        .groupBy("player_id", "player_platform_id", "player_character_id")
        .agg(expr("max(used_action_skill) as used_action_skill"))
    )
    return action_usage


# COMMAND ----------

def load(df, database, environment, spark):
    target_table_name = f"{database}{environment}.managed.fact_player_ftue_daily"

    create_fact_player_ftue_daily(spark, database, environment)
    create_fact_player_ftue_daily_view(spark, database)

    target_df = DeltaTable.forName(spark, target_table_name)

    merger_condition = 'target.merge_key = source.merge_key'

    # The template's dynamic update logic can be used here.
    # For simplicity, this example uses a static update statement.
    update_expr = {col: f"source.{col}" for col in df.columns if col not in ['dw_insert_ts', 'merge_key']}
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
    spark = create_spark_session(name=f"{database}")

    prev_max = max_timestamp(spark, f"{database}{environment}.managed.fact_player_ftue_daily", 'dw_insert_ts')
    if prev_max is None:
        prev_max = date(1999, 1, 1)
    print(prev_max)

    # 1. Extract

    (fact_player_ftue_steps, fact_player_character, oaktelemetry_gadget,
     oaktelemetry_health, oaktelemetry_context_data, oak_player_progression_sk,
     oak_vehicle, oak_player_analytics, skill_tree_info) = extract(spark, database, environment, prev_max)

    # 2. Transformations

    ftue_steps_agg_df = transformation_ftue_steps(fact_player_ftue_steps, fact_player_character)
    gadgets_df = transformation_gadgets(oaktelemetry_gadget)
    health_df = transformation_health(oaktelemetry_health)
    context_df = transformation_context(oaktelemetry_context_data, fact_player_character)
    skill_trunk_df = transformation_skill_trunk(oak_player_progression_sk, fact_player_character, skill_tree_info)
    vehicle_usage_df = transformation_vehicle_usage(oak_vehicle, fact_player_character)
    action_usage_df = transformation_action_usage(oak_player_analytics)

    # 3. Final Join
    result_df = (
        ftue_steps_agg_df.alias("ftue_steps")
        .join(gadgets_df.alias("gadgets"), ["player_id", "player_platform_id", "player_character_id"], "left")
        .join(health_df.alias("health"), ["player_id", "player_platform_id", "player_character_id"], "left")
        .join(context_df.alias("context"),
              expr(
                  "ftue_steps.player_id = ifnull(context.player_primary_id_string,context.player_primary_id_platformid) AND ftue_steps.player_platform_id = context.player_primary_id_platformid AND ftue_steps.player_character_id = context.player_character_id_string"),
              "left")
        .join(skill_trunk_df.alias("skill_trunk"), ["player_id", "player_platform_id", "player_character_id"], "left")
        .join(vehicle_usage_df.alias("vehicle_usage"), ["player_id", "player_platform_id", "player_character_id"],
              "left")
        .join(action_usage_df.alias("player_analytics"), ["player_id", "player_platform_id", "player_character_id"],
              "left")
        .groupBy(
            expr("coalesce(ftue_steps.player_id, ftue_steps.player_platform_id) as player_id"),
            expr("ftue_steps.player_platform_id"),
            expr("ftue_steps.player_character_id"),
            expr("ftue_steps.player_character"),
            expr("ftue_steps.platform"),
            expr("ftue_steps.service")
        )
        .agg(
            expr("max(ftue_steps.minutes_in_prison)").alias("minutes_in_prison"),
            expr("max(ftue_steps.minutes_in_prison_and_beach)").alias("minutes_in_prison_and_beach"),
            expr("max(ftue_steps.minutes_in_beach)").alias("minutes_in_beach"),
            expr("max(ftue_steps.is_complete_prison)").alias("is_complete_prison"),
            expr("max(ftue_steps.is_complete_beach)").alias("is_complete_beach"),
            expr("min(ftue_steps.player_character_num)").alias("player_character_num"),
            expr("min(ftue_steps.player_character_create_ts)").alias("player_character_create_ts"),
            expr("max(ftue_steps.local_player_count_prison)").alias("local_player_count_prison"),
            expr("max(ftue_steps.online_player_count_prison)").alias("online_player_count_prison"),
            expr("max(ftue_steps.total_player_count_prison)").alias("total_player_count_prison"),
            expr("max(ftue_steps.local_player_count_beach)").alias("local_player_count_beach"),
            expr("max(ftue_steps.online_player_count_beach)").alias("online_player_count_beach"),
            expr("max(ftue_steps.total_player_count_beach)").alias("total_player_count_beach"),
            expr("max(ftue_steps.completed_intro_prison)").alias("completed_intro_prison"),
            expr("max(ftue_steps.minutes_complete_intro_prison)").alias("minutes_complete_intro_prison"),
            expr("max(ftue_steps.days_complete_intro_prison)").alias("days_complete_intro_prison"),
            expr("max(ftue_steps.sessions_complete_intro_prison)").alias("sessions_complete_intro_prison"),
            expr("max(ftue_steps.completed_beach)").alias("completed_beach"),
            expr("max(ftue_steps.minutes_complete_beach)").alias("minutes_complete_beach"),
            expr("max(ftue_steps.days_complete_beach)").alias("days_complete_beach"),
            expr("max(ftue_steps.sessions_complete_beach)").alias("sessions_complete_beach"),
            expr("max(ftue_steps.completed_first_silo)").alias("completed_first_silo"),
            expr("max(ftue_steps.minutes_claim_first_silo)").alias("minutes_claim_first_silo"),
            expr("max(ftue_steps.days_claim_first_silo)").alias("days_claim_first_silo"),
            expr("max(ftue_steps.sessions_claim_first_silo)").alias("sessions_claim_first_silo"),
            expr("max(ftue_steps.completed_unlock_vehicle)").alias("completed_unlock_vehicle"),
            expr("max(ftue_steps.minutes_unlock_vehicle)").alias("minutes_unlock_vehicle"),
            expr("max(ftue_steps.days_unlock_vehicle)").alias("days_unlock_vehicle"),
            expr("max(ftue_steps.sessions_unlock_vehicle)").alias("sessions_unlock_vehicle"),
            expr("max(coalesce(gadgets.grenade_used_beach, false))").alias("grenade_used_beach"),
            expr("max(coalesce(gadgets.heavy_weapon_used_beach, false))").alias("heavy_weapon_used_beach"),
            expr("max(coalesce(player_analytics.used_action_skill, false))").alias("action_skill_used_beach"),
            expr("max(coalesce(health.repkit_uses_beach, 0))").alias("repkit_uses_beach"),
            expr("max(coalesce(health.repkit_uses_prison, 0))").alias("repkit_uses_prison"),
            expr("max(coalesce(context.downs_prison, 0))").alias("downs_prison"),
            expr("max(coalesce(context.deaths_prison, 0))").alias("deaths_prison"),
            expr("max(coalesce(context.downs_beach, 0))").alias("downs_beach"),
            expr("max(coalesce(context.deaths_beach, 0))").alias("deaths_beach"),
            expr("max(coalesce(context.go_west_on_beach_to_camp, false))").alias("go_west_on_beach_to_camp"),
            expr("max(coalesce(skill_trunk.vault_hunter_main_trunk, 'unknown'))").alias("player_character_main_trunk"),
            expr("max(coalesce(vehicle_usage.used_vehicle_same_day, false))").alias("used_vehicle_same_day"),
            expr("max(ftue_steps.completed_FTUE)").alias("completed_FTUE"),
            expr("max(ftue_steps.minutes_complete_FTUE)").alias("minutes_complete_FTUE"),
            expr("max(ftue_steps.sessions_complete_FTUE)").alias("sessions_complete_FTUE"),
            expr("max(ftue_steps.days_complete_FTUE)").alias("days_complete_FTUE")
        )
    )

    # 4. Add metadata and merge key for
    key_cols = ["player_id", "player_platform_id", "player_character_id", "player_character", "platform", "service"]
    final_df = result_df.withColumn("dw_update_ts", expr("current_timestamp()")
                                    ).withColumn("dw_insert_ts", expr("current_timestamp()")
                                                 ).withColumn("merge_key", sha2(concat_ws("||", *key_cols), 256))

    # 5. Load
    load(final_df, database, environment, spark)

run_batch(database, environment)