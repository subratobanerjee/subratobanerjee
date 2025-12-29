# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/oak2/fact_player_character_mission_stats_daily

# COMMAND ----------

from pyspark.sql.functions import col, expr, row_number, sha2, dense_rank, lower
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import date
from pyspark.sql.window import Window

input_param = dbutils_input_params()
environment = input_param.get('environment', set_environment())
database = 'oak2'
data_source = 'gearbox'
title = 'Borderlands 4'

# COMMAND ----------

def extract(spark, environment, database, prev_max):
    mission_state_df = (
    spark.table(f"{database}{environment}.raw.oaktelemetry_mission_state")
    .filter(
        expr(f"""
            to_date(maw_time_received) >= to_date('{prev_max}') - interval 2 days
            AND event_value_string NOT IN (
                'Mission_GbxMoment_FactsLevelSequence',
                'displaytestmission',
                'DisplayTextMission'
            )
        """)
    )
    .filter("buildconfig_string = 'Shipping Game'")
)

    mission_info_df = spark.table(f"{database}{environment}.reference.mission_info")

    return mission_state_df, mission_info_df

# COMMAND ----------

def transform(spark, database, environment, mission_state_df, mission_info_df):
    # ExtractedEvents logic
    extracted_events_df = (
        mission_state_df.filter(
            expr(
                """
                player_platform_id IS NOT NULL
                AND lower(event_value_string) NOT LIKE '%test%'
                AND lower(event_value_string) NOT LIKE '%example%'
            """
            )
        )
        .selectExpr(
            "ifnull(player_id,player_platform_id) as player_id",
            "player_platform_id",
            "player_character_id",
            "player_time_played",
            "player_level",
            "date",
            "event_key_string",
            "event_value_string",
            "mission_set_string",
            "mission_objective_id_string",
            "maw_time_received",
            "game_session_uvh_level_long"
        )
        .withColumn("mission_name", expr("lower(event_value_string)"))
        .withColumn("mission_objective_id", expr("lower(mission_objective_id_string)"))
        .withColumn("event_key", expr("lower(event_key_string)"))
        .withColumn("mission_set", expr("lower(mission_set_string)"))
    )

    # Add mission_type derivation
    derived_events_df = extracted_events_df.withColumn(
        "mission_type",
        expr(
            """
            CASE
                WHEN mission_name LIKE '%uvh%' THEN 'uvhunlock'
                WHEN mission_name LIKE '%side_micronations%' THEN 'sidemission'
                WHEN mission_name LIKE '%micro%' AND mission_name NOT LIKE '%uvh%' THEN 'micromission'
                WHEN mission_name LIKE '%main%' THEN 'mainmission'
                WHEN mission_name LIKE '%side%' THEN 'sidemission'
                WHEN mission_name LIKE '%contract%' THEN 'contract'
                WHEN mission_name LIKE '%dynamicevent%' THEN 'dynamicevent'
                ELSE 'uncategorized'
            END
        """
        ),
    ).withColumn(
        "is_complete_flag",
        expr(
            """CASE 
                   WHEN event_key = 'completed' AND mission_objective_id = 'none' 
                   THEN 1 
                   ELSE 0 
               END"""
        )
    )

    # RankedEvents logic
    ranked_events_df = derived_events_df.select(
        "*",
        expr(
            "dense_rank() OVER(PARTITION BY player_character_id, mission_name ORDER BY date) AS mission_play_rank"
        ),
        expr(
            "row_number() OVER(PARTITION BY player_platform_id, player_character_id, date, mission_name ORDER BY maw_time_received DESC) AS event_recent_rank"
        ),
        expr(
            "sum(is_complete_flag) OVER (PARTITION BY player_character_id, mission_name ORDER BY date, maw_time_received ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) - is_complete_flag AS prior_completions"
        )
    )

    # Join with mission_info
    join_df = (
        ranked_events_df.alias("re")
        .join(
            mission_info_df.alias("lmi"),
            on=(
                (col("re.mission_objective_id") == col("lmi.mission_objective_id"))
                & (col("re.mission_name") == col("lmi.mission_name"))
            ),
            how="left",
        )
        .selectExpr(
            "coalesce(re.player_id,re.player_platform_id) as player_id",
            "re.player_platform_id",
            "re.player_character_id",
            "re.date",
            "re.mission_name",
            "re.mission_set",
            "re.mission_type",
            "re.is_complete_flag",
            "re.mission_objective_id",
            "re.mission_play_rank",
            "re.event_recent_rank",
            "re.prior_completions",
            "re.player_time_played",
            "re.player_level",
            "re.game_session_uvh_level_long",
            "lmi.mission_step_num"
        )
    )

    # Final aggregation
    grouped_df = (
        join_df.groupBy(
            "player_id",
            "player_platform_id",
            "player_character_id",
            "date",
            "mission_name",
            "mission_set",
            "mission_type",
        )
        .agg(
            expr("CAST(MAX(is_complete_flag) AS BOOLEAN)").alias("is_complete"),
            expr(
                "CAST(CASE WHEN MAX(prior_completions) > 0 THEN true ELSE false END AS BOOLEAN)"
            ).alias("is_replay"),
            expr("COUNT(DISTINCT mission_objective_id)").alias("steps_completed"),
            expr(
                "ROUND(CAST(COUNT(DISTINCT mission_objective_id) AS DOUBLE) / NULLIF(MAX(mission_step_num), 0), 2)"
            ).alias("percent_of_mission_completed"),
            expr("ROUND(MIN(player_time_played), 2)").alias("time_spent_at_start"),
            expr("ROUND(MAX(player_time_played), 2)").alias("time_spent_at_end"),
            expr(
                "ROUND((MAX(player_time_played) - MIN(player_time_played)), 2)"
            ).alias("time_spent_in_mission"),
            expr(
                "MAX(CASE WHEN event_recent_rank = 1 THEN mission_objective_id END)"
            ).alias("last_step_completed"),
            expr("MIN(player_level)").alias("starting_player_level"),
            expr("MAX(player_level)").alias("ending_player_level"),
            expr("CAST(NULL AS DOUBLE)").alias("average_enemy_level"),
            expr("CAST(NULL AS INT)").alias("min_enemy_level"),
            expr("CAST(NULL AS INT)").alias("max_enemy_level"),
            expr("MIN(game_session_uvh_level_long)").alias("min_uvh_level"),
            expr("MAX(game_session_uvh_level_long)").alias("max_uvh_level"),
        )
        .withColumn("dw_insert_ts", expr("current_timestamp()"))
        .withColumn("dw_update_ts", expr("current_timestamp()"))
        .withColumn(
            "merge_key",
            expr(
                "sha2(concat_ws('|', player_id, player_platform_id, player_character_id, date, mission_name, mission_set, mission_type), 256)"
            ),
        )
    )

    return grouped_df

# COMMAND ----------

def load(df, database, environment, spark):
    target_table_name = f"{database}{environment}.managed.fact_player_character_mission_stats_daily"
    target_df = DeltaTable.forName(spark, target_table_name)

    merger_condition = 'target.merge_key = source.merge_key'

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
    spark = SparkSession.builder.appName(f"{database}").getOrCreate()

    create_fact_player_character_mission_stats_daily(spark, database, environment)
    create_fact_player_character_mission_stats_daily_view(spark, database, environment)

    try:
        prev_max = max_timestamp(spark, f"{database}{environment}.managed.fact_player_character_mission_stats_daily", 'dw_insert_ts')
        if prev_max is None:
            prev_max = date(1999, 1, 1)
    except Exception as e:
        print(f"Table not found, defaulting prev_max: {e}")
        prev_max = date(1999, 1, 1)

    mission_state_df, mission_info_df = extract(spark, environment, database, prev_max)
    df = transform(spark, database, environment, mission_state_df, mission_info_df)

    load(df, database, environment, spark)

# COMMAND ----------

if __name__ == "__main__":
    run_batch(database, environment)