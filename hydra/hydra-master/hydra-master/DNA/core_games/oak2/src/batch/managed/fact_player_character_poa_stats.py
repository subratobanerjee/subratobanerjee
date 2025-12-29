# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/oak2/fact_player_character_poa_stats

# COMMAND ----------

from pyspark.sql.types import DoubleType, DecimalType
from pyspark.sql.functions import col, expr, lower, lag, lit, greatest, when, coalesce, max
from pyspark.sql.window import Window
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


def extract(spark, database, environment, prev_max_date):
    poa_state_df = spark.table(f"{database}{environment}.raw.oaktelemetry_poa_state") \
        .filter(expr(f"maw_time_received >= '{prev_max_date}' - interval 5 days")) \
        .filter("buildconfig_string = 'Shipping Game'")

    poa_info_df = spark.table(f"{database}{environment}.reference.poa_info")

    mission_state_df = (
    spark.table(f"{database}{environment}.raw.oaktelemetry_mission_state")
    .filter(
        expr(f"maw_time_received >= '{prev_max_date}' - interval 10 days")
        & (~col("event_value_string").isin(
            "Mission_GbxMoment_FactsLevelSequence",
            "displaytestmission",
            "DisplayTextMission"
        ))
    )
    .filter("buildconfig_string = 'Shipping Game'")
)

    return poa_state_df, poa_info_df, mission_state_df


# COMMAND ----------

def transform_time_differences(poa_state_df):
    poa_min_ts = (
        poa_state_df
        .where(
            col("player_platform_id").isNotNull()
            & col("player_character_id").isNotNull()
        )
        .groupBy(
            expr("IFNULL(player_id,player_platform_id)").alias("player_id"),
            col("player_platform_id"),
            col("player_character_id"),
            lower(col("event_value_string")).alias("poa_name"),
            col("poa_status_string").alias("poa_status")
        )
        .agg(expr("MIN(player_time_played)").alias("status_time_played"))
    )

    window_spec = Window.partitionBy("player_id", "player_platform_id", "player_character_id", "poa_name") \
        .orderBy(col("status_time_played"))

    poa_with_prev = poa_min_ts.withColumn("prev_status_time_played", lag("status_time_played").over(window_spec))

    differences_df = (
        poa_with_prev
        .select(
            expr("IFNULL(player_id,player_platform_id) as player_id"),
            "player_platform_id",
            "player_character_id",
            "poa_name",
            "poa_status",
            expr(
                "CASE WHEN poa_status != 'Undiscovered' THEN ROUND((status_time_played - prev_status_time_played), 2) ELSE NULL END::DECIMAL(24,2)")
            .alias("minutes_since_previous_step")
        )
        .distinct()
    )
    return differences_df


# COMMAND ----------

def transform_timestamp_fields(poa_state_df):
    discovery_df = (
        poa_state_df
        .where(
            col("player_platform_id").isNotNull()
            & col("player_character_id").isNotNull()
        )
        .groupBy(
            expr("IFNULL(player_id, player_platform_id)").alias("player_id"),
            col("player_platform_id"),
            col("player_character_id"),
            lower(col("event_value_string")).alias("poa_name")
        )
        .agg(
            expr("MIN(CASE WHEN poa_status_string = 'Discovered' THEN player_time_played END)").alias(
                "discovered_time"),
            expr("MIN(CASE WHEN poa_status_string = 'Undiscovered' THEN player_time_played END)").alias(
                "undiscovered_time"),
            expr("MIN(player_time_played)").alias("start_time")
        )
    )

    result = (
        poa_state_df.alias("poa")
        .join(
            discovery_df.alias("d"),
            (expr("IFNULL(poa.player_id,poa.player_platform_id)") == col("d.player_id"))
            & (col("poa.player_platform_id") == col("d.player_platform_id"))
            & (col("poa.player_character_id") == col("d.player_character_id"))
            & (lower(col("poa.event_value_string")) == col("d.poa_name")),
            "left"
        )
        .where(
            col("poa.player_platform_id").isNotNull()
            & col("poa.player_character_id").isNotNull()
        )
        .groupBy(
            expr("IFNULL(poa.player_id,poa.player_platform_id)").alias("player_id"),
            col("poa.player_platform_id"),
            col("poa.player_character_id"),
            lower(col("poa.event_value_string")).alias("poa_name"),
            col("poa.poa_status_string").alias("poa_status")
        )
        .agg(
            expr("ROUND(MAX(poa.player_time_played) - MIN(d.start_time), 2)").alias("minutes_played"),
            expr("ROUND((MAX(poa.player_time_played) - MIN(d.start_time)) / 60.0, 2)").alias("hours_played"),
            expr(
                "ROUND((case when poa.poa_status_string NOT IN ('Undiscovered') then (MAX(poa.player_time_played) - MIN(d.undiscovered_time)) else null end), 2)").alias(
                "minutes_since_first_step"),
            expr(
                "CASE WHEN MAX(CASE WHEN poa.poa_status_string NOT IN ('Discovered', 'Undiscovered') THEN poa.player_time_played END) > MIN(d.discovered_time) "
                "THEN ROUND(MAX(CASE WHEN poa.poa_status_string NOT IN ('Discovered', 'Undiscovered') THEN poa.player_time_played END) - MIN(d.discovered_time), 2) ELSE NULL END"
            ).alias("minutes_since_discovered")
        )
    )
    return result


# COMMAND ----------
def transform_poa_stats(poa_state_df, poa_info_df, mission_state_df):
    mission_state_aggregated_df = (
        mission_state_df.alias("ms").groupBy(
            expr("IFNULL(ms.player_id, ms.player_platform_id)").alias("player_id"),
            col("ms.player_platform_id").alias("player_platform_id"),
            col("ms.player_character_id").alias("player_character_id")
        ).agg(
            max(
                when(
                    (lower(col("ms.event_value_string")) == 'mission_main_mountains1')
                    & (lower(col("ms.mission_objective_id_string")) == 'none')
                    & (lower(col("ms.event_key_string")) == 'completed'), True
                )
            ).alias("has_completed_augermine_prereq"),
            max(
                when(
                    (lower(col("ms.event_value_string")) == 'mission_main_grasslands2b')
                    & (lower(col("ms.mission_objective_id_string")) == 'none')
                    & (lower(col("ms.event_key_string")) == 'completed'), True
                )
            ).alias("has_completed_orderbunker_prereq")
        ).where(
            col("player_platform_id").isNotNull()
            & col("player_character_id").isNotNull()
        )
    )

    poa_stats_df = (
        poa_state_df.alias("poa")
        .join(
            poa_info_df.alias("p"),
            (lower(col("poa.event_value_string")) == col("p.poa_name"))
            & (lower(col("poa.poa_type_string")) == col("p.poa_type")),
            "left"
        )
        .join(
            mission_state_aggregated_df.alias("ms_agg"),
            (expr("IFNULL(poa.player_id, poa.player_platform_id)") == expr("ms_agg.player_id"))
            & (col("poa.player_platform_id") == col("ms_agg.player_platform_id"))
            & (col("poa.player_character_id") == col("ms_agg.player_character_id")),
            "left"
        )
        .where(
            col("poa.player_platform_id").isNotNull()
            & col("poa.player_character_id").isNotNull()
        )
        .groupBy(
            expr("IFNULL(poa.player_id, poa.player_platform_id)").alias("player_id"),
            col("poa.player_platform_id").alias("player_platform_id"),
            col("poa.player_character_id").alias("player_character_id"),
            lower(col("poa.event_value_string")).alias("poa_name"),
            col("p.poa_name_clean").alias("poa_name_clean"),
            col("p.poa_type_clean").alias("poa_type_clean"),
            col("poa.poa_status_string").alias("poa_status")
        )
        .agg(
            expr("MIN(poa.maw_time_received)").alias("received_on"),
            expr("MAX(poa.player_level)::BIGINT").alias("character_level"),
            expr(
                "MAX(CASE WHEN poa.poa_status_string = 'Undiscovered' THEN 1 WHEN poa.poa_status_string = 'Discovered' THEN 2 WHEN poa.poa_status_string = 'Claimable' THEN 3 WHEN poa.poa_status_string = 'Claimed' THEN 4 END)").alias(
                "poa_status_number"),
            expr("MAX(CASE WHEN poa.poa_status_string = p.final_status THEN TRUE ELSE FALSE END)").alias("is_complete"),
            expr(
                "CASE WHEN p.poa_type_clean = 'Augermine' THEN coalesce(MAX(ms_agg.has_completed_augermine_prereq), FALSE)"
                "WHEN p.poa_type_clean = 'Order Bunker' THEN coalesce(MAX(ms_agg.has_completed_orderbunker_prereq), FALSE)"
                "ELSE TRUE END"
            ).alias("prerequisite_complete")
        )
    )
    return poa_stats_df


def load(df, database, environment, spark):
    target_table_name = f"{database}{environment}.managed.fact_player_character_poa_stats"
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


def run_batch(database, environment):
    database = database.lower()
    spark = create_spark_session(name=f"{database}_poa_stats")

    target_table = f"{database}{environment}.managed.fact_player_character_poa_stats"
    create_fact_player_character_poa_stats(spark, database, environment)
    create_fact_player_character_poa_stats_view(spark, database)

    prev_max_date = max_timestamp(spark, target_table, 'dw_update_ts')
    if prev_max_date is None:
        prev_max_date = date(1999, 1, 1)

    poa_state_df, poa_info_df, mission_state_df = extract(spark, database, environment, prev_max_date)

    differences_df = transform_time_differences(poa_state_df)
    timestamp_fields_df = transform_timestamp_fields(poa_state_df)
    poa_stats_df = transform_poa_stats(poa_state_df, poa_info_df, mission_state_df)

    final_df = (
        poa_stats_df.alias("poa")
        .join(
            differences_df.alias("diff"),
            ["player_id", "player_platform_id", "player_character_id", "poa_name", "poa_status"],
            "left"
        )
        .join(
            timestamp_fields_df.alias("tf"),
            ["player_id", "player_platform_id", "player_character_id", "poa_name", "poa_status"],
            "left"
        )
        .select(
            expr("coalesce(poa.player_id, poa.player_platform_id) as player_id"),
            col("poa.player_platform_id").alias("player_platform_id"),
            col("poa.player_character_id").alias("player_character_id"),
            col("poa.received_on").alias("received_on"),
            col("poa.poa_name").alias("poa_name"),
            col("poa.poa_name_clean").alias("poa_name_clean"),
            col("poa.poa_type_clean").alias("poa_type_clean"),
            col("poa.poa_status").alias("poa_status"),
            col("poa.character_level").alias("character_level"),
            col("poa.poa_status_number").alias("poa_status_number"),
            col("poa.is_complete").alias("is_complete"),
            col("poa.prerequisite_complete").alias("prerequisite_complete"),
            expr("IF(tf.hours_played < 0, 0.0, tf.hours_played)").alias("hours_played"),
            expr("IF(tf.minutes_played < 0, 0.0, tf.minutes_played)").alias("minutes_played"),
            expr("IF(tf.minutes_since_first_step < 0, 0.0, tf.minutes_since_first_step)").alias(
                "minutes_since_first_step"),
            expr("IF(tf.minutes_since_discovered < 0, 0.0, tf.minutes_since_discovered)").alias(
                "minutes_since_discovered"),
            expr(
                "coalesce(IF(diff.minutes_since_previous_step < 0, 0.0, diff.minutes_since_previous_step), 0.0)").alias(
                "minutes_since_previous_step")
        )
        .groupBy(
        "player_id", "player_platform_id", "player_character_id", "poa_name", "poa_status"
        )
        .agg(
            max("received_on").alias("received_on"),
            max("poa_name_clean").alias("poa_name_clean"),
            max("poa_type_clean").alias("poa_type_clean"),
            max("character_level").alias("character_level"),
            max("poa_status_number").alias("poa_status_number"),
            max("is_complete").alias("is_complete"),
            max("prerequisite_complete").alias("prerequisite_complete"),
            max("hours_played").alias("hours_played"),
            max("minutes_played").alias("minutes_played"),
            max("minutes_since_first_step").alias("minutes_since_first_step"),
            max("minutes_since_discovered").alias("minutes_since_discovered"),
            max("minutes_since_previous_step").alias("minutes_since_previous_step")
        )
    )

    final_df_with_keys = final_df.withColumn("dw_update_ts", expr("current_timestamp()")) \
        .withColumn("dw_insert_ts", expr("current_timestamp()")) \
        .withColumn("merge_key", expr(
        "sha2(concat_ws('|', player_id, player_platform_id, player_character_id, poa_name, poa_status), 256)"))

    load(final_df_with_keys, database, environment, spark)


run_batch(database, environment)