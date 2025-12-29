# Databricks notebook source
# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/oak2/fact_player_character_boss_fight_daily

# COMMAND ----------

# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

from pyspark.sql.functions import col, expr, sha2, concat_ws
from pyspark.sql.types import LongType, DoubleType, IntegerType, TimestampType
from delta.tables import DeltaTable
from datetime import date

input_param = dbutils_input_params()
environment = input_param.get('environment', set_environment())
database = 'oak2'
data_source = 'gearbox'
title = 'Borderlands 4'

# COMMAND ----------

def extract(spark, database, environment, prev_max_date):
    boss_fight_df = spark.table(f"{database}{environment}.raw.oaktelemetry_boss_fight") \
        .filter(expr(f"date >= '{prev_max_date}' - interval 2 days")) \
        .filter("buildconfig_string = 'Shipping Game'")        

    boss_info_df = spark.table(f"{database}{environment}.reference.boss_info")

    return boss_fight_df, boss_info_df

# COMMAND ----------

def transform_boss_fights(boss_fight_df, boss_info_df):
    
    # This single aggregation step now mirrors the 'test_0' CTE from the SQL view
    aggregated_fights = (
        boss_fight_df
        .join(
            boss_info_df.alias("binfo"),
            expr("boss_fight_name_string = binfo.boss_name"),
            "left"
        )
        .filter(expr("player_platform_id IS NOT NULL AND player_character_id IS NOT NULL"))
        .groupBy(
            expr("ifnull(player_id,player_platform_id) as player_id"),
            "player_platform_id",
            "player_character_id",
            "date",
            expr("boss_fight_name_string as boss_name"),
            expr("buildchangelist_long as build_changelist"),
            "binfo.boss_name_clean"
        )
        .agg(
            expr("""
                AVG(
                    CASE WHEN LOWER(boss_fight_result_string) = 'failed'
                    THEN element_at(
                        bookmarks_array.bar_value_long,
                        CAST(array_position(bookmarks_array.bar_name_string, 'total_remaining_hp_percentage') AS INT)
                    )
                    ELSE NULL END
                ) AS percent_hp_remaining
            """),
            expr("count(*) as times_attempted"),
            expr("SUM(CASE WHEN lower(boss_fight_result_string) = 'success' THEN 1 ELSE 0 END) AS times_succeeded"),
            expr("SUM(CASE WHEN lower(boss_fight_result_string) != 'success' THEN 1 ELSE 0 END) AS times_failed"),
            expr("CAST(percentile_approx(CASE WHEN lower(boss_fight_result_string) = 'success' THEN player_level ELSE NULL END, 0.5) AS INT) AS median_player_level_success"),
            expr("CAST(percentile_approx(CASE WHEN lower(boss_fight_result_string) != 'success' THEN player_level ELSE NULL END, 0.5) AS INT) AS median_player_level_fail"),
            expr("percentile_approx(CASE WHEN boss_fight_result_string = 'success' THEN boss_fight_duration_s_double / 60 ELSE NULL END, 0.5) AS median_time_to_beat_mins"),
            expr("percentile_approx(CASE WHEN boss_fight_result_string != 'success' THEN boss_fight_duration_s_double / 60 ELSE NULL END, 0.5) AS median_time_to_fail_mins"),
            expr("SUM(boss_fight_duration_s_double / 60) AS time_in_boss_fight_mins")
        )
    )

    # This final select projects the columns exactly as the final SELECT in the SQL view
    result_df = (
        aggregated_fights
        .select(
            expr("coalesce(player_id,player_platform_id) as player_id"),
            "player_platform_id",
            "player_character_id",
            "date",
            "boss_name",
            "boss_name_clean",
            "times_attempted",
            "times_succeeded",
            "times_failed",
            expr("CAST(NULL AS BOOLEAN) AS is_featured"),
            expr("CASE WHEN LOWER(boss_name) LIKE '%world%' THEN 1 ELSE 0 END AS is_world_boss"),
            "time_in_boss_fight_mins",
            "median_time_to_beat_mins",
            "median_time_to_fail_mins",
            "median_player_level_success",
            "median_player_level_fail",
            "percent_hp_remaining",
            "build_changelist"
        )
    )

    return result_df

# COMMAND ----------

def load(df, database, environment, spark):
    target_table_name = f"{database}{environment}.managed.fact_player_character_boss_fight_daily"
    
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
    spark = create_spark_session(name=f"{database}_boss_fight_summary")
    
    target_table = f"{database}{environment}.managed.fact_player_character_boss_fight_daily"
    create_fact_boss_fight_daily_summary(spark, database, environment)
    create_fact_player_character_boss_fight_daily_view(spark, database, environment)

    
    prev_max_date = max_timestamp(spark, target_table, 'date')
    if prev_max_date is None:
        prev_max_date = date(1999, 1, 1)
    
    boss_fight_df, boss_info_df = extract(spark, database, environment, prev_max_date)

    result_df = transform_boss_fights(boss_fight_df, boss_info_df)                                      
    
    # Key columns now match the corrected grain from the SQL view
    key_cols = ["player_id", "player_platform_id", "player_character_id", "date", "boss_name", "build_changelist"]
    
    final_df = result_df.withColumn("dw_update_ts", expr("current_timestamp()")) \
                        .withColumn("dw_insert_ts", expr("current_timestamp()")) \
                        .withColumn("merge_key", sha2(concat_ws("||", *key_cols), 256))

    load(final_df, database, environment, spark)

run_batch(database, environment)