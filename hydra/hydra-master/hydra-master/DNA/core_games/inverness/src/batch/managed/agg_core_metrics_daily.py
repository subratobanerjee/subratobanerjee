# Databricks notebook source
import json
from pyspark.sql.functions import (
    col, 
    sha2, 
    concat_ws, 
    current_timestamp, 
    first_value,
    last_value,
    row_number,
    coalesce,
    max,
    when,
    min,
    any_value,
    rank,
    expr,
    greatest
    )
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import Window
import argparse
import logging

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers
# MAGIC

# COMMAND ----------

# MAGIC %run ../../../../../../utils/set_spark_session

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

def read_game_stats_per_daily(spark,environment):
    return (
        spark
        .read
        .table(f"inverness{environment}.managed.fact_player_game_status_daily")
        .where(expr("player_id !='anonymous' and player_id is not null and player_id NOT LIKE '%NULL%'"))
        .groupby("date", "platform", "service", "country_code")
        .agg(expr("avg(agg_game_status_1) as avg_daily_turns"),
            expr("median(agg_game_status_1) as median_daily_turns"),
            expr("sum(agg_game_status_2) as num_games_started"),
            expr("sum(agg_game_status_3) as num_games_completed"),
            expr("avg(agg_gp_2 / 60) as avg_daily_minutes"),
            expr("median(agg_gp_2 / 60) as median_daily_minutes"),
            expr("sum(agg_gp_3) as num_antiquity_turns"),
            expr("sum(agg_gp_4) as num_exploration_turns"),
            expr("sum(agg_gp_5) as num_modern_turns"),
            expr("sum(agg_gp_6) as num_atomic_turns"),
            expr("sum(agg_gp_7) as num_antiquity_age_starts"),
            expr("sum(agg_gp_8) as num_exploration_age_starts"),
            expr("sum(agg_gp_9) as num_modern_age_starts"),
            expr("sum(agg_gp_10) as num_atomic_age_starts"),
            expr("sum(agg_gp_11) as num_antiquity_age_completes"),
            expr("sum(agg_gp_12) as num_exploration_age_completes"),
            expr("sum(agg_gp_13) as num_modern_age_completes"),
            expr("sum(agg_gp_14) as num_atomic_age_completes"))
    )

def read_player_summary(spark,environment):
    return (
        spark
        .read
        .table(f"inverness{environment}.managed.fact_player_summary_ltd")
    )

def read_game_stats_per_campaign(spark,environment):
    player_df = read_player_summary(spark, environment).alias("player")
    player_campaign_df= (
        spark
        .read
        .table(f"inverness{environment}.managed.fact_player_campaign_summary")
        .where(expr("player_id !='anonymous' and player_id is not null and player_id NOT LIKE '%NULL%' and player_type= 'HUMAN'"))
        .alias("cs")
        .join(player_df, on=expr("cs.player_id = player.player_id"), how="left")
        .select(
        "cs.player_id",
        "campaign_start_ts",
        "platform",
        "service",
        "first_seen_country_code",
        "num_minutes_played",
        "num_turns_played"
        ).distinct()
    )
    return (
        player_campaign_df
        .groupby(expr("campaign_start_ts::date as campaign_start_ts"), "platform", "service", "first_seen_country_code")
        .agg(expr("avg(NUM_MINUTES_PLAYED) as avg_campaign_minutes"),
            expr("median(NUM_MINUTES_PLAYED) as median_campaign_minutes"),
            expr("avg(NUM_TURNS_PLAYED) as avg_campaign_turns"),
            expr("median(NUM_TURNS_PLAYED) as median_campaign_turns")
            )
    )

def read_game_stats_per_session(spark,environment):
    return (
        spark
        .read
        .table(f"inverness{environment}.managed.fact_player_session_daily")
       .groupby("date", "platform", "service", "country_code")
       .agg(expr("avg(agg_2) as avg_session_turns"),
            expr("median(agg_2) as median_session_turns"),
            expr("avg((session_len_sec / session_count) / 60) as avg_session_minutes"),
            expr("median(session_len_sec / 60) as median_session_minutes"))
    )

def read_retention(spark,environment):
    return (
        spark
        .read
        .table(f"dataanalytics{environment}.standard_metrics.fact_retention")
        .where(expr("title='Civilization VII'"))
        .groupby("install_date", "platform", "service", "country_code")
        .agg(expr("ifnull(SUM(day_1_count),0) as d1_retention"),
            expr("ifnull(SUM(day_7_count),0)  as d7_retention"),
            expr("ifnull(SUM(day_30_count),0)  as d30_retention"),
            expr("ifnull(SUM(day_60_count),0)  as d60_retention"),
            expr("ifnull(SUM(installs),0)  as installs"))
    )

def read_active_user(spark,environment):

    return (
        spark
        .read
        .table(f"dataanalytics{environment}.standard_metrics.fact_active_user")
        .where(expr("title='Civilization VII'"))
        .groupby("login_date", "platform", "service", "country_code")
        .agg( expr("ifnull(SUM(dau),0) as dau"),
             expr("ifnull(SUM(winback),0) as winback"))
    )



# COMMAND ----------

def extract(spark, environment):
    gs_daily_df = read_game_stats_per_daily(spark,environment).alias("daily")
    gs_campaign_df = read_game_stats_per_campaign(spark,environment).alias("campaign")
    gs_session_df = read_game_stats_per_session(spark,environment).alias("session")
    retention_df = read_retention(spark,environment).alias("retention")
    active_user_df = read_active_user(spark,environment).alias("active_user")

    joined_df = (
        gs_daily_df
        .join(gs_campaign_df, on=expr("daily.date = to_date(campaign.campaign_start_ts) and daily.platform = campaign.platform and daily.service=campaign.service and daily.country_code=campaign.first_seen_country_code"), how="left")
        .join(gs_session_df, on=expr("daily.date = session.date and daily.platform = session.platform and daily.service=session.service and daily.country_code=session.country_code"), how="left")
        .join(retention_df, on=expr("daily.date = retention.install_date and daily.platform = retention.platform and daily.service=retention.service and daily.country_code=retention.country_code"),how="left")
        .join(active_user_df, on=expr("daily.date = active_user.login_date and daily.platform = active_user.platform and daily.service=active_user.service and daily.country_code=active_user.country_code"),how="left")
    )

    return joined_df

# COMMAND ----------

def transform(df, environment, spark):
    aggregated_df = (
        df
        .groupBy(
            "daily.date",
            "daily.platform",
            "daily.service",
            "daily.country_code"
        )
        .agg(
            expr("max(avg_daily_turns) as avg_daily_turns"),
            expr("max(median_daily_turns) as median_daily_turns"),
            expr("max(num_games_started) as num_games_started"),
            expr("max(num_games_completed) as num_games_completed"),
            expr("max(avg_daily_minutes) as avg_daily_minutes"),
            expr("max(median_daily_minutes) as median_daily_minutes"),
            expr("max(num_antiquity_turns) as num_antiquity_turns"),
            expr("max(num_exploration_turns) as num_exploration_turns"),
            expr("max(num_modern_turns) as num_modern_turns"),
            expr("max(num_atomic_turns) as num_atomic_turns"),
            expr("max(num_antiquity_age_starts) as num_antiquity_age_starts"),
            expr("max(num_exploration_age_starts) as num_exploration_age_starts"),
            expr("max(num_modern_age_starts) as num_modern_age_starts"),
            expr("max(num_atomic_age_starts) as num_atomic_age_starts"),
            expr("max(num_antiquity_age_completes) as num_antiquity_age_completes"),
            expr("max(num_exploration_age_completes) as num_exploration_age_completes"),
            expr("max(num_modern_age_completes) as num_modern_age_completes"),
            expr("max(num_atomic_age_completes) as num_atomic_age_completes"),
            expr("max(avg_campaign_minutes) as avg_campaign_minutes"),
            expr("max(median_campaign_minutes) as median_campaign_minutes"),
            expr("max(avg_campaign_turns) as avg_campaign_turns"),
            expr("max(median_campaign_turns) as median_campaign_turns"),
            expr("max(avg_session_turns) as avg_session_turns"),
            expr("max(median_session_turns) as median_session_turns"),
            expr("max(avg_session_minutes) as avg_session_minutes"),
            expr("max(median_session_minutes) as median_session_minutes"),
            expr("ifnull(SUM(d1_retention),0) as d1_retention"),
            expr("ifnull(SUM(d7_retention),0) as d7_retention"),
            expr("ifnull(SUM(d30_retention),0) as d30_retention"),
            expr("ifnull(SUM(d60_retention),0) as d60_retention"),
            expr("ifnull(SUM(installs),0) as installs"),
            expr("ifnull(SUM(dau),0) as dau"),
            expr("ifnull(SUM(winback),0) as winback"),
            expr("current_timestamp() as dw_insert_ts"),
            expr("current_timestamp() as dw_update_ts")
        )
        .withColumn("merge_key", expr("sha2(concat_ws('|', daily.date, daily.platform, daily.service,daily.country_code), 256)"))
    )

    return aggregated_df

# COMMAND ----------

def load(df, environment, spark):
    (
        DeltaTable.createIfNotExists(spark)
        .tableName(f"inverness{environment}.managed.agg_core_metrics_daily")
        .addColumn("date", "string")
        .addColumn("country_code", "string")
        .addColumn("platform", "string")
        .addColumn("service", "string")
        .addColumn("avg_daily_minutes", "float")
        .addColumn("median_daily_minutes", "float")
        .addColumn("avg_session_minutes", "float")
        .addColumn("median_session_minutes", "float")
        .addColumn("avg_campaign_minutes", "float")
        .addColumn("median_campaign_minutes", "float")
        .addColumn("num_games_started", "int")
        .addColumn("num_games_completed", "int")
        .addColumn("num_antiquity_turns", "int")
        .addColumn("num_exploration_turns", "int")
        .addColumn("num_modern_turns", "int")
        .addColumn("num_atomic_turns", "int")
        .addColumn("num_antiquity_age_starts", "int")
        .addColumn("num_exploration_age_starts", "int")
        .addColumn("num_modern_age_starts", "int")
        .addColumn("num_atomic_age_starts", "int")
        .addColumn("num_antiquity_age_completes", "int")
        .addColumn("num_exploration_age_completes", "int")
        .addColumn("num_modern_age_completes", "int")
        .addColumn("num_atomic_age_completes", "int")
        .addColumn("avg_campaign_turns", "float")
        .addColumn("median_campaign_turns", "float")
        .addColumn("avg_session_turns", "float")
        .addColumn("median_session_turns", "float")
        .addColumn("avg_daily_turns", "float")
        .addColumn("median_daily_turns", "float")
        .addColumn("d60_retention", "int")
        .addColumn("d30_retention", "int")
        .addColumn("d7_retention", "int")
        .addColumn("d1_retention", "int")
        .addColumn("dau", "int")
        .addColumn("installs", "int")
        .addColumn("winback", "int")
        .addColumn("dw_insert_ts", "timestamp")
        .addColumn("dw_update_ts", "timestamp")
        .addColumn("merge_key", "string")
        .property('delta.enableIcebergCompatV2', 'true')
        .property('delta.universalFormat.enabledFormats', 'iceberg')
        .execute()
    )

    target_table = DeltaTable.forName(spark, f"inverness{environment}.managed.agg_core_metrics_daily")
    (
        target_table
        .alias("target")
        .merge(
            df.alias("source"),
            "target.merge_key = source.merge_key")
        .withSchemaEvolution()
        .whenMatchedUpdate(set=
        {
            "target.avg_daily_minutes": "source.avg_daily_minutes",
            "target.median_daily_minutes": "source.median_daily_minutes",
            "target.avg_session_minutes": "source.avg_session_minutes",
            "target.median_session_minutes": "source.median_session_minutes",
            "target.avg_campaign_minutes": "source.avg_campaign_minutes",
            "target.median_campaign_minutes": "source.median_campaign_minutes",
            "target.num_games_started": greatest("source.num_games_started","target.num_games_started"),
            "target.num_games_completed": greatest("source.num_games_completed","target.num_games_completed"),
            "target.num_antiquity_turns": greatest("source.num_antiquity_turns","target.num_antiquity_turns"),
            "target.num_exploration_turns": greatest("source.num_exploration_turns","target.num_exploration_turns"),
            "target.num_modern_turns": greatest("source.num_modern_turns","target.num_modern_turns"),
            "target.num_atomic_turns": greatest("source.num_atomic_turns","target.num_atomic_turns"),
            "target.num_antiquity_age_starts": greatest("source.num_antiquity_age_starts","target.num_antiquity_age_starts"),
            "target.num_exploration_age_starts": greatest("source.num_exploration_age_starts","target.num_exploration_age_starts"),
            "target.num_modern_age_starts": greatest("source.num_modern_age_starts","target.num_modern_age_starts"),
            "target.num_atomic_age_starts": greatest("source.num_atomic_age_starts","target.num_atomic_age_starts"),
            "target.num_antiquity_age_completes": greatest("source.num_antiquity_age_completes","target.num_antiquity_age_completes"),
            "target.num_exploration_age_completes": greatest("source.num_exploration_age_completes","target.num_exploration_age_completes"),
            "target.num_modern_age_completes": greatest("source.num_modern_age_completes","target.num_modern_age_completes"),
            "target.num_atomic_age_completes": greatest("source.num_atomic_age_completes","target.num_atomic_age_completes"),
            "target.avg_campaign_turns": "source.avg_campaign_turns",
            "target.median_campaign_turns": "source.median_campaign_turns",
            "target.avg_session_turns": "source.avg_session_turns",
            "target.median_session_turns": "source.median_session_turns",
            "target.avg_daily_turns": "source.avg_daily_turns",
            "target.median_daily_turns": "source.median_daily_turns",
            "target.d60_retention": greatest("source.d60_retention","target.d60_retention"),
            "target.d30_retention": greatest("source.d30_retention","target.d30_retention"),
            "target.d7_retention": greatest("source.d7_retention","target.d7_retention"),
            "target.d1_retention": greatest("source.d1_retention","target.d1_retention"),
            "target.dau": greatest("source.dau","target.dau"),
            "target.installs": greatest("source.installs","target.installs"),
            "target.winback": greatest("source.winback","target.winback"),
            "target.dw_update_ts": "current_timestamp()"
        })
        .whenNotMatchedInsertAll()
        .execute()
    )


# COMMAND ----------

def process_agg_core_metrics():
    environment = dbutils.widgets.get("environment")
    checkpoint_location = dbutils.widgets.get("checkpoint")
    #environment, checkpoint_location = "_dev", "dbfs:/tmp/inverness/batch/managed/run_dev/agg_core_metrics_daily"

    spark = create_spark_session()

    df = extract(spark, environment)

    df = transform(df, environment, spark)

    load(df, environment, spark)


# COMMAND ----------

# Main execution
process_agg_core_metrics()
