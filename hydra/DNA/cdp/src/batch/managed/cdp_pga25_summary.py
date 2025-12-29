# Databricks notebook source
# MAGIC %run ../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../utils/ddl/table_functions

# COMMAND ----------

import json
from pyspark.sql.functions import col, expr, lower, min, max, count_distinct, to_date, datediff, current_date, first, coalesce
from pyspark.sql.types import *
from pyspark.sql import Window
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
import argparse

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment', set_environment())
database = 'dataanalytics'
data_source = 'cdp'
title = "'PGA25'"

# COMMAND ----------

def extract(spark, environment: str, prev_max):
    roundstatus = (
        spark.table(f"bluenose{environment}.raw.roundstatus")
        .where(expr(f"CAST(insert_ts AS DATE) >= to_date('{prev_max}') - INTERVAL 2 DAYS"))
    )
    service = spark.table("reference_customer.platform.service")

    fact_player_activity = (
        spark.table(f"bluenose{environment}.intermediate.fact_player_activity")
        .where(expr(f"CAST(dw_insert_ts AS DATE) >= to_date('{prev_max}') - INTERVAL 2 DAYS"))
    )
    fact_player_transaction = (
        spark.table(f"bluenose{environment}.intermediate.fact_player_transaction")
        .where(expr(f"CAST(dw_insert_ts AS DATE) >= to_date('{prev_max}') - INTERVAL 2 DAYS"))
    )
    clubhousepass = (
        spark.table(f"bluenose{environment}.raw.clubhousepass")
        .where(expr(f"CAST(insert_ts AS DATE) >= to_date('{prev_max}') - INTERVAL 2 DAYS"))
    )
    platform = spark.table("reference_customer.platform.platform")
    accountdeleteevent = spark.table(f"coretech{environment}.sso.accountdeleteevent")
    fact_player_link_activity = spark.table(f"bluenose{environment}.managed.fact_player_link_activity")
    fact_player_game_status_daily = spark.table(f"bluenose{environment}.managed.fact_player_game_status_daily")
    applicationsession = spark.table(f"bluenose{environment}.raw.applicationsession")
    fact_player_progression = spark.table(f"bluenose{environment}.intermediate.fact_player_progression")
    franchise_pga = spark.table(f"dataanalytics{environment}.cdp_ng.franchise_pga")
    franchise_wwe = spark.table("sf_databricks_migration.sf_dbx.franchise_wwe")
    franchise_nba = spark.table("sf_databricks_migration.sf_dbx.franchise_nba")
    franchise_tsn = spark.table("sf_databricks_migration.sf_dbx.franchise_tsn")
    linkevent = spark.table(f"coretech{environment}.sso.linkevent")
    dim_title = spark.table(f"reference{environment}.title.dim_title")
    vw_ctp_profile = spark.table("reference_customer.customer.vw_ctp_profile")
    ap = spark.table(f"bluenose{environment}.raw.applicationsession").alias("ap")
    fp = spark.table(f"bluenose{environment}.managed_view.fact_player_transaction_purchase").alias("fp")

    return (roundstatus, service, fact_player_activity, fact_player_transaction, clubhousepass, platform, accountdeleteevent,
            fact_player_link_activity, fact_player_game_status_daily,
            applicationsession, fact_player_progression, franchise_pga, franchise_wwe, franchise_nba,
            franchise_tsn, linkevent, dim_title, vw_ctp_profile, ap, fp)


# COMMAND ----------

def transformation(roundstatus, service, fact_player_activity,fact_player_transaction, clubhousepass, platform, accountdeleteevent, fact_player_link_activity,
                   fact_player_game_status_daily, applicationsession, fact_player_progression,
                   franchise_pga, franchise_wwe, franchise_nba, franchise_tsn, linkevent, dim_title, vw_ctp_profile, ap, fp,
                   environment):
    """Applies transformation logic to the DataFrames."""

    services_df = (
        service
        .selectExpr(
            "case when service = 'xbl' then 'Xbox Live' else service end as join_key",
            "service",
            "vendor"
        )
        .distinct()
    )

    games_success_fail = (
        roundstatus
        .filter("lessonname IS NULL")
        .groupBy("playerPublicId")
        .agg(
            expr("max(cast(totalscore as int)) as total_score"),
            expr("max(cast(coursepar as int)) as par_score")
        )
        .selectExpr(
            "playerPublicId as player_id",
            "total_score",
            "par_score",
            "total_score - par_score as score_to_par"
        )
    )

    fact_players_ltd = (
        fact_player_activity.alias("f")
        .join(services_df.alias("s"), lower(col("s.join_key")) == lower(col("f.service")))
        .join(platform.alias("v"), col("f.platform") == col("v.src_platform"))
        .join(games_success_fail.alias("fail"), col("f.player_id") == col("fail.player_id"), how="left")
        .join(accountdeleteevent.alias("sso"), col("sso.accountId") == col("f.player_id"), how="left")
        .where(
            (col("f.platform").isNotNull()) &
            (col("sso.accountId").isNull()) &
            (col("f.received_on").cast("timestamp") >= "2025-02-04")
        )
        .selectExpr(
            "lower(f.player_id) as player_id",
            "first(v.platform) OVER (PARTITION BY f.player_id ORDER BY CAST(received_on AS TIMESTAMP) DESC) as platform",
            "first(v.vendor) OVER (PARTITION BY f.player_id ORDER BY CAST(received_on AS TIMESTAMP) DESC) as vendor",
            "first(s.service) OVER (PARTITION BY f.player_id ORDER BY CAST(received_on AS TIMESTAMP) DESC) as service",
            "max(CASE WHEN extra_info_5 IN ('demo', 'converted') THEN 1 ELSE 0 END) OVER (PARTITION BY f.player_id) as is_ftp_install",
            "CAST(NULL AS BOOLEAN) as is_preorder",
            "first(f.country_code) IGNORE NULLS OVER (PARTITION BY f.player_id ORDER BY CAST(received_on AS TIMESTAMP) DESC) as last_country_code",
            "count(CASE WHEN event_trigger IN ('RoundWon', 'RoundComplete') AND source_table = 'roundstatus' THEN 1 END) OVER (PARTITION BY f.player_id) as games_success",
            "count(CASE WHEN (event_trigger = 'RoundQuit' OR fail.score_to_par > 0) AND source_table = 'roundstatus' THEN 1 END) OVER (PARTITION BY f.player_id) as games_fail",
            "sum(CASE WHEN event_trigger IN ('RoundWon', 'RoundComplete') AND source_table = 'roundstatus' AND LOWER(extra_info_2) LIKE '%career%' THEN 1 ELSE 0 END) OVER (PARTITION BY f.player_id) as mycareer_games_success",
            "min(CAST(received_on AS DATE)) OVER (PARTITION BY f.player_id) as install_date",
            "max(CAST(received_on AS DATE)) OVER (PARTITION BY f.player_id) as last_seen_date",
            "first(CASE WHEN v.platform NOT IN ('XBSX', 'PS5', 'Windows', 'PC') THEN v.platform ELSE NULL END) IGNORE NULLS OVER (PARTITION BY f.player_id ORDER BY CAST(received_on AS TIMESTAMP) DESC) as current_gen_platform",
            "first(CASE WHEN v.platform IN ('XBSX', 'PS5', 'Windows', 'PC') THEN v.platform ELSE NULL END) IGNORE NULLS OVER (PARTITION BY f.player_id ORDER BY CAST(received_on AS TIMESTAMP) DESC) as next_gen_platform"
        )
        .distinct()
    )

    fact_player_activity_df = (
        fact_player_link_activity
        .groupBy("player_id")
        .agg(F.countDistinct("date").alias("active_days"))
        .selectExpr("player_id", "active_days")
    )

    fact_player_metrics = (
        fact_player_game_status_daily
        .groupBy("player_id")
        .agg(
            expr("SUM(CAST(agg_game_status_2 AS LONG))").alias("games_played"),
            min(expr("CASE WHEN lower(game_mode) LIKE 'ranked%' THEN date ELSE NULL END")).alias(
                "ranked_tours_first_seen"),
            max(expr("CASE WHEN lower(game_mode) LIKE 'ranked%' THEN date ELSE NULL END")).alias(
                "ranked_tours_last_seen"),
            F.sum(expr(
                "CASE WHEN lower(game_mode) LIKE 'ranked%' THEN CAST(agg_game_status_2 AS LONG) ELSE 0 END")).alias(
                "ranked_tours_games_played"),
            count_distinct(expr("CASE WHEN lower(game_mode) LIKE 'ranked%' THEN date ELSE NULL END")).alias(
                "ranked_tours_days_active"),
            min(expr("CASE WHEN lower(game_mode) LIKE 'career%' THEN date ELSE NULL END")).alias("mycareer_first_seen"),
            max(expr("CASE WHEN lower(game_mode) LIKE 'career%' THEN date ELSE NULL END")).alias("mycareer_last_seen"),
            F.sum(expr(
                "CASE WHEN lower(game_mode) LIKE 'career%' THEN CAST(agg_game_status_2 AS LONG) ELSE 0 END")).alias(
                "mycareer_games_played"),
            count_distinct(expr("CASE WHEN lower(game_mode) LIKE 'career%' THEN date ELSE NULL END")).alias(
                "mycareer_days_active")
        )
        .selectExpr(
            "player_id",
            "games_played",
            "ranked_tours_first_seen",
            "ranked_tours_last_seen",
            "ranked_tours_games_played",
            "ranked_tours_days_active",
            "mycareer_first_seen",
            "mycareer_last_seen",
            "mycareer_games_played",
            "mycareer_days_active"
        )
    )

    vc_metrics = (
        fact_player_transaction
        .filter("currency_type = 'VC' AND player_id != 'anonymous'")
        .groupBy("player_id")
        .agg(
            F.expr(
                "MIN(CASE WHEN LOWER(action_type) = 'spend' THEN CAST(received_on AS DATE) ELSE NULL END) AS vc_spent_first_date"),
            F.expr(
                "MAX(CASE WHEN LOWER(action_type) = 'spend' THEN CAST(received_on AS DATE) ELSE NULL END) AS vc_spent_last_date"),
            F.expr("SUM(CASE WHEN LOWER(action_type) = 'spend' THEN currency_amount ELSE 0 END) AS vc_spent_total"),
            F.expr(
                "MIN(CASE WHEN LOWER(action_type) = 'purchase' THEN CAST(received_on AS DATE) ELSE NULL END) AS vc_bought_first_date"),
            F.expr(
                "MAX(CASE WHEN LOWER(action_type) = 'purchase' THEN CAST(received_on AS DATE) ELSE NULL END) AS vc_bought_last_date"),
            F.expr("SUM(CASE WHEN LOWER(action_type) = 'purchase' THEN currency_amount ELSE 0 END) AS vc_bought_total"),
            F.expr("SUM(CASE WHEN LOWER(action_type) = 'earn' THEN currency_amount ELSE 0 END) AS vc_earned_total"),
            F.expr("COUNT(CASE WHEN LOWER(action_type) LIKE '%purchase%' THEN 1 ELSE 0 END) AS bought_vc_transactions")
        )
        .selectExpr(
            "*",
            "vc_bought_total > 0 AS bought_vc_converted",
            """
            CASE
                WHEN vc_bought_total = 0 THEN 0
                WHEN vc_bought_total < 5000 THEN 2
                WHEN vc_bought_total < 15000 THEN 5
                WHEN vc_bought_total < 35000 THEN 10
                WHEN vc_bought_total < 75000 THEN 20
                WHEN vc_bought_total < 200000 THEN 50
                WHEN vc_bought_total < 450000 THEN 100
                ELSE -99
            END AS bought_vc_dollar_value
            """,
            "vc_bought_total - vc_spent_total + vc_earned_total AS current_vc_balance"
        )
        .filter("vc_spent_total + vc_bought_total + vc_earned_total != 0")
    )

    clubhousepass_level = (
        clubhousepass
        .filter(
            "seasonid = 'ClubhousePassSeason4' AND buildenvironment = 'RELEASE'"
        )
        .groupBy("playerPublicId")
        .agg(
            expr("max(cast(level as int)) as max_level")
        )
        .selectExpr(
            "playerPublicId as player_id",
            "max_level"
        )
    )

    sku_details = (
        applicationsession
        .filter("activedlc IS NOT NULL")
        .groupBy(F.col("playerPublicId").alias("player_id"))
        .agg(F.expr("array_join(collect_list(activedlc), ',') AS entitlements"))
        .selectExpr(
            "player_id",
            """
            CASE
                WHEN entitlements ILIKE '%SundayRedPack%' 
                AND entitlements ILIKE '%LegendsChoicePack%' 
                AND entitlements ILIKE '%MembersPass%' 
                AND entitlements ILIKE '%AdidasButterPack%' 
                AND entitlements ILIKE '%BirdiePack%' THEN 'Legend Edition'
                WHEN entitlements ILIKE '%AdidasButterPack%' 
                AND entitlements ILIKE '%BirdiePack%' THEN 'Deluxe Edition'
                WHEN entitlements ILIKE '%StandardEdition%' THEN 'Standard Edition'
                ELSE 'Standard Edition'
            END AS sku
            """,
            "CASE WHEN sku IN ('Legend Edition', 'Deluxe Edition') THEN TRUE ELSE FALSE END AS sku_is_premium"
        )
    )
    proaccelerator_df = (
        applicationsession
        .filter("activedlc IS NOT NULL")
        .groupBy(F.col("playerPublicId").alias("player_id"))
        .agg(F.expr("array_join(collect_list(activedlc), ',') AS entitlements"))
        .selectExpr(
        "player_id",
        """
        CASE
            WHEN entitlements ILIKE '%ProAcceleratorPack%' THEN 'ProAcceleratorPack'
            ELSE 'not there'
        END AS pro_accelerator
        """,
        """
        CASE 
            WHEN entitlements ILIKE '%ProAcceleratorPack%' THEN true
            ELSE false
        END AS pro_accelerator_dlc_owned
        """
    )
    .filter(expr("entitlements ILIKE '%ProAccelerator%'"))
    .filter(expr("player_id is not null "))
    .distinct()
    )

    career_diff = (
        roundstatus
        .where(
            (col("playerPublicId").isNotNull()) &
            (lower(col("mode")).like("%career%"))
        )
        .selectExpr(
            "playerPublicId as player_id",
            "FIRST_VALUE(difficulty) OVER (PARTITION BY playerPublicId ORDER BY CAST(receivedOn AS TIMESTAMP) DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_difficulty"
        )
        .distinct()
    )

    multiplier = (
        roundstatus
        .where(
            (col("playerPublicId").isNotNull()) &
            (col("playerPublicId") != "")
        )
        .selectExpr(
            "playerPublicId as player_id",
            "FIRST_VALUE(difficultymultiplier) OVER (PARTITION BY playerPublicId ORDER BY CAST(receivedOn AS TIMESTAMP) DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as difficulty_multiplier"
        )
        .distinct()
    )

    window_spec_ovr = Window.partitionBy("player_id", "subject_id") \
        .orderBy(F.col("received_on").cast("date").desc(), F.col("extra_info_2").desc())

    window_spec_attr = Window.partitionBy("player_id", "subject_id") \
        .orderBy(F.col("received_on").cast("date").desc(), F.col("extra_info_2").desc())

    subquery = (
        fact_player_progression
        .where(F.col("subject_id").isin("attributepointsremaining", "ovr"))
        .select(
            "player_id",
            F.first(F.when(F.col("subject_id") == "ovr", F.col("extra_info_2").cast("double")))
            .over(window_spec_ovr).alias("ovr_level"),
            F.first(F.when(F.col("subject_id") == "attributepointsremaining", F.col("extra_info_2").cast("double")))
            .over(window_spec_attr).alias("att_points_balance")
        )
        .distinct()
    )

    custom_values = (
        subquery
        .groupBy("player_id")
        .agg(
            F.round(F.sum(F.col("ovr_level")), 1).alias("ovr_level"),
            F.round(F.sum(F.col("att_points_balance")), 1).alias("att_points_balance")
        )
        .selectExpr(
            "player_id",
            "ovr_level",
            "att_points_balance"
        )
    )

    played_other_titles = (
        franchise_pga.alias("p")
        .join(franchise_wwe.alias("w"), F.col("w.puid") == F.col("p.puid"), "left")
        .join(franchise_nba.alias("n"), F.col("n.puid") == F.col("p.puid"), "left")
        .join(franchise_tsn.alias("t"), F.col("t.puid") == F.col("p.puid"), "left")
        .groupBy("p.puid")
        .agg(
            F.expr(
                """
                max(
                    IF(
                        pga_has_played_PGA2K21
                        OR pga_has_played_PGA2K23
                        OR WWE_HAS_PLAYED_WWE2K24
                        OR N2K_HAS_PLAYED_NBA2K25
                        OR TSN_HAS_PLAYED_TSN2K25,
                        TRUE,
                        FALSE
                    )
                ) AS has_played_other_titles
                """
            )
        )
    )

    subquery1 = (
        linkevent.alias("le")
        .join(
            dim_title.alias("t"),
            (col("le.appId") == col("t.app_id")) &
            (lower(col("t.title")).like("%2k portal%")) &
            (col("le.firstpartyId").isNotNull()) &
            (~col("le.firstpartyId").like("%@%"))
        )
        .where(
            (col("le.accounttype") == 2) &
            (col("le.targetaccounttype") == 3) &
            (col("le.firstPartyId") != "None")
        )
        .selectExpr(
            "le.accountid as platform_id",
            f"dataanalytics{environment}.cdp_ng.fpid_decryption(le.firstpartyId) as puid"
        )
        .distinct()
    )

    subquery2 = (
        linkevent.alias("le")
        .join(
            dim_title.alias("t"),
            (col("le.appId") == col("t.app_id")) &
            (lower(col("t.title")).like("%2k portal%")) &
            (col("le.firstpartyId").isNotNull()) &
            (~col("le.firstpartyId").like("%@%"))
        )
        .where(
            (col("le.accounttype") == 3) &
            (col("le.targetaccounttype") == 2) &
            (col("le.firstPartyId") != "None")
        )
        .selectExpr(
            "le.targetaccountId as platform_id",
            f"dataanalytics{environment}.cdp_ng.fpid_decryption(le.firstpartyId) as puid"
        )
        .distinct()
    )

    subquery3 = (
        fact_player_activity.alias("f")
        .join(
            vw_ctp_profile.alias("le"),
            lower(col("f.player_id")) == lower(col("le.public_id"))
        )
        .where(col("le.first_party_id").isNotNull())
        .selectExpr(
            "f.player_id as platform_id",
            "le.first_party_id as puid"
        )
        .distinct()
    )

    fpids = subquery1.union(subquery2).union(subquery3).distinct()

    joined_dfs = ap.join(fp, expr("ap.playerpublicid = fp.player_id"), "inner")

    custom_grp_df = (
        joined_dfs
        .groupBy("playerPublicId")
        .agg(
            expr("MAX(CASE WHEN activedlc ILIKE '%SundayRedPack%' THEN TRUE ELSE FALSE END) AS has_sunday_red_pack"),
            expr("MAX(CASE WHEN activedlc ILIKE '%MembersPass%' THEN TRUE ELSE FALSE END) AS has_members_pass_pack"),
            expr(
                "MAX(CASE WHEN activedlc ILIKE '%AdidasButterPack%' THEN TRUE ELSE FALSE END) AS has_adidas_butter_pack"),
            expr("MAX(CASE WHEN SKU_TYPE = 'Pack' THEN TRUE ELSE FALSE END) AS has_starter_pack")
        )
        .withColumnRenamed("playerPublicId", "player_id")
    )

    all_metrics = (
        fact_players_ltd.alias("f")
        .join(
            fpids.alias("fpd"),
            lower(col("fpd.platform_id")) == lower(col("f.player_id"))
        )
        .join(
            fact_player_metrics.alias("m"),
            lower(col("f.player_id")) == lower(col("m.player_id")),
            how="left"
        )
        .join(
            fact_player_activity_df.alias("a"),
            lower(col("f.player_id")) == lower(col("a.player_id")),
            how="left"
        )
        .join(
            vc_metrics.alias("vcm"),
            lower(col("f.player_id")) == lower(col("vcm.player_id")),
            how="left"
        )
        .join(
            clubhousepass_level.alias("cp_lvl"),
            lower(col("f.player_id")) == lower(col("cp_lvl.player_id")),
            how="left"
        )
        .join(
            custom_values.alias("cv"),
            lower(col("f.player_id")) == lower(col("cv.player_id")),
            how="left"
        )
        .join(
            sku_details.alias("sku"),
            lower(col("f.player_id")) == lower(col("sku.player_id")),
            how="left"
        )
        .join(
            proaccelerator_df.alias("pro"),
            lower(col("f.player_id")) == lower(col("pro.player_id")),
            how="left"
        )
        .join(
            career_diff.alias("cd"),
            lower(col("f.player_id")) == lower(col("cd.player_id")),
            how="left"
        )
        .join(
            multiplier.alias("mul"),
            lower(col("f.player_id")) == lower(col("mul.player_id")),
            how="left"
        )
        .join(
            played_other_titles.alias("pot"),
            lower(col("fpd.puid")) == lower(col("pot.puid")),
            how="left"
        )
        .join(
            custom_grp_df.alias("cvd"),
            lower(col("f.player_id")) == lower(col("cvd.player_id")),
            how="left"
        )
        .groupBy(
            lower(col("f.player_id")).alias("platform_account_id"),
            col("fpd.puid"),
            col("f.vendor"),
            col("f.service")
        )
        .agg(
            max(col("f.current_gen_platform")).alias("current_gen_platform"),
            expr("CASE WHEN MAX(f.next_gen_platform) = 'Windows' THEN 'PC' ELSE MAX(f.next_gen_platform) END").alias(
                "next_gen_platform"),
            to_date(max(col("f.last_seen_date").cast("timestamp"))).alias("last_seen_date"),
            to_date(min(col("f.install_date").cast("timestamp"))).alias("install_date"),
            max(expr("COALESCE(sku.sku_is_premium, FALSE)")).alias("sku_is_premium"),
            max(col("f.is_preorder")).alias("is_preorder"),
            max(col("f.is_ftp_install")).alias("is_ftp_install"),
            max(expr("COALESCE(f.last_country_code, 'ZZ')")).alias("country_last"),
            max(expr("COALESCE(a.active_days, 0)")).alias("active_days"),
            max(expr("COALESCE(m.games_played, 0)")).alias("games_played"),
            max(expr("COALESCE(f.games_success, 0)")).alias("games_success"),
            max(expr("COALESCE(f.games_fail, 0)")).alias("games_fail"),
            max(expr("COALESCE(f.mycareer_games_success, 0)")).alias("mycareer_games_success"),
            max(expr("COALESCE(m.mycareer_days_active, 0)")).alias("mycareer_days_active"),
            min(col("m.mycareer_first_seen")).alias("mycareer_first_seen"),
            max(col("m.mycareer_last_seen")).alias("mycareer_last_seen"),
            max(expr("COALESCE(m.mycareer_games_played, 0)")).alias("mycareer_games_played"),
            min(col("m.ranked_tours_first_seen")).alias("ranked_tours_first_seen"),
            max(col("m.ranked_tours_last_seen")).alias("ranked_tours_last_seen"),
            max(expr("COALESCE(m.ranked_tours_games_played, 0)")).alias("ranked_tours_games_played"),
            max(expr("COALESCE(m.ranked_tours_days_active, 0)")).alias("ranked_tours_days_active"),
            min(col("vcm.vc_bought_first_date")).alias("vc_bought_first_date"),
            max(col("vcm.vc_bought_last_date")).alias("vc_bought_last_date"),
            max(expr("COALESCE(vcm.bought_vc_dollar_value, 0)")).alias("bought_vc_dollar_value"),
            max(expr("COALESCE(vcm.vc_bought_total, 0)")).alias("vc_bought_total"),
            max(expr("COALESCE(vcm.bought_vc_transactions, 0)")).alias("bought_vc_transactions"),
            min(col("vcm.vc_spent_first_date")).alias("vc_spent_first_date"),
            max(col("vcm.vc_spent_last_date")).alias("vc_spent_last_date"),
            max(expr("COALESCE(vcm.vc_spent_total, 0)")).alias("vc_spent_total"),
            max(expr("COALESCE(vcm.vc_earned_total, 0)")).alias("vc_earned_total"),
            max(expr("COALESCE(vcm.bought_vc_converted, FALSE)")).alias("bought_vc_converted"),
            max(expr("COALESCE(vcm.current_vc_balance, 0)")).alias("current_vc_balance"),
            max(expr("cp_lvl.max_level")).alias("max_level"),
            first(col("cv.att_points_balance"), ignorenulls=True).alias("att_points_balance"),
            first(col("cv.ovr_level"), ignorenulls=True).alias("ovr_level"),
            first(col("sku.sku"), ignorenulls=True).alias("sku"),
            max(col("pot.has_played_other_titles")).alias("other_titles"),
            first(col("cd.last_difficulty"), ignorenulls=True).alias("last_difficulty"),
            first(col("mul.difficulty_multiplier"), ignorenulls=True).alias("difficulty_multiplier"),
            max(expr("COALESCE(cvd.has_sunday_red_pack, FALSE)")).alias("has_sunday_red_pack"),
            max(expr("COALESCE(cvd.has_members_pass_pack, FALSE)")).alias("has_members_pass_pack"),
            max(expr("COALESCE(cvd.has_adidas_butter_pack, FALSE)")).alias("has_adidas_butter_pack"),
            max(expr("COALESCE(cvd.has_starter_pack, FALSE)")).alias("has_starter_pack"),
            max(expr("COALESCE(pro.pro_accelerator_dlc_owned, FALSE)")).alias("pro_accelerator_dlc_owned")
        )
        .selectExpr(
            "platform_account_id",
            "puid",
            "vendor",
            "service",
            "current_gen_platform",
            "next_gen_platform",
            "last_seen_date",
            "install_date",
            "sku_is_premium",
            "is_preorder",
            "is_ftp_install",
            "country_last",
            "active_days",
            "games_played",
            "games_success",
            "games_fail",
            "mycareer_games_success",
            "mycareer_days_active",
            "mycareer_first_seen",
            "mycareer_last_seen",
            "mycareer_games_played",
            "ranked_tours_first_seen",
            "ranked_tours_last_seen",
            "ranked_tours_games_played",
            "ranked_tours_days_active",
            "vc_bought_first_date",
            "vc_bought_last_date",
            "bought_vc_dollar_value",
            "vc_bought_total",
            "bought_vc_transactions",
            "vc_spent_first_date",
            "vc_spent_last_date",
            "vc_spent_total",
            "vc_earned_total",
            "bought_vc_converted",
            "current_vc_balance",
            "max_level",
            "att_points_balance",
            "ovr_level",
            "sku",
            "other_titles",
            "last_difficulty",
            "difficulty_multiplier",
            "has_sunday_red_pack",
            "has_members_pass_pack",
            "has_adidas_butter_pack",
            "has_starter_pack",
            "pro_accelerator_dlc_owned"
        )
        .distinct()
    )

    final_df = (
        all_metrics
        .selectExpr(
            f"vendor || ':' || dataanalytics{environment}.cdp_ng.salted_puid(puid) as brand_firstpartyid",
            f"dataanalytics{environment}.cdp_ng.salted_puid(puid) as salted_puid",
            "puid",
            "vendor as pga2k25_vendor",
            "service as pga2k25_service",
            "current_gen_platform as pga2k25_current_gen_platform",
            "next_gen_platform as pga2k25_next_gen_platform",
            "country_last as pga2k25_country_last",
            "install_date as pga2k25_install_date",
            "cast(null as string) as pga2k25_install_date_grouping",
            "cast(null as string) as pga2k25_viable",
            "cast(null as string) as pga2k25_viable_date",
            "sku_is_premium as pga2k25_premium_sku",
            "is_preorder as pga2k25_is_pre_order",
            "cast(null as string) as pga2k25_expansion_pack_1",
            "cast(null as string) as pga2k25_expansion_pack_2",
            "cast(null as string) as pga2k25_expansion_pack_3",
            "cast(null as string) as pga2k25_expansion_pack_4",
            "cast(null as string) as pga2k25_price_known",
            "cast(null as string) as pga2k25_price_probabilistic",
            "is_ftp_install as pga2k25_is_ftp_install",
            "cast(last_seen_date as date) as pga2k25_last_seen_date",
            """case
                when datediff(current_date, last_seen_date) + 1 between 0 and 13 then '0-13'
                when datediff(current_date, last_seen_date) + 1 between 14 and 20 then '14-20'
                when datediff(current_date, last_seen_date) + 1 between 21 and 40 then '21-40'
                when datediff(current_date, last_seen_date) + 1 > 40 then '40+'
                else '0-13'
            end as pga2k25_last_seen_grouping""",
            "cast(null as string) as pga2k25_player_type",
            "active_days as pga2k25_days_active",
            "games_played as pga2k25_games_played",
            "cast(null as string) as pga2k25_time_spent",
            "games_success as pga2k25_games_success",
            "games_fail as pga2k25_games_fail",
            "round((games_success / nullif(games_fail,0)) * 100, 2) as pga2k25_success_rate",
            "cast(null as string) as pga2k25_days_active_grouping",
            "cast(null as string) as pga2k25_games_played_grouping",
            "cast(null as string) as pga2k25_success_rate_grouping",
            "cast(null as string) as pga2k25_game_progress_grouping",
            "cast(null as string) as pga2k25_mode_preference",
            "cast(null as string) as pga2k25_character_preference",
            "vc_bought_total > 0 as pga2k25_bought_vc_converted",
            "bought_vc_transactions as pga2k25_bought_vc_transactions",
            "cast(null as string) as pga2k25_bought_vc_transactions_grouping",
            "cast(vc_bought_total as decimal(18,2)) as pga2k25_bought_vc_total",
            "cast(null as string) as pga2k25_bought_vc_grouping",
            "cast(vc_bought_first_date as date) as pga2k25_bought_vc_first_date",
            "cast(null as string) as pga2k25_bought_vc_first_date_grouping",
            "cast(vc_bought_last_date as date) as pga2k25_bought_vc_last_date",
            "cast(null as string) as pga2k25_bought_vc_last_date_grouping",
            "cast(bought_vc_dollar_value as decimal(18,2)) as pga2k25_bought_vc_dollar_value",
            "cast(null as string) as pga2k25_bought_vc_dollar_grouping",
            "cast(null as string) as pga2k25_earned_bought_gifted_ratio_grouping",
            "cast(null as string) as pga2k25_has_earned_vc",
            "cast(null as string) as pga2k25_earned_vc_total",
            "cast(null as string) as pga2k25_earned_vc_grouping",
            "cast(null as string) as pga2k25_earned_vc_first_date",
            "cast(null as string) as pga2k25_earned_vc_first_date_grouping",
            "cast(null as string) as pga2k25_earned_vc_last_date",
            "cast(null as string) as pga2k25_earned_vc_actions",
            "cast(null as string) as pga2k25_earned_vc_actions_grouping",
            "cast(null as string) as pga2k25_has_granted_vc",
            "cast(null as string) as pga2k25_granted_vc_total",
            "cast(null as string) as pga2k25_granted_vc_grouping",
            "cast(null as string) as pga2k25_granted_vc_last_date",
            "cast(null as string) as pga2k25_granted_vc_first_date",
            "cast(null as string) as pga2k25_granted_vc_first_date_grouping",
            "cast(null as string) as pga2k25_granted_vc_transactions",
            "cast(null as string) as pga2k25_granted_vc_transactions_grouping",
            "cast(null as string) as pga2k25_has_gifted_vc",
            "cast(null as string) as pga2k25_gifted_vc_total",
            "cast(null as string) as pga2k25_gifted_vc_grouping",
            "cast(null as string) as pga2k25_gifted_vc_last_date",
            "cast(null as string) as pga2k25_gifted_vc_first_date",
            "cast(null as string) as pga2k25_gifted_vc_first_date_grouping",
            "cast(null as string) as pga2k25_gifted_vc_transactions",
            "cast(null as string) as pga2k25_gifted_vc_transactions_grouping",
            "vc_spent_total > 0 as pga2k25_has_spent_vc",
            "vc_spent_total as pga2k25_vc_spent_total",
            "cast(null as string) as pga2k25_vc_spent_grouping",
            "vc_spent_first_date as pga2k25_vc_spent_first_date",
            "cast(null as string) as pga2k25_vc_spent_first_date_grouping",
            "vc_spent_last_date as pga2k25_vc_spent_last_date",
            "cast(null as string) as pga2k25_vc_spent_actions",
            "cast(null as string) as pga2k25_vc_spent_actions_grouping",
            "vc_bought_total - vc_spent_total + vc_earned_total as pga2k25_current_vc_balance",
            "cast(null as string) as pga2k25_current_vc_balance_grouping",
            "ranked_tours_first_seen as pga2k25_ranked_tours_first_seen",
            "cast(null as string) as pga2k25_ranked_tours_first_seen_grouping",
            "ranked_tours_last_seen as pga2k25_ranked_tours_last_seen",
            """case
                when datediff(current_date, ranked_tours_last_seen) + 1 between 0 and 13 then '0-13'
                when datediff(current_date, ranked_tours_last_seen) + 1 between 14 and 20 then '14-20'
                when datediff(current_date, ranked_tours_last_seen) + 1 between 21 and 40 then '21-40'
                when datediff(current_date, ranked_tours_last_seen) + 1 > 40 then '40+'
                else '0-13'
            end as pga2k25_ranked_tours_last_seen_grouping""",
            "ranked_tours_days_active as pga2k25_ranked_tours_days_active",
            "ranked_tours_games_played as pga2k25_ranked_tours_games_played",
            "cast(null as string) as pga2k25_ranked_tours_time_spent",
            "cast(null as string) as pga2k25_ranked_tours_games_success",
            "cast(null as string) as pga2k25_ranked_tours_games_fail",
            "cast(null as string) as pga2k25_ranked_tours_success_rate",
            "cast(null as string) as pga2k25_ranked_tours_success_rate_grouping",
            "cast(null as string) as pga2k25_ranked_tours_days_active_grouping",
            "cast(null as string) as pga2k25_ranked_tours_games_played_grouping",
            "mycareer_first_seen as pga2k25_mycareer_first_seen",
            "cast(null as string) as pga2k25_mycareer_first_seen_grouping",
            "mycareer_last_seen as pga2k25_mycareer_last_seen",
            """case
                when datediff(current_date, mycareer_last_seen) + 1 between 0 and 13 then '0-13'
                when datediff(current_date, mycareer_last_seen) + 1 between 14 and 20 then '14-20'
                when datediff(current_date, mycareer_last_seen) + 1 between 21 and 40 then '21-40'
                when datediff(current_date, mycareer_last_seen) + 1 > 40 then '40+'
                else '0-13'
            end as pga2k25_mycareer_last_seen_grouping""",
            "mycareer_days_active as pga2k25_mycareer_days_active",
            "mycareer_games_played as pga2k25_mycareer_games_played",
            "cast(null as string) as pga2k25_mycareer_time_spent",
            "mycareer_games_success as pga2k25_mycareer_games_success",
            "cast(null as string) as pga2k25_mycareer_games_fail",
            "cast(null as string) as pga2k25_mycareer_success_rate",
            "cast(null as string) as pga2k25_mycareer_success_rate_grouping",
            "cast(null as string) as pga2k25_mycareer_days_active_grouping",
            "cast(null as string) as pga2k25_mycareer_games_played_grouping",
            "cast(null as string) as pga2k25_ranked_tours_has_earned_vc",
            "cast(null as string) as pga2k25_ranked_tours_earned_vc_total",
            "cast(null as string) as pga2k25_ranked_tours_earned_vc_grouping",
            "cast(null as string) as pga2k25_ranked_tours_earned_vc_first_date",
            "cast(null as string) as pga2k25_ranked_tours_earned_first_date_grouping",
            "cast(null as string) as pga2k25_ranked_tours_earned_vc_last_date",
            "cast(null as string) as pga2k25_ranked_tours_earned_vc_transactions",
            "cast(null as string) as pga2k25_ranked_tours_vc_spent_total",
            "cast(null as string) as pga2k25_ranked_tours_vc_spent_grouping",
            "cast(null as string) as pga2k25_ranked_tours_vc_spent_actions",
            "cast(null as string) as pga2k25_ranked_tours_vc_spent_first_date",
            "cast(null as string) as pga2k25_ranked_tours_vc_spent_first_date_grouping",
            "cast(null as string) as pga2k25_ranked_tours_vc_spent_last_date",
            "cast(null as string) as pga2k25_mycareer_has_earned_vc",
            "cast(null as string) as pga2k25_mycareer_earned_vc_total",
            "cast(null as string) as pga2k25_mycareer_earned_vc_grouping",
            "cast(null as string) as pga2k25_mycareer_earned_vc_transactions",
            "cast(null as string) as pga2k25_mycareer_earned_vc_first_date",
            "cast(null as string) as pga2k25_mycareer_earned_first_date_grouping",
            "cast(null as string) as pga2k25_mycareer_earned_vc_last_date",
            "cast(null as string) as pga2k25_mycareer_vc_spent_total",
            "cast(null as string) as pga2k25_mycareer_vc_spent_actions",
            "cast(null as string) as pga2k25_mycareer_vc_spent_first_date",
            "cast(null as string) as pga2k25_mycareer_vc_spent_first_date_grouping",
            "cast(null as string) as pga2k25_mycareer_vc_spent_last_date",
            "cast(coalesce(sku, 'Standard Edition') as string) as pga2k25_custom_grouping_01",
            "cast(coalesce(other_titles, false) as boolean) as pga2k25_custom_grouping_02",
            "cast(ovr_level as string) as pga2k25_custom_grouping_03",
            "cast(null as string) as pga2k25_custom_grouping_04",
            "cast(last_difficulty as string) as pga2k25_custom_grouping_05",
            "cast(att_points_balance as string) as pga2k25_custom_grouping_06",
            "cast(difficulty_multiplier as string) as pga2k25_custom_grouping_07",
            "cast(has_sunday_red_pack as boolean) as pga2k25_custom_grouping_08",
            "cast(has_members_pass_pack as boolean) pga2k25_custom_grouping_09",
            "cast(has_adidas_butter_pack as boolean) pga2k25_custom_grouping_10",
            "cast(has_starter_pack as boolean) pga2k25_custom_grouping_11",
            "cast(max_level as string) as pga2k25_custom_grouping_12",
            "cast(pro_accelerator_dlc_owned as boolean) as pga2k25_custom_grouping_13",
            "cast(null as string) as pga2k25_custom_grouping_14",
            "cast(null as string) as pga2k25_custom_grouping_15",
            "cast(null as string) as pga2k25_custom_grouping_16",
            "cast(null as string) as pga2k25_custom_grouping_17",
            "cast(null as string) as pga2k25_custom_grouping_18",
            "cast(null as string) as pga2k25_custom_grouping_19",
            "cast(null as string) as pga2k25_custom_grouping_20",
            "cast(null as string) as pga2k25_custom_value_01",
            "cast(null as string) as pga2k25_custom_value_02",
            "cast(null as string) as pga2k25_custom_value_03",
            "cast(null as string) as pga2k25_custom_value_04",
            "cast(null as string) as pga2k25_custom_value_05",
            "cast(null as string) as pga2k25_custom_value_06",
            "cast(null as string) as pga2k25_custom_value_07",
            "cast(null as string) as pga2k25_custom_value_08",
            "cast(null as string) as pga2k25_custom_value_09",
            "cast(null as string) as pga2k25_custom_value_10"
        )
    )

    return final_df

# COMMAND ----------

def create_pga2k25_summary_view(environment, spark):
    table_name = f"dataanalytics{environment}.cdp_ng.pga2k25_summary"
    df = spark.table(table_name)

    excluded_columns = {'dw_insert_ts', 'dw_update_ts', 'merge_key'}
    selected_columns = [col for col in df.columns if col not in excluded_columns]
    selected_columns_str = ",\n    ".join(selected_columns)

    sql = f"""
    CREATE OR REPLACE VIEW dataanalytics{environment}.cdp_ng.vw_pga2k25_summary AS (
        SELECT
            {selected_columns_str}
        FROM {table_name}
    )
    """
    spark.sql(sql)

# COMMAND ----------

def load(spark, df, database, environment):
    # Perform the merge operation into the Delta table
    final_table = DeltaTable.forName(spark, f"{database}{environment}.cdp_ng.pga2k25_summary")

    # get schema of the table
    table_schema = spark.read.table(f"{database}{environment}.cdp_ng.pga2k25_summary").schema

    # create empty dataframe using the schema
    out_df = spark.createDataFrame([], table_schema)

    # union the incoming dataframe with the empty dataframe, we can set allowMissingColumns to False to be more strict
    df = df.selectExpr(
        "*",
        "CURRENT_TIMESTAMP() as dw_insert_ts",
        "CURRENT_TIMESTAMP() as dw_update_ts",
        "SHA2(CONCAT_WS('|', brand_firstpartyid, salted_puid, puid), 256) as merge_key"
    )
    out_df = out_df.unionByName(df, allowMissingColumns=True)

    # dynamically setup merge condition based on the aggregate columns in this database's fact_player_session_daily table
    agg_cols = [col_name for col_name in out_df.columns if
                col_name not in ['dw_insert_ts', 'dw_update_ts', 'merge_key']]
    merge_condition = " OR ".join(f"old.{col_name} <> new.{col_name}" for col_name in agg_cols)

    # create the update column dict
    update_set = {}
    for col_name in agg_cols:
        update_set[f"old.{col_name}"] = f"new.{col_name}"

    update_set[f"old.dw_update_ts"] = "CURRENT_TIMESTAMP()"

    # merge the table
    (
        final_table.alias('old')
        .merge(
            out_df.alias('new'),
            "new.merge_key = old.merge_key"
        )
        .whenMatchedUpdate(condition=merge_condition, set=update_set)
        .whenNotMatchedInsertAll()
        .execute()
    )

    create_pga2k25_summary_view(environment, spark)
    print("view created successfully.")

# COMMAND ----------

def create_pga2k25_summary_table(spark, database):
    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.cdp_ng.pga2k25_summary (
        brand_firstpartyid STRING,
        salted_puid STRING,
        puid STRING,
        pga2k25_vendor STRING,
        pga2k25_service STRING,
        pga2k25_current_gen_platform STRING,
        pga2k25_next_gen_platform STRING,
        pga2k25_country_last STRING,
        pga2k25_install_date DATE,
        pga2k25_install_date_grouping STRING,
        pga2k25_viable STRING,
        pga2k25_viable_date STRING,
        pga2k25_premium_sku BOOLEAN,
        pga2k25_is_pre_order BOOLEAN,
        pga2k25_expansion_pack_1 STRING,
        pga2k25_expansion_pack_2 STRING,
        pga2k25_expansion_pack_3 STRING,
        pga2k25_expansion_pack_4 STRING,
        pga2k25_price_known STRING,
        pga2k25_price_probabilistic STRING,
        pga2k25_is_ftp_install INT,
        pga2k25_last_seen_date DATE,
        pga2k25_last_seen_grouping STRING,
        pga2k25_player_type STRING,
        pga2k25_days_active BIGINT,
        pga2k25_games_played BIGINT,
        pga2k25_time_spent STRING,
        pga2k25_games_success INT,
        pga2k25_games_fail INT,
        pga2k25_success_rate INT,
        pga2k25_days_active_grouping STRING,
        pga2k25_games_played_grouping STRING,
        pga2k25_success_rate_grouping STRING,
        pga2k25_game_progress_grouping STRING,
        pga2k25_mode_preference STRING,
        pga2k25_character_preference STRING,
        pga2k25_bought_vc_converted BOOLEAN,
        pga2k25_bought_vc_transactions INT,
        pga2k25_bought_vc_transactions_grouping STRING,
        pga2k25_bought_vc_total DECIMAL(18, 2),
        pga2k25_bought_vc_grouping STRING,
        pga2k25_bought_vc_first_date DATE,
        pga2k25_bought_vc_first_date_grouping STRING,
        pga2k25_bought_vc_last_date DATE,
        pga2k25_bought_vc_last_date_grouping STRING,
        pga2k25_bought_vc_dollar_value DECIMAL(18, 2),
        pga2k25_bought_vc_dollar_grouping STRING,
        pga2k25_earned_bought_gifted_ratio_grouping STRING,
        pga2k25_has_earned_vc STRING,
        pga2k25_earned_vc_total STRING,
        pga2k25_earned_vc_grouping STRING,
        pga2k25_earned_vc_first_date STRING,
        pga2k25_earned_vc_first_date_grouping STRING,
        pga2k25_earned_vc_last_date STRING,
        pga2k25_earned_vc_actions STRING,
        pga2k25_earned_vc_actions_grouping STRING,
        pga2k25_has_granted_vc STRING,
        pga2k25_granted_vc_total STRING,
        pga2k25_granted_vc_grouping STRING,
        pga2k25_granted_vc_last_date STRING,
        pga2k25_granted_vc_first_date STRING,
        pga2k25_granted_vc_first_date_grouping STRING,
        pga2k25_granted_vc_transactions STRING,
        pga2k25_granted_vc_transactions_grouping STRING,
        pga2k25_has_gifted_vc STRING,
        pga2k25_gifted_vc_total STRING,
        pga2k25_gifted_vc_grouping STRING,
        pga2k25_gifted_vc_last_date STRING,
        pga2k25_gifted_vc_first_date STRING,
        pga2k25_gifted_vc_first_date_grouping STRING,
        pga2k25_gifted_vc_transactions STRING,
        pga2k25_gifted_vc_transactions_grouping STRING,
        pga2k25_has_spent_vc BOOLEAN,
        pga2k25_vc_spent_total DECIMAL(38, 2),
        pga2k25_vc_spent_grouping STRING,
        pga2k25_vc_spent_first_date DATE,
        pga2k25_vc_spent_first_date_grouping STRING,
        pga2k25_vc_spent_last_date DATE,
        pga2k25_vc_spent_actions STRING,
        pga2k25_vc_spent_actions_grouping STRING,
        pga2k25_current_vc_balance DECIMAL(38, 2),
        pga2k25_current_vc_balance_grouping STRING,
        pga2k25_ranked_tours_first_seen DATE,
        pga2k25_ranked_tours_first_seen_grouping STRING,
        pga2k25_ranked_tours_last_seen DATE,
        pga2k25_ranked_tours_last_seen_grouping STRING,
        pga2k25_ranked_tours_days_active BIGINT,
        pga2k25_ranked_tours_games_played BIGINT,
        pga2k25_ranked_tours_time_spent STRING,
        pga2k25_ranked_tours_games_success STRING,
        pga2k25_ranked_tours_games_fail STRING,
        pga2k25_ranked_tours_success_rate STRING,
        pga2k25_ranked_tours_success_rate_grouping STRING,
        pga2k25_ranked_tours_days_active_grouping STRING,
        pga2k25_ranked_tours_games_played_grouping STRING,
        pga2k25_mycareer_first_seen DATE,
        pga2k25_mycareer_first_seen_grouping STRING,
        pga2k25_mycareer_last_seen DATE,
        pga2k25_mycareer_last_seen_grouping STRING,
        pga2k25_mycareer_days_active BIGINT,
        pga2k25_mycareer_games_played BIGINT,
        pga2k25_mycareer_time_spent STRING,
        pga2k25_mycareer_games_success STRING,
        pga2k25_mycareer_games_fail STRING,
        pga2k25_mycareer_success_rate STRING,
        pga2k25_mycareer_success_rate_grouping STRING,
        pga2k25_mycareer_days_active_grouping STRING,
        pga2k25_mycareer_games_played_grouping STRING,
        pga2k25_ranked_tours_has_earned_vc STRING,
        pga2k25_ranked_tours_earned_vc_total STRING,
        pga2k25_ranked_tours_earned_vc_grouping STRING,
        pga2k25_ranked_tours_earned_vc_first_date STRING,
        pga2k25_ranked_tours_earned_first_date_grouping STRING,
        pga2k25_ranked_tours_earned_vc_last_date STRING,
        pga2k25_ranked_tours_earned_vc_transactions STRING,
        pga2k25_ranked_tours_vc_spent_total STRING,
        pga2k25_ranked_tours_vc_spent_grouping STRING,
        pga2k25_ranked_tours_vc_spent_actions STRING,
        pga2k25_ranked_tours_vc_spent_first_date STRING,
        pga2k25_ranked_tours_vc_spent_first_date_grouping STRING,
        pga2k25_ranked_tours_vc_spent_last_date STRING,
        pga2k25_mycareer_has_earned_vc STRING,
        pga2k25_mycareer_earned_vc_total STRING,
        pga2k25_mycareer_earned_vc_grouping STRING,
        pga2k25_mycareer_earned_vc_transactions STRING,
        pga2k25_mycareer_earned_vc_first_date STRING,
        pga2k25_mycareer_earned_first_date_grouping STRING,
        pga2k25_mycareer_earned_vc_last_date STRING,
        pga2k25_mycareer_vc_spent_total STRING,
        pga2k25_mycareer_vc_spent_actions STRING,
        pga2k25_mycareer_vc_spent_first_date STRING,
        pga2k25_mycareer_vc_spent_first_date_grouping STRING,
        pga2k25_mycareer_vc_spent_last_date STRING,
        pga2k25_custom_grouping_01 STRING,
        pga2k25_custom_grouping_02 BOOLEAN,
        pga2k25_custom_grouping_03 STRING,
        pga2k25_custom_grouping_04 STRING,
        pga2k25_custom_grouping_05 STRING,
        pga2k25_custom_grouping_06 STRING,
        pga2k25_custom_grouping_07 STRING,
        pga2k25_custom_grouping_08 BOOLEAN,
        pga2k25_custom_grouping_09 BOOLEAN,
        pga2k25_custom_grouping_10 BOOLEAN,
        pga2k25_custom_grouping_11 BOOLEAN,
        pga2k25_custom_grouping_12 STRING,
        pga2k25_custom_grouping_13 BOOLEAN,
        pga2k25_custom_grouping_14 STRING,
        pga2k25_custom_grouping_15 STRING,
        pga2k25_custom_grouping_16 STRING,
        pga2k25_custom_grouping_17 STRING,
        pga2k25_custom_grouping_18 STRING,
        pga2k25_custom_grouping_19 STRING,
        pga2k25_custom_grouping_20 STRING,
        pga2k25_custom_value_01 STRING,
        pga2k25_custom_value_02 STRING,
        pga2k25_custom_value_03 STRING,
        pga2k25_custom_value_04 STRING,
        pga2k25_custom_value_05 STRING,
        pga2k25_custom_value_06 STRING,
        pga2k25_custom_value_07 STRING,
        pga2k25_custom_value_08 STRING,
        pga2k25_custom_value_09 STRING,
        pga2k25_custom_value_10 STRING,
        dw_insert_ts timestamp default current_timestamp(),
        dw_update_ts timestamp default current_timestamp(),
        merge_key string
    )
    """

    properties = {
        "delta.feature.allowColumnDefaults": "supported"
    }

    create_table(spark, sql, properties)
    return f"Table pga2k25_summary created"

# COMMAND ----------

def run_batch(environment):
    """Main execution function"""

    spark = create_spark_session(name=f"{database}")
    create_pga2k25_summary_table(spark, database)

    prev_max = max_timestamp(spark, f"dataanalytics{environment}.cdp_ng.pga2k25_summary", 'dw_insert_ts')
    if prev_max is None:
        prev_max = date(1999, 1, 1)
    print(prev_max)

    (
        roundstatus,
        service,
        fact_player_activity,
        fact_player_transaction,
        clubhousepass,
        platform,
        accountdeleteevent,
        fact_player_link_activity,
        fact_player_game_status_daily,
        applicationsession,
        fact_player_progression,
        franchise_pga,
        franchise_wwe,
        franchise_nba,
        franchise_tsn,
        linkevent,
        dim_title,
        vw_ctp_profile,
        ap,
        fp
    ) = extract(spark, environment, prev_max)

    output_df = transformation(
        roundstatus,
        service,
        fact_player_activity,
        fact_player_transaction,
        clubhousepass,
        platform,
        accountdeleteevent,
        fact_player_link_activity,
        fact_player_game_status_daily,
        applicationsession,
        fact_player_progression,
        franchise_pga,
        franchise_wwe,
        franchise_nba,
        franchise_tsn,
        linkevent,
        dim_title,
        vw_ctp_profile,
        ap,
        fp,
        environment
    )

    load(spark, output_df, database, environment)

# COMMAND ----------

if __name__ == "__main__":
    run_batch(environment)