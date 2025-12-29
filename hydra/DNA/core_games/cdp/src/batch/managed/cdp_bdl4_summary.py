# Databricks notebook source


# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/table_functions

# COMMAND ----------

# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

import json
from pyspark.sql.functions import expr
from pyspark.sql.types import *
from datetime import date
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
# COMMAND ----------

def create_bdl4_summary_table(environment, spark):
    sql = f"""
    CREATE TABLE IF NOT EXISTS dataanalytics{environment}.cdp_ng.bdl4_summary (
    brand_firstpartyid                              STRING,
    salted_puid                                     STRING,
    puid                                            STRING,
    bdl_bdl4_service                                STRING,
    bdl_bdl4_vendor                                 STRING,
    bdl_bdl4_current_gen_platform                   STRING,
    bdl_bdl4_next_gen_platform                      STRING,
    bdl_bdl4_country_last                           STRING,
    bdl_bdl4_install_date                           DATE,
    bdl_bdl4_install_date_grouping                  STRING,
    bdl_bdl4_viable                                 BOOLEAN,
    bdl_bdl4_viable_date                            DATE,
    bdl_bdl4_premium_sku                            BOOLEAN,
    bdl_bdl4_is_pre_order                           BOOLEAN,
    bdl_bdl4_price_known                            FLOAT,
    bdl_bdl4_price_probabilistic                    FLOAT,
    bdl_bdl4_is_ftp_install                         BOOLEAN,
    bdl_bdl4_expansion_pack_1                       BOOLEAN,
    bdl_bdl4_expansion_pack_2                       BOOLEAN,
    bdl_bdl4_expansion_pack_3                       BOOLEAN,
    bdl_bdl4_expansion_pack_4                       BOOLEAN,
    bdl_bdl4_expansion_pack_5                       BOOLEAN,
    bdl_bdl4_expansion_pack_6                       BOOLEAN,
    bdl_bdl4_last_seen_date                         DATE,
    bdl_bdl4_last_seen_grouping                     STRING,
    bdl_bdl4_player_type_grouping                   STRING,
    bdl_bdl4_days_active                            BIGINT,
    bdl_bdl4_games_played                           BIGINT,
    bdl_bdl4_time_spent                             DOUBLE,
    bdl_bdl4_games_success                          BIGINT,
    bdl_bdl4_games_fail                             BIGINT,
    bdl_bdl4_success_rate                           DOUBLE,
    bdl_bdl4_success_rate_grouping                  STRING,
    bdl_bdl4_days_active_grouping                   STRING,
    bdl_bdl4_games_played_grouping                  STRING,
    bdl_bdl4_game_progress_grouping                 STRING,
    bdl_bdl4_mode_preference                        STRING,
    bdl_bdl4_character_preference                   STRING,
    bdl_bdl4_solo_first_seen                        DATE,
    bdl_bdl4_solo_first_seen_grouping               STRING,
    bdl_bdl4_solo_last_seen                         DATE,
    bdl_bdl4_solo_last_seen_grouping                STRING,
    bdl_bdl4_solo_days_active                       FLOAT,
    bdl_bdl4_solo_games_played                      FLOAT,
    bdl_bdl4_solo_time_spent                        FLOAT,
    bdl_bdl4_solo_games_success                     FLOAT,
    bdl_bdl4_solo_games_fail                        FLOAT,
    bdl_bdl4_solo_success_rate                      FLOAT,
    bdl_bdl4_solo_success_rate_grouping             STRING,
    bdl_bdl4_solo_days_active_grouping              STRING,
    bdl_bdl4_solo_games_played_grouping             STRING,
    bdl_bdl4_solo_has_earned_vc                     BOOLEAN,
    bdl_bdl4_solo_earned_vc_total                   FLOAT,
    bdl_bdl4_solo_earned_vc_grouping                STRING,
    bdl_bdl4_solo_earned_vc_first_date              DATE,
    bdl_bdl4_solo_earned_first_date_grouping        STRING,
    bdl_bdl4_solo_earned_vc_last_date               DATE,
    bdl_bdl4_solo_earned_vc_transactions            FLOAT,
    bdl_bdl4_solo_vc_spent_total                    FLOAT,
    bdl_bdl4_solo_vc_spent_grouping                 STRING,
    bdl_bdl4_solo_vc_spent_actions                  FLOAT,
    bdl_bdl4_solo_vc_spent_first_date               DATE,
    bdl_bdl4_solo_vc_spent_first_date_grouping      STRING,
    bdl_bdl4_solo_vc_spent_last_date                DATE,
    bdl_bdl4_multiplayer_first_seen                 DATE,
    bdl_bdl4_multiplayer_first_seen_grouping        STRING,
    bdl_bdl4_multiplayer_last_seen                  DATE,
    bdl_bdl4_multiplayer_last_seen_grouping         STRING,
    bdl_bdl4_multiplayer_days_active                FLOAT,
    bdl_bdl4_multiplayer_games_played               FLOAT,
    bdl_bdl4_multiplayer_time_spent                 FLOAT,
    bdl_bdl4_multiplayer_games_success              FLOAT,
    bdl_bdl4_multiplayer_games_fail                 FLOAT,
    bdl_bdl4_multiplayer_success_rate               FLOAT,
    bdl_bdl4_multiplayer_success_rate_grouping      STRING,
    bdl_bdl4_multiplayer_days_active_grouping       STRING,
    bdl_bdl4_multiplayer_games_played_grouping      STRING,
    bdl_bdl4_multiplayer_has_earned_vc              BOOLEAN,
    bdl_bdl4_multiplayer_earned_vc_total            FLOAT,
    bdl_bdl4_multiplayer_earned_vc_grouping         STRING,
    bdl_bdl4_multiplayer_earned_vc_first_date       DATE,
    bdl_bdl4_multiplayer_earned_first_date_grouping STRING,
    bdl_bdl4_multiplayer_earned_vc_last_date        DATE,
    bdl_bdl4_multiplayer_earned_vc_transactions     FLOAT,
    bdl_bdl4_multiplayer_vc_spent_total             FLOAT,
    bdl_bdl4_multiplayer_vc_spent_grouping          STRING,
    bdl_bdl4_multiplayer_vc_spent_actions           FLOAT,
    bdl_bdl4_multiplayer_vc_spent_first_date        DATE,
    bdl_bdl4_multiplayer_vc_spent_first_date_grouping STRING,
    bdl_bdl4_multiplayer_vc_spent_last_date         DATE,
    bdl_bdl4_online_first_seen                      DATE,
    bdl_bdl4_online_first_seen_grouping             STRING,
    bdl_bdl4_online_last_seen                       DATE,
    bdl_bdl4_online_last_seen_grouping              STRING,
    bdl_bdl4_online_days_active                     FLOAT,
    bdl_bdl4_online_games_played                    FLOAT,
    bdl_bdl4_online_time_spent                      FLOAT,
    bdl_bdl4_online_games_success                   FLOAT,
    bdl_bdl4_online_games_fail                      FLOAT,
    bdl_bdl4_online_success_rate                    FLOAT,
    bdl_bdl4_online_success_rate_grouping           STRING,
    bdl_bdl4_online_days_active_grouping            STRING,
    bdl_bdl4_online_games_played_grouping           STRING,
    bdl_bdl4_online_has_earned_vc                   BOOLEAN,
    bdl_bdl4_online_earned_vc_total                 FLOAT,
    bdl_bdl4_online_earned_vc_grouping              STRING,
    bdl_bdl4_online_earned_vc_first_date            DATE,
    bdl_bdl4_online_earned_first_date_grouping      STRING,
    bdl_bdl4_online_earned_vc_last_date             DATE,
    bdl_bdl4_online_earned_vc_transactions          FLOAT,
    bdl_bdl4_online_vc_spent_total                  FLOAT,
    bdl_bdl4_online_vc_spent_grouping               STRING,
    bdl_bdl4_online_vc_spent_actions                FLOAT,
    bdl_bdl4_online_vc_spent_first_date             DATE,
    bdl_bdl4_online_vc_spent_first_date_grouping    STRING,
    bdl_bdl4_online_vc_spent_last_date              DATE,
    bdl_bdl4_bought_vc_converted                    BOOLEAN,
    bdl_bdl4_bought_vc_transactions                 FLOAT,
    bdl_bdl4_bought_vc_transactions_grouping        STRING,
    bdl_bdl4_bought_vc_total                        FLOAT,
    bdl_bdl4_bought_vc_grouping                     STRING,
    bdl_bdl4_bought_vc_first_date                   DATE,
    bdl_bdl4_bought_vc_first_date_grouping          STRING,
    bdl_bdl4_bought_vc_last_date                    DATE,
    bdl_bdl4_bought_vc_last_date_grouping           STRING,
    bdl_bdl4_bought_vc_dollar_value                 FLOAT,
    bdl_bdl4_bought_vc_dollar_grouping              STRING,
    bdl_bdl4_earned_bought_gifted_ratio_grouping    STRING,
    bdl_bdl4_has_earned_vc                          BOOLEAN,
    bdl_bdl4_earned_vc_total                        FLOAT,
    bdl_bdl4_earned_vc_grouping                     STRING,
    bdl_bdl4_earned_vc_first_date                   DATE,
    bdl_bdl4_earned_vc_first_date_grouping          STRING,
    bdl_bdl4_earned_vc_last_date                    DATE,
    bdl_bdl4_earned_vc_actions                      FLOAT,
    bdl_bdl4_earned_vc_actions_grouping             STRING,
    bdl_bdl4_has_gifted_vc                          BOOLEAN,
    bdl_bdl4_gifted_vc_total                        FLOAT,
    bdl_bdl4_gifted_vc_grouping                     STRING,
    bdl_bdl4_gifted_vc_last_date                    DATE,
    bdl_bdl4_gifted_vc_first_date                   DATE,
    bdl_bdl4_gifted_vc_first_date_grouping          STRING,
    bdl_bdl4_gifted_vc_transactions                 FLOAT,
    bdl_bdl4_gifted_vc_transactions_grouping        STRING,
    bdl_bdl4_has_spent_vc                           BOOLEAN,
    bdl_bdl4_vc_spent_total                         FLOAT,
    bdl_bdl4_vc_spent_grouping                      STRING,
    bdl_bdl4_vc_spent_first_date                    DATE,
    bdl_bdl4_vc_spent_first_date_grouping           STRING,
    bdl_bdl4_vc_spent_last_date                     DATE,
    bdl_bdl4_vc_spent_actions                       FLOAT,
    bdl_bdl4_vc_spent_actions_grouping              STRING,
    bdl_bdl4_current_vc_balance                     FLOAT,
    bdl_bdl4_current_vc_balance_grouping            STRING,
    bdl_bdl4_custom_grouping_01                     STRING,
    bdl_bdl4_custom_grouping_02                     STRING,
    bdl_bdl4_custom_grouping_03                     STRING,
    bdl_bdl4_custom_grouping_04                     STRING,
    bdl_bdl4_custom_grouping_05                     STRING,
    bdl_bdl4_custom_grouping_06                     STRING,
    bdl_bdl4_custom_grouping_07                     STRING,
    bdl_bdl4_custom_grouping_08                     STRING,
    bdl_bdl4_custom_grouping_09                     STRING,
    bdl_bdl4_custom_grouping_10                     STRING,
    bdl_bdl4_custom_value_01                        DECIMAL(18, 2),
    bdl_bdl4_custom_value_02                        DECIMAL(18, 2),
    bdl_bdl4_custom_value_03                        DECIMAL(18, 2),
    bdl_bdl4_custom_value_04                        DECIMAL(18, 2),
    bdl_bdl4_custom_value_05                        DECIMAL(18, 2),
    bdl_bdl4_custom_value_06                        DECIMAL(18, 2),
    bdl_bdl4_custom_value_07                        DECIMAL(18, 2),
    bdl_bdl4_custom_value_08                        DECIMAL(18, 2),
    bdl_bdl4_custom_value_09                        DECIMAL(18, 2),
    bdl_bdl4_custom_value_10                        DECIMAL(18, 2),
    dw_update_ts TIMESTAMP,
    dw_insert_ts TIMESTAMP,
    merge_key STRING
    )"""

    properties = {
        "delta.feature.allowColumnDefaults": "supported"
    }

    create_table(spark, sql, properties)
    return (
        f"dbfs:/tmp/dataanalytics/cdp_ng/streaming/run{environment}/bdl4_summary"
    )

# COMMAND ----------

def read_services(spark):
    return (
        spark.read.table("reference_customer.platform.service")
        .selectExpr(
            "CASE service WHEN 'xbl' THEN 'Xbox Live' ELSE SERVICE END AS JOIN_KEY",
            "service",
            "vendor"
        ).distinct()
    )

def read_platforms(spark):
    return (
        spark.read.table("REFERENCE_CUSTOMER.PLATFORM.PLATFORM")
        .selectExpr(
            "SRC_PLATFORM",
            "CASE WHEN PLATFORM IN ('PC', 'Linux', 'OSX') THEN 'PC' ELSE PLATFORM END AS PLATFORM",
            "CASE WHEN VENDOR IN ('PC', 'Linux', 'Apple') THEN 'PC' ELSE VENDOR END AS VENDOR",
            "FIRST_VALUE(PLATFORM) OVER (PARTITION BY VENDOR ORDER BY DW_CREATE_TS DESC) as current_gen_platform"
        ).distinct()
    )

def read_fact_player_ltd(environment, spark, prev_max_date):
    return (
        spark.read.table(f"dataanalytics{environment}.standard_metrics.fact_player_ltd")
        .filter(expr(f"CAST(dw_update_ts AS DATE) >= '{prev_max_date}' - INTERVAL 3 DAY"))
        .filter(expr("title = 'Borderlands 4'"))
        .filter(expr("platform<>'Unknown' and service <> 'Unknown'"))
    )

def read_account_delete_events(environment, spark):
    return spark.read.table(f"coretech{environment}.sso.accountdeleteevent")

def read_game_status_daily(environment, spark):
    return spark.read.table(f"oak2{environment}.managed.fact_player_game_status_daily")

def read_fpids(spark):
    return spark.read.table("reference_customer.customer.vw_fpid_all")

def read_player_character(environment, spark):
    return spark.read.table(f"oak2{environment}.managed.fact_player_character")

def read_platform_id_mapping(environment, spark):
    mapping_df=(
        spark
        .read
        .table(f"oak2{environment}.reference.platform_id_mapping")
        .select(
            "maw_user_platformid",
            "player_platform_id"
        )
        .distinct()
    )
    return mapping_df

def read_country_lookup(environment, spark):
    country_lookup_df = (
        spark
        .read
        .table(f"oak2{environment}.intermediate.fact_player_activity")
        .where("source_table = 'loginevent'")
        .where("country_code IS NOT NULL AND country_code != 'ZZ'")
        .select(
            expr("extra_info_1"),
            expr("first_value(country_code) over (partition by extra_info_1 order by received_on desc) as country_code")
        )
        .distinct()
    )
    return country_lookup_df

def read_player_sku(environment, spark):

    sku_df = (
        spark
        .read
        .table(f"oak2{environment}.managed_view.fact_player_sku")
        .select(
            "player_id",
            "sku",
            "oak2_premium_sku",
            "oak2_preorder",
            "oak2_bundle_pack"
        )
        .distinct()
    )
    return sku_df

def read_player_entitlement(environment, spark):
    entitlement_df = (
        spark.read.table(f"oak2{environment}.managed.fact_player_entitlement")
        .withColumn(
            "bdl_bdl4_expansion_pack_3",
            F.expr("""
                CASE WHEN entitlement IN (
                    '60aae73e43c24dbcb7e1882ab5fd8585',
                    'BOUNTYPACK000000',
                    '44574d39-484b-304a-c048-364737423c00',
                    '3829670',
                    '3f9276596c5a4b2abea78709f014f76b',
                    'BL4BOUNTYPACK100',
                    '314a5039-354d-304d-c037-4c3735430500',
                    '1337359'
                ) THEN true ELSE false END
            """)
        )
        .selectExpr("player_id", "bdl_bdl4_expansion_pack_3")
        .distinct()
    )
    return entitlement_df

# COMMAND ----------

def transform(environment, spark, fact_player_ltd_df, platforms_df, account_delete_df, game_status_daily_df, fpid_df, player_character_df, sku_df, mapping_df, country_lookup_df, entitlement_df):

    character_preference = (
        player_character_df.alias("character")
        .join(mapping_df.alias("m"), expr("character.player_platform_id = m.player_platform_id"))
        .groupBy(expr("maw_user_platformid as player_platform_id"), "player_character")
        .agg(expr("count(*) as play_count"))
        .selectExpr(
            "*",
            "row_number() OVER (PARTITION BY player_platform_id ORDER BY play_count DESC) as rn"
        )
        .where("rn = 1")
        .selectExpr("player_platform_id", "player_character as character_preference")
    )

    fact_players_ltd = (
        fact_player_ltd_df.alias("f")
        .join(platforms_df.alias("v"), expr("f.platform = v.src_platform"))
        .join(account_delete_df.alias("sso"), expr("sso.accountId = f.player_id"), "left_anti")
        .join(country_lookup_df.alias("country"), expr("lower(f.player_id) = lower(country.extra_info_1)"), "left")
        .filter(expr("f.title = 'Borderlands 4'"))
        .select(
                expr("lower(f.player_id) as player_id"),
                expr("""
                    first_value(v.platform) over (
                        partition by f.player_id 
                        order by f.install_date, f.last_seen desc
                    ) as platform
                """),
                expr("""
                    first_value(v.vendor) over (
                        partition by f.player_id 
                        order by f.install_date, f.last_seen desc
                    ) as vendor
                """),
                expr("""
                    first_value(f.service) over (
                        partition by f.player_id 
                        order by f.install_date, f.last_seen desc
                    ) as service
                """),
                expr("min(f.viable_date) over (partition by f.player_id) as viable_date"),
                expr("""
                    coalesce(country.country_code, f.country_code, "ZZ") as last_country_code
                """),
                expr("""
                    first_value(case when v.platform not in ('XBSX', 'PS5', 'Windows', 'PC','NSW2') then v.platform else null end) 
                    ignore nulls over (
                        partition by lower(f.player_id) 
                        order by f.install_date, f.last_seen desc
                    ) as current_gen_platform
                """),
                expr("""
                    first_value(case when v.platform in ('XBSX', 'PS5', 'Windows', 'PC','NSW2') then v.platform else null end) 
                    ignore nulls over (
                        partition by f.player_id 
                        order by f.install_date, f.last_seen desc
                    ) as next_gen_platform
                """),
                expr("case when f.install_date < '2025-09-12' then '2025-09-12' else f.install_date end as install_date"),
                expr("case when f.last_seen < '2025-09-12' then '2025-09-12' else f.last_seen end as last_seen_date"),
                expr("f.total_active_days as active_days"),
                expr("f.total_logins as total_logins")
            ).distinct()
    )

    mode_preference = (
        game_status_daily_df.alias("daily")
        .join(mapping_df.alias("m"), expr("daily.player_platform_id = m.player_platform_id"))
        .groupBy(expr("maw_user_platformid as player_platform_id"))
        .agg(
            expr("MAX(COALESCE(hours_played, 0))").alias("total_hours_played"),
            expr("MAX(COALESCE(multiplayer_hours_played, 0))").alias("mp_hours"),
            expr("MAX(COALESCE(online_multiplayer_hours_played, 0))").alias("on_mp_hours"),
            expr("MAX(COALESCE(split_screen_hours_played, 0))").alias("ss_mp_hours"),
            expr("MAX(main_story_completion)").alias("main_story_completion")
        )
        .selectExpr(
            "player_platform_id",
            """CASE
                WHEN total_hours_played >= mp_hours AND total_hours_played >= on_mp_hours AND total_hours_played >= ss_mp_hours THEN 'Solo'
                WHEN mp_hours >= total_hours_played AND mp_hours >= on_mp_hours AND mp_hours >= ss_mp_hours THEN 'Multiplayer'
                WHEN on_mp_hours >= total_hours_played AND on_mp_hours >= mp_hours AND on_mp_hours >= ss_mp_hours THEN 'Online'
                WHEN ss_mp_hours >= total_hours_played AND ss_mp_hours >= mp_hours AND ss_mp_hours >= on_mp_hours THEN 'Split Screen'
                ELSE NULL
            END as mode_preference""",
            "main_story_completion",
            "total_hours_played"
        )
    )

    fpids = fpid_df.filter("FPID IS NOT NULL").selectExpr("PLAYER_ID as platform_id", "FPID AS puid")

    all_metrics_unaggregated = (
        fact_players_ltd.alias("f")
        .join(fpids.alias("fpd"), expr("LOWER(fpd.platform_id) = LOWER(f.player_id)"))
        .join(mode_preference.alias("mp"), expr("LOWER(mp.player_platform_id) = LOWER(f.player_id)"), "left")
        .join(character_preference.alias("cp"), expr("LOWER(cp.player_platform_id) = LOWER(f.player_id)"), "left")
        .join(sku_df.alias("sku"), expr("LOWER(sku.player_id) = LOWER(f.player_id)"), "left")
        .join(entitlement_df.alias("ent"), expr("LOWER(ent.player_id) = LOWER(f.player_id)"), "left")
        .select(
            expr("LOWER(f.player_id) AS platform_account_id"),
            expr("fpd.puid"),
            expr("f.vendor"),
            expr("f.service"),
            expr("f.last_country_code as country_last"),
            expr("f.install_date"),
            expr("f.last_seen_date"),
            expr("f.viable_date"),
            expr("f.active_days"),
            expr("f.total_logins"),
            expr("f.current_gen_platform"),
            expr("f.next_gen_platform"),
            expr("CAST(f.viable_date IS NOT NULL AND f.viable_date <= CURRENT_DATE AS BOOLEAN) as is_viable"),
            expr("mp.mode_preference"),
            expr("mp.main_story_completion"),
            expr("mp.total_hours_played"),
            expr("cp.character_preference"),
            expr("sku.sku"),
            expr("sku.oak2_premium_sku"),
            expr("sku.oak2_preorder"),
            expr("sku.oak2_bundle_pack"),
            expr("ent.bdl_bdl4_expansion_pack_3")

        )
    )
    
    all_metrics = (
        all_metrics_unaggregated
        .groupBy("platform_account_id")
        .agg(
            expr("max(puid) as puid"),
            expr("max(vendor) as vendor"),
            expr("max(service) as service"),
            expr("max(country_last) as country_last"),
            expr("max(install_date) as install_date"),
            expr("max(last_seen_date) as last_seen_date"),
            expr("max(viable_date) as viable_date"),
            expr("max(active_days) as active_days"),
            expr("max(total_logins) as total_logins"),
            expr("max(current_gen_platform) as current_gen_platform"),
            expr("max(next_gen_platform) as next_gen_platform"),
            expr("max(is_viable) as is_viable"),
            expr("max(mode_preference) as mode_preference"),
            expr("max(main_story_completion) as main_story_completion"),
            expr("max(total_hours_played) as total_hours_played"),
            expr("max(character_preference) as character_preference"),
            expr("max(sku) as sku"),
            expr("max(oak2_premium_sku) as oak2_premium_sku"),
            expr("max(oak2_preorder) as oak2_preorder"),
            expr("max(oak2_bundle_pack) as oak2_bundle_pack"),
            expr("max(ifnull(bdl_bdl4_expansion_pack_3, false)) as bdl_bdl4_expansion_pack_3")
        )
    )
    
    final_df = all_metrics.selectExpr(
        f"vendor || ':' || dataanalytics{environment}.cdp_ng.salted_puid(puid) AS brand_firstpartyid",
        f"dataanalytics{environment}.cdp_ng.salted_puid(puid) AS salted_puid",
        "puid",
        "vendor AS bdl_bdl4_vendor",
        "service AS bdl_bdl4_service",
        "current_gen_platform as bdl_bdl4_current_gen_platform",
        "next_gen_platform as bdl_bdl4_next_gen_platform",
        "country_last AS bdl_bdl4_country_last",
        "install_date AS bdl_bdl4_install_date",
        "is_viable as bdl_bdl4_viable",
        "case when viable_date = '9999-01-01' then null else viable_date end as bdl_bdl4_viable_date",
        "last_seen_date AS bdl_bdl4_last_seen_date",
        "total_logins AS bdl_bdl4_games_played",
        "total_hours_played * 60 as bdl_bdl4_time_spent",
        "CAST(NULL AS BIGINT) as bdl_bdl4_games_success",
        "CAST(NULL AS BIGINT) as bdl_bdl4_games_fail",
        "CAST(NULL AS DOUBLE) as bdl_bdl4_success_rate",
        "coalesce(oak2_premium_sku, false) as bdl_bdl4_premium_sku",
        "coalesce(oak2_preorder, false) as bdl_bdl4_is_pre_order",
        "CAST(NULL AS BOOLEAN) as bdl_bdl4_expansion_pack_1",
        "CAST(NULL AS BOOLEAN) as bdl_bdl4_expansion_pack_2",
        "CAST(bdl_bdl4_expansion_pack_3 AS BOOLEAN) as bdl_bdl4_expansion_pack_3",
        "CAST(NULL AS BOOLEAN) as bdl_bdl4_expansion_pack_4",
        "CAST(NULL AS BOOLEAN) as bdl_bdl4_expansion_pack_5",
        "CAST(NULL AS BOOLEAN) as bdl_bdl4_expansion_pack_6",
        """CASE
            WHEN DATEDIFF(day, last_seen_date, CURRENT_DATE) + 1 BETWEEN 0 AND 13 THEN '0-13'
            WHEN DATEDIFF(day, last_seen_date, CURRENT_DATE) + 1 BETWEEN 14 AND 20 THEN '14-20'
            WHEN DATEDIFF(day, last_seen_date, CURRENT_DATE) + 1 BETWEEN 21 AND 40 THEN '21-40'
            WHEN DATEDIFF(day, last_seen_date, CURRENT_DATE) + 1 BETWEEN 41 AND 60 THEN '41-60'
            WHEN DATEDIFF(day, last_seen_date, CURRENT_DATE) + 1 > 60 THEN '60+'
            ELSE '0-13'
        END as bdl_bdl4_last_seen_grouping""",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_install_date_grouping",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_days_active_grouping",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_games_played_grouping",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_success_rate_grouping",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_game_progress_grouping",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_player_type_grouping",
        "CAST(NULL AS FLOAT) as bdl_bdl4_price_known",
        "CAST(NULL AS FLOAT) as bdl_bdl4_price_probabilistic",
        "CAST(NULL AS BOOLEAN) as bdl_bdl4_is_ftp_install",
        "active_days as bdl_bdl4_days_active",
        "mode_preference as bdl_bdl4_mode_preference",
        "character_preference as bdl_bdl4_character_preference",
        "CAST(NULL AS DATE) as bdl_bdl4_solo_first_seen",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_solo_first_seen_grouping",
        "CAST(NULL AS DATE) as bdl_bdl4_solo_last_seen",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_solo_last_seen_grouping",
        "CAST(NULL AS FLOAT) as bdl_bdl4_solo_days_active",
        "CAST(NULL AS FLOAT) as bdl_bdl4_solo_games_played",
        "CAST(NULL AS FLOAT) as bdl_bdl4_solo_time_spent",
        "CAST(NULL AS FLOAT) as bdl_bdl4_solo_games_success",
        "CAST(NULL AS FLOAT) as bdl_bdl4_solo_games_fail",
        "CAST(NULL AS FLOAT) as bdl_bdl4_solo_success_rate",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_solo_success_rate_grouping",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_solo_days_active_grouping",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_solo_games_played_grouping",
        "CAST(NULL AS BOOLEAN) as bdl_bdl4_solo_has_earned_vc",
        "CAST(NULL AS FLOAT) as bdl_bdl4_solo_earned_vc_total",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_solo_earned_vc_grouping",
        "CAST(NULL AS DATE) as bdl_bdl4_solo_earned_vc_first_date",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_solo_earned_first_date_grouping",
        "CAST(NULL AS DATE) as bdl_bdl4_solo_earned_vc_last_date",
        "CAST(NULL AS FLOAT) as bdl_bdl4_solo_earned_vc_transactions",
        "CAST(NULL AS FLOAT) as bdl_bdl4_solo_vc_spent_total",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_solo_vc_spent_grouping",
        "CAST(NULL AS FLOAT) as bdl_bdl4_solo_vc_spent_actions",
        "CAST(NULL AS DATE) as bdl_bdl4_solo_vc_spent_first_date",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_solo_vc_spent_first_date_grouping",
        "CAST(NULL AS DATE) as bdl_bdl4_solo_vc_spent_last_date",
        "CAST(NULL AS DATE) as bdl_bdl4_multiplayer_first_seen",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_multiplayer_first_seen_grouping",
        "CAST(NULL AS DATE) as bdl_bdl4_multiplayer_last_seen",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_multiplayer_last_seen_grouping",
        "CAST(NULL AS FLOAT) as bdl_bdl4_multiplayer_days_active",
        "CAST(NULL AS FLOAT) as bdl_bdl4_multiplayer_games_played",
        "CAST(NULL AS FLOAT) as bdl_bdl4_multiplayer_time_spent",
        "CAST(NULL AS FLOAT) as bdl_bdl4_multiplayer_games_success",
        "CAST(NULL AS FLOAT) as bdl_bdl4_multiplayer_games_fail",
        "CAST(NULL AS FLOAT) as bdl_bdl4_multiplayer_success_rate",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_multiplayer_success_rate_grouping",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_multiplayer_days_active_grouping",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_multiplayer_games_played_grouping",
        "CAST(NULL AS BOOLEAN) as bdl_bdl4_multiplayer_has_earned_vc",
        "CAST(NULL AS FLOAT) as bdl_bdl4_multiplayer_earned_vc_total",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_multiplayer_earned_vc_grouping",
        "CAST(NULL AS DATE) as bdl_bdl4_multiplayer_earned_vc_first_date",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_multiplayer_earned_first_date_grouping",
        "CAST(NULL AS DATE) as bdl_bdl4_multiplayer_earned_vc_last_date",
        "CAST(NULL AS FLOAT) as bdl_bdl4_multiplayer_earned_vc_transactions",
        "CAST(NULL AS FLOAT) as bdl_bdl4_multiplayer_vc_spent_total",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_multiplayer_vc_spent_grouping",
        "CAST(NULL AS FLOAT) as bdl_bdl4_multiplayer_vc_spent_actions",
        "CAST(NULL AS DATE) as bdl_bdl4_multiplayer_vc_spent_first_date",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_multiplayer_vc_spent_first_date_grouping",
        "CAST(NULL AS DATE) as bdl_bdl4_multiplayer_vc_spent_last_date",
        "CAST(NULL AS DATE) as bdl_bdl4_online_first_seen",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_online_first_seen_grouping",
        "CAST(NULL AS DATE) as bdl_bdl4_online_last_seen",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_online_last_seen_grouping",
        "CAST(NULL AS FLOAT) as bdl_bdl4_online_days_active",
        "CAST(NULL AS FLOAT) as bdl_bdl4_online_games_played",
        "CAST(NULL AS FLOAT) as bdl_bdl4_online_time_spent",
        "CAST(NULL AS FLOAT) as bdl_bdl4_online_games_success",
        "CAST(NULL AS FLOAT) as bdl_bdl4_online_games_fail",
        "CAST(NULL AS FLOAT) as bdl_bdl4_online_success_rate",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_online_success_rate_grouping",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_online_days_active_grouping",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_online_games_played_grouping",
        "CAST(NULL AS BOOLEAN) as bdl_bdl4_online_has_earned_vc",
        "CAST(NULL AS FLOAT) as bdl_bdl4_online_earned_vc_total",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_online_earned_vc_grouping",
        "CAST(NULL AS DATE) as bdl_bdl4_online_earned_vc_first_date",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_online_earned_first_date_grouping",
        "CAST(NULL AS DATE) as bdl_bdl4_online_earned_vc_last_date",
        "CAST(NULL AS FLOAT) as bdl_bdl4_online_earned_vc_transactions",
        "CAST(NULL AS FLOAT) as bdl_bdl4_online_vc_spent_total",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_online_vc_spent_grouping",
        "CAST(NULL AS FLOAT) as bdl_bdl4_online_vc_spent_actions",
        "CAST(NULL AS DATE) as bdl_bdl4_online_vc_spent_first_date",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_online_vc_spent_first_date_grouping",
        "CAST(NULL AS DATE) as bdl_bdl4_online_vc_spent_last_date",
        "CAST(NULL AS BOOLEAN) as bdl_bdl4_bought_vc_converted",
        "CAST(NULL AS FLOAT) as bdl_bdl4_bought_vc_transactions",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_bought_vc_transactions_grouping",
        "CAST(NULL AS FLOAT) as bdl_bdl4_bought_vc_total",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_bought_vc_grouping",
        "CAST(NULL AS DATE) as bdl_bdl4_bought_vc_first_date",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_bought_vc_first_date_grouping",
        "CAST(NULL AS DATE) as bdl_bdl4_bought_vc_last_date",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_bought_vc_last_date_grouping",
        "CAST(NULL AS FLOAT) as bdl_bdl4_bought_vc_dollar_value",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_bought_vc_dollar_grouping",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_earned_bought_gifted_ratio_grouping",
        "CAST(NULL AS BOOLEAN) as bdl_bdl4_has_earned_vc",
        "CAST(NULL AS FLOAT) as bdl_bdl4_earned_vc_total",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_earned_vc_grouping",
        "CAST(NULL AS DATE) as bdl_bdl4_earned_vc_first_date",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_earned_vc_first_date_grouping",
        "CAST(NULL AS DATE) as bdl_bdl4_earned_vc_last_date",
        "CAST(NULL AS FLOAT) as bdl_bdl4_earned_vc_actions",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_earned_vc_actions_grouping",
        "CAST(NULL AS BOOLEAN) as bdl_bdl4_has_gifted_vc",
        "CAST(NULL AS FLOAT) as bdl_bdl4_gifted_vc_total",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_gifted_vc_grouping",
        "CAST(NULL AS DATE) as bdl_bdl4_gifted_vc_last_date",
        "CAST(NULL AS DATE) as bdl_bdl4_gifted_vc_first_date",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_gifted_vc_first_date_grouping",
        "CAST(NULL AS FLOAT) as bdl_bdl4_gifted_vc_transactions",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_gifted_vc_transactions_grouping",
        "CAST(NULL AS BOOLEAN) as bdl_bdl4_has_spent_vc",
        "CAST(NULL AS FLOAT) as bdl_bdl4_vc_spent_total",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_vc_spent_grouping",
        "CAST(NULL AS DATE) as bdl_bdl4_vc_spent_first_date",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_vc_spent_first_date_grouping",
        "CAST(NULL AS DATE) as bdl_bdl4_vc_spent_last_date",
        "CAST(NULL AS FLOAT) as bdl_bdl4_vc_spent_actions",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_vc_spent_actions_grouping",
        "CAST(NULL AS FLOAT) as bdl_bdl4_current_vc_balance",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_current_vc_balance_grouping",
        "sku as bdl_bdl4_custom_grouping_01",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_custom_grouping_02",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_custom_grouping_03",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_custom_grouping_04",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_custom_grouping_05",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_custom_grouping_06",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_custom_grouping_07",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_custom_grouping_08",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_custom_grouping_09",
        "CAST(NULL AS VARCHAR(255)) as bdl_bdl4_custom_grouping_10",
        "main_story_completion as bdl_bdl4_custom_value_01",
        "CAST(NULL AS DECIMAL(18, 2)) as bdl_bdl4_custom_value_02",
        "CAST(NULL AS DECIMAL(18, 2)) as bdl_bdl4_custom_value_03",
        "CAST(NULL AS DECIMAL(18, 2)) as bdl_bdl4_custom_value_04",
        "CAST(NULL AS DECIMAL(18, 2)) as bdl_bdl4_custom_value_05",
        "CAST(NULL AS DECIMAL(18, 2)) as bdl_bdl4_custom_value_06",
        "CAST(NULL AS DECIMAL(18, 2)) as bdl_bdl4_custom_value_07",
        "CAST(NULL AS DECIMAL(18, 2)) as bdl_bdl4_custom_value_08",
        "CAST(NULL AS DECIMAL(18, 2)) as bdl_bdl4_custom_value_09",
        "CAST(NULL AS DECIMAL(18, 2)) as bdl_bdl4_custom_value_10"
    )
    return final_df

# COMMAND ----------

def load(df, environment, spark):
    """
    Merges the transformed DataFrame into the target summary table with a conditional update.
    """
    target_table_name = f"dataanalytics{environment}.cdp_ng.bdl4_summary"
    target_df = DeltaTable.forName(spark, target_table_name)
    
    merger_condition = 'target.merge_key = source.merge_key'

    key_cols = ["PUID", "BRAND_FIRSTPARTYID", "SALTED_PUID", "merge_key", "dw_insert_ts", "dw_update_ts"]
    update_check_cols = [col for col in df.columns if col not in key_cols]
    
    # Used null safe operator '<=>' which is safer than '!=' for columns which contain nulls
    update_condition = " OR ".join([f"NOT (target.{col} <=> source.{col})" for col in update_check_cols])

    # update logic for matched records
    update_expr = {col: f"source.{col}" for col in df.columns if col not in ['dw_insert_ts', 'merge_key']}
    update_expr["dw_update_ts"] = "current_timestamp()"

    # insert logic for new records
    insert_expr = {col: f"source.{col}" for col in df.columns}

    (
        target_df.alias("target")
        .merge(df.alias("source"), merger_condition)
        .whenMatchedUpdate(condition=update_condition, set=update_expr)
        .whenNotMatchedInsert(values=insert_expr)
        .execute()
    )

# COMMAND ----------

def run_batch():
    # Parse the environment argument
    input_param = dbutils_input_params()
    environment = input_param.get('environment', set_environment())
    spark = SparkSession.builder.appName("Hydra").getOrCreate()

    # Determine watermark from the target table
    target_table_name = f"dataanalytics{environment}.cdp_ng.bdl4_summary"
    try:
        prev_max_date = spark.read.table(target_table_name).selectExpr("max(bdl_bdl4_last_seen_date)").collect()[0][0]
        if prev_max_date is None:
            prev_max_date = date(1999, 1, 1)
    except Exception:
        prev_max_date = date(1999, 1, 1)

    # Extract all data sources
    fact_player_ltd_df = read_fact_player_ltd(environment, spark, prev_max_date)
    platforms_df = read_platforms(spark)
    account_delete_df = read_account_delete_events(environment, spark)
    game_status_daily_df = read_game_status_daily(environment, spark)
    fpid_df = read_fpids(spark)
    player_character = read_player_character(environment,spark)
    sku_df = read_player_sku(environment,spark)
    mapping_df = read_platform_id_mapping(environment,spark)
    country_lookup_df = read_country_lookup(environment, spark)
    entitlement_df = read_player_entitlement(environment, spark)


    df = transform(environment, spark, fact_player_ltd_df, platforms_df, account_delete_df, game_status_daily_df, fpid_df, player_character, sku_df, mapping_df, country_lookup_df, entitlement_df)
    create_bdl4_summary_table(environment, spark)

    df_to_load = df.selectExpr(
        "*",
        "current_timestamp() as dw_insert_ts",
        "current_timestamp() as dw_update_ts",
        "sha2(concat_ws('|', PUID, BRAND_FIRSTPARTYID, SALTED_PUID), 256) as merge_key"
    )
    
    load(df_to_load, environment, spark)

# COMMAND ----------

if __name__ == "__main__":
    run_batch()
