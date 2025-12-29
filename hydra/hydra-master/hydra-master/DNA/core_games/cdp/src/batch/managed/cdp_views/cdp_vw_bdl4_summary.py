# Databricks notebook source
# MAGIC %run ../../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../../utils/ddl/table_functions
# MAGIC

# COMMAND ----------

def create_bdl4_summary_view(environment, spark):
  sql_create_view = f"""
   CREATE OR REPLACE VIEW dataanalytics{environment}.cdp_ng.vw_bdl4_summary
AS
WITH platforms AS (
  SELECT DISTINCT
    SRC_PLATFORM,
    CASE WHEN PLATFORM IN ('PC', 'Linux', 'OSX') THEN 'PC' ELSE PLATFORM END AS PLATFORM,
    CASE WHEN VENDOR IN ('PC', 'Linux', 'Apple') THEN 'PC' ELSE VENDOR END AS VENDOR,
    FIRST_VALUE(PLATFORM) OVER (PARTITION BY VENDOR ORDER BY DW_CREATE_TS DESC) as current_gen_platform
  FROM REFERENCE_CUSTOMER.PLATFORM.PLATFORM
),

fact_players_ltd AS (
  SELECT DISTINCT
    lower(f.player_id) as player_id,
    first_value(v.platform) over (
        partition by f.player_id 
        order by f.install_date, f.last_seen desc
    ) as platform,
    first_value(v.vendor) over (
        partition by f.player_id 
        order by f.install_date, f.last_seen desc
    ) as vendor,
    first_value(f.service) over (
        partition by f.player_id 
        order by f.install_date, f.last_seen desc
    ) as service,
    min(f.viable_date) over (partition by f.player_id) as viable_date,
    first_value(f.country_code) ignore nulls over (
        partition by lower(f.player_id) 
        order by f.install_date, f.last_seen desc
    ) as last_country_code,
    first_value(case when v.platform not in ('XBSX', 'PS5', 'Windows', 'PC','NSW2') then v.platform else null end) 
    ignore nulls over (
        partition by lower(f.player_id) 
        order by f.install_date, f.last_seen desc
    ) as current_gen_platform,
    first_value(case when v.platform in ('XBSX', 'PS5', 'Windows', 'PC','NSW2') then v.platform else null end) 
    ignore nulls over (
        partition by f.player_id 
        order by f.install_date, f.last_seen desc
    ) as next_gen_platform,
    f.install_date as install_date,
    f.last_seen as last_seen_date,
    f.total_active_days as active_days,
    f.total_logins as total_logins
  FROM dataanalytics{environment}.standard_metrics.fact_player_ltd f
  JOIN platforms v ON f.platform = v.src_platform
  WHERE f.title = 'Borderlands 4'
  AND NOT EXISTS (
      SELECT 1 FROM coretech{environment}.sso.accountdeleteevent sso WHERE sso.accountId = f.player_id
  )
),

mode_preference AS (
  SELECT
    player_platform_id,
    CASE
      WHEN SUM(COALESCE(hours_played, 0)) >= SUM(COALESCE(multiplayer_hours_played, 0)) AND SUM(COALESCE(hours_played, 0)) >= SUM(COALESCE(online_multiplayer_hours_played, 0)) AND SUM(COALESCE(hours_played, 0)) >= SUM(COALESCE(split_screen_hours_played, 0))
        THEN 'Solo'
      WHEN SUM(COALESCE(multiplayer_hours_played, 0)) >= SUM(COALESCE(hours_played, 0)) AND SUM(COALESCE(multiplayer_hours_played, 0)) >= SUM(COALESCE(online_multiplayer_hours_played, 0)) AND SUM(COALESCE(multiplayer_hours_played, 0)) >= SUM(COALESCE(split_screen_hours_played, 0))
        THEN 'Multiplayer'
      WHEN SUM(COALESCE(online_multiplayer_hours_played, 0)) >= SUM(COALESCE(hours_played, 0)) AND SUM(COALESCE(online_multiplayer_hours_played, 0)) >= SUM(COALESCE(multiplayer_hours_played, 0)) AND SUM(COALESCE(online_multiplayer_hours_played, 0)) >= SUM(COALESCE(split_screen_hours_played, 0))
        THEN 'Online'
      WHEN SUM(COALESCE(split_screen_hours_played, 0)) >= SUM(COALESCE(hours_played, 0)) AND SUM(COALESCE(split_screen_hours_played, 0)) >= SUM(COALESCE(multiplayer_hours_played, 0)) AND SUM(COALESCE(split_screen_hours_played, 0)) >= SUM(COALESCE(online_multiplayer_hours_played, 0))
        THEN 'Split Screen'
      ELSE NULL
    END as mode_preference,
    MAX(main_story_completion) AS main_story_completion,
    SUM(COALESCE(hours_played, 0)) as total_hours_played
  FROM oak2{environment}.managed.fact_player_game_status_daily
  GROUP BY player_platform_id
),

character_preference AS (
  SELECT player_platform_id, character_preference
  FROM (
    SELECT
      player_platform_id,
      player_character as character_preference,
      ROW_NUMBER() OVER(PARTITION BY player_platform_id ORDER BY COUNT(*) DESC) as rn
    FROM oak2{environment}.managed.fact_player_character
    GROUP BY player_platform_id, player_character
  )
  WHERE rn = 1
),

fpids AS (
  SELECT
    PLAYER_ID as platform_id,
    FPID AS puid
  FROM reference_customer.customer.vw_fpid_all
  WHERE FPID IS NOT NULL
),

all_metrics_unaggregated AS (
  SELECT
    LOWER(f.player_id) AS platform_account_id,
    fpd.puid,
    f.vendor,
    f.service,
    f.last_country_code as country_last,
    f.install_date,
    f.last_seen_date,
    f.viable_date,
    f.active_days,
    f.total_logins,
    f.current_gen_platform,
    f.next_gen_platform,
    CAST(f.viable_date IS NOT NULL AND f.viable_date <= CURRENT_DATE AS BOOLEAN) as is_viable,
    mp.mode_preference,
    mp.main_story_completion,
    mp.total_hours_played,
    cp.character_preference
  FROM fact_players_ltd f
  JOIN fpids fpd ON LOWER(fpd.platform_id) = LOWER(f.player_id)
  LEFT JOIN mode_preference mp ON LOWER(mp.player_platform_id) = LOWER(f.player_id)
  LEFT JOIN character_preference cp ON LOWER(cp.player_platform_id) = LOWER(f.player_id)
),

all_metrics AS (
    SELECT
        platform_account_id,
        max(puid) as puid,
        max(vendor) as vendor,
        max(service) as service,
        max(country_last) as country_last,
        max(install_date) as install_date,
        max(last_seen_date) as last_seen_date,
        max(viable_date) as viable_date,
        max(active_days) as active_days,
        max(total_logins) as total_logins,
        max(current_gen_platform) as current_gen_platform,
        max(next_gen_platform) as next_gen_platform,
        max(is_viable) as is_viable,
        max(mode_preference) as mode_preference,
        max(main_story_completion) as main_story_completion,
        max(total_hours_played) as total_hours_played,
        max(character_preference) as character_preference
    FROM all_metrics_unaggregated
    GROUP BY platform_account_id
)

SELECT
  vendor || ':' || dataanalytics{environment}.cdp_ng.salted_puid(puid) AS brand_firstpartyid,
  dataanalytics{environment}.cdp_ng.salted_puid(puid) AS salted_puid,
  puid,
  service AS bdl_bdl4_service,
  vendor AS bdl_bdl4_vendor,
  current_gen_platform as bdl_bdl4_current_gen_platform,
  next_gen_platform as bdl_bdl4_next_gen_platform,
  country_last AS bdl_bdl4_country_last,
  install_date AS bdl_bdl4_install_date,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_install_date_grouping,
  is_viable as bdl_bdl4_viable,
  viable_date as bdl_bdl4_viable_date,
  CAST(NULL AS BOOLEAN) as bdl_bdl4_premium_sku,
  CAST(NULL AS BOOLEAN) as bdl_bdl4_is_pre_order,
  CAST(NULL AS FLOAT) as bdl_bdl4_price_known,
  CAST(NULL AS FLOAT) as bdl_bdl4_price_probabilistic,
  CAST(NULL AS BOOLEAN) as bdl_bdl4_is_ftp_install,
  CAST(NULL AS BOOLEAN) as bdl_bdl4_expansion_pack_1,
  CAST(NULL AS BOOLEAN) as bdl_bdl4_expansion_pack_2,
  CAST(NULL AS BOOLEAN) as bdl_bdl4_expansion_pack_3,
  CAST(NULL AS BOOLEAN) as bdl_bdl4_expansion_pack_4,
  CAST(NULL AS BOOLEAN) as bdl_bdl4_expansion_pack_5,
  CAST(NULL AS BOOLEAN) as bdl_bdl4_expansion_pack_6,
  last_seen_date AS bdl_bdl4_last_seen_date,
  CASE
    WHEN DATEDIFF(day, last_seen_date, CURRENT_DATE) + 1 BETWEEN 0 AND 13 THEN '0-13'
    WHEN DATEDIFF(day, last_seen_date, CURRENT_DATE) + 1 BETWEEN 14 AND 20 THEN '14-20'
    WHEN DATEDIFF(day, last_seen_date, CURRENT_DATE) + 1 BETWEEN 21 AND 40 THEN '21-40'
    WHEN DATEDIFF(day, last_seen_date, CURRENT_DATE) + 1 BETWEEN 41 AND 60 THEN '41-60'
    WHEN DATEDIFF(day, last_seen_date, CURRENT_DATE) + 1 > 60 THEN '60+'
    ELSE '0-13'
  END as bdl_bdl4_last_seen_grouping,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_player_type_grouping,
  active_days as bdl_bdl4_days_active,
  total_logins AS bdl_bdl4_games_played,
  total_hours_played * 60 as bdl_bdl4_time_spent,
  CAST(NULL AS BIGINT) as bdl_bdl4_games_success,
  CAST(NULL AS BIGINT) as bdl_bdl4_games_fail,
  CAST(NULL AS DOUBLE) as bdl_bdl4_success_rate,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_success_rate_grouping,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_days_active_grouping,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_games_played_grouping,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_game_progress_grouping,
  mode_preference as bdl_bdl4_mode_preference,
  character_preference as bdl_bdl4_character_preference,
  CAST(NULL AS DATE) as bdl_bdl4_solo_first_seen,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_solo_first_seen_grouping,
  CAST(NULL AS DATE) as bdl_bdl4_solo_last_seen,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_solo_last_seen_grouping,
  CAST(NULL AS FLOAT) as bdl_bdl4_solo_days_active,
  CAST(NULL AS FLOAT) as bdl_bdl4_solo_games_played,
  CAST(NULL AS FLOAT) as bdl_bdl4_solo_time_spent,
  CAST(NULL AS FLOAT) as bdl_bdl4_solo_games_success,
  CAST(NULL AS FLOAT) as bdl_bdl4_solo_games_fail,
  CAST(NULL AS FLOAT) as bdl_bdl4_solo_success_rate,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_solo_success_rate_grouping,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_solo_days_active_grouping,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_solo_games_played_grouping,
  CAST(NULL AS BOOLEAN) as bdl_bdl4_solo_has_earned_vc,
  CAST(NULL AS FLOAT) as bdl_bdl4_solo_earned_vc_total,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_solo_earned_vc_grouping,
  CAST(NULL AS DATE) as bdl_bdl4_solo_earned_vc_first_date,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_solo_earned_first_date_grouping,
  CAST(NULL AS DATE) as bdl_bdl4_solo_earned_vc_last_date,
  CAST(NULL AS FLOAT) as bdl_bdl4_solo_earned_vc_transactions,
  CAST(NULL AS FLOAT) as bdl_bdl4_solo_vc_spent_total,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_solo_vc_spent_grouping,
  CAST(NULL AS FLOAT) as bdl_bdl4_solo_vc_spent_actions,
  CAST(NULL AS DATE) as bdl_bdl4_solo_vc_spent_first_date,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_solo_vc_spent_first_date_grouping,
  CAST(NULL AS DATE) as bdl_bdl4_solo_vc_spent_last_date,
  CAST(NULL AS DATE) as bdl_bdl4_multiplayer_first_seen,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_multiplayer_first_seen_grouping,
  CAST(NULL AS DATE) as bdl_bdl4_multiplayer_last_seen,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_multiplayer_last_seen_grouping,
  CAST(NULL AS FLOAT) as bdl_bdl4_multiplayer_days_active,
  CAST(NULL AS FLOAT) as bdl_bdl4_multiplayer_games_played,
  CAST(NULL AS FLOAT) as bdl_bdl4_multiplayer_time_spent,
  CAST(NULL AS FLOAT) as bdl_bdl4_multiplayer_games_success,
  CAST(NULL AS FLOAT) as bdl_bdl4_multiplayer_games_fail,
  CAST(NULL AS FLOAT) as bdl_bdl4_multiplayer_success_rate,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_multiplayer_success_rate_grouping,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_multiplayer_days_active_grouping,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_multiplayer_games_played_grouping,
  CAST(NULL AS BOOLEAN) as bdl_bdl4_multiplayer_has_earned_vc,
  CAST(NULL AS FLOAT) as bdl_bdl4_multiplayer_earned_vc_total,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_multiplayer_earned_vc_grouping,
  CAST(NULL AS DATE) as bdl_bdl4_multiplayer_earned_vc_first_date,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_multiplayer_earned_first_date_grouping,
  CAST(NULL AS DATE) as bdl_bdl4_multiplayer_earned_vc_last_date,
  CAST(NULL AS FLOAT) as bdl_bdl4_multiplayer_earned_vc_transactions,
  CAST(NULL AS FLOAT) as bdl_bdl4_multiplayer_vc_spent_total,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_multiplayer_vc_spent_grouping,
  CAST(NULL AS FLOAT) as bdl_bdl4_multiplayer_vc_spent_actions,
  CAST(NULL AS DATE) as bdl_bdl4_multiplayer_vc_spent_first_date,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_multiplayer_vc_spent_first_date_grouping,
  CAST(NULL AS DATE) as bdl_bdl4_multiplayer_vc_spent_last_date,
  CAST(NULL AS DATE) as bdl_bdl4_online_first_seen,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_online_first_seen_grouping,
  CAST(NULL AS DATE) as bdl_bdl4_online_last_seen,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_online_last_seen_grouping,
  CAST(NULL AS FLOAT) as bdl_bdl4_online_days_active,
  CAST(NULL AS FLOAT) as bdl_bdl4_online_games_played,
  CAST(NULL AS FLOAT) as bdl_bdl4_online_time_spent,
  CAST(NULL AS FLOAT) as bdl_bdl4_online_games_success,
  CAST(NULL AS FLOAT) as bdl_bdl4_online_games_fail,
  CAST(NULL AS FLOAT) as bdl_bdl4_online_success_rate,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_online_success_rate_grouping,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_online_days_active_grouping,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_online_games_played_grouping,
  CAST(NULL AS BOOLEAN) as bdl_bdl4_online_has_earned_vc,
  CAST(NULL AS FLOAT) as bdl_bdl4_online_earned_vc_total,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_online_earned_vc_grouping,
  CAST(NULL AS DATE) as bdl_bdl4_online_earned_vc_first_date,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_online_earned_first_date_grouping,
  CAST(NULL AS DATE) as bdl_bdl4_online_earned_vc_last_date,
  CAST(NULL AS FLOAT) as bdl_bdl4_online_earned_vc_transactions,
  CAST(NULL AS FLOAT) as bdl_bdl4_online_vc_spent_total,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_online_vc_spent_grouping,
  CAST(NULL AS FLOAT) as bdl_bdl4_online_vc_spent_actions,
  CAST(NULL AS DATE) as bdl_bdl4_online_vc_spent_first_date,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_online_vc_spent_first_date_grouping,
  CAST(NULL AS DATE) as bdl_bdl4_online_vc_spent_last_date,
  CAST(NULL AS BOOLEAN) as bdl_bdl4_bought_vc_converted,
  CAST(NULL AS FLOAT) as bdl_bdl4_bought_vc_transactions,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_bought_vc_transactions_grouping,
  CAST(NULL AS FLOAT) as bdl_bdl4_bought_vc_total,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_bought_vc_grouping,
  CAST(NULL AS DATE) as bdl_bdl4_bought_vc_first_date,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_bought_vc_first_date_grouping,
  CAST(NULL AS DATE) as bdl_bdl4_bought_vc_last_date,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_bought_vc_last_date_grouping,
  CAST(NULL AS FLOAT) as bdl_bdl4_bought_vc_dollar_value,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_bought_vc_dollar_grouping,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_earned_bought_gifted_ratio_grouping,
  CAST(NULL AS BOOLEAN) as bdl_bdl4_has_earned_vc,
  CAST(NULL AS FLOAT) as bdl_bdl4_earned_vc_total,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_earned_vc_grouping,
  CAST(NULL AS DATE) as bdl_bdl4_earned_vc_first_date,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_earned_vc_first_date_grouping,
  CAST(NULL AS DATE) as bdl_bdl4_earned_vc_last_date,
  CAST(NULL AS FLOAT) as bdl_bdl4_earned_vc_actions,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_earned_vc_actions_grouping,
  CAST(NULL AS BOOLEAN) as bdl_bdl4_has_gifted_vc,
  CAST(NULL AS FLOAT) as bdl_bdl4_gifted_vc_total,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_gifted_vc_grouping,
  CAST(NULL AS DATE) as bdl_bdl4_gifted_vc_last_date,
  CAST(NULL AS DATE) as bdl_bdl4_gifted_vc_first_date,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_gifted_vc_first_date_grouping,
  CAST(NULL AS FLOAT) as bdl_bdl4_gifted_vc_transactions,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_gifted_vc_transactions_grouping,
  CAST(NULL AS BOOLEAN) as bdl_bdl4_has_spent_vc,
  CAST(NULL AS FLOAT) as bdl_bdl4_vc_spent_total,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_vc_spent_grouping,
  CAST(NULL AS DATE) as bdl_bdl4_vc_spent_first_date,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_vc_spent_first_date_grouping,
  CAST(NULL AS DATE) as bdl_bdl4_vc_spent_last_date,
  CAST(NULL AS FLOAT) as bdl_bdl4_vc_spent_actions,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_vc_spent_actions_grouping,
  CAST(NULL AS FLOAT) as bdl_bdl4_current_vc_balance,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_current_vc_balance_grouping,
  CAST(NULL AS VARCHAR(50)) as bdl_bdl4_custom_grouping_01,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_custom_grouping_02,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_custom_grouping_03,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_custom_grouping_04,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_custom_grouping_05,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_custom_grouping_06,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_custom_grouping_07,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_custom_grouping_08,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_custom_grouping_09,
  CAST(NULL AS VARCHAR(255)) as bdl_bdl4_custom_grouping_10,
  main_story_completion as bdl_bdl4_custom_value_01,
  CAST(NULL AS DECIMAL(18, 2)) as bdl_bdl4_custom_value_02,
  CAST(NULL AS DECIMAL(18, 2)) as bdl_bdl4_custom_value_03,
  CAST(NULL AS DECIMAL(18, 2)) as bdl_bdl4_custom_value_04,
  CAST(NULL AS DECIMAL(18, 2)) as bdl_bdl4_custom_value_05,
  CAST(NULL AS DECIMAL(18, 2)) as bdl_bdl4_custom_value_06,
  CAST(NULL AS DECIMAL(18, 2)) as bdl_bdl4_custom_value_07,
  CAST(NULL AS DECIMAL(18, 2)) as bdl_bdl4_custom_value_08,
  CAST(NULL AS DECIMAL(18, 2)) as bdl_bdl4_custom_value_09,
  CAST(NULL AS DECIMAL(18, 2)) as bdl_bdl4_custom_value_10
FROM all_metrics;
  """

  df = spark.sql(sql_create_view)
  return df