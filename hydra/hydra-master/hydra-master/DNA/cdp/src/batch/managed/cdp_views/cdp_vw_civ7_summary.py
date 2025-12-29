# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/table_functions
# MAGIC

# COMMAND ----------

def create_civ7_summary_view(environment, spark):
  sql_create_view = f"""
   create view if not exists dataanalytics{environment}.cdp_ng.vw_civ7_summary
    as
    with services as (
      select
        case service
          when 'xbl' then 'Xbox Live'
          else SERVICE
        end JOIN_KEY,
        service,
        vendor
      from
        reference_customer.platform.service
      group by
        1,
        2,
        3
    ),
    platforms as (
        SELECT distinct SRC_PLATFORM
        , IFF(PLATFORM  IN ('PC', 'Linux', 'OSX'), 'PC', PLATFORM)     PLATFORM
        , IFF(VENDOR    IN ('PC', 'Linux', 'Apple'), 'PC', VENDOR)     VENDOR
        FROM REFERENCE_CUSTOMER.PLATFORM.PLATFORM
    ),
    fact_players_ltd as (
      SELECT DISTINCT
        lower(f.player_id) player_id,
        First_value(v.platform) OVER (
            partition BY f.player_id
            ORDER BY f.received_on::timestamp DESC
          ) platform,
        First_value(v.vendor) OVER (
            partition BY f.player_id
            ORDER BY f.received_on::timestamp DESC
          ) vendor,
        First_value(s.service) OVER (
            partition BY f.player_id
            ORDER BY f.received_on::timestamp DESC
          ) service,
        Min(fp.viable_date) OVER (partition BY fp.player_id) viable_date,
        First_value(f.country_code)
          ignore nulls OVER (
            partition BY lower(f.player_id)
            ORDER BY f.received_on::timestamp DESC
          ) last_country_code,
        min(f.received_on::timestamp) OVER (partition BY lower(f.player_id)) install_date,
        max(f.received_on::timestamp) OVER (partition BY lower(f.player_id)) last_seen_date,
        first_value(iff(v.platform NOT IN ('XBSX', 'PS5', 'Windows', 'PC'), v.platform, NULL))
          ignore nulls OVER (
            partition BY lower(f.player_id), f.player_id
            ORDER BY f.received_on::timestamp DESC
          ) current_gen_platform,
        first_value(iff(v.platform IN ('XBSX', 'PS5', 'Windows', 'PC'), v.platform, NULL))
          ignore nulls OVER (
            partition BY f.player_id
            ORDER BY f.received_on::timestamp DESC
          ) next_gen_platform
      FROM
        inverness{environment}.intermediate.fact_player_activity f
          LEFT JOIN
            dataanalytics{environment}.standard_metrics.fact_player_ltd fp
            ON
              lower(f.player_id) = lower(fp.player_id)
              AND fp.title = 'Civilization VII'
          JOIN services s ON lower(s.join_key) = lower(f.service)
          JOIN platforms v ON f.platform = v.src_platform
          LEFT JOIN coretech{environment}.sso.accountdeleteevent sso ON sso.accountId = f.player_id
      WHERE
        f.platform IS NOT NULL
        and sso.accountId is null
        and f.received_on::timestamp >= '2025-02-05'::timestamp
    ),
    player_entitlement_details (
        select distinct player_id,sku,civ7_premium_sku,civ7_preorder from inverness_prod.managed_view.fact_player_sku
    ),
    single_player_metrics as (
      select distinct
        player_id,
        min(date) as single_player_first_seen,
        max(date) as single_player_last_seen,
        count(distinct date) as single_player_active_days,
        sum(num_games) as single_player_games_played,
        sum(coalesce(game_duration_sec, 0)) as single_player_time_spent,
        sum(num_games_completed) as single_player_games_success,
        ifnull(sum(num_games) - sum(num_games_completed), 0) as single_player_games_fail,
        try_divide(sum(num_games_completed), sum(num_games)) as single_player_success_rate
      from
        inverness{environment}.managed_view.fact_player_game_status_daily
        where date::timestamp >= '2025-02-05'::timestamp
      group by
        player_id
    ),
    multiplayer_metrics as (
 select
        playerpublicid as player_id,
        min(session_date) as multiplayer_first_seen,
        max(session_date) as multiplayer_last_seen,
        max(session_duration) as multiplayer_time_spent,
        count(distinct session_date) as multiplayer_days_active,
        count(distinct case
            when campaignstatusevent = 'CampaignEnter' then campaigninstanceid
          end) as multiplayer_games_played,
        count(distinct case
            when victoryevent = 'Victory Achieved' then campaigninstanceid
          end) as multiplayer_games_success,
        ifnull(multiplayer_games_played - multiplayer_games_success, 0) as multiplayer_games_fail,
        try_divide(multiplayer_games_success, multiplayer_games_played) as multiplayer_success_rate
      from
        (
          select
            cs.playerpublicid,
            cs.campaigninstanceid,
            to_date(cast(cs.receivedon as timestamp)) as session_date,
            timestampdiff(
              SECOND,
              min((cast(cs.receivedon as timestamp))) over (
                  partition by cs.applicationsessioninstanceid
                ),
              max((cast(cs.receivedon as timestamp))) over (
                  partition by cs.applicationsessioninstanceid
                )
            ) as session_duration,
            campaignstatusevent,
            victoryevent
          from
            inverness{environment}.raw.campaignstatus cs
              join
                inverness{environment}.raw.campaignsetupstatus css
                on css.campaignsetupinstanceid = cs.campaignsetupinstanceid
              join inverness{environment}.raw.lobbystatus ls on cs.campaigninstanceid = ls.campaigninstanceid and cs.lobbyinstanceid=ls.lobbyinstanceid
              join (select max(victoryevent) as victoryevent,campaigninstanceid,lobbyinstanceid from inverness{environment}.raw.victorystatus  where  playertype='HUMAN' and cast(receivedon as timestamp)>='2025-02-05'::timestamp 
              group by campaigninstanceid,lobbyinstanceid) vs on cs.campaigninstanceid = vs.campaigninstanceid and cs.lobbyinstanceid=vs.lobbyinstanceid
          where
            cs.playerPublicId is not null
            and cs.playerPublicId != 'anonymous'
            and cs.playertype='HUMAN'
            and css.gametype in ('INTERNET', 'LAN', 'WIRELESS')
            and css.campaignsetupevent ='Complete'
            and cast(cs.receivedon as timestamp)>='2025-02-05'::timestamp
            and cast(css.receivedon as timestamp)>='2025-02-05'::timestamp
            and cast(ls.receivedon as timestamp)>='2025-02-05'::timestamp

        )
      group by
        playerpublicid
    ),
    player_mode as (
      select distinct
        playerpublicid as player_id,
        first_value(gametype) over (
            partition by playerPublicId
            order by receivedOn::timestamp desc
          ) as latest_game_type,
        case
          when latest_game_type = 'SINGLEPLAYER' then 'Single Player'
          when latest_game_type in ('INTERNET', 'LAN', 'WIRELESS') then 'Multi Player'
          else latest_game_type
        end as game_type,
        first_value(leader) over (
            partition by playerPublicId
            order by receivedOn::timestamp desc
          ) as leader
      from
        inverness{environment}.raw.campaignsetupstatus
      where
        playerpublicid is not null
        and leader is not null
        and gametype is not null
        and  cast(receivedon as timestamp)>='2025-02-05'::timestamp
    ),
    player_type as (
      select distinct
        playerpublicid as player_id,
        first_value(playertype) over (
            partition by playerPublicId
            order by receivedOn::timestamp desc
          ) as player_type
      from
        inverness{environment}.raw.campaignstatus
        where cast(receivedon as timestamp)>='2025-02-05'::timestamp
    ),
    fpids as (
      select distinct
        le.accountid platform_id,
        dataanalytics{environment}.cdp_ng.fpid_decryption(le.firstpartyId) puid
      from
        coretech{environment}.sso.linkevent le
          join
            reference{environment}.title.dim_title t
            on
              le.appId = t.app_id
              and title ilike '%2K Portal%'
              and le.firstpartyId is not null
          join
            inverness{environment}.intermediate.fact_player_activity f
            on lower(f.player_id) = lower(le.accountid)
      where
        accounttype = 2
        and targetaccounttype = 3
        and le.firstPartyId != 'None'
        and dataanalytics{environment}.cdp_ng.fpid_decryption(le.firstpartyId) not ilike 'dd1%-?'
        and dataanalytics{environment}.cdp_ng.fpid_decryption(le.firstpartyId) not ilike '%@%'
      union
      select distinct
        le.targetaccountId platform_id,
        dataanalytics{environment}.cdp_ng.fpid_decryption(le.firstpartyId) puid
      from
        coretech{environment}.sso.linkevent le
          join
            reference{environment}.title.dim_title t
            on
              le.appId = t.app_id
              and title ilike '%2K Portal%'
              and le.firstpartyId is not null
          join
            inverness{environment}.intermediate.fact_player_activity f
            on lower(f.player_id) = lower(le.targetaccountId)
      where
        accounttype = 3
        and targetaccounttype = 2
        and le.firstPartyId != 'None'
        and dataanalytics{environment}.cdp_ng.fpid_decryption(le.firstpartyId) not ilike 'dd1%-?'
        and dataanalytics{environment}.cdp_ng.fpid_decryption(le.firstpartyId) not ilike '%@%'
      union
      select distinct
        f.player_id platform_id,
        first_party_id puid
      from
        inverness{environment}.intermediate.fact_player_activity f
          join reference_customer.customer.vw_ctp_profile le on lower(f.player_id) = lower(le.public_id)
      where
        le.first_party_id is not null
        and first_party_id not ilike 'dd1%-?'
        and first_party_id ilike '%@%'
    ),
    all_metrics as (
      select distinct
        lower(f.player_id) platform_account_id,
        first_value(fpd.puid) over (
            partition by lower(f.player_id)
            order by to_date(min(f.install_date)::timestamp)
          ) puid,
        f.vendor vendor,
        f.service service,
        max(pt.player_type) player_type,
        max(current_gen_platform) current_gen_platform,
        iff(max(next_gen_platform) = 'Windows', 'PC', max(next_gen_platform)) next_gen_platform,
        max(ifnull(f.last_country_code, 'ZZ')) country_last,
        to_date(min(f.install_date)::timestamp) install_date,
        cast(ifnull(min(f.viable_date), '9999-01-01') as date) viable_date,
        cast(
          min(f.viable_date) is not null
          and min(f.viable_date) <= current_date as boolean
        ) is_viable,
        to_date(max(f.last_seen_date)::timestamp) last_seen_date,
        case
          when datediff(day, max(f.last_seen_date), current_date) + 1 between 0 and 13 then '0-13'
          when datediff(day, max(f.last_seen_date), current_date) + 1 between 14 and 20 then '14-20'
          when datediff(day, max(f.last_seen_date), current_date) + 1 between 21 and 40 then '21-40'
          when datediff(day, max(f.last_seen_date), current_date) + 1 between 41 and 60 then '41-60'
          when datediff(day, max(f.last_seen_date), current_date) + 1 > 60 then '60+'
          else '0-13'
        end as last_seen_date_grouping,
        min(spm.single_player_first_seen) single_player_first_seen,
        max(spm.single_player_last_seen) single_player_last_seen,
        case
          when
            datediff(day, max(spm.single_player_last_seen), current_date) + 1 between 0 and 7
          then
            '0-7'
          when
            datediff(day, max(spm.single_player_last_seen), current_date) + 1 between 8 and 14
          then
            '8-14'
          when
            datediff(day, max(spm.single_player_last_seen), current_date) + 1 between 15 and 21
          then
            '15-21'
          when datediff(day, max(spm.single_player_last_seen), current_date) + 1 > 30 then '30+'
          else 'null'
        end as single_player_last_seen_grouping,
        ifnull(max(spm.single_player_active_days), 0) single_player_active_days,
        ifnull(max(spm.single_player_games_played), 0) single_player_games_played,
        round(ifnull(max(spm.single_player_time_spent), 0), 2) single_player_time_spent,
        ifnull(max(spm.single_player_games_success), 0) single_player_games_success,
        ifnull(max(spm.single_player_games_fail), 0) single_player_games_fail,
        round(ifnull(max(spm.single_player_success_rate), 0), 2) single_player_success_rate,
        min(mpm.multiplayer_first_seen) multiplayer_first_seen,
        max(mpm.multiplayer_last_seen) multiplayer_last_seen,
        case
          when
            datediff(day, max(mpm.multiplayer_last_seen), current_date) + 1 between 0 and 7
          then
            '0-7'
          when
            datediff(day, max(mpm.multiplayer_last_seen), current_date) + 1 between 8 and 14
          then
            '8-14'
          when
            datediff(day, max(mpm.multiplayer_last_seen), current_date) + 1 between 15 and 21
          then
            '15-21'
          when datediff(day, max(mpm.multiplayer_last_seen), current_date) + 1 > 30 then '30+'
          else 'null'
        end as multiplayer_last_seen_grouping,
        ifnull(max(mpm.multiplayer_days_active), 0) multiplayer_days_active,
        ifnull(max(mpm.multiplayer_games_played), 0) multiplayer_games_played,
        round(ifnull(max(mpm.multiplayer_time_spent), 0), 2) multiplayer_time_spent,
        ifnull(max(mpm.multiplayer_games_success), 0) multiplayer_games_success,
        ifnull(max(mpm.multiplayer_games_fail), 0) multiplayer_games_fail,
        round(ifnull(max(mpm.multiplayer_success_rate), 0), 2) multiplayer_success_rate,
        ifnull(max(single_player_active_days + multiplayer_days_active), 0) days_active,
        case
          when days_active between 0 and 1 then '0-1'
          when days_active between 2 and 5 then '2-5'
          when days_active between 6 and 10 then '6-10'
          when days_active > 10 then '10+'
          else '0-1'
        end as days_active_grouping,
        max(pe.sku) sku,
        max(ifnull(pe.civ7_premium_sku, False)) civ7_premium_sku,
        max(ifnull(pe.civ7_preorder, False)) civ7_preorder,
        max(pm.game_type) mode_preference,
        max(pm.leader) character_preference
      from
        fact_players_ltd f
          join fpids fpd on lower(fpd.platform_id) = lower(f.player_id)
          left join player_entitlement_details pe on lower(f.player_id) = lower(pe.player_id)
          left join single_player_metrics spm on lower(f.player_id) = lower(spm.player_id)
          left join multiplayer_metrics mpm on lower(f.player_id) = lower(mpm.player_id)
          left join player_mode pm on lower(f.player_id) = lower(pm.player_id)
          left join player_type pt on lower(f.player_id) = lower(pt.player_id)
      group by
        lower(f.player_id),
        puid,
        vendor,
        service
    )
    select
      vendor || ':' || dataanalytics{environment}.cdp_ng.salted_puid(puid) brand_firstpartyid,
      dataanalytics{environment}.cdp_ng.salted_puid(puid) salted_puid,
      puid,
      platform_account_id,
      vendor civ_civ7_vendor,
      service civ_civ7_service,
      current_gen_platform civ_civ7_current_gen_platform,
      next_gen_platform civ_civ7_next_gen_platform,
      country_last civ_civ7_country_last,
      install_date civ_civ7_install_date,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_install_date_grouping,
      is_viable civ_civ7_viable,
      viable_date civ_civ7_viable_date,
      civ7_premium_sku as civ_civ7_premium_sku,
      civ7_preorder as civ_civ7_is_pre_order,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_expansion_pack_1,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_expansion_pack_2,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_expansion_pack_3,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_expansion_pack_4,
      CAST(NULL AS FLOAT) as civ_civ7_price_known,
      CAST(NULL AS FLOAT) as civ_civ7_price_probabilistic,
      CAST(NULL AS BOOLEAN) as civ_civ7_is_ftp_install,
      last_seen_date civ_civ7_last_seen_date,
      last_seen_date_grouping civ_civ7_last_seen_grouping,
      player_type civ_civ7_player_type,
      days_active civ_civ7_days_active,
      ifnull(single_player_games_played + multiplayer_games_played, 0) civ_civ7_games_played,
      ifnull(single_player_time_spent + multiplayer_time_spent, 0) civ_civ7_time_spent,
      ifnull(single_player_games_success + multiplayer_games_success, 0) civ_civ7_games_success,
      ifnull(single_player_games_fail + multiplayer_games_fail, 0) civ_civ7_games_fail,
      ifnull(single_player_success_rate + multiplayer_success_rate, 0) civ_civ7_success_rate,
      days_active_grouping civ_civ7_days_active_grouping,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_games_played_grouping,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_success_rate_grouping,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_game_progress_grouping,
      mode_preference as civ_civ7_mode_preference,
      character_preference as civ_civ7_character_preference,
      CAST(NULL AS BOOLEAN) as civ_civ7_bought_vc_converted,
      CAST(NULL AS FLOAT) as civ_civ7_bought_vc_transactions,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_bought_vc_transactions_grouping,
      CAST(NULL AS FLOAT) as civ_civ7_bought_vc_total,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_bought_vc_grouping,
      CAST(NULL AS DATE) as civ_civ7_bought_vc_first_date,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_bought_vc_first_date_grouping,
      CAST(NULL AS DATE) as civ_civ7_bought_vc_last_date,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_bought_vc_last_date_grouping,
      CAST(NULL AS FLOAT) as civ_civ7_bought_vc_dollar_value,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_bought_vc_dollar_grouping,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_earned_bought_gifted_ratio_grouping,
      CAST(NULL AS BOOLEAN) as civ_civ7_has_earned_vc,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_earned_vc_total,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_earned_vc_grouping,
      CAST(NULL AS DATE) as civ_civ7_earned_vc_first_date,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_earned_vc_first_date_grouping,
      CAST(NULL AS DATE) as civ_civ7_earned_vc_last_date,
      CAST(NULL AS FLOAT) as civ_civ7_earned_vc_actions,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_earned_vc_actions_grouping,
      CAST(NULL AS BOOLEAN) as civ_civ7_has_gifted_vc,
      CAST(NULL AS FLOAT) as civ_civ7_gifted_vc_total,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_gifted_vc_grouping,
      CAST(NULL AS DATE) as civ_civ7_gifted_vc_last_date,
      CAST(NULL AS DATE) as civ_civ7_gifted_vc_first_date,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_gifted_vc_first_date_grouping,
      CAST(NULL AS FLOAT) as civ_civ7_gifted_vc_transactions,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_gifted_vc_transactions_grouping,
      CAST(NULL AS BOOLEAN) as civ_civ7_has_spent_vc,
      CAST(NULL AS FLOAT) as civ_civ7_vc_spent_total,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_vc_spent_grouping,
      CAST(NULL AS DATE) as civ_civ7_vc_spent_first_date,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_vc_spent_first_date_grouping,
      CAST(NULL AS DATE) as civ_civ7_vc_spent_last_date,
      CAST(NULL AS FLOAT) as civ_civ7_vc_spent_actions,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_vc_spent_actions_grouping,
      CAST(NULL AS FLOAT) as civ_civ7_current_vc_balance,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_current_vc_balance_grouping,
      single_player_first_seen as civ_civ7_single_player_first_seen,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_single_player_first_seen_grouping,
      single_player_last_seen as civ_civ7_single_player_last_seen,
      single_player_last_seen_grouping as civ_civ7_single_player_last_seen_grouping,
      single_player_active_days as civ_civ7_single_player_days_active,
      single_player_games_played as civ_civ7_single_player_games_played,
      single_player_time_spent as civ_civ7_single_player_time_spent,
      single_player_games_success as civ_civ7_single_player_games_success,
      single_player_games_fail as civ_civ7_single_player_games_fail,
      single_player_success_rate as civ_civ7_single_player_success_rate,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_single_player_success_rate_grouping,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_single_player_days_active_grouping,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_single_player_games_played_grouping,
      multiplayer_first_seen as civ_civ7_multiplayer_first_seen,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_multiplayer_first_seen_grouping,
      multiplayer_last_seen as civ_civ7_multiplayer_last_seen,
      multiplayer_last_seen_grouping as civ_civ7_multiplayer_last_seen_grouping,
      multiplayer_days_active as civ_civ7_multiplayer_days_active,
      multiplayer_games_played as civ_civ7_multiplayer_games_played,
      multiplayer_time_spent as civ_civ7_multiplayer_time_spent,
      multiplayer_games_success as civ_civ7_multiplayer_games_success,
      multiplayer_games_fail as civ_civ7_multiplayer_games_fail,
      multiplayer_success_rate as civ_civ7_multiplayer_success_rate,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_multiplayer_success_rate_grouping,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_multiplayer_days_active_grouping,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_multiplayer_games_played_grouping,
      CAST(NULL AS DATE) as civ_civ7_other_first_seen,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_other_first_seen_grouping,
      CAST(NULL AS DATE) as civ_civ7_other_last_seen,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_other_last_seen_grouping,
      CAST(NULL AS FLOAT) as civ_civ7_other_days_active,
      CAST(NULL AS FLOAT) as civ_civ7_other_games_played,
      CAST(NULL AS FLOAT) as civ_civ7_other_time_spent,
      CAST(NULL AS FLOAT) as civ_civ7_other_games_success,
      CAST(NULL AS FLOAT) as civ_civ7_other_games_fail,
      CAST(NULL AS FLOAT) as civ_civ7_other_success_rate,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_other_success_rate_grouping,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_other_days_active_grouping,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_other_games_played_grouping,
      CAST(NULL AS BOOLEAN) as civ_civ7_single_player_has_earned_vc,
      CAST(NULL AS FLOAT) as civ_civ7_single_player_earned_vc_total,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_single_player_earned_vc_grouping,
      CAST(NULL AS DATE) as civ_civ7_single_player_earned_vc_first_date,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_single_player_earned_first_date_grouping,
      CAST(NULL AS DATE) as civ_civ7_single_player_earned_vc_last_date,
      CAST(NULL AS FLOAT) as civ_civ7_single_player_earned_vc_transactions,
      CAST(NULL AS FLOAT) as civ_civ7_single_player_vc_spent_total,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_single_player_vc_spent_grouping,
      CAST(NULL AS FLOAT) as civ_civ7_single_player_vc_spent_actions,
      CAST(NULL AS DATE) as civ_civ7_single_player_vc_spent_first_date,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_single_player_vc_spent_first_date_grouping,
      CAST(NULL AS DATE) as civ_civ7_single_player_vc_spent_last_date,
      CAST(NULL AS BOOLEAN) as civ_civ7_multiplayer_has_earned_vc,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_multiplayer_earned_vc_total,
      CAST(NULL AS DATE) as civ_civ7_multiplayer_earned_vc_grouping,
      CAST(NULL AS FLOAT) as civ_civ7_multiplayer_earned_vc_transactions,
      CAST(NULL AS DATE) as civ_civ7_multiplayer_earned_vc_first_date,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_multiplayer_earned_first_date_grouping,
      CAST(NULL AS DATE) as civ_civ7_multiplayer_earned_vc_last_date,
      CAST(NULL AS FLOAT) as civ_civ7_multiplayer_vc_spent_total,
      CAST(NULL AS FLOAT) as civ_civ7_multiplayer_vc_spent_actions,
      CAST(NULL AS DATE) as civ_civ7_multiplayer_vc_spent_first_date,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_multiplayer_vc_spent_first_date_grouping,
      CAST(NULL AS DATE) as civ_civ7_multiplayer_vc_spent_last_date,
      CAST(NULL AS BOOLEAN) as civ_civ7_other_has_earned_vc,
      CAST(NULL AS FLOAT) as civ_civ7_other_earned_vc_total,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_other_earned_vc_grouping,
      CAST(NULL AS FLOAT) as civ_civ7_other_earned_vc_transactions,
      CAST(NULL AS DATE) as civ_civ7_other_earned_vc_first_date,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_other_earned_first_date_grouping,
      CAST(NULL AS DATE) as civ_civ7_other_earned_vc_last_date,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_other_vc_spent_total,
      CAST(NULL AS FLOAT) as civ_civ7_other_vc_spent_actions,
      CAST(NULL AS DATE) as civ_civ7_other_vc_spent_first_date,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_other_vc_spent_first_date_grouping,
      CAST(NULL AS DATE) as civ_civ7_other_vc_spent_last_date,
      cast(ifnull(sku, 'Standard') as varchar(50)) as civ_civ7_custom_grouping_01,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_custom_grouping_02,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_custom_grouping_03,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_custom_grouping_04,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_custom_grouping_05,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_custom_grouping_06,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_custom_grouping_07,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_custom_grouping_08,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_custom_grouping_09,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_custom_grouping_10,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_custom_grouping_11,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_custom_grouping_12,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_custom_grouping_13,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_custom_grouping_14,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_custom_grouping_15,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_custom_grouping_16,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_custom_grouping_17,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_custom_grouping_18,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_custom_grouping_19,
      CAST(NULL AS VARCHAR(255)) as civ_civ7_custom_grouping_20,
      CAST(NULL AS DECIMAL(18, 2)) as civ_civ7_custom_value_01,
      CAST(NULL AS DECIMAL(18, 2)) as civ_civ7_custom_value_02,
      CAST(NULL AS DECIMAL(18, 2)) as civ_civ7_custom_value_03,
      CAST(NULL AS DECIMAL(18, 2)) as civ_civ7_custom_value_04,
      CAST(NULL AS DECIMAL(18, 2)) as civ_civ7_custom_value_05,
      CAST(NULL AS DECIMAL(18, 2)) as civ_civ7_custom_value_06,
      CAST(NULL AS DECIMAL(18, 2)) as civ_civ7_custom_value_07,
      CAST(NULL AS DECIMAL(18, 2)) as civ_civ7_custom_value_08,
      CAST(NULL AS DECIMAL(18, 2)) as civ_civ7_custom_value_09,
      CAST(NULL AS DECIMAL(18, 2)) as civ_civ7_custom_value_10,
      CAST(NULL AS DECIMAL(18, 2)) as civ_civ7_custom_value_11,
      CAST(NULL AS DECIMAL(18, 2)) as civ_civ7_custom_value_12,
      CAST(NULL AS DECIMAL(18, 2)) as civ_civ7_custom_value_13,
      CAST(NULL AS DECIMAL(18, 2)) as civ_civ7_custom_value_14,
      CAST(NULL AS DECIMAL(18, 2)) as civ_civ7_custom_value_15,
      CAST(NULL AS DECIMAL(18, 2)) as civ_civ7_custom_value_16,
      CAST(NULL AS DECIMAL(18, 2)) as civ_civ7_custom_value_17,
      CAST(NULL AS DECIMAL(18, 2)) as civ_civ7_custom_value_18,
      CAST(NULL AS DECIMAL(18, 2)) as civ_civ7_custom_value_19,
      CAST(NULL AS DECIMAL(18, 2)) as civ_civ7_custom_value_20
    from
      all_metrics m

  """

  df = spark.sql(sql_create_view)
  return df


# COMMAND ----------

def create_civ7_summary_table(environment, spark):
    sql = f"""
    CREATE TABLE IF NOT EXISTS dataanalytics{environment}.cdp_ng.civ7_summary (
        brand_firstpartyid STRING,
        salted_puid STRING,
        puid STRING,
        civ_civ7_vendor STRING,
        civ_civ7_service STRING,
        civ_civ7_current_gen_platform STRING,
        civ_civ7_next_gen_platform STRING,
        civ_civ7_country_last STRING,
        civ_civ7_install_date Date,
        civ_civ7_install_date_grouping STRING,
        civ_civ7_viable BOOLEAN,
        civ_civ7_viable_date Date,
        civ_civ7_premium_sku BOOLEAN,
        civ_civ7_is_pre_order BOOLEAN,
        civ_civ7_expansion_pack_1 STRING,
        civ_civ7_expansion_pack_2 STRING,
        civ_civ7_expansion_pack_3 STRING,
        civ_civ7_expansion_pack_4 STRING,
        civ_civ7_price_known FLOAT,
        civ_civ7_price_probabilistic FLOAT,
        civ_civ7_is_ftp_install BOOLEAN,
        civ_civ7_last_seen_date Date,
        civ_civ7_last_seen_grouping STRING,
        civ_civ7_player_type STRING,
        civ_civ7_days_active FLOAT,
        civ_civ7_games_played FLOAT,
        civ_civ7_time_spent FLOAT,
        civ_civ7_games_success FLOAT,
        civ_civ7_games_fail FLOAT,
        civ_civ7_success_rate DECIMAL(18, 2),
        civ_civ7_days_active_grouping STRING,
        civ_civ7_games_played_grouping STRING,
        civ_civ7_success_rate_grouping STRING,
        civ_civ7_game_progress_grouping STRING,
        civ_civ7_mode_preference STRING,
        civ_civ7_character_preference STRING,
        civ_civ7_bought_vc_converted BOOLEAN,
        civ_civ7_bought_vc_transactions FLOAT,
        civ_civ7_bought_vc_transactions_grouping STRING,
        civ_civ7_bought_vc_total FLOAT,
        civ_civ7_bought_vc_grouping STRING,
        civ_civ7_bought_vc_first_date Date,
        civ_civ7_bought_vc_first_date_grouping STRING,
        civ_civ7_bought_vc_last_date Date,
        civ_civ7_bought_vc_last_date_grouping STRING,
        civ_civ7_bought_vc_dollar_value FLOAT,
        civ_civ7_bought_vc_dollar_grouping STRING,
        civ_civ7_earned_bought_gifted_ratio_grouping STRING,
        civ_civ7_has_earned_vc BOOLEAN,
        civ_civ7_earned_vc_total FLOAT,
        civ_civ7_earned_vc_grouping STRING,
        civ_civ7_earned_vc_first_date Date,
        civ_civ7_earned_vc_first_date_grouping STRING,
        civ_civ7_earned_vc_last_date Date,
        civ_civ7_earned_vc_actions FLOAT,
        civ_civ7_earned_vc_actions_grouping STRING,
        civ_civ7_has_gifted_vc BOOLEAN,
        civ_civ7_gifted_vc_total FLOAT,
        civ_civ7_gifted_vc_grouping STRING,
        civ_civ7_gifted_vc_last_date Date,
        civ_civ7_gifted_vc_first_date Date,
        civ_civ7_gifted_vc_first_date_grouping STRING,
        civ_civ7_gifted_vc_transactions FLOAT,
        civ_civ7_gifted_vc_transactions_grouping STRING,
        civ_civ7_has_spent_vc BOOLEAN,
        civ_civ7_vc_spent_total FLOAT,
        civ_civ7_vc_spent_grouping STRING,
        civ_civ7_vc_spent_first_date Date,
        civ_civ7_vc_spent_first_date_grouping STRING,
        civ_civ7_vc_spent_last_date Date,
        civ_civ7_vc_spent_actions FLOAT,
        civ_civ7_vc_spent_actions_grouping STRING,
        civ_civ7_current_vc_balance FLOAT,
        civ_civ7_current_vc_balance_grouping STRING,
        civ_civ7_single_player_first_seen Date,
        civ_civ7_single_player_first_seen_grouping STRING,
        civ_civ7_single_player_last_seen Date,
        civ_civ7_single_player_last_seen_grouping STRING,
        civ_civ7_single_player_days_active FLOAT,
        civ_civ7_single_player_games_played FLOAT,
        civ_civ7_single_player_time_spent FLOAT,
        civ_civ7_single_player_games_success FLOAT,
        civ_civ7_single_player_games_fail FLOAT,
        civ_civ7_single_player_success_rate FLOAT,
        civ_civ7_single_player_success_rate_grouping STRING,
        civ_civ7_single_player_days_active_grouping STRING,
        civ_civ7_single_player_games_played_grouping STRING,
        civ_civ7_multiplayer_first_seen Date,
        civ_civ7_multiplayer_first_seen_grouping STRING,
        civ_civ7_multiplayer_last_seen Date,
        civ_civ7_multiplayer_last_seen_grouping STRING,
        civ_civ7_multiplayer_days_active FLOAT,
        civ_civ7_multiplayer_games_played FLOAT,
        civ_civ7_multiplayer_time_spent FLOAT,
        civ_civ7_multiplayer_games_success FLOAT,
        civ_civ7_multiplayer_games_fail FLOAT,
        civ_civ7_multiplayer_success_rate FLOAT,
        civ_civ7_multiplayer_success_rate_grouping STRING,
        civ_civ7_multiplayer_days_active_grouping STRING,
        civ_civ7_multiplayer_games_played_grouping STRING,
        civ_civ7_other_first_seen Date,
        civ_civ7_other_first_seen_grouping STRING,
        civ_civ7_other_last_seen Date,
        civ_civ7_other_last_seen_grouping STRING,
        civ_civ7_other_days_active FLOAT,
        civ_civ7_other_games_played FLOAT,
        civ_civ7_other_time_spent FLOAT,
        civ_civ7_other_games_success FLOAT,
        civ_civ7_other_games_fail FLOAT,
        civ_civ7_other_success_rate FLOAT,
        civ_civ7_other_success_rate_grouping STRING,
        civ_civ7_other_days_active_grouping STRING,
        civ_civ7_other_games_played_grouping STRING,
        civ_civ7_single_player_has_earned_vc BOOLEAN,
        civ_civ7_single_player_earned_vc_total FLOAT,
        civ_civ7_single_player_earned_vc_grouping STRING,
        civ_civ7_single_player_earned_vc_first_date Date,
        civ_civ7_single_player_earned_first_date_grouping STRING,
        civ_civ7_single_player_earned_vc_last_date Date,
        civ_civ7_single_player_earned_vc_transactions FLOAT,
        civ_civ7_single_player_vc_spent_total FLOAT,
        civ_civ7_single_player_vc_spent_grouping STRING,
        civ_civ7_single_player_vc_spent_actions FLOAT,
        civ_civ7_single_player_vc_spent_first_date Date,
        civ_civ7_single_player_vc_spent_first_date_grouping STRING,
        civ_civ7_single_player_vc_spent_last_date Date,
        civ_civ7_multiplayer_has_earned_vc BOOLEAN,
        civ_civ7_multiplayer_earned_vc_total STRING,
        civ_civ7_multiplayer_earned_vc_grouping Date,
        civ_civ7_multiplayer_earned_vc_transactions STRING,
        civ_civ7_multiplayer_earned_vc_first_date Date,
        civ_civ7_multiplayer_earned_first_date_grouping FLOAT,
        civ_civ7_multiplayer_earned_vc_last_date FLOAT,
        civ_civ7_multiplayer_vc_spent_total STRING,
        civ_civ7_multiplayer_vc_spent_actions FLOAT,
        civ_civ7_multiplayer_vc_spent_first_date Date,
        civ_civ7_multiplayer_vc_spent_first_date_grouping STRING,
        civ_civ7_multiplayer_vc_spent_last_date Date,
        civ_civ7_other_has_earned_vc BOOLEAN,
        civ_civ7_other_earned_vc_total STRING,
        civ_civ7_other_earned_vc_grouping Date,
        civ_civ7_other_earned_vc_transactions STRING,
        civ_civ7_other_earned_vc_first_date Date,
        civ_civ7_other_earned_first_date_grouping FLOAT,
        civ_civ7_other_earned_vc_last_date FLOAT,
        civ_civ7_other_vc_spent_total STRING,
        civ_civ7_other_vc_spent_actions FLOAT,
        civ_civ7_other_vc_spent_first_date Date,
        civ_civ7_other_vc_spent_first_date_grouping STRING,
        civ_civ7_other_vc_spent_last_date Date,
        civ_civ7_custom_grouping_01 STRING,
        civ_civ7_custom_grouping_02 STRING,
        civ_civ7_custom_grouping_03 STRING,
        civ_civ7_custom_grouping_04 STRING,
        civ_civ7_custom_grouping_05 STRING,
        civ_civ7_custom_grouping_06 STRING,
        civ_civ7_custom_grouping_07 STRING,
        civ_civ7_custom_grouping_08 STRING,
        civ_civ7_custom_grouping_09 STRING,
        civ_civ7_custom_grouping_10 STRING,
        civ_civ7_custom_grouping_11 STRING,
        civ_civ7_custom_grouping_12 STRING,
        civ_civ7_custom_grouping_13 STRING,
        civ_civ7_custom_grouping_14 STRING,
        civ_civ7_custom_grouping_15 STRING,
        civ_civ7_custom_grouping_16 STRING,
        civ_civ7_custom_grouping_17 STRING,
        civ_civ7_custom_grouping_18 STRING,
        civ_civ7_custom_grouping_19 STRING,
        civ_civ7_custom_grouping_20 STRING,
        civ_civ7_custom_value_01 DECIMAL(18, 2),
        civ_civ7_custom_value_02 DECIMAL(18, 2),
        civ_civ7_custom_value_03 DECIMAL(18, 2),
        civ_civ7_custom_value_04 DECIMAL(18, 2),
        civ_civ7_custom_value_05 DECIMAL(18, 2),
        civ_civ7_custom_value_06 DECIMAL(18, 2),
        civ_civ7_custom_value_07 DECIMAL(18, 2),
        civ_civ7_custom_value_08 DECIMAL(18, 2),
        civ_civ7_custom_value_09 DECIMAL(18, 2),
        civ_civ7_custom_value_10 DECIMAL(18, 2),
        civ_civ7_custom_value_11 DECIMAL(18, 2),
        civ_civ7_custom_value_12 DECIMAL(18, 2),
        civ_civ7_custom_value_13 DECIMAL(18, 2),
        civ_civ7_custom_value_14 DECIMAL(18, 2),
        civ_civ7_custom_value_15 DECIMAL(18, 2),
        civ_civ7_custom_value_16 DECIMAL(18, 2),
        civ_civ7_custom_value_17 DECIMAL(18, 2),
        civ_civ7_custom_value_18 DECIMAL(18, 2),
        civ_civ7_custom_value_19 DECIMAL(18, 2),
        civ_civ7_custom_value_20 DECIMAL(18, 2)
    )
    """

    properties = {
        "delta.feature.allowColumnDefaults": "supported"  # this won't work until iceberg v3 is released
    }

    create_table(spark, sql, properties)
    return (
        f"dbfs:/tmp/dataanalytics/cdp_ng/streaming/run{environment}/civ7_summary"
    )


# COMMAND ----------

def run_batch():
    # Parse the environment argument
    input_param = dbutils_input_params()
    environment = input_param.get('environment', set_environment())

    # Create a Spark session
    spark = SparkSession.builder.appName("Hydra").getOrCreate()

    # Create the view
    df = create_civ7_summary_view(environment, spark)
    checkpoint_location = create_civ7_summary_table(environment ,spark)

    spark.sql(f"""select * from dataanalytics{environment}.cdp_ng.vw_civ7_summary""").show()

# COMMAND ----------

if __name__ == "__main__":
    run_batch()
