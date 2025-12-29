# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------
# MAGIC %run ../../../../../../utils/ddl/table_functions

# COMMAND ----------
def create_pga2k25_summary_view(environment, spark):
  sql_create_view = f"""
   create view if not exists dataanalytics{environment}.cdp_ng.vw_pga2k25_summary
    as
   with services as (
    select
      case
        service
        WHEN 'xbl' THEN 'Xbox Live' -- WHEN 'steam' THEN 'PC'
        else service
      end as join_key,
      service,
      vendor
    from
      reference_customer.platform.service
    group by
      1,
      2,
      3
  ),
  games_success_fail as (
    select
      playerPublicId player_id,
      max(totalscore :: INT) AS total_score,
      max(coursepar :: INT) AS par_score,
      total_score - par_score AS score_to_par
    from
      bluenose{environment}.raw.roundstatus
    where
      lessonname is null
    group by
      player_id
  ),
  fact_players_ltd as (
    select
      distinct lower(f.player_id) player_id,
      first_value(v.platform) over (
        partition by f.player_id
        order by
          received_on :: timestamp desc
      ) platform,
      first_value(v.vendor) over (
        partition by f.player_id
        order by
          received_on :: timestamp desc
      ) vendor,
      first_value(s.service) over (
        partition by f.player_id
        order by
          received_on :: timestamp desc
      ) service,
      max(iff(extra_info_5 in ('demo', 'converted'), 1, 0)) over (partition by f.player_id) is_ftp_install,
      cast(NULL as boolean) is_preorder --  todo
  ,
      first_value(f.country_code) ignore nulls over (
        partition by f.player_id
        order by
          received_on :: timestamp desc
      ) as last_country_code,
      count_if(
        event_trigger in ('RoundWon', 'RoundComplete')
        and source_table = 'roundstatus'
      ) over (partition by f.player_id) games_success,
      count_if(
        (
          event_trigger = 'RoundQuit'
          or fail.score_to_par > 0
        )
        and source_table = 'roundstatus'
      ) over (partition by f.player_id) games_fail,
      count_if(
        event_trigger in ('RoundWon', 'RoundComplete')
        and source_table = 'roundstatus'
        and EXTRA_INFO_2 ilike '%career%'
      ) over (partition by f.player_id) mycareer_games_success,
      min(received_on :: date) over (partition by f.player_id) as install_date,
      max(received_on :: date) over (partition by f.player_id) as last_seen_date,
      first_value(
        iff(
          V.PLATFORM NOT IN ('XBSX', 'PS5', 'Windows', 'PC'),
          v.platform,
          null
        )
      ) ignore nulls over (
        partition by f.player_id
        order by
          received_on :: timestamp desc
      ) current_gen_platform,
      first_value(
        iff(
          v.platform in ('XBSX', 'PS5', 'Windows', 'PC'),
          V.PLATFORM,
          NULL
        )
      ) ignore nulls over (
        partition by f.player_id
        order by
          received_on :: timestamp desc
      ) next_gen_platform
    from
      bluenose{environment}.intermediate.fact_player_activity f
      join services s on lower(s.join_key) = lower(f.service)
      join reference_customer.platform.platform v on f.platform = v.src_platform
      left join games_success_fail fail on f.player_id = fail.player_id
      LEFT JOIN coretech{environment}.sso.accountdeleteevent sso ON sso.accountId = f.player_id
    WHERE
      f.platform IS NOT NULL
      and sso.accountId is null
      and f.received_on :: timestamp >= '2025-02-04' :: timestamp
  ),
  fact_player_activity as (
    select
      player_id,
      count(distinct date) as active_days
    from
      bluenose{environment}.managed.fact_player_link_activity f
    group by
      1
  ),
  fact_player_metrics as (
    select
      player_id --  platform, service ?
  ,
      sum(agg_game_status_2) games_played,
      min(iff(game_mode ilike 'ranked%', date, null)) ranked_tours_first_seen,
      max(iff(game_mode ilike 'ranked%', date, null)) ranked_tours_last_seen,
      sum(
        iff(game_mode ilike 'ranked%', agg_game_status_2, 0)
      ) ranked_tours_games_played,
      count(
        distinct iff(lower(game_mode) ilike 'ranked%', date, null)
      ) ranked_tours_days_active,
      min(iff(game_mode ilike 'career', date, null)) mycareer_first_seen,
      max(iff(game_mode ilike 'career', date, null)) mycareer_last_seen,
      sum(
        iff(game_mode ilike 'career', agg_game_status_2, 0)
      ) mycareer_games_played,
      count(
        distinct iff(game_mode ilike 'career', date, null)
      ) mycareer_days_active
    from
      bluenose{environment}.managed.fact_player_game_status_daily
    group by
      player_id
  ),
  vc_metrics as (
    SELECT
      player_id,
      min(
        iff(
          lower(action_type) == 'spend',
          received_on :: date,
          null
        )
      ) vc_spent_first_date,
      max(
        iff(
          lower(action_type) == 'spend',
          received_on :: date,
          null
        )
      ) vc_spent_last_date,
      sum(
        iff(
          lower(action_type) == 'spend',
          currency_amount,
          0
        )
      ) vc_spent_total,
      min(
        iff(
          lower(action_type) == 'purchase',
          received_on :: date,
          null
        )
      ) vc_bought_first_date,
      max(
        iff(
          lower(action_type) == 'purchase',
          received_on :: date,
          null
        )
      ) vc_bought_last_date,
      sum(
        iff(
          lower(action_type) == 'purchase',
          currency_amount,
          0
        )
      ) vc_bought_total,
      sum(
        iff(
          lower(action_type) == 'earn',
          currency_amount,
          0
        )
      ) vc_earned_total,
      count(
        case
          when action_type ilike '%purchase%' then 1
          else 0
        end
      ) bought_vc_transactions,
      vc_bought_total > 0 bought_vc_converted,
      case
        when vc_bought_total = 0 then 0
        when vc_bought_total < 5000 then 2
        when vc_bought_total < 15000 then 5
        when vc_bought_total < 35000 then 10
        when vc_bought_total < 75000 then 20
        when vc_bought_total < 200000 then 50
        when vc_bought_total < 450000 then 100
        else -99
      end as bought_vc_dollar_value,
      vc_bought_total - vc_spent_total + vc_earned_total current_vc_balance --  ??? todo vc_bought_total - vc_spent_total + vc_earned_total
    from
      bluenose{environment}.intermediate.fact_player_transaction
    where
      currency_type = 'VC'
      and player_id != 'anonymous'
    group by
      player_id
    having
      vc_spent_total + vc_bought_total + vc_earned_total != 0
  ),
  sku_details as (
    with player_entitlements as (
      select
        distinct playerPublicId as player_id,
        array_join(collect_list(activedlc), ',') as entitlements
      from
        bluenose{environment}.raw.applicationsession
      where
        activedlc is not null
      group by
        1
    )
    select
      player_id,
      case
        when entitlements ilike '%SundayRedPack%'
        and entitlements ilike '%LegendsChoicePack%'
        AND entitlements ilike '%MembersPass%'
        AND entitlements ilike '%AdidasButterPack%'
        AND entitlements ilike '%BirdiePack%' then 'Legend Edition'
        when entitlements ilike '%AdidasButterPack%'
        and entitlements ilike '%BirdiePack%' then 'Deluxe Edition'
        when entitlements ilike '%StandardEdition%' then 'Standard Edition'
        else 'Standard Edition'
      end as sku,
      iff(
        sku in ('Legend Edition', 'Deluxe Edition'),
        True,
        False
      ) sku_is_premium
    from
      player_entitlements
  ),
  career_diff as (
    select
      distinct playerPublicId as player_id,
      first_value(difficulty) over (
        partition by playerPublicId
        order by
          receivedOn :: timestamp desc ROWS BETWEEN UNBOUNDED PRECEDING
          AND UNBOUNDED FOLLOWING
      ) as last_difficulty
    from
      bluenose{environment}.raw.roundstatus
    where
      mode ilike '%Career%'
      and playerPublicId is not null
  ),
  multiplier as (
    select
      distinct playerPublicId as player_id,
      first_value(difficultymultiplier) over (
        partition by playerPublicId
        order by
          receivedOn :: timestamp desc ROWS BETWEEN UNBOUNDED PRECEDING
          AND UNBOUNDED FOLLOWING
      ) as difficulty_multiplier
    from
      bluenose{environment}.raw.roundstatus
    where
      playerPublicId is not null
      and playerPublicId != ''
  ),
  custom_values as (
    select
      player_id,
      sum(ovr_level) ovr_level,
      sum(att_points_balance) att_points_balance
    from
      (
        select
          distinct player_id --,  received_on::date, subject_id, extra_info_2
  ,
          first_value(iff(subject_id == 'ovr', extra_info_2, null)) over (
            partition by player_id,
            subject_id
            order by
              received_on :: date desc
          ) ovr_level,
          first_value(
            iff(
              subject_id == 'attributepointsremaining',
              extra_info_2,
              null
            )
          ) over (
            partition by player_id,
            subject_id
            order by
              received_on :: date desc
          ) att_points_balance
        from
          bluenose{environment}.intermediate.fact_player_progression
        where
          subject_id in ('attributepointsremaining', 'ovr')
      )
    group by
      player_id
  ),
  played_other_titles as (
    select
      distinct p.puid,
      max(
        iff(
          (
            pga_has_played_PGA2K21
            or pga_has_played_PGA2K23
            or WWE_HAS_PLAYED_WWE2K24
            or N2K_HAS_PLAYED_NBA2K25
            or TSN_HAS_PLAYED_TSN2K25
          ),
          True,
          False
        )
      ) as has_played_other_titles
    from
      dataanalytics{environment}.cdp_ng.franchise_pga p
      left join sf_databricks_migration.sf_dbx.franchise_wwe w on w.puid = p.puid
      left join sf_databricks_migration.sf_dbx.franchise_nba n on n.puid = p.puid
      left join sf_databricks_migration.sf_dbx.franchise_tsn t on t.puid = p.puid
    group by
      p.puid
  ),
  fpids as (
    select
      distinct le.accountid platform_id,
      dataanalytics{environment}.cdp_ng.fpid_decryption(le.firstpartyId) puid
    from
      coretech{environment}.sso.linkevent le
      join reference{environment}.title.dim_title t on le.appId = t.app_id
      and title ilike '%2K Portal%'
      and le.firstpartyId is not null
      and le.firstpartyId not ilike '%@%'
    where
      accounttype = 2
      and targetaccounttype = 3
      and le.firstPartyId != 'None' -- fix AES_CRYPTO_ERROR
    union
    select
      distinct le.targetaccountId platform_id,
      dataanalytics{environment}.cdp_ng.fpid_decryption(le.firstpartyId) puid
    from
      coretech{environment}.sso.linkevent le
      join reference{environment}.title.dim_title t on le.appId = t.app_id
      and title ilike '%2K Portal%'
      and le.firstpartyId is not null
      and le.firstpartyId not ilike '%@%'
    where
      accounttype = 3
      and targetaccounttype = 2
      and le.firstPartyId != 'None' -- fix AES_CRYPTO_ERROR
    union
    select
      distinct f.player_id platform_id,
      first_party_id puid
    from
      bluenose{environment}.intermediate.fact_player_activity f
      join reference_customer.customer.vw_ctp_profile le on lower(f.player_id) = lower(le.public_id)
    where
      le.first_party_id is not null
  ),
  all_metrics as (
    select
      distinct lower(f.player_id) platform_account_id,
      fpd.puid puid,
      f.vendor,
      f.service,
      max(current_gen_platform) current_gen_platform,
      iff(
        max(next_gen_platform) = 'Windows',
        'PC',
        max(next_gen_platform)
      ) next_gen_platform,
      to_date(max(f.last_seen_date) :: timestamp) last_seen_date,
      to_date(min(f.install_date) :: timestamp) install_date,
      max(ifnull(sku.sku_is_premium, false)) sku_is_premium,
      max(f.is_preorder) is_preorder,
      max(f.is_ftp_install) is_ftp_install,
      max(ifnull(f.last_country_code, 'ZZ')) country_last,
      ifnull(max(a.active_days), 0) active_days,
      ifnull(max(m.games_played), 0) games_played,
      ifnull(max(f.games_success), 0) games_success,
      ifnull(max(f.games_fail), 0) games_fail,
      ifnull(max(f.mycareer_games_success), 0) mycareer_games_success,
      ifnull(max(m.mycareer_days_active), 0) mycareer_days_active,
      min(m.mycareer_first_seen) mycareer_first_seen,
      max(m.mycareer_last_seen) mycareer_last_seen,
      ifnull(max(m.mycareer_games_played), 0) mycareer_games_played,
      min(m.ranked_tours_first_seen) ranked_tours_first_seen,
      max(m.ranked_tours_last_seen) ranked_tours_last_seen,
      ifnull(max(m.ranked_tours_games_played), 0) ranked_tours_games_played,
      ifnull(max(m.ranked_tours_days_active), 0) ranked_tours_days_active,
      min(vcm.vc_bought_first_date) vc_bought_first_date,
      max(vcm.vc_bought_last_date) vc_bought_last_date,
      ifnull(max(vcm.bought_vc_dollar_value), 0) bought_vc_dollar_value,
      ifnull(max(vcm.vc_bought_total), 0) vc_bought_total,
      ifnull(max(vcm.bought_vc_transactions), 0) bought_vc_transactions,
      min(vcm.vc_spent_first_date) vc_spent_first_date,
      max(vcm.vc_spent_last_date) vc_spent_last_date,
      ifnull(max(vcm.vc_spent_total), 0) vc_spent_total,
      ifnull(max(vcm.vc_earned_total), 0) vc_earned_total,
      ifnull(max(vcm.bought_vc_converted), false) bought_vc_converted,
      ifnull(max(vcm.current_vc_balance), 0) current_vc_balance,
      any_value(cv.att_points_balance) att_points_balance,
      any_value(cv.ovr_level) ovr_level,
      any_value(sku.sku) sku,
      max(pot.has_played_other_titles) other_titles,
      any_value(cd.last_difficulty) last_difficulty,
      any_value(mul.difficulty_multiplier) difficulty_multiplier
    from
      fact_players_ltd f
      join fpids fpd on lower(fpd.platform_id) = lower(f.player_id)
      left join fact_player_metrics m on lower(f.player_id) = lower(m.player_id)
      left join fact_player_activity a on lower(f.player_id) = lower(a.player_id)
      left join vc_metrics vcm on lower(f.player_id) = lower(vcm.player_id)
      left join custom_values cv on lower(f.player_id) = lower(cv.player_id)
      left join sku_details sku on lower(f.player_id) = lower(sku.player_id)
      left join career_diff cd on lower(f.player_id) = lower(cd.player_id)
      left join multiplier mul on lower(f.player_id) = lower(mul.player_id)
      left join played_other_titles pot on lower(fpd.puid) = lower(pot.puid)
    group by
      lower(f.player_id),
      fpd.puid,
      -- platform_account_id,
      vendor,
      service
  )
  select
    vendor || ':' || dataanalytics{environment}.cdp_ng.salted_puid(puid) brand_firstpartyid,
    dataanalytics{environment}.cdp_ng.salted_puid(puid) salted_puid,
    puid puid,
    vendor pga2k25_vendor,
    service pga2k25_service,
    current_gen_platform pga2k25_current_gen_platform,
    next_gen_platform pga2k25_next_gen_platform,
    country_last pga2k25_country_last,
    install_date pga2k25_install_date,
    cast(null as varchar(50)) pga2k25_install_date_grouping,
    cast(null as varchar(50)) pga2k25_viable,
    cast(null as varchar(50)) pga2k25_viable_date,
    sku_is_premium pga2k25_premium_sku,
    is_preorder pga2k25_is_pre_order,
    cast(null as varchar(50)) pga2k25_expansion_pack_1,
    cast(null as varchar(50)) pga2k25_expansion_pack_2,
    cast(null as varchar(50)) pga2k25_expansion_pack_3,
    cast(null as varchar(50)) pga2k25_expansion_pack_4,
    cast(null as varchar(50)) pga2k25_price_known,
    cast(null as varchar(50)) pga2k25_price_probabilistic,
    is_ftp_install pga2k25_is_ftp_install,
    cast(last_seen_date as date) pga2k25_last_seen_date,
    case
      when datediff(day, last_seen_date, current_date) + 1 between 0
      and 13 then '0-13'
      when datediff(day, last_seen_date, current_date) + 1 between 14
      and 20 then '14-20'
      when datediff(day, last_seen_date, current_date) + 1 between 21
      and 40 then '21-40'
      when datediff(day, last_seen_date, current_date) + 1 > 40 then '40+'
      else '0-13'
    end as pga2k25_last_seen_grouping,
    cast(null as string) pga2k25_player_type,
    active_days pga2k25_days_active,
    games_played pga2k25_games_played,
    cast(null as varchar(50)) pga2k25_time_spent,
    games_success pga2k25_games_success,
    games_fail pga2k25_games_fail,
    round((games_success / games_fail) * 100, 2) pga2k25_success_rate,
    cast(null as varchar(50)) pga2k25_days_active_grouping,
    cast(null as varchar(50)) pga2k25_games_played_grouping,
    cast(null as varchar(50)) pga2k25_success_rate_grouping,
    cast(null as varchar(50)) pga2k25_game_progress_grouping,
    cast(null as varchar(50)) pga2k25_mode_preference,
    cast(null as varchar(50)) pga2k25_character_preference,
    vc_bought_total > 0 pga2k25_bought_vc_converted,
    bought_vc_transactions pga2k25_bought_vc_transactions,
    cast(null as varchar(50)) pga2k25_bought_vc_transactions_grouping,
    cast(vc_bought_total as decimal(18, 2)) pga2k25_bought_vc_total,
    cast(null as varchar(50)) pga2k25_bought_vc_grouping,
    cast(vc_bought_first_date as date) pga2k25_bought_vc_first_date,
    cast(null as varchar(50)) pga2k25_bought_vc_first_date_grouping,
    cast(vc_bought_last_date as date) pga2k25_bought_vc_last_date,
    cast(null as varchar(50)) pga2k25_bought_vc_last_date_grouping,
    cast(bought_vc_dollar_value as decimal(18, 2)) pga2k25_bought_vc_dollar_value,
    cast(null as varchar(50)) pga2k25_bought_vc_dollar_grouping,
    cast(null as varchar(50)) pga2k25_earned_bought_gifted_ratio_grouping,
    cast(null as varchar(50)) pga2k25_has_earned_vc,
    cast(null as varchar(50)) pga2k25_earned_vc_total,
    cast(null as varchar(50)) pga2k25_earned_vc_grouping,
    cast(null as varchar(50)) pga2k25_earned_vc_first_date,
    cast(null as varchar(50)) pga2k25_earned_vc_first_date_grouping,
    cast(null as varchar(50)) pga2k25_earned_vc_last_date,
    cast(null as varchar(50)) pga2k25_earned_vc_actions,
    cast(null as varchar(50)) pga2k25_earned_vc_actions_grouping,
    cast(null as varchar(50)) pga2k25_has_granted_vc,
    cast(null as varchar(50)) pga2k25_granted_vc_total,
    cast(null as varchar(50)) pga2k25_granted_vc_grouping,
    cast(null as varchar(50)) pga2k25_granted_vc_last_date,
    cast(null as varchar(50)) pga2k25_granted_vc_first_date,
    cast(null as varchar(50)) pga2k25_granted_vc_first_date_grouping,
    cast(null as varchar(50)) pga2k25_granted_vc_transactions,
    cast(null as varchar(50)) pga2k25_granted_vc_transactions_grouping,
    cast(null as varchar(50)) pga2k25_has_gifted_vc,
    cast(null as varchar(50)) pga2k25_gifted_vc_total,
    cast(null as varchar(50)) pga2k25_gifted_vc_grouping,
    cast(null as varchar(50)) pga2k25_gifted_vc_last_date,
    cast(null as varchar(50)) pga2k25_gifted_vc_first_date,
    cast(null as varchar(50)) pga2k25_gifted_vc_first_date_grouping,
    cast(null as varchar(50)) pga2k25_gifted_vc_transactions,
    cast(null as varchar(50)) pga2k25_gifted_vc_transactions_grouping,
    vc_spent_total > 0 pga2k25_has_spent_vc,
    vc_spent_total pga2k25_vc_spent_total,
    cast(null as varchar(50)) pga2k25_vc_spent_grouping,
    vc_spent_first_date pga2k25_vc_spent_first_date,
    cast(null as varchar(50)) pga2k25_vc_spent_first_date_grouping,
    vc_spent_last_date pga2k25_vc_spent_last_date,
    cast(null as varchar(50)) pga2k25_vc_spent_actions,
    cast(null as varchar(50)) pga2k25_vc_spent_actions_grouping,
    vc_bought_total - vc_spent_total + vc_earned_total pga2k25_current_vc_balance,
    cast(null as varchar(50)) pga2k25_current_vc_balance_grouping,
    ranked_tours_first_seen pga2k25_ranked_tours_first_seen,
    cast(null as varchar(50)) pga2k25_ranked_tours_first_seen_grouping,
    ranked_tours_last_seen pga2k25_ranked_tours_last_seen,
    case
      when datediff(day, ranked_tours_last_seen, current_date) + 1 between 0
      and 13 then '0-13'
      when datediff(day, ranked_tours_last_seen, current_date) + 1 between 14
      and 20 then '14-20'
      when datediff(day, ranked_tours_last_seen, current_date) + 1 between 21
      and 40 then '21-40'
      when datediff(day, ranked_tours_last_seen, current_date) + 1 > 40 then '40+'
      else '0-13'
    end as pga2k25_ranked_tours_last_seen_grouping,
    ranked_tours_days_active pga2k25_ranked_tours_days_active,
    ranked_tours_games_played pga2k25_ranked_tours_games_played,
    cast(null as varchar(50)) pga2k25_ranked_tours_time_spent,
    cast(null as varchar(50)) pga2k25_ranked_tours_games_success,
    cast(null as varchar(50)) pga2k25_ranked_tours_games_fail,
    cast(null as varchar(50)) pga2k25_ranked_tours_success_rate,
    cast(null as varchar(50)) pga2k25_ranked_tours_success_rate_grouping,
    cast(null as varchar(50)) pga2k25_ranked_tours_days_active_grouping,
    cast(null as varchar(50)) pga2k25_ranked_tours_games_played_grouping,
    mycareer_first_seen pga2k25_mycareer_first_seen,
    cast(null as varchar(50)) pga2k25_mycareer_first_seen_grouping,
    mycareer_last_seen pga2k25_mycareer_last_seen,
    case
      when datediff(day, mycareer_last_seen, current_date) + 1 between 0
      and 13 then '0-13'
      when datediff(day, mycareer_last_seen, current_date) + 1 between 14
      and 20 then '14-20'
      when datediff(day, mycareer_last_seen, current_date) + 1 between 21
      and 40 then '21-40'
      when datediff(day, mycareer_last_seen, current_date) + 1 > 40 then '40+'
      else '0-13'
    end as pga2k25_mycareer_last_seen_grouping,
    mycareer_days_active pga2k25_mycareer_days_active,
    mycareer_games_played pga2k25_mycareer_games_played,
    cast(null as varchar(50)) pga2k25_mycareer_time_spent,
    mycareer_games_success pga2k25_mycareer_games_success,
    cast(null as varchar(50)) pga2k25_mycareer_games_fail,
    cast(null as varchar(50)) pga2k25_mycareer_success_rate,
    cast(null as varchar(50)) pga2k25_mycareer_success_rate_grouping,
    cast(null as varchar(50)) pga2k25_mycareer_days_active_grouping,
    cast(null as varchar(50)) pga2k25_mycareer_games_played_grouping,
    cast(null as varchar(50)) pga2k25_ranked_tours_has_earned_vc,
    cast(null as varchar(50)) pga2k25_ranked_tours_earned_vc_total,
    cast(null as varchar(50)) pga2k25_ranked_tours_earned_vc_grouping,
    cast(null as varchar(50)) pga2k25_ranked_tours_earned_vc_first_date,
    cast(null as varchar(50)) pga2k25_ranked_tours_earned_first_date_grouping,
    cast(null as varchar(50)) pga2k25_ranked_tours_earned_vc_last_date,
    cast(null as varchar(50)) pga2k25_ranked_tours_earned_vc_transactions,
    cast(null as varchar(50)) pga2k25_ranked_tours_vc_spent_total,
    cast(null as varchar(50)) pga2k25_ranked_tours_vc_spent_grouping,
    cast(null as varchar(50)) pga2k25_ranked_tours_vc_spent_actions,
    cast(null as varchar(50)) pga2k25_ranked_tours_vc_spent_first_date,
    cast(null as varchar(50)) pga2k25_ranked_tours_vc_spent_first_date_grouping,
    cast(null as varchar(50)) pga2k25_ranked_tours_vc_spent_last_date,
    cast(null as varchar(50)) pga2k25_mycareer_has_earned_vc,
    cast(null as varchar(50)) pga2k25_mycareer_earned_vc_total,
    cast(null as varchar(50)) pga2k25_mycareer_earned_vc_grouping,
    cast(null as varchar(50)) pga2k25_mycareer_earned_vc_transactions,
    cast(null as varchar(50)) pga2k25_mycareer_earned_vc_first_date,
    cast(null as varchar(50)) pga2k25_mycareer_earned_first_date_grouping,
    cast(null as varchar(50)) pga2k25_mycareer_earned_vc_last_date,
    cast(null as varchar(50)) pga2k25_mycareer_vc_spent_total,
    cast(null as varchar(50)) pga2k25_mycareer_vc_spent_actions,
    cast(null as varchar(50)) pga2k25_mycareer_vc_spent_first_date,
    cast(null as varchar(50)) pga2k25_mycareer_vc_spent_first_date_grouping,
    cast(null as varchar(50)) pga2k25_mycareer_vc_spent_last_date,
    cast(ifnull(sku, 'Standard Edition') as varchar(50)) pga2k25_custom_grouping_01,
    cast(ifnull(other_titles, false) as boolean) pga2k25_custom_grouping_02,
    cast(ovr_level as varchar(50)) pga2k25_custom_grouping_03,
    cast(null as varchar(50)) pga2k25_custom_grouping_04,
    cast(last_difficulty as varchar(50)) pga2k25_custom_grouping_05,
    cast(att_points_balance as varchar(50)) pga2k25_custom_grouping_06,
    cast(difficulty_multiplier as varchar(50)) pga2k25_custom_grouping_07,
    cast(null as varchar(50)) pga2k25_custom_grouping_08,
    cast(null as varchar(50)) pga2k25_custom_grouping_09,
    cast(null as varchar(50)) pga2k25_custom_grouping_10,
    cast(null as varchar(50)) pga2k25_custom_grouping_11,
    cast(null as varchar(50)) pga2k25_custom_grouping_12,
    cast(null as varchar(50)) pga2k25_custom_grouping_13,
    cast(null as varchar(50)) pga2k25_custom_grouping_14,
    cast(null as varchar(50)) pga2k25_custom_grouping_15,
    cast(null as varchar(50)) pga2k25_custom_grouping_16,
    cast(null as varchar(50)) pga2k25_custom_grouping_17,
    cast(null as varchar(50)) pga2k25_custom_grouping_18,
    cast(null as varchar(50)) pga2k25_custom_grouping_19,
    cast(null as varchar(50)) pga2k25_custom_grouping_20,
    cast(null as varchar(50)) pga2k25_custom_value_01,
    cast(null as varchar(50)) pga2k25_custom_value_02,
    cast(null as varchar(50)) pga2k25_custom_value_03,
    cast(null as varchar(50)) pga2k25_custom_value_04,
    cast(null as varchar(50)) pga2k25_custom_value_05,
    cast(null as varchar(50)) pga2k25_custom_value_06,
    cast(null as varchar(50)) pga2k25_custom_value_07,
    cast(null as varchar(50)) pga2k25_custom_value_08,
    cast(null as varchar(50)) pga2k25_custom_value_09,
    cast(null as varchar(50)) pga2k25_custom_value_10
  from
    all_metrics f


  """
  
  df = spark.sql(sql_create_view)
  return df


# COMMAND ----------
def create_pga2k25_summary_table(environment, spark):
    sql = f"""
    CREATE TABLE IF NOT EXISTS dataanalytics{environment}.cdp_ng.pga2k25_summary (
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
        pga2k25_custom_grouping_08 STRING,
        pga2k25_custom_grouping_09 STRING,
        pga2k25_custom_grouping_10 STRING,
        pga2k25_custom_grouping_11 STRING,
        pga2k25_custom_grouping_12 STRING,
        pga2k25_custom_grouping_13 STRING,
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
        pga2k25_custom_value_10 STRING
    )
    """

    properties = {
        "delta.feature.allowColumnDefaults": "supported"  # this won't work until iceberg v3 is released
    }

    create_table(spark, sql, properties)
    return (
        f"dbfs:/tmp/dataanalytics/cdp_ng/streaming/run{environment}/pga2k25_summary"
    )

# COMMAND ----------

def run_batch():
    # Parse the environment argument
    input_param = dbutils_input_params()
    environment = input_param.get('environment', set_environment())

    # Create a Spark session
    spark = SparkSession.builder.appName("Hydra").getOrCreate()
    
    # Create the view
    df = create_pga2k25_summary_view(environment, spark)
    checkpoint_location = create_pga2k25_summary_table(environment ,spark)

    spark.sql(f"""select * from dataanalytics{environment}.cdp_ng.vw_pga2k25_summary""").show()

# COMMAND ----------

if __name__ == "__main__":
    run_batch()


