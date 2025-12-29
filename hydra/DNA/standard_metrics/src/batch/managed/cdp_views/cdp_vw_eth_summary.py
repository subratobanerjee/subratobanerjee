from pyspark.sql import SparkSession
import argparse

# COMMAND ----------

def arg_parser():
    parser = argparse.ArgumentParser(description="Script to consume parameters for Databricks job")
    parser.add_argument("--environment", type=str, required=True, help="Environment name, e.g. _stg or _prod")

    args = parser.parse_args()
    environment = args.environment
    return environment

# COMMAND ----------

def create(environment, spark):
    # Define the SQL query based on the environment
    sql_query_events = f"""
    create or replace view dataanalytics{environment}.cdp_ng.vw_eth_summary( 
    parent_account_id,
    brand_firstpartyid,
    salted_puid,
    puid,
    platform_account_id,
    eth_vendor,
    eth_service,
    eth_current_gen_platform,
    eth_next_gen_platform,
    eth_country_last,
    eth_install_date,
    eth_install_date_grouping,
    eth_viable,
    eth_viable_date,
    eth_premium_sku,
    eth_is_pre_order,
    eth_expansion_pack_1,
    eth_expansion_pack_2,
    eth_expansion_pack_3,
    eth_expansion_pack_4,
    eth_price_known,
    eth_price_probabilistic,
    eth_is_ftp_install,
    eth_account_level,
    eth_last_seen_date,
    eth_last_seen_grouping,
    eth_last_session_start,
    eth_last_session_grouping,
    eth_last_session_end,
    eth_player_type,
    eth_days_active,
    eth_games_played,
    eth_time_spent,
    eth_kills,
    eth_kills_grouping,
    eth_deaths,
    eth_deaths_grouping,
    eth_kill_death_assists_ratio,
    eth_kill_death_assists_ratio_grouping,
    eth_rescues,
    eth_rescues_grouping,
    eth_revives,
    eth_revives_grouping,
    eth_distance_travelled,
    eth_distance_travelled_grouping,
    eth_games_success,
    eth_games_fail,
    eth_success_rate,
    eth_days_active_grouping,
    eth_games_played_grouping,
    eth_success_rate_grouping,
    eth_game_progress_grouping,
    eth_queue_size_preference,
    eth_party_size_preference,
    eth_mode_preference,
    eth_hero_preference,
    eth_hero_type_preference,
    eth_playstyle_preference,
    eth_foundation_preference,
    eth_last_played_arbor,
    eth_last_played_artemis,
    eth_last_played_breeze,
    eth_last_played_conduit,
    eth_last_played_gemini,
    eth_last_played_genome,
    eth_last_played_giacomo,
    eth_last_played_permafrost,
    eth_last_played_prism,
    eth_last_played_rayo,
    eth_last_played_rojas,
    eth_last_played_strider,
    eth_last_played_tempo,
    eth_last_played_terminal,
    eth_last_played_solo,
    eth_last_played_duo,
    eth_last_played_trios,
    eth_last_played_quads,
    eth_hero_mastery_arbor,
    eth_hero_mastery_artemis,
    eth_hero_mastery_breeze,
    eth_hero_mastery_conduit,
    eth_hero_mastery_gemini,
    eth_hero_mastery_genome,
    eth_hero_mastery_giacomo,
    eth_hero_mastery_permafrost,
    eth_hero_mastery_prism,
    eth_hero_mastery_rayo,
    eth_hero_mastery_rojas,
    eth_hero_mastery_strider,
    eth_hero_mastery_tempo,
    eth_hero_mastery_terminal,
    eth_has_converted_general,
    eth_total_dollar_spent,
    eth_last_purchased_base_pack,
    eth_last_purchased_deluxe_pack,
    eth_last_purchased_premium_pack,
    eth_last_purchased_hero_pass,
    eth_bought_vc_converted,
    eth_bought_vc_transactions,
    eth_bought_vc_transactions_grouping,
    eth_bought_vc_total_currency,
    eth_bought_vc_total_currency_grouping,
    eth_bought_vc_first_date,
    eth_bought_vc_first_date_grouping,
    eth_bought_vc_last_date,
    eth_bought_vc_last_date_grouping,
    eth_bought_vc_dollar_value,
    eth_bought_vc_dollar_grouping,
    eth_has_earned_vc_other,
    eth_earned_vc_other_total,
    eth_earned_vc_other_grouping,
    eth_earned_vc_other_first_date,
    eth_earned_vc_other_first_date_grouping,
    eth_earned_vc_other_last_date,
    eth_earned_vc_other_last_date_grouping,
    eth_earned_vc_other_actions,
    eth_earned_vc_other_actions_grouping,
    eth_earned_vc_battlepass_total,
    eth_earned_vc_battlepass_grouping,
    eth_earned_vc_battlepass_first_date,
    eth_earned_vc_battlepass_first_date_grouping,
    eth_earned_vc_battlepass_last_date,
    eth_earned_vc_battlepass_last_date_grouping,
    eth_earned_vc_battlepass_actions,
    eth_earned_vc_battlepass_actions_grouping,
    eth_has_spent_vc,
    eth_vc_spent_total,
    eth_vc_spent_grouping,
    eth_vc_spent_first_date,
    eth_vc_spent_first_date_grouping,
    eth_vc_spent_last_date,
    eth_vc_spent_last_date_grouping,
    eth_vc_spent_costume_total,
    eth_vc_spent_costume_grouping,
    eth_vc_spent_costume_first_date,
    eth_vc_spent_costume_first_date_grouping,
    eth_vc_spent_costume_last_date,
    eth_vc_spent_costume_last_date_grouping,
    eth_vc_spent_bundle_total,
    eth_vc_spent_bundle_grouping,
    eth_vc_spent_bundle_first_date,
    eth_vc_spent_bundle_first_date_grouping,
    eth_vc_spent_bundle_last_date,
    eth_vc_spent_bundle_last_date_grouping,
    eth_vc_spent_actions,
    eth_vc_spent_actions_grouping,
    eth_current_vc_balance,
    eth_current_huc_balance,
    eth_current_battlepass_currency_balance,
    eth_current_trials_core_balance,
    eth_store_first_seen,
    eth_store_first_seen_grouping,
    eth_store_last_seen,
    eth_store_last_seen_grouping,
    eth_total_store_visits,
    eth_vc_spent_arbor_total,
    eth_vc_spent_arbor_grouping,
    eth_vc_spent_artemis_total,
    eth_vc_spent_artemis_grouping,
    eth_vc_spent_breeze_total,
    eth_vc_spent_breeze_grouping,
    eth_vc_spent_conduit_total,
    eth_vc_spent_conduit_grouping,
    eth_vc_spent_gemini_total,
    eth_vc_spent_gemini_grouping,
    eth_vc_spent_genome_total,
    eth_vc_spent_genome_grouping,
    eth_vc_spent_giacomo_total,
    eth_vc_spent_giacomo_grouping,
    eth_vc_spent_permafrost_total,
    eth_vc_spent_permafrost_grouping,
    eth_vc_spent_prism_total,
    eth_vc_spent_prism_grouping,
    eth_vc_spent_rayo_total,
    eth_vc_spent_rayo_grouping,
    eth_vc_spent_rojas_total,
    eth_vc_spent_rojas_grouping,
    eth_vc_spent_strider_total,
    eth_vc_spent_strider_grouping,
    eth_vc_spent_tempo_total,
    eth_vc_spent_tempo_grouping,
    eth_vc_spent_terminal_total,
    eth_vc_spent_terminal_grouping,
    eth_vc_spent_hero_total,
    eth_vc_spent_hero_total_grouping,
    eth_vc_spending_hero_preference,
    eth_vc_spent_items_agnostic_total,
    eth_vc_spent_items_agnostic_total_grouping,
    eth_vc_spent_hero_unlocks_total,
    eth_vc_spent_hero_unlocks_total_grouping,
    eth_vc_spent_hero_unlocks_transactions,
    eth_vc_spent_hero_unlocks_transactions_grouping,
    eth_has_unlocked_hero_a,
    eth_has_unlocked_hero_b,
    eth_has_unlocked_hero_c,
    eth_has_unlocked_hero_d,
    eth_has_unlocked_hero_e,
    eth_has_unlocked_hero_f,
    eth_has_unlocked_hero_g,
    eth_has_unlocked_hero_h,
    eth_has_purchased_battle_pass,
    eth_has_purchased_standard_battle_pass,
    eth_has_purchased_premium_battle_pass,
    eth_has_purchased_tier_skips,
    eth_first_purchased_battle_pass,
    eth_first_purchased_battle_pass_grouping,
    eth_first_purchased_battle_pass_standard,
    eth_first_purchased_battle_pass_premium,
    eth_first_purchased_tier_skips,
    eth_last_purchased_battle_pass,
    eth_last_purchased_battle_pass_grouping,
    eth_last_purchased_battle_pass_standard,
    eth_last_purchased_battle_pass_premium,
    eth_last_purchased_tier_skips,
    eth_total_seasons_purchased_pass,
    eth_total_seasons_purchased_standard_pass,
    eth_total_seasons_purchased_premium_pass,
    eth_total_seasons_purchased_tier_skips,
    eth_total_purchased_tier_skips,
    eth_current_season_level_reached,
    eth_previous_season_level_reached,
    eth_ltd_highest_season_level_reached,
    eth_has_purchased_seasonal_pack,
    eth_has_purchased_seasonal_pack_grouping,
    eth_first_purchased_seasonal_pack,
    eth_first_purchased_seasonal_pack_grouping,
    eth_last_purchased_seasonal_pack,
    eth_last_purchased_seasonal_pack_grouping,
    eth_total_seasonal_pack_purchases,
    eth_trials_first_seen,
    eth_trials_first_seen_grouping,
    eth_trials_last_seen,
    eth_trials_last_seen_grouping,
    eth_trials_days_active,
    eth_trials_games_played,
    eth_trials_time_spent,
    eth_trials_total_extractions,
    eth_trials_games_success,
    eth_trials_games_fail,
    eth_trials_success_rate,
    eth_trials_success_rate_grouping,
    eth_trials_days_active_grouping,
    eth_trials_games_played_grouping,
    eth_trials_max_medal,
    eth_trials_max_medal_count,
    eth_trials_max_medal_count_grouping,
    eth_trials_min_medal,
    eth_gauntlet_first_seen,
    eth_gauntlet_first_seen_grouping,
    eth_gauntlet_last_seen,
    eth_gauntlet_last_seen_grouping,
    eth_gauntlet_days_active,
    eth_gauntlet_games_played,
    eth_gauntlet_time_spent,
    eth_gauntlet_games_success,
    eth_gauntlet_games_fail,
    eth_gauntlet_success_rate,
    eth_gauntlet_success_rate_grouping,
    eth_gauntlet_days_active_grouping,
    eth_gauntlet_games_played_grouping,
    eth_gauntlet_max_medal,
    eth_gauntlet_max_medal_count,
    eth_gauntlet_max_medal_count_grouping,
    eth_gauntlet_min_medal,
    eth_pg_first_seen,
    eth_pg_first_seen_grouping,
    eth_pg_last_seen,
    eth_pg_last_seen_grouping,
    eth_pg_days_active,
    eth_pg_games_played,
    eth_pg_time_spent,
    eth_pg_games_success,
    eth_pg_games_fail,
    eth_pg_success_rate,
    eth_pg_success_rate_grouping,
    eth_pg_days_active_grouping,
    eth_pg_games_played_grouping,
    eth_pg_max_medal,
    eth_pg_max_medal_count,
    eth_pg_max_medal_count_grouping,
    eth_pg_min_medal,
    eth_total_friends,
    eth_friend_reach,
    eth_is_tier_1_friend_count,
    eth_is_tier_2_friend_count,
    eth_is_tier_3_friend_count,
    eth_has_tier_1_friend_count,
    eth_has_tier_2_friend_count,
    eth_has_tier_3_friend_count,
    eth_friend_circle_tightness,
    eth_games_played_with_friends,
    eth_last_played_with_friends,
    eth_last_played_with_friends_grouping,
    eth_games_played_in_party,
    eth_last_played_in_party,
    eth_last_played_in_party_grouping,
    eth_social_value,
    eth_ftue_first_stage_complete,
    eth_last_seen_season,
    eth_current_season_first_seen,
    eth_current_season_first_seen_grouping,
    eth_current_season_last_seen,
    eth_current_season_last_seen_grouping,
    eth_current_season_days_active,
    eth_current_season_games_played,
    eth_current_season_time_spent,
    eth_current_season_games_success,
    eth_current_season_games_fail,
    eth_current_season_success_rate,
    eth_current_season_success_rate_grouping,
    eth_current_season_days_active_grouping,
    eth_current_season_games_played_grouping,
    eth_current_season_trials_medal_rank,
    eth_current_season_gauntlet_medal_rank,
    eth_current_season_pg_medal_rank,
    -- eth_season_0_first_seen,
    -- eth_season_0_first_seen_grouping,
    -- eth_season_0_last_seen,
    -- eth_season_0_last_seen_grouping,
    -- eth_season_0_days_active,
    -- eth_season_0_games_played,
    -- eth_season_0_time_spent,
    -- eth_season_0_games_success,
    -- eth_season_0_games_fail,
    -- eth_season_0_success_rate,
    -- eth_season_0_success_rate_grouping,
    -- eth_season_0_days_active_grouping,
    -- eth_season_0_games_played_grouping,
    -- eth_season_0_trials_medal_rank,
    -- eth_season_0_gauntlet_medal_rank,
    -- eth_season_0_pg_medal_rank,
    -- eth_season_1_first_seen,
    -- eth_season_1_first_seen_grouping,
    -- eth_season_1_last_seen,
    -- eth_season_1_last_seen_grouping,
    -- eth_season_1_days_active,
    -- eth_season_1_games_played,
    -- eth_season_1_time_spent,
    -- eth_season_1_games_success,
    -- eth_season_1_games_fail,
    -- eth_season_1_success_rate,
    -- eth_season_1_success_rate_grouping,
    -- eth_season_1_days_active_grouping,
    -- eth_season_1_games_played_grouping,
    -- eth_season_1_trials_medal_rank,
    -- eth_season_1_gauntlet_medal_rank,
    -- eth_season_1_pg_medal_rank,
    eth_custom_grouping_01,
    eth_custom_grouping_02,
    eth_custom_grouping_03,
    eth_custom_grouping_04,
    eth_custom_grouping_05,
    eth_custom_grouping_06,
    eth_custom_grouping_07,
    eth_custom_grouping_08,
    eth_custom_grouping_09,
    eth_custom_grouping_10,
    eth_custom_grouping_11,
    eth_custom_grouping_12,
    eth_custom_grouping_13,
    eth_custom_grouping_14,
    eth_custom_grouping_15,
    eth_custom_grouping_16,
    eth_custom_grouping_17,
    eth_custom_grouping_18,
    eth_custom_grouping_19,
    eth_custom_grouping_20,
    eth_custom_value_01,
    eth_custom_value_02,
    eth_custom_value_03,
    eth_custom_value_04,
    eth_custom_value_05,
    eth_custom_value_06,
    eth_custom_value_07,
    eth_custom_value_08,
    eth_custom_value_09,
    eth_custom_value_10,
    eth_custom_value_11,
    eth_custom_value_12,
    eth_custom_value_13,
    eth_custom_value_14,
    eth_custom_value_15,
    eth_custom_value_16,
    eth_custom_value_17,
    eth_custom_value_18,
    eth_custom_value_19,
    eth_custom_value_20
    )
    as 
        with services as (
            select case service when 'xbl' then  'Xbox Live' else SERVICE end JOIN_KEY 
              , service
              , vendor  
            from reference_customer.platform.service
            group by 1, 2, 3
        ), 

        fact_players_ltd as (
            select distinct f.parent_id                                                          player_id -- parent ID used to match telemetry
            , f.player_id                                                                        public_platform_id
            , last_value(v.platform) 
                over (partition by f.parent_id, f.player_id  -- check if player_id is required
                    order by max(f.received_on::timestamp) over (partition by f.parent_id),      -- last seen date
                    min(received_on::timestamp) over (partition by f.parent_id),                 -- install date
                    hash(f.country_code))                                                        platform
            , last_value(v.vendor) 
                over (partition by f.parent_id, f.player_id 
                    order by max(f.received_on::timestamp) over (partition by f.parent_id),      
                    min(received_on::timestamp) over (partition by f.parent_id),                 
                    hash(f.country_code))                                                        vendor
            , last_value(s.service) 
                over (partition by f.parent_id, f.player_id
                    order by max(f.received_on::timestamp) over (partition by f.parent_id),      
                    min(received_on::timestamp) over (partition by f.parent_id),                 
                    hash(f.country_code))                                                        service
            , min(fp.viable_date)  over (partition by fp.parent_id)                              viable_date
            , last_value(f.country_code) 
                ignore nulls over (partition by f.parent_id 
                  order by max(f.received_on::timestamp) over (partition by f.parent_id),      
                    min(received_on::timestamp) over (partition by f.parent_id),                 
                    hash(f.country_code))                                                       last_country_code
            , min(f.received_on::timestamp) over  (partition by f.parent_id)                    install_date
            , max(f.received_on::timestamp) over  (partition by f.parent_id)                    last_seen_date
            , last_value(iff(f.platform not in ('XBSX', 'PS5', 'Windows'), v.platform, null)) 
                ignore nulls over  (partition by f.parent_id, f.player_id
                  order by max(f.received_on::timestamp) over (partition by f.parent_id),      
                    min(received_on::timestamp) over (partition by f.parent_id),                 
                    hash(f.country_code))                                                       current_gen_platform
            , last_value(iff(f.platform  in ('XBSX', 'PS5', 'Windows'), v.platform, null))
                ignore nulls over (partition by f.parent_id, f.player_id
                  order by max(f.received_on::timestamp) over (partition by f.parent_id),      
                    min(received_on::timestamp) over (partition by f.parent_id),                 
                    hash(f.country_code))                                                       next_gen_platform

            from horizon{environment}.intermediate.fact_player_activity f
            left join dataanalytics{environment}.standard_metrics.fact_player_ltd fp 
            on lower(f.parent_id) = lower(fp.parent_id) and lower(f.player_id) = lower(fp.player_id)
            join services s on lower(s.join_key) = lower(f.service)
            join reference_customer.platform.platform v  ON f.platform  = v.src_platform 
            -- where f.platform <> 'NA' -- is this required?
            where fp.title = 'Horizon Beta'
        ),
        
        player_install_grouping as (
            select distinct *
            from (select parent_id as player_id
            , min(received_on::timestamp) over (partition by player_id)  install_date
            , case when install_date::timestamp between start_date and end_date then phase
            end as install_date_grouping
            from horizon{environment}.intermediate.fact_player_activity 
            join horizon_datasci_prod.lookups.game_phase_lookup
            )
            where install_date_grouping is not null
        ),

        season_last_seen as (
            select distinct *
            from (select parent_id as player_id
            , max(received_on::timestamp) over (partition by player_id)  install_date
            , case when install_date::timestamp between start_date and end_date then phase
            end as season_last_seen
            from horizon{environment}.intermediate.fact_player_activity 
            join horizon_datasci_prod.lookups.game_phase_lookup
            )
            where season_last_seen is not null
        ),

        player_session_details (
            select distinct user_id                                                                 player_id
            , session_id
            , min(to_timestamp(receivedon)) over (partition by playerpublicid, session_id)          last_session_start
            , max(to_timestamp(receivedon)) over (partition by playerpublicid, session_id)          last_session_end
            , case when datediff(day, last_session_start, current_timestamp()) + 1 > 365 
                      then '365+'
                when datediff(day, last_session_start, current_timestamp()) + 1 
                      between 181 and 365 then '181-365'
                when datediff(day, last_session_start, current_timestamp()) + 1 
                      between 91 and 180 then '91-180'
                when datediff(day, last_session_start, current_timestamp()) + 1 
                      between 61 and 90 then '61-90'
                when datediff(day, last_session_start, current_timestamp()) + 1 
                      between 41 and 60 then '41-60'
                when datediff(day, last_session_start, current_timestamp()) + 1 
                      between 21 and 40 then '21-40'
                when datediff(day, last_session_start, current_timestamp()) + 1 
                      between 14 and 20 then '14-20'
                when datediff(day, last_session_start, current_timestamp()) + 1 
                      between 8 and 13 then '8-13'
                when datediff(day, last_session_start, current_timestamp()) + 1 
                      between 3 and 7 then '3-7'
                when datediff(day, last_session_start, current_timestamp()) + 1 
                      between 1 and 2 then '1-2'
                else '0' end                                                                    last_session_grouping
            from horizon{environment}.raw.application_session_status
            -- join dataanalytics{environment}.standard_metrics.fact_player_ltd on player_id = playerPublicId
            -- join horizon{environment}.managed.fact_player_match_summary on user_id = player_id
            where user_id is not null
            -- and player_id != playerPublicId
            qualify row_number() over (partition by user_id order by last_session_start) = 1            
        ),

        player_metrics as (
          select distinct full_account_id
                , count(distinct date)                                         active_days -- need to set min date
                , count(distinct match_id)                                     games_played
                , sum(match_duration)                                          time_spent
                , sum(kills)                                                   kills
                , sum(deaths)                                                  deaths
                , try_divide(sum(kills) + sum(assists), sum(deaths))           kill_death_assists_ratio 
                , sum(rescues)                                                 rescues
                , sum(revives)                                                 revives
                --  , 0                                                            distance_travelled -- update this
                , ifnull((sum(case 
                    when match_type = 'Trials' then extractions_completed
                    else 0 end)), 0) 
                  + ifnull((sum(case 
                    when match_type = 'Gauntlet' and gauntlet_team_wins = 3 then 1
                    else 0 end)), 0)                                            games_success -- only for trials and gauntlet
                , ifnull(games_played - games_success, 0)                      games_fail
                , try_divide(games_success, games_played)                      success_rate
                , case when try_divide(sum(case when group_size = 1 then 1 else 0 end),games_played) > 0.5 then 'solos'
                  when try_divide(sum(case when group_size = 2 then 1 else 0 end),games_played) > 0.5 then 'duos'
                  when try_divide(sum(case when group_size = 3 then 1 else 0 end),games_played) > 0.5 then 'trios'
                  when try_divide(sum(case when group_size = 4 then 1 else 0 end),games_played) > 0.5 then 'quads'
                  else 'mixed'
                  end                                                          queue_size_preference
                --  , case 
                --    when try_divide(sum(case when party_size = 1 then 1 else 0 end),games_played) > 0.5 then 'solos'
                --    when try_divide(sum(case when party_size  = 2 then 1 else 0 end),games_played) > 0.5 then 'duos'
                --    when try_divide(sum(case when party_size  = 3 then 1 else 0 end),games_played) > 0.5 then 'trios'
                --    when try_divide(sum(case when party_size  = 4 then 1 else 0 end),games_played) > 0.5 then 'quads'
                --    else 'mixed'
                --    end                                                           party_size_preference -- party_size not available yet
                , case when try_divide(sum(case when MATCH_TYPE = 'Trials' then 1 else 0 end),games_played) > 0.6 then 'trials'
                  when try_divide(sum(case when MATCH_TYPE = 'Gauntlet' then 1 else 0 end),games_played) > 0.6 then 'gauntlet'
                  when try_divide(sum(case when MATCH_TYPE = 'BR' then 1 else 0 end),games_played) > 0.6 then 'br'
                  else 'mixed'
                  end                                                          mode_preference

          from horizon{environment}.managed.fact_player_match_summary
          where match_type in ('Trials', 'Gauntlet')
          group by full_account_id
        ),

        player_hero_preference as (
            with hero_stats as (
              select
              full_account_id,
              hero_class_name,
              count(distinct match_id) as num_games
              from horizon{environment}.managed.fact_player_match_summary
              where match_type in ('Trials', 'Gauntlet')
              group by 1,2
            ),
            top_two_heroes as (
              select
              full_account_id,
              hero_class_name,
              num_games,
              sum(num_games) over (partition by full_account_id) as total_num_of_games,
              row_number() over (partition by full_account_id order by num_games desc) as rank
              from hero_stats
              )
            select distinct
            t1.full_account_id,
            case when abs(t1.num_games / t1.total_num_of_games - t2.num_games / t2.total_num_of_games) < 0.1 then 'mixed'
                  when t1.num_games / t1.total_num_of_games > 0.4 then t1.hero_class_name
                  else 'mixed'
                  end as hero_preference
            from (select * from top_two_heroes where rank = 1) t1
            join (select * from top_two_heroes where rank = 2) t2
            on t1.full_account_id = t2.full_account_id
        ),

        player_foundation_preference as (
            with foundation_stats as (
              select
              full_account_id,
              foundation,
              count(*) as foundation_count,
              sum(count(distinct match_id)) over (partition by full_account_id) as games_played
              from horizon{environment}.managed.fact_player_match_summary
              left join horizon{environment}.reference.ref_hero on hero_class_name = hero_id
              where match_type in ('Trials', 'Gauntlet')
              group by 1,2
            ),
            ranked_foundations as (
              select
              full_account_id,
              foundation,
              foundation_count,
              games_played,
              row_number() over (partition by full_account_id order by foundation_count desc) as rank
              from foundation_stats
            )
            select distinct
              t1.full_account_id,
              case when ABS(t1.foundation_count / t1.games_played - t2.foundation_count / t2.games_played) < 0.1 then 'mixed'
              when t1.foundation = 'Forum' AND t1.foundation_count / t1.games_played > 0.4 then 'forum'
              when t1.foundation = 'Institute' AND t1.foundation_count / t1.games_played > 0.4 then 'institute'
              when t1.foundation = 'Pathfinders' AND t1.foundation_count / t1.games_played > 0.4 then 'pathfinders'
              else 'mixed'
              end as foundation_preference
            from (select * from ranked_foundations where rank = 1) t1
            left join (select * from ranked_foundations where rank = 2) t2
            on t1.full_account_id = t2.full_account_id
        ),
        
        medal_heirarchy as (
          select distinct full_account_id, match_type, ifnull(medal, 'NoMedal') medal, 
          case 
            when medal = 'Mythic' then 1
            when medal = 'Legend' then 2                               
            when medal = 'Diamond' then 3 
            when medal = 'Platinum' then 4
            when medal = 'Gold' then 5
            when medal = 'Silver' then 6
            when medal = 'Bronze' then 7
            else 8
            end medal_heirarchy,
          count(distinct match_id) as medal_count
          from horizon{environment}.managed.fact_player_match_summary
          where match_type in ('Trials', 'Gauntlet')
          group by 1,2,3,4
        ),
        player_medals as (
          select distinct m.full_account_id,
          m.match_type,
          first_value(m.medal) over (partition by m.full_account_id, m.match_type order by m.medal_heirarchy)                 medal_max,
          first_value(m.medal) over (partition by m.full_account_id, m.match_type order by m.medal_heirarchy desc)            medal_min
          from medal_heirarchy m
        ),

        trials_player_metrics as (
          select distinct                                                       s.full_account_id
          , min(start_ts)                                                       trials_first_seen
          , max(start_ts)                                                       trials_last_seen
          , count(distinct match_id)                                            trials_games_played
          , sum(match_duration)                                                 trials_time_spent
          , sum(extractions_completed)                                          trials_total_extractions
          , trials_total_extractions                                            trials_games_success
          , trials_games_played - trials_total_extractions                      trials_games_fail
          , try_divide(trials_games_success, trials_games_played)               trials_success_rate
          , any_value(m.medal_max)                                              trials_max_medal
          , max(case when mh.medal = m.medal_max 
              then mh.medal_count else 0 end)                                   trials_max_medal_count
          , any_value(m.medal_min)                                              trials_min_medal
          from horizon{environment}.managed.fact_player_match_summary s
          left join player_medals m using(full_account_id, match_type)
          left join medal_heirarchy mh using(full_account_id, match_type)
          where match_type in ('Trials')
          group by full_account_id
        )
        ,

        gauntlet_player_metrics as (
          select distinct                                                       s.full_account_id
          , min(start_ts)                                                       gauntlet_first_seen
          , max(start_ts)                                                       gauntlet_last_seen
          , count(distinct match_id)                                            gauntlet_games_played
          , sum(match_duration)                                                 gauntlet_time_spent
          , sum(case 
              when gauntlet_team_wins = 3 then 1
              else 0 end)                                                       gauntlet_games_success
          , gauntlet_games_played - gauntlet_games_success                      gauntlet_games_fail
          , try_divide(gauntlet_games_success, gauntlet_games_played)           gauntlet_success_rate
          , any_value(m.medal_max)                                              gauntlet_max_medal
          , max(case when mh.medal = m.medal_max 
              then mh.medal_count else 0 end)                                   gauntlet_max_medal_count
          , any_value(m.medal_min)                                              gauntlet_min_medal
          from horizon{environment}.managed.fact_player_match_summary s
          left join player_medals m using(full_account_id, match_type)
          left join medal_heirarchy mh using(full_account_id, match_type)
          where match_type in ('Gauntlet')
          group by full_account_id
        ),
        
        core_balance as (
          with flattened_data as (
            select player_id,
            occurred_on::timestamp              occurred_on,
            cast(f.amount AS decimal(38, 0))    amount,
            f.register AS register
            from horizon{environment}.intermediate.fact_player_entitlement
            lateral view explode(from_json(resulting_balance, 'ARRAY<STRUCT<amount: DECIMAL(38,0), register: STRING>>')) as f
            where name ilike 'WalletBalanceChangeEvent'
          )
          select player_id,
          max(occurred_on)          max_occurred_on,
          sum(amount)               current_trials_core_balance
          from flattened_data
          where register ilike '%EARNED%' or register ilike '%GRANTED%'
          group by player_id
        ),

        ftue_stages as (
          select distinct player.user_id                                        player_id
          , max(case when event_trigger = 'step_completed' 
            and extra_details.ftue_step_type in ('Ftue.Type.Tupou.TalkToTupou', 
            'Ftue.Type.Sibeko.TalkToSibeko', 'Ftue.Type.Dimitra.TalkToDimitra') 
            then 1 else 0 end)                                                  ftue_first_stage_complete
          from horizon{environment}.raw.ftue_status
          where player.user_id is not null 
          group by player_id
        ),

        discord_linking as ( -- updated
          with parent_links AS
            (
                SELECT distinct
                    accountId as full_account,
                    to_timestamp(occurredOn::int) as occurredOn,
                    'LinkEvent' as event_name,
                    targetAccountId  AS platform_account,
                    linkType
                FROM coretech{environment}.sso.linkevent
                WHERE accountType = 3 and linkType in ('discord')
            ),
            parent_unlinks AS
            (
                SELECT distinct
                    accountId as full_account,
                    to_timestamp(occurredOn::int) as occurredOn,
                    'UnlinkEvent' as event_name,
                    targetAccountId  AS platform_account,
                    linkType
                FROM coretech{environment}.sso.unlinkevent
                WHERE accountType = 3 and linkType in ('discord') and to_timestamp(occurredOn::int) < '2024-10-17'
            ),
            platform_links AS
            (
                SELECT distinct
                    accountId as full_account,
                    to_timestamp(occurredOn::int) as occurredOn,
                    'LinkEvent' as event_name,
                    targetAccountId  AS platform_account,
                    case 
                        when onlineServiceType = 26 then 'discord' 
                    end as linkType
                FROM coretech{environment}.sso.linkevent
                WHERE targetAccountType = 3 and onlineServiceType = 26
            ),
            platform_unlinks AS
            (
                SELECT distinct
                    accountId as full_account,
                    to_timestamp(occurredOn::int) as occurredOn,
                    'UnlinkEvent' as event_name,
                    targetAccountId  AS platform_account,
                    case 
                        when onlineServiceType = 26 then 'discord' 
                    end as linkType
                FROM coretech{environment}.sso.unlinkevent
                WHERE targetAccountType = 3 and onlineServiceType = 26 and to_timestamp(occurredOn::int) < '2024-10-17'
            )
            ,all_links AS
            (
                SELECT * FROM parent_links
                UNION ALL 
                SELECT * FROM platform_links
                UNION ALL 
                SELECT * FROM parent_unlinks
                UNION ALL 
                SELECT * FROM platform_unlinks
                UNION ALL
                SELECT * EXCEPT (S3_DATE, EVENT_RANK) FROM horizon_datasci{environment}.lookups.hist_link_events where LINK_TYPE ilike '%discord%'
            ),
            last_event as
            (
            SELECT 
                    *, 
                    row_number() over (partition by platform_account ORDER BY occurredOn desc) AS event_rank 
              FROM all_links
              qualify event_rank = 1    
            )
            select distinct
            full_account as player_id,
            linkType as link_type 
            from 
            last_event
            where event_name = 'LinkEvent'
        ),
        
        google_linking as ( -- updated
            WITH parent_links AS
            (
                SELECT distinct
                    accountId as full_account,
                    to_timestamp(occurredOn::int) as occurredOn,
                    'LinkEvent' as event_name,
                    targetAccountId  AS platform_account,
                    linkType
                FROM coretech{environment}.sso.linkevent
                WHERE accountType = 3 and linkType in ('google')
            ),
            parent_unlinks AS
            (
                SELECT distinct
                    accountId as full_account,
                    to_timestamp(occurredOn::int) as occurredOn,
                    'UnlinkEvent' as event_name,
                    targetAccountId  AS platform_account,
                    linkType
                FROM coretech{environment}.sso.unlinkevent
                WHERE accountType = 3 and linkType in ('google') and to_timestamp(occurredOn::int) < '2024-10-17'
            ),
            platform_links AS
            (
                SELECT distinct
                    accountId as full_account,
                    to_timestamp(occurredOn::int) as occurredOn,
                    'LinkEvent' as event_name,
                    targetAccountId  AS platform_account,
                    case 
                        when onlineServiceType = 18 then 'google' 
                    end as linkType
                FROM coretech{environment}.sso.linkevent
                WHERE targetAccountType = 3 and onlineServiceType = 18
            ),
            platform_unlinks AS
            (
                SELECT distinct
                    accountId as full_account,
                    to_timestamp(occurredOn::int) as occurredOn,
                    'UnlinkEvent' as event_name,
                    targetAccountId  AS platform_account,
                    case 
                        when onlineServiceType = 18 then 'google' 
                    end as linkType
                FROM coretech{environment}.sso.unlinkevent
                WHERE targetAccountType = 3 and onlineServiceType = 18 and to_timestamp(occurredOn::int) < '2024-10-17'
            )
            ,all_links AS
            (
                SELECT * FROM parent_links
                UNION ALL 
                SELECT * FROM platform_links
                UNION ALL 
                SELECT * FROM parent_unlinks
                UNION ALL 
                SELECT * FROM platform_unlinks
                UNION ALL
                SELECT * EXCEPT (S3_DATE, EVENT_RANK) FROM horizon_datasci{environment}.lookups.hist_link_events where LINK_TYPE ilike '%google%'
            ),
            last_event as
            (
            SELECT 
                    *, 
                    row_number() over (partition by platform_account ORDER BY occurredOn desc) AS event_rank 
              FROM all_links
              qualify event_rank = 1    
            )
            select distinct
            full_account as player_id,
            linkType as link_type 
            from 
            last_event
            where event_name = 'LinkEvent'
        ),
        
        fpids as (
            select last_value(le.targetaccountId) 
              over (partition by le.firstpartyId order by le.date)                               parent_account_id,
            f.player_id                                                                          platform_id, 
            dataanalytics{environment}.cdp_ng.fpid_decryption(le.firstpartyId)                            puid  
            from dataanalytics{environment}.standard_metrics.fact_player_ltd f
            join coretech{environment}.sso.linkevent le 
            on lower(f.parent_id) = lower(le.targetaccountId) and lower(f.player_id) = lower(le.accountId) and le.firstpartyId is not null
            where accounttype = 2 and targetaccounttype = 3
            union
            select last_value(le.accountId)
              over (partition by le.firstpartyId order by le.date)                              parent_account_id,
            f.player_id                                                                         platform_id, 
            dataanalytics{environment}.cdp_ng.fpid_decryption(le.firstpartyId)                           puid 
            from dataanalytics{environment}.standard_metrics.fact_player_ltd f
            join coretech{environment}.sso.linkevent le 
            on lower(f.parent_id) = lower(le.accountId) and lower(f.player_id) = lower(le.targetaccountId) and le.firstpartyId is not null
            where accounttype = 3 and targetaccounttype = 2

        ),

          all_metrics as (

          select f.player_id                                                            parent_account_id
            , fpd.puid                                                                  puid
            -- , ctp.first_party_id                                                        puid -- updated 
            , f.vendor                                                                  vendor
            , f.service                                                                 service
            , f.public_platform_id                                                      platform_account_id
            , max(current_gen_platform)                                                 current_gen_platform
            , iff(max(next_gen_platform) = 'Windows', 'PC', max(next_gen_platform))     next_gen_platform

            , min(ifnull(f.last_country_code , 'ZZ'))                                   country_last

            , to_date(min(f.install_date)::timestamp)                                   install_date   
            , min(ifnull(pig.install_date_grouping, 'invalid_grouping'))                install_date_grouping

            , cast(min(f.viable_date) as date)                                          viable_date
            , cast(min(f.viable_date) is not null as boolean)                           is_viable
            , to_date(max(f.last_seen_date)::timestamp)                                 last_seen_date
            , case when datediff(day, max(f.last_seen_date), current_date) + 1 between 0 and 13 then '0-13'
                  when datediff(day, max(f.last_seen_date), current_date) + 1 between 14 and 20 then '14-20'
                  when datediff(day, max(f.last_seen_date), current_date) + 1 between 21 and 40 then '21-40'
                  when datediff(day, max(f.last_seen_date), current_date) + 1 between 41 and 60 then '41-60'
                  when datediff(day, max(f.last_seen_date), current_date) + 1 > 60 then '60+' 
                  else '0-13' 
                  end as                                                               last_seen_date_grouping
            , max(ps.last_session_start)                                                last_session_start
            , max(ps.last_session_grouping)                                             last_session_grouping
            , max(ps.last_session_end)                                                  last_session_end
            , ifnull(max(pm.active_days),0)                                             days_active
            , case when days_active between 0 and 1 then '0-1'
                  when days_active between 2 and 5 then '2-5'
                  when days_active between 6 and 10 then '6-10'
                  when days_active > 10 then '10+' 
                  else '0-1' 
                  end as                                                               days_active_grouping
            , ifnull(max(pm.games_played),0)                                            games_played 
            , round(ifnull(max(pm.time_spent),0), 2)                                    time_spent
            , ifnull(max(pm.kills),0)                                                   kills
            , ifnull(max(pm.deaths),0)                                                  deaths
            , round(ifnull(max(pm.kill_death_assists_ratio),0), 2)                      kill_death_assists_ratio 
            , ifnull(max(pm.rescues),0)                                                 rescues
            , ifnull(max(pm.revives),0)                                                 revives
            , ifnull(max(pm.games_success),0)                                           games_success
            , ifnull(max(pm.games_fail),0)                                              games_fail
            , round(ifnull(max(pm.success_rate),0), 2)                                  success_rate
            , ifnull(max(pm.queue_size_preference), 'mixed')                            queue_size_preference
            -- , ifnull(max(pm.party_size_preference), 'mixed')                            party_size_preference -- update this
            , ifnull(max(pm.mode_preference), 'mixed')                                  mode_preference
            , ifnull(max(hp.hero_preference), 'mixed')                                  hero_preference
            , ifnull(max(fp.foundation_preference), 'mixed')                            foundation_preference
            , ifnull(max(current_trials_core_balance), 0)                               current_trials_core_balance -- update this
            , to_date(min(tpm.trials_first_seen)::timestamp)                            trials_first_seen
            , to_date(min(tpm.trials_last_seen)::timestamp)                             trials_last_seen
            , ifnull(max(tpm.trials_games_played),0)                                    trials_games_played 
            , round(ifnull(max(tpm.trials_time_spent),0), 2)                            trials_time_spent
            , ifnull(max(tpm.trials_total_extractions),0)                               trials_total_extractions
            , ifnull(max(tpm.trials_games_success),0)                                   trials_games_success
            , ifnull(max(tpm.trials_games_fail),0)                                      trials_games_fail
            , round(ifnull(max(tpm.trials_success_rate),0), 2)                          trials_success_rate
            , ifnull(max(tpm.trials_max_medal),'NoMedal')                               trials_max_medal
            , ifnull(max(tpm.trials_max_medal_count),0)                                 trials_max_medal_count
            , ifnull(max(tpm.trials_min_medal),'NoMedal')                               trials_min_medal
            , to_date(min(gpm.gauntlet_first_seen)::timestamp)                          gauntlet_first_seen
            , to_date(min(gpm.gauntlet_last_seen)::timestamp)                           gauntlet_last_seen
            , ifnull(max(gpm.gauntlet_games_played),0)                                  gauntlet_games_played 
            , round(ifnull(max(gpm.gauntlet_time_spent),0), 2)                          gauntlet_time_spent
            , ifnull(max(gpm.gauntlet_games_success),0)                                 gauntlet_games_success
            , ifnull(max(gpm.gauntlet_games_fail),0)                                    gauntlet_games_fail
            , round(ifnull(max(gpm.gauntlet_success_rate),0), 2)                        gauntlet_success_rate
            , ifnull(max(gpm.gauntlet_max_medal),'NoMedal')                             gauntlet_max_medal
            , ifnull(max(gpm.gauntlet_max_medal_count),0)                               gauntlet_max_medal_count
            , ifnull(max(gpm.gauntlet_min_medal),'NoMedal')                             gauntlet_min_medal
            , min(ifnull(sl.season_last_seen, 'bcd'))                                   last_seen_season
            , max(ifnull(fs.ftue_first_stage_complete, 0))                              ftue_first_stage_complete
            , any_value(iff(dl.link_type is not null, 'discord_linked', 'not_linked'))  discord_linking
            , any_value(iff(gl.link_type is not null, 'google_linked', 'not_linked'))   google_linking -- updated
            from fact_players_ltd f
            -- join reference_customer.customer.vw_ctp_profile ctp -- changed to inner join
            -- on lower(f.player_id) = lower(ctp.parent_account_id) and lower(f.public_platform_id) = lower(ctp.public_id)
            join fpids fpd
            on lower(fpd.parent_account_id) = lower(f.player_id) and lower(fpd.platform_id) = lower(f.public_platform_id)
            left join player_install_grouping pig on lower(f.player_id) = lower(pig.player_id)
            left join season_last_seen sl on lower(f.player_id) = lower(sl.player_id)
            left join player_session_details ps on lower(f.player_id) = lower(ps.player_id)
            left join player_metrics pm on lower(f.player_id) = lower(pm.full_account_id)
            left join player_hero_preference hp on lower(f.player_id) = lower(hp.full_account_id)
            left join player_foundation_preference fp on lower(f.player_id) = lower(fp.full_account_id)
            left join trials_player_metrics tpm on lower(f.player_id) = lower(tpm.full_account_id)
            left join gauntlet_player_metrics gpm on lower(f.player_id) = lower(gpm.full_account_id)
            left join core_balance cb on lower(f.player_id) = lower(cb.player_id)
            left join ftue_stages fs on lower(f.player_id) = lower(fs.player_id)
            left join discord_linking dl on lower(f.player_id) = lower(dl.player_id)
            left join google_linking gl on lower(f.player_id) = lower(gl.player_id) -- updated
            group  by f.player_id, puid, vendor, service, platform_account_id
        ) 
              
      select 
      
                                                                                            parent_account_id,
            vendor || ':' ||  dataanalytics{environment}.cdp_ng.salted_puid(puid)                    brand_firstpartyid,
            dataanalytics{environment}.cdp_ng.salted_puid(puid)                                      salted_puid,
                                                                                            puid,
                                                                                            platform_account_id,
                                                                                            
            vendor                                                                          eth_vendor,
            service                                                                         eth_service,
            current_gen_platform                                                            eth_current_gen_platform,
            next_gen_platform                                                               eth_next_gen_platform,
            country_last                                                                    eth_country_last,
            install_date                                                                    eth_install_date,
            install_date_grouping                                                           eth_install_date_grouping,
            is_viable                                                                       eth_viable,
            viable_date                                                                     eth_viable_date,
            cast(null as varchar(50))                                                       eth_premium_sku,
            cast(null as varchar(50))                                                       eth_is_pre_order,
            cast(null as varchar(50))                                                       eth_expansion_pack_1,
            cast(null as varchar(50))                                                       eth_expansion_pack_2,
            cast(null as varchar(50))                                                       eth_expansion_pack_3,
            cast(null as varchar(50))                                                       eth_expansion_pack_4,
            cast(null as varchar(50))                                                       eth_price_known,
            cast(null as varchar(50))                                                       eth_price_probabilistic,
            cast(null as varchar(50))                                                       eth_is_ftp_install,
            cast(null as varchar(50))                                                       eth_account_level,
            last_seen_date                                                                  eth_last_seen_date,
            last_seen_date_grouping                                                         eth_last_seen_grouping,
            last_session_start                                                              eth_last_session_start,
            last_session_grouping                                                           eth_last_session_grouping,
            last_session_end                                                                eth_last_session_end,
            cast(null as varchar(50))                                                       eth_player_type,
            days_active                                                                     eth_days_active,
            games_played                                                                    eth_games_played,
            time_spent                                                                      eth_time_spent,
            kills                                                                           eth_kills,
            cast(null as varchar(50))                                                       eth_kills_grouping,
            deaths                                                                          eth_deaths,
            cast(null as varchar(50))                                                       eth_deaths_grouping,
            kill_death_assists_ratio                                                        eth_kill_death_assists_ratio,
            cast(null as varchar(50))                                                       eth_kill_death_assists_ratio_grouping,
            rescues                                                                         eth_rescues,
            cast(null as varchar(50))                                                       eth_rescues_grouping,
            revives                                                                         eth_revives,
            cast(null as varchar(50))                                                       eth_revives_grouping,
            cast(null as integer)                                                           eth_distance_travelled,
            cast(null as varchar(50))                                                       eth_distance_travelled_grouping,
            games_success                                                                   eth_games_success,
            games_fail                                                                      eth_games_fail,
            success_rate                                                                    eth_success_rate,
            cast(null as varchar(50))                                                       eth_days_active_grouping,
            cast(null as varchar(50))                                                       eth_games_played_grouping,
            cast(null as varchar(50))                                                       eth_success_rate_grouping,
            cast(null as varchar(50))                                                       eth_game_progress_grouping,
            queue_size_preference                                                           eth_queue_size_preference,
            cast(null as varchar(50))                                                       eth_party_size_preference,
            mode_preference                                                                 eth_mode_preference,
            hero_preference                                                                 eth_hero_preference,
            cast(null as varchar(50))                                                       eth_hero_type_preference,
            cast(null as varchar(50))                                                       eth_playstyle_preference,
            foundation_preference                                                           eth_foundation_preference,
            cast(null as varchar(50))                                                       eth_last_played_arbor,
            cast(null as varchar(50))                                                       eth_last_played_artemis,
            cast(null as varchar(50))                                                       eth_last_played_breeze,
            cast(null as varchar(50))                                                       eth_last_played_conduit,
            cast(null as varchar(50))                                                       eth_last_played_gemini,
            cast(null as varchar(50))                                                       eth_last_played_genome,
            cast(null as varchar(50))                                                       eth_last_played_giacomo,
            cast(null as varchar(50))                                                       eth_last_played_permafrost,
            cast(null as varchar(50))                                                       eth_last_played_prism,
            cast(null as varchar(50))                                                       eth_last_played_rayo,
            cast(null as varchar(50))                                                       eth_last_played_rojas,
            cast(null as varchar(50))                                                       eth_last_played_strider,
            cast(null as varchar(50))                                                       eth_last_played_tempo,
            cast(null as varchar(50))                                                       eth_last_played_terminal,
            cast(null as varchar(50))                                                       eth_last_played_solo,
            cast(null as varchar(50))                                                       eth_last_played_duo,
            cast(null as varchar(50))                                                       eth_last_played_trios,
            cast(null as varchar(50))                                                       eth_last_played_quads,
            cast(null as varchar(50))                                                       eth_hero_mastery_arbor,
            cast(null as varchar(50))                                                       eth_hero_mastery_artemis,
            cast(null as varchar(50))                                                       eth_hero_mastery_breeze,
            cast(null as varchar(50))                                                       eth_hero_mastery_conduit,
            cast(null as varchar(50))                                                       eth_hero_mastery_gemini,
            cast(null as varchar(50))                                                       eth_hero_mastery_genome,
            cast(null as varchar(50))                                                       eth_hero_mastery_giacomo,
            cast(null as varchar(50))                                                       eth_hero_mastery_permafrost,
            cast(null as varchar(50))                                                       eth_hero_mastery_prism,
            cast(null as varchar(50))                                                       eth_hero_mastery_rayo,
            cast(null as varchar(50))                                                       eth_hero_mastery_rojas,
            cast(null as varchar(50))                                                       eth_hero_mastery_strider,
            cast(null as varchar(50))                                                       eth_hero_mastery_tempo,
            cast(null as varchar(50))                                                       eth_hero_mastery_terminal,
            cast(null as varchar(50))                                                       eth_has_converted_general,
            cast(null as varchar(50))                                                       eth_total_dollar_spent,
            cast(null as varchar(50))                                                       eth_last_purchased_base_pack,
            cast(null as varchar(50))                                                       eth_last_purchased_deluxe_pack,
            cast(null as varchar(50))                                                       eth_last_purchased_premium_pack,
            cast(null as varchar(50))                                                       eth_last_purchased_hero_pass,
            cast(null as boolean)                                                           eth_bought_vc_converted,
            cast(null as varchar(50))                                                       eth_bought_vc_transactions,
            cast(null as varchar(50))                                                       eth_bought_vc_transactions_grouping,
            cast(null as varchar(50))                                                       eth_bought_vc_total_currency,
            cast(null as varchar(50))                                                       eth_bought_vc_total_currency_grouping,
            cast(null as varchar(50))                                                       eth_bought_vc_first_date,
            cast(null as varchar(50))                                                       eth_bought_vc_first_date_grouping,
            cast(null as varchar(50))                                                       eth_bought_vc_last_date,
            cast(null as varchar(50))                                                       eth_bought_vc_last_date_grouping,
            cast(null as varchar(50))                                                       eth_bought_vc_dollar_value,
            cast(null as varchar(50))                                                       eth_bought_vc_dollar_grouping,
            cast(null as varchar(50))                                                       eth_has_earned_vc_other,
            cast(null as varchar(50))                                                       eth_earned_vc_other_total,
            cast(null as varchar(50))                                                       eth_earned_vc_other_grouping,
            cast(null as varchar(50))                                                       eth_earned_vc_other_first_date,
            cast(null as varchar(50))                                                       eth_earned_vc_other_first_date_grouping,
            cast(null as varchar(50))                                                       eth_earned_vc_other_last_date,
            cast(null as varchar(50))                                                       eth_earned_vc_other_last_date_grouping,
            cast(null as varchar(50))                                                       eth_earned_vc_other_actions,
            cast(null as varchar(50))                                                       eth_earned_vc_other_actions_grouping,
            cast(null as varchar(50))                                                       eth_earned_vc_battlepass_total,
            cast(null as varchar(50))                                                       eth_earned_vc_battlepass_grouping,
            cast(null as varchar(50))                                                       eth_earned_vc_battlepass_first_date,
            cast(null as varchar(50))                                                       eth_earned_vc_battlepass_first_date_grouping,
            cast(null as varchar(50))                                                       eth_earned_vc_battlepass_last_date,
            cast(null as varchar(50))                                                       eth_earned_vc_battlepass_last_date_grouping,
            cast(null as varchar(50))                                                       eth_earned_vc_battlepass_actions,
            cast(null as varchar(50))                                                       eth_earned_vc_battlepass_actions_grouping,
            cast(null as varchar(50))                                                       eth_has_spent_vc,
            cast(null as varchar(50))                                                       eth_vc_spent_total,
            cast(null as varchar(50))                                                       eth_vc_spent_grouping,
            cast(null as varchar(50))                                                       eth_vc_spent_first_date,
            cast(null as varchar(50))                                                       eth_vc_spent_first_date_grouping,
            cast(null as varchar(50))                                                       eth_vc_spent_last_date,
            cast(null as varchar(50))                                                       eth_vc_spent_last_date_grouping,
            cast(null as varchar(50))                                                       eth_vc_spent_costume_total,
            cast(null as varchar(50))                                                       eth_vc_spent_costume_grouping,
            cast(null as varchar(50))                                                       eth_vc_spent_costume_first_date,
            cast(null as varchar(50))                                                       eth_vc_spent_costume_first_date_grouping,
            cast(null as varchar(50))                                                       eth_vc_spent_costume_last_date,
            cast(null as varchar(50))                                                       eth_vc_spent_costume_last_date_grouping,
            cast(null as varchar(50))                                                       eth_vc_spent_bundle_total,
            cast(null as varchar(50))                                                       eth_vc_spent_bundle_grouping,
            cast(null as varchar(50))                                                       eth_vc_spent_bundle_first_date,
            cast(null as varchar(50))                                                       eth_vc_spent_bundle_first_date_grouping,
            cast(null as varchar(50))                                                       eth_vc_spent_bundle_last_date,
            cast(null as varchar(50))                                                       eth_vc_spent_bundle_last_date_grouping,
            cast(null as varchar(50))                                                       eth_vc_spent_actions,
            cast(null as varchar(50))                                                       eth_vc_spent_actions_grouping,
            cast(null as varchar(50))                                                       eth_current_vc_balance,
            cast(null as varchar(50))                                                       eth_current_huc_balance,
            cast(null as varchar(50))                                                       eth_current_battlepass_currency_balance,
            current_trials_core_balance                                                     eth_current_trials_core_balance,
            cast(null as varchar(50))                                                       eth_store_first_seen,
            cast(null as varchar(50))                                                       eth_store_first_seen_grouping,
            cast(null as varchar(50))                                                       eth_store_last_seen,
            cast(null as varchar(50))                                                       eth_store_last_seen_grouping,
            cast(null as varchar(50))                                                       eth_total_store_visits,
            cast(null as varchar(50))                                                       eth_vc_spent_arbor_total,
            cast(null as varchar(50))                                                       eth_vc_spent_arbor_grouping,
            cast(null as varchar(50))                                                       eth_vc_spent_artemis_total,
            cast(null as varchar(50))                                                       eth_vc_spent_artemis_grouping,
            cast(null as varchar(50))                                                       eth_vc_spent_breeze_total,
            cast(null as varchar(50))                                                       eth_vc_spent_breeze_grouping,
            cast(null as varchar(50))                                                       eth_vc_spent_conduit_total,
            cast(null as varchar(50))                                                       eth_vc_spent_conduit_grouping,
            cast(null as varchar(50))                                                       eth_vc_spent_gemini_total,
            cast(null as varchar(50))                                                       eth_vc_spent_gemini_grouping,
            cast(null as varchar(50))                                                       eth_vc_spent_genome_total,
            cast(null as varchar(50))                                                       eth_vc_spent_genome_grouping,
            cast(null as varchar(50))                                                       eth_vc_spent_giacomo_total,
            cast(null as varchar(50))                                                       eth_vc_spent_giacomo_grouping,
            cast(null as varchar(50))                                                       eth_vc_spent_permafrost_total,
            cast(null as varchar(50))                                                       eth_vc_spent_permafrost_grouping,
            cast(null as varchar(50))                                                       eth_vc_spent_prism_total,
            cast(null as varchar(50))                                                       eth_vc_spent_prism_grouping,
            cast(null as varchar(50))                                                       eth_vc_spent_rayo_total,
            cast(null as varchar(50))                                                       eth_vc_spent_rayo_grouping,
            cast(null as varchar(50))                                                       eth_vc_spent_rojas_total,
            cast(null as varchar(50))                                                       eth_vc_spent_rojas_grouping,
            cast(null as varchar(50))                                                       eth_vc_spent_strider_total,
            cast(null as varchar(50))                                                       eth_vc_spent_strider_grouping,
            cast(null as varchar(50))                                                       eth_vc_spent_tempo_total,
            cast(null as varchar(50))                                                       eth_vc_spent_tempo_grouping,
            cast(null as varchar(50))                                                       eth_vc_spent_terminal_total,
            cast(null as varchar(50))                                                       eth_vc_spent_terminal_grouping,
            cast(null as varchar(50))                                                       eth_vc_spent_hero_total,
            cast(null as varchar(50))                                                       eth_vc_spent_hero_total_grouping,
            cast(null as varchar(50))                                                       eth_vc_spending_hero_preference,
            cast(null as varchar(50))                                                       eth_vc_spent_items_agnostic_total,
            cast(null as varchar(50))                                                       eth_vc_spent_items_agnostic_total_grouping,
            cast(null as varchar(50))                                                       eth_vc_spent_hero_unlocks_total,
            cast(null as varchar(50))                                                       eth_vc_spent_hero_unlocks_total_grouping,
            cast(null as varchar(50))                                                       eth_vc_spent_hero_unlocks_transactions,
            cast(null as varchar(50))                                                       eth_vc_spent_hero_unlocks_transactions_grouping,
            cast(null as varchar(50))                                                       eth_has_unlocked_hero_a,
            cast(null as varchar(50))                                                       eth_has_unlocked_hero_b,
            cast(null as varchar(50))                                                       eth_has_unlocked_hero_c,
            cast(null as varchar(50))                                                       eth_has_unlocked_hero_d,
            cast(null as varchar(50))                                                       eth_has_unlocked_hero_e,
            cast(null as varchar(50))                                                       eth_has_unlocked_hero_f,
            cast(null as varchar(50))                                                       eth_has_unlocked_hero_g,
            cast(null as varchar(50))                                                       eth_has_unlocked_hero_h,
            cast(null as varchar(50))                                                       eth_has_purchased_battle_pass,
            cast(null as varchar(50))                                                       eth_has_purchased_standard_battle_pass,
            cast(null as varchar(50))                                                       eth_has_purchased_premium_battle_pass,
            cast(null as varchar(50))                                                       eth_has_purchased_tier_skips,
            cast(null as varchar(50))                                                       eth_first_purchased_battle_pass,
            cast(null as varchar(50))                                                       eth_first_purchased_battle_pass_grouping,
            cast(null as varchar(50))                                                       eth_first_purchased_battle_pass_standard,
            cast(null as varchar(50))                                                       eth_first_purchased_battle_pass_premium,
            cast(null as varchar(50))                                                       eth_first_purchased_tier_skips,
            cast(null as varchar(50))                                                       eth_last_purchased_battle_pass,
            cast(null as varchar(50))                                                       eth_last_purchased_battle_pass_grouping,
            cast(null as varchar(50))                                                       eth_last_purchased_battle_pass_standard,
            cast(null as varchar(50))                                                       eth_last_purchased_battle_pass_premium,
            cast(null as varchar(50))                                                       eth_last_purchased_tier_skips,
            cast(null as varchar(50))                                                       eth_total_seasons_purchased_pass,
            cast(null as varchar(50))                                                       eth_total_seasons_purchased_standard_pass,
            cast(null as varchar(50))                                                       eth_total_seasons_purchased_premium_pass,
            cast(null as varchar(50))                                                       eth_total_seasons_purchased_tier_skips,
            cast(null as varchar(50))                                                       eth_total_purchased_tier_skips,
            cast(null as varchar(50))                                                       eth_current_season_level_reached,
            cast(null as varchar(50))                                                       eth_previous_season_level_reached,
            cast(null as varchar(50))                                                       eth_ltd_highest_season_level_reached,
            cast(null as varchar(50))                                                       eth_has_purchased_seasonal_pack,
            cast(null as varchar(50))                                                       eth_has_purchased_seasonal_pack_grouping,
            cast(null as varchar(50))                                                       eth_first_purchased_seasonal_pack,
            cast(null as varchar(50))                                                       eth_first_purchased_seasonal_pack_grouping,
            cast(null as varchar(50))                                                       eth_last_purchased_seasonal_pack,
            cast(null as varchar(50))                                                       eth_last_purchased_seasonal_pack_grouping,
            cast(null as varchar(50))                                                       eth_total_seasonal_pack_purchases,
            trials_first_seen                                                               eth_trials_first_seen,
            cast(null as varchar(50))                                                       eth_trials_first_seen_grouping,
            trials_last_seen                                                                eth_trials_last_seen,
            cast(null as varchar(50))                                                       eth_trials_last_seen_grouping,
            cast(null as varchar(50))                                                       eth_trials_days_active,
            trials_games_played                                                             eth_trials_games_played,
            trials_time_spent                                                               eth_trials_time_spent,
            trials_total_extractions                                                        eth_trials_total_extractions,
            trials_games_success                                                            eth_trials_games_success,
            trials_games_fail                                                               eth_trials_games_fail,
            trials_success_rate                                                             eth_trials_success_rate,
            cast(null as varchar(50))                                                       eth_trials_success_rate_grouping,
            cast(null as varchar(50))                                                       eth_trials_days_active_grouping,
            cast(null as varchar(50))                                                       eth_trials_games_played_grouping,
            trials_max_medal                                                                eth_trials_max_medal,
            trials_max_medal_count                                                          eth_trials_max_medal_count,
            cast(null as varchar(50))                                                       eth_trials_max_medal_count_grouping,
            trials_min_medal                                                                eth_trials_min_medal,
            gauntlet_first_seen                                                             eth_gauntlet_first_seen,
            cast(null as varchar(50))                                                       eth_gauntlet_first_seen_grouping,
            gauntlet_last_seen                                                              eth_gauntlet_last_seen,
            cast(null as varchar(50))                                                       eth_gauntlet_last_seen_grouping,
            cast(null as varchar(50))                                                       eth_gauntlet_days_active,
            gauntlet_games_played                                                           eth_gauntlet_games_played,
            gauntlet_time_spent                                                             eth_gauntlet_time_spent,
            gauntlet_games_success                                                          eth_gauntlet_games_success,
            gauntlet_games_fail                                                             eth_gauntlet_games_fail,
            gauntlet_success_rate                                                           eth_gauntlet_success_rate,
            cast(null as varchar(50))                                                       eth_gauntlet_success_rate_grouping,
            cast(null as varchar(50))                                                       eth_gauntlet_days_active_grouping,
            cast(null as varchar(50))                                                       eth_gauntlet_games_played_grouping,
            gauntlet_max_medal                                                              eth_gauntlet_max_medal,
            gauntlet_max_medal_count                                                        eth_gauntlet_max_medal_count,
            cast(null as varchar(50))                                                       eth_gauntlet_max_medal_count_grouping,
            gauntlet_min_medal                                                              eth_gauntlet_min_medal,
            cast(null as varchar(50))                                                       eth_pg_first_seen,
            cast(null as varchar(50))                                                       eth_pg_first_seen_grouping,
            cast(null as varchar(50))                                                       eth_pg_last_seen,
            cast(null as varchar(50))                                                       eth_pg_last_seen_grouping,
            cast(null as varchar(50))                                                       eth_pg_days_active,
            cast(null as varchar(50))                                                       eth_pg_games_played,
            cast(null as varchar(50))                                                       eth_pg_time_spent,
            cast(null as varchar(50))                                                       eth_pg_games_success,
            cast(null as varchar(50))                                                       eth_pg_games_fail,
            cast(null as varchar(50))                                                       eth_pg_success_rate,
            cast(null as varchar(50))                                                       eth_pg_success_rate_grouping,
            cast(null as varchar(50))                                                       eth_pg_days_active_grouping,
            cast(null as varchar(50))                                                       eth_pg_games_played_grouping,
            cast(null as varchar(50))                                                       eth_pg_max_medal,
            cast(null as varchar(50))                                                       eth_pg_max_medal_count,
            cast(null as varchar(50))                                                       eth_pg_max_medal_count_grouping,
            cast(null as varchar(50))                                                       eth_pg_min_medal,
            cast(null as varchar(50))                                                       eth_total_friends,
            cast(null as varchar(50))                                                       eth_friend_reach,
            cast(null as varchar(50))                                                       eth_is_tier_1_friend_count,
            cast(null as varchar(50))                                                       eth_is_tier_2_friend_count,
            cast(null as varchar(50))                                                       eth_is_tier_3_friend_count,
            cast(null as varchar(50))                                                       eth_has_tier_1_friend_count,
            cast(null as varchar(50))                                                       eth_has_tier_2_friend_count,
            cast(null as varchar(50))                                                       eth_has_tier_3_friend_count,
            cast(null as varchar(50))                                                       eth_friend_circle_tightness,
            cast(null as varchar(50))                                                       eth_games_played_with_friends,
            cast(null as varchar(50))                                                       eth_last_played_with_friends,
            cast(null as varchar(50))                                                       eth_last_played_with_friends_grouping,
            cast(null as varchar(50))                                                       eth_games_played_in_party,
            cast(null as varchar(50))                                                       eth_last_played_in_party,
            cast(null as varchar(50))                                                       eth_last_played_in_party_grouping,
            cast(null as varchar(50))                                                       eth_social_value,
            ftue_first_stage_complete                                                       eth_ftue_first_stage_complete,
            last_seen_season                                                                eth_last_seen_season,
            cast(null as varchar(50))                                                       eth_current_season_first_seen,
            cast(null as varchar(50))                                                       eth_current_season_first_seen_grouping,
            cast(null as varchar(50))                                                       eth_current_season_last_seen,
            cast(null as varchar(50))                                                       eth_current_season_last_seen_grouping,
            cast(null as varchar(50))                                                       eth_current_season_days_active,
            cast(null as varchar(50))                                                       eth_current_season_games_played,
            cast(null as varchar(50))                                                       eth_current_season_time_spent,
            cast(null as varchar(50))                                                       eth_current_season_games_success,
            cast(null as varchar(50))                                                       eth_current_season_games_fail,
            cast(null as varchar(50))                                                       eth_current_season_success_rate,
            cast(null as varchar(50))                                                       eth_current_season_success_rate_grouping,
            cast(null as varchar(50))                                                       eth_current_season_days_active_grouping,
            cast(null as varchar(50))                                                       eth_current_season_games_played_grouping,
            cast(null as varchar(50))                                                       eth_current_season_trials_medal_rank,
            cast(null as varchar(50))                                                       eth_current_season_gauntlet_medal_rank,
            cast(null as varchar(50))                                                       eth_current_season_pg_medal_rank,
            -- cast(null as varchar(50))                                                       eth_season_0_first_seen,
            -- cast(null as varchar(50))                                                       eth_season_0_first_seen_grouping,
            -- cast(null as varchar(50))                                                       eth_season_0_last_seen,
            -- cast(null as varchar(50))                                                       eth_season_0_last_seen_grouping,
            -- cast(null as varchar(50))                                                       eth_season_0_days_active,
            -- cast(null as varchar(50))                                                       eth_season_0_games_played,
            -- cast(null as varchar(50))                                                       eth_season_0_time_spent,
            -- cast(null as varchar(50))                                                       eth_season_0_games_success,
            -- cast(null as varchar(50))                                                       eth_season_0_games_fail,
            -- cast(null as varchar(50))                                                       eth_season_0_success_rate,
            -- cast(null as varchar(50))                                                       eth_season_0_success_rate_grouping,
            -- cast(null as varchar(50))                                                       eth_season_0_days_active_grouping,
            -- cast(null as varchar(50))                                                       eth_season_0_games_played_grouping,
            -- cast(null as varchar(50))                                                       eth_season_0_trials_medal_rank,
            -- cast(null as varchar(50))                                                       eth_season_0_gauntlet_medal_rank,
            -- cast(null as varchar(50))                                                       eth_season_0_pg_medal_rank,
            -- cast(null as varchar(50))                                                       eth_season_1_first_seen,
            -- cast(null as varchar(50))                                                       eth_season_1_first_seen_grouping,
            -- cast(null as varchar(50))                                                       eth_season_1_last_seen,
            -- cast(null as varchar(50))                                                       eth_season_1_last_seen_grouping,
            -- cast(null as varchar(50))                                                       eth_season_1_days_active,
            -- cast(null as varchar(50))                                                       eth_season_1_games_played,
            -- cast(null as varchar(50))                                                       eth_season_1_time_spent,
            -- cast(null as varchar(50))                                                       eth_season_1_games_success,
            -- cast(null as varchar(50))                                                       eth_season_1_games_fail,
            -- cast(null as varchar(50))                                                       eth_season_1_success_rate,
            -- cast(null as varchar(50))                                                       eth_season_1_success_rate_grouping,
            -- cast(null as varchar(50))                                                       eth_season_1_days_active_grouping,
            -- cast(null as varchar(50))                                                       eth_season_1_games_played_grouping,
            -- cast(null as varchar(50))                                                       eth_season_1_trials_medal_rank,
            -- cast(null as varchar(50))                                                       eth_season_1_gauntlet_medal_rank,
            -- cast(null as varchar(50))                                                       eth_season_1_pg_medal_rank,
            discord_linking                                                                 eth_custom_grouping_01,
            google_linking                                                                  eth_custom_grouping_02,
            cast(null as varchar(50))                                                       eth_custom_grouping_03,
            cast(null as varchar(50))                                                       eth_custom_grouping_04,
            cast(null as varchar(50))                                                       eth_custom_grouping_05,
            cast(null as varchar(50))                                                       eth_custom_grouping_06,
            cast(null as varchar(50))                                                       eth_custom_grouping_07,
            cast(null as varchar(50))                                                       eth_custom_grouping_08,
            cast(null as varchar(50))                                                       eth_custom_grouping_09,
            cast(null as varchar(50))                                                       eth_custom_grouping_10,
            cast(null as varchar(50))                                                       eth_custom_grouping_11,
            cast(null as varchar(50))                                                       eth_custom_grouping_12,
            cast(null as varchar(50))                                                       eth_custom_grouping_13,
            cast(null as varchar(50))                                                       eth_custom_grouping_14,
            cast(null as varchar(50))                                                       eth_custom_grouping_15,
            cast(null as varchar(50))                                                       eth_custom_grouping_16,
            cast(null as varchar(50))                                                       eth_custom_grouping_17,
            cast(null as varchar(50))                                                       eth_custom_grouping_18,
            cast(null as varchar(50))                                                       eth_custom_grouping_19,
            cast(null as varchar(50))                                                       eth_custom_grouping_20,
            cast(null as varchar(50))                                                       eth_custom_value_01,
            cast(null as varchar(50))                                                       eth_custom_value_02,
            cast(null as varchar(50))                                                       eth_custom_value_03,
            cast(null as varchar(50))                                                       eth_custom_value_04,
            cast(null as varchar(50))                                                       eth_custom_value_05,
            cast(null as varchar(50))                                                       eth_custom_value_06,
            cast(null as varchar(50))                                                       eth_custom_value_07,
            cast(null as varchar(50))                                                       eth_custom_value_08,
            cast(null as varchar(50))                                                       eth_custom_value_09,
            cast(null as varchar(50))                                                       eth_custom_value_10,
            cast(null as varchar(50))                                                       eth_custom_value_11,
            cast(null as varchar(50))                                                       eth_custom_value_12,
            cast(null as varchar(50))                                                       eth_custom_value_13,
            cast(null as varchar(50))                                                       eth_custom_value_14,
            cast(null as varchar(50))                                                       eth_custom_value_15,
            cast(null as varchar(50))                                                       eth_custom_value_16,
            cast(null as varchar(50))                                                       eth_custom_value_17,
            cast(null as varchar(50))                                                       eth_custom_value_18,
            cast(null as varchar(50))                                                       eth_custom_value_19,
            cast(null as varchar(50))                                                       eth_custom_value_20                      

          from all_metrics m
    """
    # Execute the SQL query and return the result as a DataFrame
    df = spark.sql(sql_query_events)
    return df

# COMMAND ----------

def run_batch():
    # Parse the environment argument
    environment = arg_parser()
    
    # Create a Spark session
    spark = SparkSession.builder.appName("Hydra").getOrCreate()
    
    # Create the view
    df = create(environment, spark)
    spark.sql(f"""select * from dataanalytics{environment}.cdp_ng.vw_eth_summary""").show()

# COMMAND ----------

if __name__ == "__main__":
    run_batch()
