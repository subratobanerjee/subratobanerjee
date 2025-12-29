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
    create or replace view dataanalytics{environment}.cdp_ng.vw_eth_seasons(
        parent_account_id,
        brand_firstpartyid,
        salted_puid,
        puid,
        eth_vendor,
        eth_service,
        eth_current_gen_platform,
        eth_next_gen_platform,
        eth_country_last,
        eth_install_date,
        eth_install_date_grouping,
        eth_viable,
        eth_viable_date,
        eth_season,
        eth_season_first_seen,
        eth_season_first_seen_grouping,
        eth_season_last_seen,
        eth_season_last_seen_grouping,
        eth_season_days_active,
        eth_season_games_played,
        eth_season_time_spent,
        eth_season_games_success,
        eth_season_games_fail,
        eth_season_success_rate,
        eth_season_success_rate_grouping,
        eth_season_days_active_grouping,
        eth_season_games_played_grouping,
        eth_season_trials_medal_rank,
        eth_season_gauntlet_medal_rank,
        eth_season_has_converted,
        eth_season_purchased_battlepass,
        eth_season_owns_battlepass,
        eth_season_total_dollar_spend,
        eth_season_total_dollar_spend_grouping,
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
        ) as
            with summary as (
                select distinct
                parent_account_id
                , platform_account_id
                , brand_firstpartyid
                , salted_puid
                , puid
                , eth_vendor
                , eth_service
                , eth_current_gen_platform
                , eth_next_gen_platform
                , eth_country_last
                , eth_install_date
                , eth_install_date_grouping
                , eth_viable
                , eth_viable_date
                from dataanalytics{environment}.cdp_ng.vw_eth_summary
            ),
        -- with game_modes as (
        --     select 'bcd' phase,	'bcd' intra_phase,	'2024-07-16T00:00:00.000+00:00'::timestamp start_date,	'2024-10-16T23:59:59.000+00:00'::timestamp end_date
        -- )
            player_install_grouping as (
                select distinct parent_id,
                ifnull(season, 'invalid_season') season,
                min(install_date::timestamp) over (partition by parent_id, season) as season_first_seen,
                max(install_date::timestamp) over (partition by parent_id, season) as season_last_seen
                from (select parent_id
                , received_on::timestamp install_date
                , case when install_date::timestamp between start_date and end_date then phase
                end as season
                from horizon{environment}.intermediate.fact_player_activity 
                join horizon_datasci{environment}.lookups.game_phase_lookup
                -- join game_modes
                )
                -- where season is not null -- add this when we get season values?
            )
            ,
            season_player_metrics as (
                select distinct full_account_id
                , season
                , count(distinct date)                                         active_days -- need to set min date
                , count(distinct match_id)                                     games_played
                , sum(match_duration)                                          time_spent
                , ifnull((sum(case 
                when match_type = 'Trials' then extractions_completed
                else 0 end)), 0) 
                + ifnull((sum(case 
                when match_type = 'Gauntlet' and gauntlet_team_wins = 3 then 1
                else 0 end)), 0)                                               games_success -- only for trials and gauntlet
                , ifnull(games_played - games_success, 0)                      games_fail
                , try_divide(games_success, games_played)                      success_rate
                from player_install_grouping
                join horizon{environment}.managed.fact_player_match_summary on parent_id = full_account_id
                group by 1,2
            )
            ,

            all_metrics as (
                select distinct
                p.parent_id parent_account_id
                , ifnull(p.season, 'invalid_season')                                        season
                , min(p.season_first_seen)                                                  season_first_seen
                , ifnull(p.season, 'invalid_season')                                        season_first_seen_grouping
                , max(p.season_last_seen)                                                   season_last_seen
                , case when datediff(day, max(p.season_last_seen), max(l.end_date::timestamp)) + 1 between 0 and 13 then '0-13'
                    when datediff(day, max(p.season_last_seen), max(l.end_date::timestamp)) + 1 between 14 and 20 then '14-20'
                    when datediff(day, max(p.season_last_seen), max(l.end_date::timestamp)) + 1 between 21 and 40 then '21-40'
                    when datediff(day, max(p.season_last_seen), max(l.end_date::timestamp)) + 1 between 41 and 60 then '41-60'
                    when datediff(day, max(p.season_last_seen), max(l.end_date::timestamp)) + 1 > 60 then '60+' 
                    else '0-13' 
                    end as                                                               season_last_seen_grouping
                , ifnull(max(pm.active_days),0)                                             season_days_active
                , ifnull(max(pm.games_played),0)                                            season_games_played 
                , round(ifnull(max(pm.time_spent),0), 2)                                    season_time_spent
                , ifnull(max(pm.games_success),0)                                           season_games_success
                , ifnull(max(pm.games_fail),0)                                              season_games_fail
                , round(ifnull(max(pm.success_rate),0), 2)                                  season_success_rate
                , percent_rank() over (order by ifnull(max(pm.success_rate),0)) as rank_per,
                    case 
                    when rank_per < 0.25 then '0.25'
                    when rank_per < 0.5 then '0.5'
                    when rank_per < 0.75 then '0.75'
                    when rank_per < 0.99 then '0.99'
                    else '1'
                    end as                                                                  season_success_rate_grouping
                , case when season_days_active between 0 and 1 then '0-1'
                    when season_days_active between 2 and 5 then '2-5'
                    when season_days_active between 6 and 10 then '6-10'
                    when season_days_active between 11 and 20 then '11-20'
                    when season_days_active between 21 and 40 then '21-40'
                    when season_days_active between 41 and 60 then '41-60'
                    when season_days_active > 60 then '60+' 
                    else '0-1' 
                    end as                                                                  season_days_active_grouping
                , case when season_games_played between 0 and 10 then '0-10'
                    when season_games_played between 11 and 30 then '11-30'
                    when season_games_played between 31 and 60 then '31-60'
                    when season_games_played between 61 and 100 then '101-200'
                    when season_games_played between 101 and 200 then '101-200'
                    when season_games_played between 201 and 400 then '201-400'
                    when season_games_played > 400 then '400+' 
                    else '0-1' 
                    end as                                                                  season_games_played_grouping
                from player_install_grouping p 
                left join season_player_metrics pm on lower(p.parent_id) = lower(pm.full_account_id) and p.season = pm.season
                left join horizon_datasci{environment}.lookups.game_phase_lookup l on l.phase = p.season
                group by p.parent_id, p.season
            )

            select distinct
                a.parent_account_id,
                brand_firstpartyid,
                salted_puid,
                f.puid,
                eth_vendor,
                eth_service,
                eth_current_gen_platform,
                eth_next_gen_platform,
                eth_country_last,
                eth_install_date,
                eth_install_date_grouping,
                eth_viable,
                eth_viable_date,
                season                                                                          eth_season,
                season_first_seen                                                               eth_season_first_seen,
                season_first_seen_grouping                                                      eth_season_first_seen_grouping,
                season_last_seen                                                                eth_season_last_seen,
                season_last_seen_grouping                                                       eth_season_last_seen_grouping,
                season_days_active                                                              eth_season_days_active,
                season_games_played                                                             eth_season_games_played,
                season_time_spent                                                               eth_season_time_spent,
                season_games_success                                                            eth_season_games_success,
                season_games_fail                                                               eth_season_games_fail,
                season_success_rate                                                             eth_season_success_rate,
                season_success_rate_grouping                                                    eth_season_success_rate_grouping,
                season_days_active_grouping                                                     eth_season_days_active_grouping,
                season_games_played_grouping                                                    eth_season_games_played_grouping,
                cast(null as varchar(50))                                                       eth_season_trials_medal_rank,
                cast(null as varchar(50))                                                       eth_season_gauntlet_medal_rank,
                cast(null as varchar(50))                                                       eth_season_has_converted,
                cast(null as varchar(50))                                                       eth_season_purchased_battlepass,
                cast(null as varchar(50))                                                       eth_season_owns_battlepass,
                cast(null as varchar(50))                                                       eth_season_total_dollar_spend,
                cast(null as varchar(50))                                                       eth_season_total_dollar_spend_grouping,
                cast(null as varchar(50))                                                       eth_custom_grouping_01,
                cast(null as varchar(50))                                                       eth_custom_grouping_02,
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

                from all_metrics a
                left join summary f on lower(a.parent_account_id) = lower(f.parent_account_id)
                where platform_account_id is not null
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
    spark.sql(f"""select * from dataanalytics{environment}.cdp_ng.vw_eth_seasons""").show()

# COMMAND ----------

if __name__ == "__main__":
    run_batch()
