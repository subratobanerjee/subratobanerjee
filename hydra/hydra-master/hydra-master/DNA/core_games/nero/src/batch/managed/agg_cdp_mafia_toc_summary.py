# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/table_functions

# COMMAND ----------

from pyspark.sql.functions import expr, coalesce
from delta.tables import DeltaTable

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment', set_environment())
database = 'dataanalytics'
data_source = 'cdp'
title = "'Mafia: The Old Country'"

# COMMAND ----------

def read_services(environment):

    services_df = (
        spark
        .read
        .table(f"reference_customer.platform.service")
        .select(
            expr("case when service = 'xbl' then 'Xbox Live' else service end as join_key"),
            "service",
            "vendor"
        )
        .distinct()
    )
    return services_df

# COMMAND ----------

def read_platforms(environment):

    platforms_df = (
        spark
        .read
        .table(f"reference_customer.platform.platform")
        .select(
            "src_platform",
            expr("case when platform in ('PC', 'Linux', 'OSX') then 'PC' else platform end as platform"),
            expr("case when vendor in ('PC', 'Linux', 'Apple') then 'PC' else vendor end as vendor")
        )
        .distinct()
    )
    return platforms_df

# COMMAND ----------

def read_title(environment):
    title_df = (
        spark
        .read
        .table(f"reference{environment}.title.dim_title")
        .where(expr(
            "TITLE in ('Mafia: The Old Country','%2K Portal%')"))
        .select(
            expr("display_platform as platform"),
            expr("display_service as service"),
            "app_id",
            "title"
        )

    )
    return title_df

# COMMAND ----------

def read_summary_ltd(environment): 
    current_min = (
        spark.read
        .table(f"dataanalytics{environment}.cdp_ng.mafia_toc_summary")
        .select(expr(f"ifnull(min(maf_toc_last_seen_date::date),'1999-01-01')::date as min_date"))
    ).collect()[0]['min_date'] 

    summary_ltd_df = (
        spark
        .read
        .table(f"nero{environment}.managed.fact_player_summary_ltd")
        .where(expr(f"install_date>='2025-08-07' and last_seen_date::date >= '{current_min}' - INTERVAL 3 DAY"))
    )
    return summary_ltd_df

# COMMAND ----------

def read_fact_player_ltd(environment):
    current_min = (
        spark.read
        .table(f"dataanalytics{environment}.cdp_ng.mafia_toc_summary")
        .select(expr(f"ifnull(min(maf_toc_last_seen_date::date),'1999-01-01')::date as min_date"))
    ).collect()[0]['min_date']

    fact_player_ltd_df = (
        spark
        .read
        .table(f"dataanalytics_prod.standard_metrics.fact_player_ltd")
        .where(expr(f"title = 'Mafia: The Old Country' and last_seen::date >= '{current_min}' - INTERVAL 6 DAY"))
    )
    return fact_player_ltd_df

# COMMAND ----------

def read_fpids(environment):
    summary_ltd_df = read_summary_ltd(environment).alias("f")

    """Read FPID data from vw_ctp_profile view"""

    fpids_df = (
        spark
        .read
        .table(f"reference_customer.customer.vw_ctp_profile")
        .alias("fpid")
        .where(expr("""
            first_party_id is not null
            and first_party_id not ilike 'dd1%-?'
            and first_party_id not ilike '%@%'
        """))
        .join(summary_ltd_df,expr("lower(f.player_id) = lower(fpid.public_id)"), "inner")
        .select(expr("f.player_id as platform_id"),
                expr("first_party_id AS puid")
                )
        .distinct()

    )
    return fpids_df

# COMMAND ----------

def read_deleteevent(environment):
    account_delete_df = (
        spark
        .read
        .table(f"coretech{environment}.sso.accountdeleteevent")
    )
    return account_delete_df


# COMMAND ----------

def read_fact_players_ltd(environment, services_df, platforms_df):

    summary_ltd_df = read_summary_ltd(environment).alias("f")
    fact_player_ltd_df = read_fact_player_ltd(environment).alias("fp")
    account_delete_df = read_deleteevent(environment).alias("sso")
 
    fact_players_df = (
        summary_ltd_df
        .join(
            fact_player_ltd_df,expr("lower(f.player_id) = lower(fp.player_id)"),
            "inner"
        )
        .join(
            services_df.alias("s"),
            expr("lower(s.join_key) = lower(f.service)")
        )
        .join(
            platforms_df.alias("v"),
            expr("f.platform = v.src_platform")
        )
        .join(
            account_delete_df,
            expr("sso.accountId = f.player_id"),
            "left"
        )
        .where(expr("f.platform is not null and sso.accountId is null"))
        .select(
            expr("lower(f.player_id) as player_id"),
            expr("""first_value(v.platform) over (
                partition by f.player_id 
                order by f.install_date, f.last_seen_date desc
            ) as platform"""),
            expr("""first_value(v.vendor) over (
                partition by f.player_id 
                order by f.install_date, f.last_seen_date desc
            ) as vendor"""),
            expr("""first_value(s.service) over (
                partition by f.player_id 
                order by f.install_date, f.last_seen_date desc
            ) as service"""),
            expr("min(fp.viable_date) over (partition by fp.player_id) as viable_date"),
            expr("""first_value(fp.country_code) ignore nulls over (
                partition by lower(f.player_id) 
                order by f.install_date, f.last_seen_date desc
            ) as last_country_code"""),
            expr("""first_value(case when v.platform not in ('XBSX', 'PS5', 'Windows', 'PC','NSW2') then v.platform else null end) 
                ignore nulls over (
                    partition by lower(f.player_id), f.player_id 
                    order by f.install_date, f.last_seen_date desc
                ) as current_gen_platform"""),
            expr("""first_value(case when v.platform in ('XBSX', 'PS5', 'Windows', 'PC','NSW2') then v.platform else null end) 
                ignore nulls over (
                    partition by f.player_id 
                    order by f.install_date, f.last_seen_date desc
                ) as next_gen_platform"""),
            "f.install_date",
            "f.last_seen_date",
            "f.last_mission_complete_num",
            "f.last_mission_complete_name",
            "f.total_freeride_minutes",
            "fp.total_logins"
        )
        .distinct()
    )
    return fact_players_df

# COMMAND ----------

def read_player_entitlement_details(environment):

    entitlement_df = (
        spark
        .read
        .table(f"nero{environment}.managed_view.fact_player_sku")
        .select(
            "player_id",
            "sku",
            "nero_premium_sku",
            "nero_preorder",
            "maf_toc_expansion_pack_1",
            "maf_toc_expansion_pack_2"
        )
        .distinct()
    )
    return entitlement_df

# COMMAND ----------

def read_session_time(environment):

    session_df = (
        spark
        .read
        .table(f"nero{environment}.managed.fact_player_session_daily")
        .where(expr("session_start_date::date >= '2025-08-07'"))
        .groupBy("player_id", "platform", "service")
        .agg(
            expr("sum(round(session_len_sec/60,2)) as total_minutes_session_played"),
            expr("count(distinct session_start_date::date) as days_active")
        )
    )
    return session_df

# COMMAND ----------

def read_game_status(environment):

    game_status_df = (
        spark
        .read
        .table(f"nero{environment}.managed.fact_player_game_status_daily")
        .where(expr("date >= '2025-08-07'"))
        .groupBy("player_id")
        .agg(
            expr('''
            min(case
            when game_mode ilike '%Plotline.Main%' then date
            else null
            end) as story_first_seen
            '''),
            expr('''
            max(case
            when game_mode ilike '%Plotline.Main%' then date
            else null
            end) as story_last_seen
            '''),
            expr('''
            count(distinct case
            when game_mode ilike '%Plotline.Main%' then  date
            else null
            end) as story_days_active
            '''),
            expr('''
            sum(case
            when game_mode ilike '%Plotline.Main%' then agg_gp_5
            else null
            end) as story_games_played
            '''),
            expr('''
            sum(case
            when game_mode ilike '%Plotline.Main%' then agg_game_status_2
            else null
            end) as story_time_spent
            '''),
            expr('''
            min(case
            when game_mode ilike '%Plotline.FreeRide%' then date
            else null
            end) as freeride_first_seen
            '''),
            expr('''
            max(case
            when game_mode ilike '%Plotline.FreeRide%' then date
            else null
            end) as freeride_last_seen
            '''),
            expr('''
            count(distinct case
            when game_mode ilike '%Plotline.FreeRide%' then  date
            else null
            end) as freeride_days_active
            '''),
            expr('''
            sum(case
            when game_mode ilike '%Plotline.FreeRide%' then agg_gp_5
            else null
            end) as freeride_games_played
            '''),
            expr('''
            sum(case
            when game_mode ilike '%Plotline.FreeRide%' then agg_game_status_2
            else null
            end) as freeride_time_spent
            ''')
        )
        .distinct()
    )
    return game_status_df

# COMMAND ----------

def extract(environment):

    services_df = read_services(environment).alias("services")
    platforms_df = read_platforms(environment).alias("platforms")
    fpids_df = read_fpids(environment).alias("fpids")
    fact_players_df = read_fact_players_ltd(environment, services_df, platforms_df).alias("fact_players")
    entitlement_df = read_player_entitlement_details(environment).alias("entitlement")
    session_df = read_session_time(environment).alias("session")
    game_status_df = read_game_status(environment).alias("game_status")
    
    return services_df, platforms_df, fpids_df, fact_players_df, entitlement_df, session_df, game_status_df

# COMMAND ----------

def transform(services_df, platforms_df, fpids_df, fact_players_df, entitlement_df, session_df,game_status_df):
    """Transform data to create CDP summary"""
    
    # Create all_metrics aggregation
    all_metrics_df = (
        fact_players_df.alias("f")
        .join(
            fpids_df.alias("fpd"),
            expr("lower(fpd.platform_id) = lower(f.player_id)")
        )
        .join(
            entitlement_df.alias("pe"),
            expr("lower(f.player_id) = lower(pe.player_id)"),
            "left"
        )
        .join(
            session_df.alias("st"),
            expr("lower(f.player_id) = lower(st.player_id)"),
            "left"
        )
        .join(
            game_status_df.alias("gs"),
            expr("lower(f.player_id) = lower(gs.player_id)"),
            "left"
        )
        .groupBy(
            expr("lower(f.player_id) as platform_account_id"),
            "fpd.puid",
            "f.vendor",
            "f.service"
        )
        .agg(
            expr("first_value(fpd.puid) over (partition by lower(f.player_id) order by to_date(min(f.install_date)::timestamp)) as first_puid"),
            expr("max(current_gen_platform) as current_gen_platform"),
            expr("case when max(next_gen_platform) = 'Windows' then 'PC' else max(next_gen_platform) end as next_gen_platform"),
            expr("max(ifnull(f.last_country_code, 'ZZ')) as country_last"),
            expr("to_date(min(f.install_date)::timestamp) as install_date"),
            expr("cast(min(f.viable_date) as date) as viable_date"),
            expr("""cast(
                min(f.viable_date) is not null and min(f.viable_date) <= current_date as boolean
            ) as is_viable"""),
            expr("to_date(max(f.last_seen_date)::timestamp) as last_seen_date"),
            expr("""case
                when datediff(day, max(f.last_seen_date), current_date) + 1 between 0 and 13 then '0-13'
                when datediff(day, max(f.last_seen_date), current_date) + 1 between 14 and 20 then '14-20'
                when datediff(day, max(f.last_seen_date), current_date) + 1 between 21 and 40 then '21-40'
                when datediff(day, max(f.last_seen_date), current_date) + 1 between 41 and 60 then '41-60'
                when datediff(day, max(f.last_seen_date), current_date) + 1 > 60 then '60+'
                else '0-13'
            end as last_seen_date_grouping"""),
            expr("max(ifnull(st.days_active, 0)) as maf_days_active"),
            expr("""case
                when max(ifnull(st.days_active, 0)) between 0 and 1 then '0-1'
                when max(ifnull(st.days_active, 0)) between 2 and 5 then '2-5'
                when max(ifnull(st.days_active, 0)) between 6 and 10 then '6-10'
                when max(ifnull(st.days_active, 0)) > 10 then '10+'
                else '0-1'
            end as days_active_grouping"""),
            expr("max(pe.sku) as sku"),
            expr("max(ifnull(pe.nero_premium_sku, false)) as nero_premium_sku"),
            expr("max(ifnull(pe.nero_preorder, false)) as nero_preorder"),
            expr("max(ifnull(pe.maf_toc_expansion_pack_1, false)) as maf_toc_expansion_pack_1"),
            expr("max(ifnull(pe.maf_toc_expansion_pack_2, false)) as maf_toc_expansion_pack_2"),
            expr("max(ifnull(total_minutes_session_played, 0)) as time_spent"),
            expr("max(ifnull(last_mission_complete_num, 'None')) as last_mission_complete_num"),
            expr("max(ifnull(last_mission_complete_name, 'None')) as last_mission_complete_name"),
            expr("max(ifnull(total_freeride_minutes, 0)) as total_freeride_minutes"),
            expr("max(f.total_logins) as games_played"),
            expr("min(story_first_seen) as story_first_seen"),
            expr("max(story_last_seen) as story_last_seen"),
            expr("max(story_days_active) as story_days_active"),
            expr("max(story_games_played) as story_games_played"),
            expr("max(story_time_spent) as story_time_spent"),
            expr("min(freeride_first_seen) as freeride_first_seen"),
            expr("max(freeride_last_seen) as freeride_last_seen"),
            expr("max(freeride_days_active) as freeride_days_active"),
            expr("max(freeride_games_played) as freeride_games_played"),
            expr("max(freeride_time_spent) as freeride_time_spent")
        )
    )
    
    # Final output transformation
    output_df = (
        all_metrics_df
        .select(
            expr("vendor || ':' || dataanalytics_prod.cdp_ng.salted_puid(puid) as brand_firstpartyid"),
            expr("dataanalytics_prod.cdp_ng.salted_puid(puid) as salted_puid"),
            expr("first_puid as puid"),
            "platform_account_id",
            expr("vendor as maf_toc_vendor"),
            expr("service as maf_toc_service"),
            expr("current_gen_platform as maf_toc_current_gen_platform"),
            expr("next_gen_platform as maf_toc_next_gen_platform"),
            expr("country_last as maf_toc_country_last"),
            expr("install_date as maf_toc_install_date"),
            expr("cast(null as varchar(255)) as maf_toc_install_date_grouping"),
            expr("is_viable as maf_toc_viable"),
            expr("case when viable_date = '9999-01-01' then null else viable_date end as maf_toc_viable_date"),
            expr("nero_premium_sku as maf_toc_premium_sku"),
            expr("nero_preorder as maf_toc_is_pre_order"),
            expr("maf_toc_expansion_pack_1 as maf_toc_expansion_pack_1"),
            expr("maf_toc_expansion_pack_2 as maf_toc_expansion_pack_2"),
            expr("cast(null as varchar(255)) as maf_toc_expansion_pack_3"),
            expr("cast(null as varchar(255)) as maf_toc_expansion_pack_4"),
            expr("cast(null as float) as maf_toc_price_known"),
            expr("cast(null as float) as maf_toc_price_probabilistic"),
            expr("cast(null as boolean) as maf_toc_is_ftp_install"),
            expr("last_seen_date as maf_toc_last_seen_date"),
            expr("last_seen_date_grouping as maf_toc_last_seen_grouping"),
            expr("cast(null as varchar(255)) as maf_toc_player_type"),
            expr("maf_days_active as maf_toc_days_active"),
            expr("games_played as maf_toc_games_played"),
            expr("time_spent as maf_toc_time_spent"),
            expr("cast(null as float) as maf_toc_games_success"),
            expr("cast(null as float) as maf_toc_games_fail"),
            expr("cast(null as float) as maf_toc_success_rate"),
            expr("cast(null as varchar(255)) as maf_toc_success_rate_grouping"),
            expr("days_active_grouping as maf_toc_days_active_grouping"),
            expr("cast(null as varchar(255)) as maf_toc_games_played_grouping"),
            expr("cast(null as varchar(255)) as maf_toc_game_progress_grouping"),
            expr("cast(null as varchar(255)) as maf_toc_mode_preference"),
            expr("cast(null as varchar(255)) as maf_toc_character_preference"),
            expr("story_first_seen as maf_toc_story_first_seen"),
            expr("cast(null as varchar(255)) as maf_toc_story_first_seen_grouping"),
            expr("story_last_seen as maf_toc_story_last_seen"),
            expr("cast(null as varchar(255)) as maf_toc_story_last_seen_grouping"),
            expr("story_days_active as maf_toc_story_days_active"),
            expr("story_games_played as maf_toc_story_games_played"),
            expr("story_time_spent as maf_toc_story_time_spent"),
            expr("cast(null as float) as maf_toc_story_games_success"),
            expr("cast(null as float) as maf_toc_story_games_fail"),
            expr("cast(null as float) as maf_toc_story_success_rate"),
            expr("cast(null as varchar(255)) as maf_toc_story_success_rate_grouping"),
            expr("cast(null as varchar(255)) as maf_toc_story_days_active_grouping"),
            expr("cast(null as varchar(255)) as maf_toc_story_games_played_grouping"),
            expr("cast(null as boolean) as maf_toc_story_has_earned_vc"),
            expr("cast(null as float) as maf_toc_story_earned_vc_total"),
            expr("cast(null as varchar(255)) as maf_toc_story_earned_vc_grouping"),
            expr("cast(null as date) as maf_toc_story_earned_vc_first_date"),
            expr("cast(null as varchar(255)) as maf_toc_story_earned_first_date_grouping"),
            expr("cast(null as date) as maf_toc_story_earned_vc_last_date"),
            expr("cast(null as float) as maf_toc_story_earned_vc_transactions"),
            expr("cast(null as float) as maf_toc_story_vc_spent_total"),
            expr("cast(null as varchar(255)) as maf_toc_story_vc_spent_grouping"),
            expr("cast(null as float) as maf_toc_story_vc_spent_actions"),
            expr("cast(null as date) as maf_toc_story_vc_spent_first_date"),
            expr("cast(null as varchar(255)) as maf_toc_story_vc_spent_first_date_grouping"),
            expr("cast(null as date) as maf_toc_story_vc_spent_last_date"),
            expr("freeride_first_seen as maf_toc_freeride_first_seen"),
            expr("cast(null as varchar(255)) as maf_toc_freeride_first_seen_grouping"),
            expr("freeride_last_seen as maf_toc_freeride_last_seen"),
            expr("cast(null as varchar(255)) as maf_toc_freeride_last_seen_grouping"),
            expr("freeride_days_active as maf_toc_freeride_days_active"),
            expr("freeride_games_played as maf_toc_freeride_games_played"),
            expr("freeride_time_spent as maf_toc_freeride_time_spent"),
            expr("cast(null as float) as maf_toc_freeride_games_success"),
            expr("cast(null as float) as maf_toc_freeride_games_fail"),
            expr("cast(null as float) as maf_toc_freeride_success_rate"),
            expr("cast(null as varchar(255)) as maf_toc_freeride_success_rate_grouping"),
            expr("cast(null as varchar(255)) as maf_toc_freeride_days_active_grouping"),
            expr("cast(null as varchar(255)) as maf_toc_freeride_games_played_grouping"),
            expr("cast(null as boolean) as maf_toc_freeride_has_earned_vc"),
            expr("cast(null as float) as maf_toc_freeride_earned_vc_total"),
            expr("cast(null as varchar(255)) as maf_toc_freeride_earned_vc_grouping"),
            expr("cast(null as date) as maf_toc_freeride_earned_vc_first_date"),
            expr("cast(null as varchar(255)) as maf_toc_freeride_earned_first_date_grouping"),
            expr("cast(null as date) as maf_toc_freeride_earned_vc_last_date"),
            expr("cast(null as float) as maf_toc_freeride_earned_vc_transactions"),
            expr("cast(null as float) as maf_toc_freeride_vc_spent_total"),
            expr("cast(null as varchar(255)) as maf_toc_freeride_vc_spent_grouping"),
            expr("cast(null as float) as maf_toc_freeride_vc_spent_actions"),
            expr("cast(null as date) as maf_toc_freeride_vc_spent_first_date"),
            expr("cast(null as varchar(255)) as maf_toc_freeride_vc_spent_first_date_grouping"),
            expr("cast(null as date) as maf_toc_freeride_vc_spent_last_date"),
            expr("cast(null as date) as maf_toc_other_first_seen"),
            expr("cast(null as varchar(255)) as maf_toc_other_first_seen_grouping"),
            expr("cast(null as date) as maf_toc_other_last_seen"),
            expr("cast(null as varchar(255)) as maf_toc_other_last_seen_grouping"),
            expr("cast(null as float) as maf_toc_other_days_active"),
            expr("cast(null as float) as maf_toc_other_games_played"),
            expr("cast(null as float) as maf_toc_other_time_spent"),
            expr("cast(null as float) as maf_toc_other_games_success"),
            expr("cast(null as float) as maf_toc_other_games_fail"),
            expr("cast(null as float) as maf_toc_other_success_rate"),
            expr("cast(null as varchar(255)) as maf_toc_other_success_rate_grouping"),
            expr("cast(null as varchar(255)) as maf_toc_other_days_active_grouping"),
            expr("cast(null as varchar(255)) as maf_toc_other_games_played_grouping"),
            expr("cast(null as boolean) as maf_toc_other_has_earned_vc"),
            expr("cast(null as float) as maf_toc_other_earned_vc_total"),
            expr("cast(null as varchar(255)) as maf_toc_other_earned_vc_grouping"),
            expr("cast(null as date) as maf_toc_other_earned_vc_first_date"),
            expr("cast(null as varchar(255)) as maf_toc_other_earned_first_date_grouping"),
            expr("cast(null as date) as maf_toc_other_earned_vc_last_date"),
            expr("cast(null as float) as maf_toc_other_earned_vc_transactions"),
            expr("cast(null as float) as maf_toc_other_vc_spent_total"),
            expr("cast(null as varchar(255)) as maf_toc_other_vc_spent_grouping"),
            expr("cast(null as float) as maf_toc_other_vc_spent_actions"),
            expr("cast(null as date) as maf_toc_other_vc_spent_first_date"),
            expr("cast(null as varchar(255)) as maf_toc_other_vc_spent_first_date_grouping"),
            expr("cast(null as date) as maf_toc_other_vc_spent_last_date"),
            expr("cast(null as boolean) as maf_toc_bought_vc_converted"),
            expr("cast(null as float) as maf_toc_bought_vc_transactions"),
            expr("cast(null as varchar(255)) as maf_toc_bought_vc_transactions_grouping"),
            expr("cast(null as float) as maf_toc_bought_vc_total"),
            expr("cast(null as varchar(255)) as maf_toc_bought_vc_grouping"),
            expr("cast(null as date) as maf_toc_bought_vc_first_date"),
            expr("cast(null as varchar(255)) as maf_toc_bought_vc_first_date_grouping"),
            expr("cast(null as date) as maf_toc_bought_vc_last_date"),
            expr("cast(null as varchar(255)) as maf_toc_bought_vc_last_date_grouping"),
            expr("cast(null as float) as maf_toc_bought_vc_dollar_value"),
            expr("cast(null as varchar(255)) as maf_toc_bought_vc_dollar_grouping"),
            expr("cast(null as varchar(255)) as maf_toc_earned_bought_gifted_ratio_grouping"),
            expr("cast(null as boolean) as maf_toc_has_earned_vc"),
            expr("cast(null as varchar(255)) as maf_toc_earned_vc_total"),
            expr("cast(null as varchar(255)) as maf_toc_earned_vc_grouping"),
            expr("cast(null as date) as maf_toc_earned_vc_first_date"),
            expr("cast(null as varchar(255)) as maf_toc_earned_vc_first_date_grouping"),
            expr("cast(null as date) as maf_toc_earned_vc_last_date"),
            expr("cast(null as float) as maf_toc_earned_vc_actions"),
            expr("cast(null as varchar(255)) as maf_toc_earned_vc_actions_grouping"),
            expr("cast(null as boolean) as maf_toc_has_gifted_vc"),
            expr("cast(null as float) as maf_toc_gifted_vc_total"),
            expr("cast(null as varchar(255)) as maf_toc_gifted_vc_grouping"),
            expr("cast(null as date) as maf_toc_gifted_vc_last_date"),
            expr("cast(null as date) as maf_toc_gifted_vc_first_date"),
            expr("cast(null as varchar(255)) as maf_toc_gifted_vc_first_date_grouping"),
            expr("cast(null as float) as maf_toc_gifted_vc_transactions"),
            expr("cast(null as varchar(255)) as maf_toc_gifted_vc_transactions_grouping"),
            expr("cast(null as boolean) as maf_toc_has_spent_vc"),
            expr("cast(null as float) as maf_toc_vc_spent_total"),
            expr("cast(null as varchar(255)) as maf_toc_vc_spent_grouping"),
            expr("cast(null as date) as maf_toc_vc_spent_first_date"),
            expr("cast(null as varchar(255)) as maf_toc_vc_spent_first_date_grouping"),
            expr("cast(null as date) as maf_toc_vc_spent_last_date"),
            expr("cast(null as float) as maf_toc_vc_spent_actions"),
            expr("cast(null as varchar(255)) as maf_toc_vc_spent_actions_grouping"),
            expr("cast(null as float) as maf_toc_current_vc_balance"),
            expr("cast(null as varchar(255)) as maf_toc_current_vc_balance_grouping"),
            expr("last_mission_complete_name as maf_toc_custom_grouping_01"),
            expr("total_freeride_minutes as maf_toc_custom_grouping_02"),
            expr("sku as maf_toc_custom_grouping_03"),
            expr("cast(null as varchar(255)) as maf_toc_custom_grouping_04"),
            expr("cast(null as varchar(255)) as maf_toc_custom_grouping_05"),
            expr("cast(null as varchar(255)) as maf_toc_custom_grouping_06"),
            expr("cast(null as varchar(255)) as maf_toc_custom_grouping_07"),
            expr("cast(null as varchar(255)) as maf_toc_custom_grouping_08"),
            expr("cast(null as varchar(255)) as maf_toc_custom_grouping_09"),
            expr("cast(null as varchar(255)) as maf_toc_custom_grouping_10"),
            expr("last_mission_complete_num as maf_toc_custom_value_01"),
            expr("cast(null as float) as maf_toc_custom_value_02"),
            expr("cast(null as float) as maf_toc_custom_value_03"),
            expr("cast(null as float) as maf_toc_custom_value_04"),
            expr("cast(null as float) as maf_toc_custom_value_05"),
            expr("cast(null as float) as maf_toc_custom_value_06"),
            expr("cast(null as float) as maf_toc_custom_value_07"),
            expr("cast(null as float) as maf_toc_custom_value_08"),
            expr("cast(null as float) as maf_toc_custom_value_09"),
            expr("cast(null as float) as maf_toc_custom_value_10") 
        )
    )
    
    return output_df

# COMMAND ----------

def load_mafia_toc_summary(spark, df, database, environment):

    # Perform the merge operation into the Delta table
    final_table = DeltaTable.forName(spark, f"{database}{environment}.cdp_ng.mafia_toc_summary")

    # get schema of the table
    table_schema = spark.read.table(f"{database}{environment}.cdp_ng.mafia_toc_summary").schema

    # create empty dataframe using the schema
    out_df = spark.createDataFrame([], table_schema)

    # union the incoming dataframe with the empty dataframe, we can set allowMissingColumns to False to be more strict
    df = df.selectExpr(
            "*",
            "CURRENT_TIMESTAMP() as dw_insert_ts",
            "CURRENT_TIMESTAMP() as dw_update_ts",
            "SHA2(CONCAT_WS('|', brand_firstpartyid, salted_puid, puid,platform_account_id), 256) as merge_key"
        )
    out_df = out_df.unionByName(df, allowMissingColumns=True)

    # dynamically setup merge condition based on the aggregate columns in this database's fact_player_session_daily table
    agg_cols = [col_name for col_name in out_df.columns if col_name not in ['dw_insert_ts','dw_update_ts','merge_key']]
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

# COMMAND ----------

def create_mafia_toc_summary_table(spark, database):
    """Create the mafia_toc_summary table"""

    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.cdp_ng.mafia_toc_summary (
        brand_firstpartyid string,
        salted_puid string,
        puid string,
        platform_account_id string,
        maf_toc_vendor string,
        maf_toc_service string,
        maf_toc_current_gen_platform string,
        maf_toc_next_gen_platform string,
        maf_toc_country_last string,
        maf_toc_install_date date,
        maf_toc_install_date_grouping string,
        maf_toc_viable boolean,
        maf_toc_viable_date date,
        maf_toc_premium_sku boolean,
        maf_toc_is_pre_order boolean,
        maf_toc_expansion_pack_1 boolean,
        maf_toc_expansion_pack_2 boolean,
        maf_toc_expansion_pack_3 string,
        maf_toc_expansion_pack_4 string,
        maf_toc_price_known float,
        maf_toc_price_probabilistic float,
        maf_toc_is_ftp_install boolean,
        maf_toc_last_seen_date date,
        maf_toc_last_seen_grouping string,
        maf_toc_player_type string,
        maf_toc_days_active int,
        maf_toc_games_played int,
        maf_toc_time_spent decimal(38,2),
        maf_toc_games_success float,
        maf_toc_games_fail float,
        maf_toc_success_rate float,
        maf_toc_success_rate_grouping string,
        maf_toc_days_active_grouping string,
        maf_toc_games_played_grouping string,
        maf_toc_game_progress_grouping string,
        maf_toc_mode_preference string,
        maf_toc_character_preference string,
        maf_toc_story_first_seen date,
        maf_toc_story_first_seen_grouping varchar(255),
        maf_toc_story_last_seen date,
        maf_toc_story_last_seen_grouping varchar(255),
        maf_toc_story_days_active float,
        maf_toc_story_games_played float,
        maf_toc_story_time_spent float,
        maf_toc_story_games_success float,
        maf_toc_story_games_fail float,
        maf_toc_story_success_rate float,
        maf_toc_story_success_rate_grouping varchar(255),
        maf_toc_story_days_active_grouping varchar(255),
        maf_toc_story_games_played_grouping varchar(255),
        maf_toc_story_has_earned_vc boolean,
        maf_toc_story_earned_vc_total float,
        maf_toc_story_earned_vc_grouping varchar(255),
        maf_toc_story_earned_vc_first_date date,
        maf_toc_story_earned_first_date_grouping varchar(255),
        maf_toc_story_earned_vc_last_date date,
        maf_toc_story_earned_vc_transactions float,
        maf_toc_story_vc_spent_total float,
        maf_toc_story_vc_spent_grouping varchar(255),
        maf_toc_story_vc_spent_actions float,
        maf_toc_story_vc_spent_first_date date,
        maf_toc_story_vc_spent_first_date_grouping varchar(255),
        maf_toc_story_vc_spent_last_date date,
        maf_toc_freeride_first_seen date,
        maf_toc_freeride_first_seen_grouping varchar(255),
        maf_toc_freeride_last_seen date,
        maf_toc_freeride_last_seen_grouping varchar(255),
        maf_toc_freeride_days_active float,
        maf_toc_freeride_games_played float,
        maf_toc_freeride_time_spent float,
        maf_toc_freeride_games_success float,
        maf_toc_freeride_games_fail float,
        maf_toc_freeride_success_rate float,
        maf_toc_freeride_success_rate_grouping varchar(255),
        maf_toc_freeride_days_active_grouping varchar(255),
        maf_toc_freeride_games_played_grouping varchar(255),
        maf_toc_freeride_has_earned_vc boolean,
        maf_toc_freeride_earned_vc_total float,
        maf_toc_freeride_earned_vc_grouping varchar(255),
        maf_toc_freeride_earned_vc_first_date date,
        maf_toc_freeride_earned_first_date_grouping varchar(255),
        maf_toc_freeride_earned_vc_last_date date,
        maf_toc_freeride_earned_vc_transactions float,
        maf_toc_freeride_vc_spent_total float,
        maf_toc_freeride_vc_spent_grouping varchar(255),
        maf_toc_freeride_vc_spent_actions float,
        maf_toc_freeride_vc_spent_first_date date,
        maf_toc_freeride_vc_spent_first_date_grouping varchar(255),
        maf_toc_freeride_vc_spent_last_date date,
        maf_toc_other_first_seen date,
        maf_toc_other_first_seen_grouping varchar(255),
        maf_toc_other_last_seen date,
        maf_toc_other_last_seen_grouping varchar(255),
        maf_toc_other_days_active float,
        maf_toc_other_games_played float,
        maf_toc_other_time_spent float,
        maf_toc_other_games_success float,
        maf_toc_other_games_fail float,
        maf_toc_other_success_rate float,
        maf_toc_other_success_rate_grouping varchar(255),
        maf_toc_other_days_active_grouping varchar(255),
        maf_toc_other_games_played_grouping varchar(255),
        maf_toc_other_has_earned_vc boolean,
        maf_toc_other_earned_vc_total float,
        maf_toc_other_earned_vc_grouping varchar(255),
        maf_toc_other_earned_vc_first_date date,
        maf_toc_other_earned_first_date_grouping varchar(255),
        maf_toc_other_earned_vc_last_date date,
        maf_toc_other_earned_vc_transactions float,
        maf_toc_other_vc_spent_total float,
        maf_toc_other_vc_spent_grouping varchar(255),
        maf_toc_other_vc_spent_actions float,
        maf_toc_other_vc_spent_first_date date,
        maf_toc_other_vc_spent_first_date_grouping varchar(255),
        maf_toc_other_vc_spent_last_date date,
        maf_toc_bought_vc_converted boolean,
        maf_toc_bought_vc_transactions float,
        maf_toc_bought_vc_transactions_grouping string,
        maf_toc_bought_vc_total float,
        maf_toc_bought_vc_grouping string,
        maf_toc_bought_vc_first_date date,
        maf_toc_bought_vc_first_date_grouping string,
        maf_toc_bought_vc_last_date date,
        maf_toc_bought_vc_last_date_grouping string,
        maf_toc_bought_vc_dollar_value float,
        maf_toc_bought_vc_dollar_grouping string,
        maf_toc_earned_bought_gifted_ratio_grouping string,
        maf_toc_has_earned_vc boolean,
        maf_toc_earned_vc_total string,
        maf_toc_earned_vc_grouping string,
        maf_toc_earned_vc_first_date date,
        maf_toc_earned_vc_first_date_grouping string,
        maf_toc_earned_vc_last_date date,
        maf_toc_earned_vc_actions float,
        maf_toc_earned_vc_actions_grouping string,
        maf_toc_has_gifted_vc boolean,
        maf_toc_gifted_vc_total float,
        maf_toc_gifted_vc_grouping string,
        maf_toc_gifted_vc_last_date date,
        maf_toc_gifted_vc_first_date date,
        maf_toc_gifted_vc_first_date_grouping string,
        maf_toc_gifted_vc_transactions float,
        maf_toc_gifted_vc_transactions_grouping string,
        maf_toc_has_spent_vc boolean,
        maf_toc_vc_spent_total float,
        maf_toc_vc_spent_grouping string,
        maf_toc_vc_spent_first_date date,
        maf_toc_vc_spent_first_date_grouping string,
        maf_toc_vc_spent_last_date date,
        maf_toc_vc_spent_actions float,
        maf_toc_vc_spent_actions_grouping string,
        maf_toc_current_vc_balance float,
        maf_toc_current_vc_balance_grouping string,
        maf_toc_custom_grouping_01 string,
        maf_toc_custom_grouping_02 decimal(38,2),
        maf_toc_custom_grouping_03 string,
        maf_toc_custom_grouping_04 string,
        maf_toc_custom_grouping_05 string,
        maf_toc_custom_grouping_06 string,
        maf_toc_custom_grouping_07 string,
        maf_toc_custom_grouping_08 string,
        maf_toc_custom_grouping_09 string,
        maf_toc_custom_grouping_10 string,
        maf_toc_custom_value_01 float,
        maf_toc_custom_value_02 float,
        maf_toc_custom_value_03 float,
        maf_toc_custom_value_04 float,
        maf_toc_custom_value_05 float,
        maf_toc_custom_value_06 float,
        maf_toc_custom_value_07 float,
        maf_toc_custom_value_08 float,
        maf_toc_custom_value_09 float,
        maf_toc_custom_value_10 float,
        dw_insert_ts timestamp default current_timestamp(),
        dw_update_ts timestamp default current_timestamp(),
        merge_key string
    )
    """

    properties = {
        "delta.feature.allowColumnDefaults": "supported" 
    }

    create_table(spark, sql, properties)
    return f"Table mafia_toc_summary created"



# COMMAND ----------

def run_batch():
    """Main execution function"""
    spark = create_spark_session(name=f"{database}")
    create_mafia_toc_summary_table(spark, database)

    services_df, platforms_df, fpids_df, fact_players_df, entitlement_df, session_df, game_status_df = extract(environment)
    output_df = transform(services_df, platforms_df, fpids_df, fact_players_df, entitlement_df, session_df, game_status_df)
    load_mafia_toc_summary(spark, output_df, database, environment)

# COMMAND ----------

run_batch()
