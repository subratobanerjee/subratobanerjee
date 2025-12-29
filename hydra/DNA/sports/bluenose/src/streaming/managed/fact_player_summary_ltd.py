# Databricks notebook source
# MAGIC %run ../../../../../../gibraltar/managed/fact_player_summary_ltd

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'bluenose'

# COMMAND ----------

def bluenose_custom_ltd_1 (micro_batch_df, batch_id, spark):
    full_game_df = (
        micro_batch_df
        .where(expr("coalesce(extra_info_5, 'demo') = 'full game'"))
        .groupBy('player_id', 'platform', 'service')
        .agg(
            expr("min(received_on) as first_seen"),
            expr("max(received_on) as last_seen")
        )
        .select(
            'player_id',
            'platform',
            'service',
            expr("'full game' as game_type"),
            'first_seen',
            'last_seen'
        )
    )
    demo_game_df = (
        micro_batch_df
        .where(expr("coalesce(extra_info_5, 'demo') = 'demo'"))
        .groupBy('player_id', 'platform', 'service')
        .agg(
            expr("min(received_on) as first_seen"),
            expr("max(received_on) as last_seen")
        )
        .select(
            'player_id',
            'platform',
            'service',
            expr("'demo' as game_type"),
            'first_seen',
            'last_seen'
        )
    )

    # Full Outer Join Full Game vs Demo 
    game_type_df = (
        full_game_df.alias('fg')
        .join(
            demo_game_df.alias('dm'),
            expr("""
                fg.player_id = dm.player_id AND
                fg.platform = dm.platform AND
                fg.service = dm.service
            """),
            how='full_outer'
        )
        .select(
            expr("coalesce(fg.player_id, dm.player_id) as player_id"),
            expr("coalesce(fg.platform, dm.platform) as platform"),
            expr("coalesce(fg.service, dm.service) as service"),
            expr("-1::string as config_1"),
            expr("""
                CASE
                    WHEN fg.game_type IS NOT NULL AND dm.game_type IS NOT NULL AND dm.first_seen < fg.first_seen THEN 'converted'
                    WHEN fg.game_type IS NOT NULL THEN 'full game'
                    ELSE 'demo'
                END AS ltd_string_1
            """),
            expr("fg.first_seen as ltd_ts_1"),
            expr("dm.first_seen as ltd_ts_2"),
            expr("fg.last_seen as ltd_ts_3"),
            expr("dm.last_seen as ltd_ts_4")
            )
        )

    return game_type_df

# COMMAND ----------

def bluenose_custom_ltd_2 (micro_batch_df, batch_id, spark):
    sso_player_mapping = (
                        spark.read
                        .table(f"reference{environment}.sso_mapping.dna_obfuscated_id_mapping")
                        .where(expr("title = 'PGA Tour 2K25'"))
                        .select('unobfuscated_platform_id')
                     )


    fullgame_missing_players = (
                    spark.read
                    .table(f"bluenose{environment}.managed_view.fact_player_summary_ltd")
                    .where(expr("full_game_first_seen is not null and is_linked = False"))
                    .select('player_id')
                 )

    player_mapping = sso_player_mapping.union(fullgame_missing_players)

    linked_df = (
        micro_batch_df.alias('p')
        .join(
            player_mapping.alias('pm'),
            expr("""p.player_id = pm.unobfuscated_platform_id"""),
            how='left')
        .select(
            'player_id',
            'platform',
            'service',
            expr("-1::string as config_1"),
            expr('case when unobfuscated_platform_id is not null then True else False end as ltd_bool_1')
        ).distinct()
    )

    return linked_df

# COMMAND ----------

local_defaults = { 
                   'additional_filter' : """((source_table in ( 'loginevent' ) and EXTRA_INFO_5 = 'full game' and extra_info_1 = 'True' )
                                              or
                                            (source_table in ( 'roundstatus' ) and EXTRA_INFO_5 = 'full game')
                                              or 
                                           ( source_table in ('loginevent', 'roundstatus') and EXTRA_INFO_5 = 'demo' ) )""",
                   'ltd_string_2' : 'first_value(country_code,True) OVER (PARTITION BY player_id, platform, service ORDER BY received_on)',
                   'ltd_string_3' : 'last_value(country_code,True) OVER (PARTITION BY player_id, platform, service ORDER BY received_on)',
                   'custom_attribute_functions' : [bluenose_custom_ltd_1, bluenose_custom_ltd_2],
                   'merge_update_conditions' :[
                                                    { 
                                                     'condition' : """greatest(target.last_seen,target.ltd_ts_3,target.ltd_ts_4) < greatest(source.last_seen,source.ltd_ts_3,source.ltd_ts_4)
                                                                      or least(target.first_seen,target.ltd_ts_1,target.ltd_ts_2) > least(source.first_seen,source.ltd_ts_1,source.ltd_ts_2)
                                                                      or target.ltd_string_1 != source.ltd_string_1
                                                                      or target.ltd_string_2 != source.ltd_string_2
                                                                      or target.ltd_string_3 != source.ltd_string_3
                                                                      or target.ltd_bool_1 != source.ltd_bool_1""",
                                                     'set_fields' : {
                                                                        'first_seen' : 'least(target.first_seen,source.first_seen)',
                                                                        'last_seen' : 'greatest(target.last_seen,source.last_seen)',
                                                                        'first_seen_lang':"""case when target.first_seen > source.first_seen then ifnull(source.first_seen_lang,target.first_seen_lang)
                                                                                                   else ifnull(target.first_seen_lang,source.first_seen_lang)
                                                                                            end""",
                                                                        'last_seen_lang': 'ifnull(source.last_seen_lang,target.last_seen_lang)',
                                                                        'ltd_string_1' : """case when target.ltd_string_1 is null then source.ltd_string_1
                                                                                                 when target.ltd_string_1 ='demo' and  source.ltd_string_1 = 'full game' and least(target.ltd_ts_2,source.ltd_ts_2) < least(target.ltd_ts_1,source.ltd_ts_1) then 'converted'
                                                                                                 when target.ltd_string_1 = 'converted' and least(target.ltd_ts_2,source.ltd_ts_2) > least(target.ltd_ts_1,source.ltd_ts_1) then 'full game' 
                                                                                                 when source.ltd_string_1 = 'converted' and least(target.ltd_ts_2,source.ltd_ts_2) < least(target.ltd_ts_1,source.ltd_ts_1) then 'converted'
                                                                                                 else target.ltd_string_1 
                                                                                            end""",
                                                                        'ltd_string_2' :"""case when target.first_seen > source.first_seen then ifnull(source.ltd_string_2,target.ltd_string_2)
                                                                                                else ifnull(target.ltd_string_2,source.ltd_string_2)
                                                                                            end""",
                                                                        'ltd_string_3' :'ifnull(source.ltd_string_3,target.ltd_string_3)',
                                                                        'ltd_ts_1' : 'least(target.ltd_ts_1,source.ltd_ts_1)',
                                                                        'ltd_ts_2' : 'least(target.ltd_ts_2,source.ltd_ts_2)',
                                                                        'ltd_ts_3' : 'greatest(target.ltd_ts_3,source.ltd_ts_3)',
                                                                        'ltd_ts_4' : 'greatest(target.ltd_ts_4,source.ltd_ts_4)',
                                                                        'ltd_bool_1': 'ifnull(source.ltd_bool_1,target.ltd_bool_1)',
                                                                        'dw_update_ts': 'source.dw_update_ts'
                                                                    }
                                                    }
                                               ]
                }

view_mapping = { 'ltd_ts_1' : 'ltd_ts_1 as full_game_first_seen',
                 'ltd_ts_2' : 'ltd_ts_2 as demo_first_seen',
                 'ltd_ts_3' : 'ltd_ts_3 as full_game_last_seen',
                 'ltd_ts_4' : 'ltd_ts_4 as demo_last_seen',
                 'ltd_string_2' : 'ltd_string_2 as first_seen_country_code',
                 'ltd_string_3' : 'ltd_string_2 as last_seen_country_code',
                 'ltd_string_1' : 'ltd_string_1 as player_type',
                 'ltd_bool_1' : 'case when ltd_ts_1 is not null then True else ltd_bool_1 end as is_linked'

}

stream_fact_player_summary_ltd(database,local_defaults,view_mapping,"batch")
