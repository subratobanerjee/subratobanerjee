# Databricks notebook source
# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../gibraltar/intermediate/gbx_fact_player_session

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'oak2'
data_source ='gearbox'
title = 'Borderlands 4'

# COMMAND ----------

source_tables = [
    {
        "catalog": "oak2",
        "schema": "raw",
        "table": "oaktelemetry_game_session",
        "column_mapping": {
            "application_session_id": "execution_guid",
            "player_platform_id": "player_platform_id",
            "player_id": "coalesce(player_id,player_platform_id)",
            "received_on": "maw_time_received",
            "platform":"platform_string",
            "filter": "player_platform_id is not null and player_platform_id not ilike '%Null%' and buildconfig_string = 'Shipping Game'"
        }
    },
    {
        "catalog": "oak2",
        "schema": "raw",
        "table": "oaktelemetry_context_data",
        "column_mapping": {
            "application_session_id": "execution_guid",
            "player_platform_id": "player_primary_id_platformid",
            "player_id": "coalesce(player_primary_id_string,player_primary_id_platformid)",
            "received_on": "maw_time_received",
            "platform":"platform_string",
            "filter": "player_primary_id_platformid is not null and player_primary_id_platformid not ilike '%Null%' and buildconfig_string = 'Shipping Game'"
        }
    }
]

table_columns = {
    "session_type": "game",
    "local_multiplayer_instances": "(CASE WHEN max(session_players) > 1 and last(player_connection) = 'local' then max(session_players) else 0 end)",
    "online_multiplayer_instances": "(CASE WHEN max(session_players) > 1 and last(player_connection) = 'online' then max(session_players) else 0 end)",
    "solo_instances": "CASE WHEN max(session_players) = 1 or last(game_session_host_configuration_string) = 'Solo' then 1 else 0 end",
    "agg_1": "(unix_timestamp(max(received_on::timestamp)) - unix_timestamp(min(received_on::timestamp))) / 60.0",
    **{f"agg_{i}": "Null" for i in range(2, 6)},
    "events_filter": None,
    "dna_logins_filter": None
}

global_settings = {
    "watermark_duration": "30 minutes",
    "session_window_duration": "1 day"
    }

stream_player_session(source_tables, table_columns,database,title,global_settings,"batch")
