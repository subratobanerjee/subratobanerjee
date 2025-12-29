# Databricks notebook source
# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../gibraltar/intermediate/fact_player_session

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'inverness'
data_source ='dna'
title = 'Civilization VII'

# COMMAND ----------

source_tables = [
    {
        "catalog": "inverness",
        "schema": "raw",
        "table": "campaignsetupstatus",
        "column_mapping": {
            "application_session_id": "applicationsessioninstanceid",
            "player_id": "playerPublicId",
            "received_on": "receivedOn",
            "country_code":"countryCode",
            "filter": "playerPublicId is not null and playerPublicId != 'anonymous' and playerPublicId NOT ILIKE '%Null%'"
        }
    },
    {"catalog": "inverness",
        "schema": "intermediate",
        "table": "fact_player_activity",
        "column_mapping": {
            "application_session_id": "session_id",
            "player_id": "player_id",
            "filter": "source_table <> 'loginevent' and player_id is not null and player_id != 'anonymous' and player_id NOT ILIKE '%Null%'",
            "received_on": "received_on",
            "country_code":"country_code"
        }
    }
]

table_columns = {
    "session_type": "application",
    "local_multiplayer_instances": "Null",
    "online_multiplayer_instances": "COUNT(DISTINCT CASE WHEN GAMETYPE = 'INTERNET' AND CAMPAIGNSETUPEVENT = 'Complete' THEN CAMPAIGNSETUPINSTANCEID END)",
    "solo_instances": "COUNT(DISTINCT CASE WHEN GAMETYPE = 'SINGLEPLAYER' AND CAMPAIGNSETUPEVENT = 'Complete' THEN CAMPAIGNSETUPINSTANCEID END)",
    "agg_1": "COUNT(CASE WHEN EVENT_TRIGGER = 'CampaignTurnEnd' THEN TRUE END)",
    "agg_2": "COUNT(CASE WHEN EVENT_TRIGGER = 'AppSuspend' THEN TRUE END)",
    **{f"agg_{i}": "Null" for i in range(3, 6)},
    "events_filter": None,
    "dna_logins_filter": None
}

global_settings = {
    "watermark_duration": "30 minutes",
    "session_window_duration": "1 day"
    }

stream_player_session(source_tables, table_columns,database,title,global_settings,"batch")
