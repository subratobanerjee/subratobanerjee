# Databricks notebook source
# MAGIC %run ../../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../../gibraltar/intermediate/fact_player_session

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'wwe2k25'
data_source ='dna'
title = 'WWE 2K25'

global_settings = {
    "watermark_duration": "30 minutes",
    "session_window_duration": "10 minutes"
}

source_tables = [
    {
        "catalog": "wwe2k25",
        "schema": "raw",
        "table": "sessionStarted",
        "column_mapping": {
            "application_session_id": "eventdata_sessionid",
            "player_id": "playerPublicId",
            "app_public_id": "appPublicId",
            "received_on": "receivedOn",
            "country_code": "countryCode",
            "filter": "playerPublicId is not null and playerPublicId != 'anonymous' AND eventdata_sessionid is not null"
        }
    },
    {
        "catalog": "wwe2k25",
        "schema": "raw",
        "table": "sessionEnded",
        "column_mapping": {
            "application_session_id": "eventdata_sessionid",
            "player_id": "playerPublicId",
            "app_public_id": "appPublicId",
            "received_on": "receivedOn",
            "country_code": "countryCode",
            "filter": "playerPublicId is not null and playerPublicId != 'anonymous' AND eventdata_sessionid is not null"
        }
    },
    {
        "catalog": "wwe2k25",
        "schema": "raw",
        "table": "sessionFlow",
        "column_mapping": {
            "application_session_id": "eventdata_sessionid",
            "player_id": "playerPublicId",
            "app_public_id": "appPublicId",
            "received_on": "receivedOn",
            "country_code": "countryCode",
            "filter": "playerPublicId is not null and playerPublicId != 'anonymous' AND eventdata_sessionid is not null"
        }
    }
]

table_columns = {
    "session_type": "Null",
    "local_multiplayer_instances": "Null",
    "online_multiplayer_instances": "Null",
    "solo_instances": "Null",
    **{f"agg_{i}": "Null" for i in range(1, 6)}
}

stream_player_session(source_tables, table_columns, database, title, global_settings,"batch")