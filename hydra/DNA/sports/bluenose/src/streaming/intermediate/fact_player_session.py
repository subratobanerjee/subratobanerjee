# Databricks notebook source
# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../gibraltar/intermediate/fact_player_session

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'bluenose'
data_source ='dna'
title = 'PGA Tour 2K25'

# COMMAND ----------

global_settings = {
    "watermark_duration": "30 minutes",
    "session_window_duration": "10 minutes"
    }


source_tables = [
    {
        "catalog": "bluenose",
        "schema": "raw",
        "table": "applicationsession",
        "column_mapping": {
            "application_session_id": "applicationsessionid",
            "player_id": "playerPublicId",
            "received_on": "receivedOn",
            # "filter": "applicationsessionid is not null",
            "filter":"playerPublicId is not null and playerPublicId != 'anonymous' AND buildenvironment = 'RELEASE'",
            "country_code":"countryCode"
        }
    },
    {
        "catalog": "bluenose",
        "schema": "raw",
        "table": "roundstatus",
        "column_mapping": {
            "application_session_id": "applicationsessionid",
            "country_code":"countryCode",
            "player_id": "playerPublicId",
            "filter":"playerPublicId is not null and playerPublicId != 'anonymous' AND buildenvironment = 'RELEASE'",
        }
    }
]

table_columns = {
    "session_type": "CASE WHEN APPGROUPID IN ('d3963fb6e4f54a658e1a14846f0c9a8c', '317c0552032c4804bb10d81b89f4c37e') THEN 'full game' WHEN APPGROUPID = '886bcab8848046d0bd89f5f1ce3b057b' THEN 'demo' END",
    "local_multiplayer_instances": "COUNT(DISTINCT CASE WHEN MULTIPLAYERTYPE = 'Local' THEN ROUNDINSTANCEID END)",
    "online_multiplayer_instances": "COUNT(DISTINCT CASE WHEN  MULTIPLAYERTYPE IN ('CasualMatchmaking', 'CasualPrivateMatch', 'SocietyPrivateMatch', 'RankedMatchmaking') THEN ROUNDINSTANCEID END)",
    "solo_instances": "COUNT(DISTINCT CASE WHEN MULTIPLAYERTYPE = 'Solo' THEN ROUNDINSTANCEID END)",
    "agg_1": "COUNT(CASE WHEN ROUNDSTATUS = 'HoleComplete'  THEN TRUE END)",
    "agg_2": "COUNT(DISTINCT CASE WHEN ROUNDSTATUS = 'RoundComplete' THEN ROUNDINSTANCEID END)",
    **{f"agg_{i}": "Null" for i in range(3, 6)}
}

stream_player_session(source_tables, table_columns,database,title,global_settings,"batch")