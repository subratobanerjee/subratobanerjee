# Databricks notebook source
# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../gibraltar/intermediate/fact_player_session

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'nero'
data_source ='dna'
title = 'Mafia: The Old Country'

# COMMAND ----------

source_tables = [
    {"catalog": "nero",
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
    "session_type": "Unknown",
    "local_multiplayer_instances": "Null",
    "online_multiplayer_instances": "Null",
    "solo_instances": "Null",
    **{f"agg_{i}": "Null" for i in range(1, 6)}
}

global_settings = {
    "watermark_duration": "30 minutes",
    "session_window_duration": "1 day"
    }

stream_player_session(source_tables, table_columns,database,title,global_settings,"batch")
