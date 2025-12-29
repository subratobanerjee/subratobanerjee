# Databricks notebook source
# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../gibraltar/intermediate/fact_player_activity

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'oak2'
data_source ='gearbox'
title = "'Borderlands 4'"

# COMMAND ----------

events = {
    "oaktelemetry_context_data": {"event_trigger": "event_name_string",
                                  "player_id": "coalesce(player_primary_id_string,player_primary_id_platformid,'Unknown')",
                                  "received_on": "maw_time_received",
                                  "country_code": "Null",
                                  "platform": "platform_string", 
                                  "service": "Null",
                                  "session_id": "execution_guid",
                                  "extra_info_1": "player_primary_id_platformid",
                                  "filter": "player_primary_id_platformid is not null and player_primary_id_platformid not ilike '%Null%' and buildconfig_string = 'Shipping Game'"
                                }

}

local_defaults =  {
    'event_trigger': 'event_name_string'
}

stream_player_activity(database,title,events,local_defaults,data_source,"batch")


# COMMAND ----------

# dbutils.fs.rm("dbfs:/tmp/oak2/intermediate/streaming/run_dev/fact_player_activity", True)
# spark.sql("drop table oak2_dev.intermediate.fact_player_activity")
