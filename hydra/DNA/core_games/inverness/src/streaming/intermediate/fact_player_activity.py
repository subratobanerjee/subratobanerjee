# Databricks notebook source
# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../gibraltar/intermediate/fact_player_activity

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'inverness'
data_source ='dna'
title = "'Civilization VII'"

# COMMAND ----------

events = {
    "campaignstatus": {"event_trigger": "campaignStatusEvent",
                       "session_id": "applicationSessionInstanceId",
                       "extra_info_1": "age",
                       "extra_info_2": "civilizationSelected",
                       "extra_info_3": "leader",
                       "extra_info_4": "turnNumber",
                       "extra_info_5": "campaignInstanceId",
                       "extra_info_6": "playerSlot",
                       "extra_info_7": "playerType",
                       "build_changelist":"'N/A'",
                       "filter": "UPPER(playerType) = 'HUMAN'"
                       },
    "applicationsessionstatus": {"event_trigger": "applicationSessionEvent",
                                 "session_id": "applicationSessionInstanceId",
                                 "language_setting":"locale",
                                 "extra_info_8": "buildenv",
                                 "extra_info_9": "buildversion",
                                 "extra_info_10": "playineditor",
                                 "filter": "buildchangelist <> 0"
                                 }


}

local_defaults =  {
    'event_trigger': 'eventname',
    'build_changelist': 'buildChangelist'
}

stream_player_activity(database,title,events,local_defaults,data_source,"batch")

