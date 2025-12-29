# Databricks notebook source
# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../gibraltar/intermediate/fact_player_activity

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'nero'
data_source ='dna'
title = "'Mafia: The Old Country'"

# COMMAND ----------

events = {
    "missionstatus": {"event_trigger": "mission_status",
                       "session_id": "sessionid",
                       "extra_info_1": "mission_name",
                       "extra_info_2": "mission_num",
                       "extra_info_3": "mission_run_id",
                       "extra_info_4": "attempt_counter",
                       "extra_info_5": "current_checkpoint_name",
                       "extra_info_6": "difficulty",
                       "extra_info_7": "narrative_name",
                       "extra_info_8": "objective_name",
                       "extra_info_9": "occurredOn::timestamp",
                       "extra_info_10": "sequenceNumber",
                        "filter": "playerpublicid != 'anonymous' and playerpublicid is not null"
                       },
    "applicationstatus": {"event_trigger": "application_session_status",
                                 "session_id": "sessionid",
                                 "language_setting":"language_game",
                                 "build_changelist": "build_configuration",
                                 "extra_info_1": "application_type",
                                 "extra_info_2": "gameplay_time",
                                 "extra_info_3": "active_dlc",
                                 "extra_info_4": "two_k_account_rewards",
                                 "extra_info_5": "build_version",
                                 "extra_info_7": "application_background_instance_id",
                                 "extra_info_6": "language_audio",
                                 "extra_info_8": "build_date",
                                 "extra_info_9": "occurredOn::timestamp",
                                 "extra_info_10": "sequenceNumber",
                                 "filter": "playerpublicid != 'anonymous' and playerpublicid is not null"
                                 },

    "playerstatus": {"event_trigger": "player_status",
                       "session_id": "sessionid",
                       "extra_info_1": "attempt_counter",
                       "extra_info_2": "play_state",
                       "extra_info_3": "mission_run_id",
                       "extra_info_4": "health",
                       "extra_info_5": "gameplay_type_session_id",
                       "extra_info_6": "sub_gameplay_type_session_id",
                       "extra_info_7": "npcdrag",
                       "extra_info_8": "crouch",
                       "extra_info_9": "occurredOn::timestamp",
                       "extra_info_10": "sequenceNumber",
                        "filter": "playerpublicid != 'anonymous' and playerpublicid is not null"
                       },
    
    "settingsstatus": {
                        "event_trigger" : "settingsstatus",
                        "session_id": "sessionid",
                        "language_setting": "language_text",
                        "extra_info_1": "language_speech",
                        "extra_info_3": "language_subtitles",
                        "extra_info_4": "gameplay_difficulty",
                        "extra_info_9": "occurredOn::timestamp",
                       "extra_info_10": "sequenceNumber",
                        "filter": "playerpublicid != 'anonymous' and playerpublicid is not null"
                    }


}

local_defaults =  {
    'event_trigger': 'eventname',
    'build_changelist' : 'Null'
}

stream_player_activity(database,title,events,local_defaults,data_source,"batch")


# COMMAND ----------

# dbutils.fs.rm("dbfs:/tmp/nero/intermediate/streaming/run_dev/fact_player_activity", True)
# spark.sql("drop table nero_dev.intermediate.fact_player_activity")
