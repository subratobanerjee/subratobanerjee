# Databricks notebook source
# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../gibraltar/intermediate/fact_player_activity

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'bluenose'
data_source ='dna'
title = "'PGA Tour 2K25','PGA Tour 2K25: Demo'"

# COMMAND ----------

events = {
    "roundstatus": {"event_trigger": "roundstatus",
                       "session_id": "applicationsessionid",
                       "extra_info_1": "modesessionid",
                       "extra_info_2": "mode",
                       "extra_info_3": "submode",
                       "extra_info_4": "multiplayertype",
                       "extra_info_5": "CASE WHEN APPGROUPID IN ('d3963fb6e4f54a658e1a14846f0c9a8c', '317c0552032c4804bb10d81b89f4c37e') THEN 'full game' WHEN APPGROUPID = '886bcab8848046d0bd89f5f1ce3b057b' THEN 'demo' END",
                       "extra_info_6": "roundinstanceid",
                       "extra_info_8": "totalscore - coursepar",
                       "extra_info_9": "totalgameplaytime",
                       "extra_info_10": "lessonisonboarding",
                       "filter": "buildchangelist <> 0 AND buildenvironment = 'RELEASE'"
                       },

    "accountlinkstatus": {
        "event_trigger": "accountlinkingscreenstatus",
        "session_id": "applicationsessionid",
        "extra_info_1": "modesessionid",
        "extra_info_2": "mode",
        "extra_info_3": "submode",
        "extra_info_5": "CASE WHEN APPGROUPID IN ('d3963fb6e4f54a658e1a14846f0c9a8c', '317c0552032c4804bb10d81b89f4c37e') THEN 'full game' WHEN APPGROUPID = '886bcab8848046d0bd89f5f1ce3b057b' THEN 'demo' END",
        "extra_info_7": "accountlinkingscreenoutcome",
        "filter": "buildchangelist <> 0 AND buildenvironment = 'RELEASE'"

    },
      "gamemode": {
          "event_trigger": "name",
          "session_id": "applicationsessionid",
          "extra_info_1": "modesessionid",
          "extra_info_2": "mode",
          "extra_info_3": "submode",
          "extra_info_5": "CASE WHEN APPGROUPID IN ('d3963fb6e4f54a658e1a14846f0c9a8c', '317c0552032c4804bb10d81b89f4c37e') THEN 'full game' WHEN APPGROUPID = '886bcab8848046d0bd89f5f1ce3b057b' THEN 'demo' END",
          "extra_info_7": "screen",
          "filter": "buildchangelist <> 0 AND buildenvironment = 'RELEASE'"
      },
        "promotion": {
                            "event_trigger": "promotionstatus",
                            "session_id": "applicationsessionid",
                            "extra_info_1": "modesessionid",
                            "extra_info_2": "mode",
                            "extra_info_3": "submode",
                            "extra_info_5": "CASE WHEN APPGROUPID IN ('d3963fb6e4f54a658e1a14846f0c9a8c', '317c0552032c4804bb10d81b89f4c37e') THEN 'full game' WHEN APPGROUPID = '886bcab8848046d0bd89f5f1ce3b057b' THEN 'demo' END",
                            "extra_info_7": "screen",
                            'extra_info_9': "placementid",
                            "extra_info_10": "promoid",
                            "filter": "buildchangelist <> 0 AND buildenvironment = 'RELEASE'"
                     },

        "characterprogression": {
                            "event_trigger": "characterprogressionstatus",
                            "session_id": "applicationsessionid",
                            "extra_info_1": "modesessionid",
                            "extra_info_2": "mode",
                            "extra_info_3": "submode",
                            "extra_info_5": "CASE WHEN APPGROUPID IN ('d3963fb6e4f54a658e1a14846f0c9a8c', '317c0552032c4804bb10d81b89f4c37e') THEN 'full game' WHEN APPGROUPID = '886bcab8848046d0bd89f5f1ce3b057b' THEN 'demo' END",
                            "filter": "characterprogressionstatus = 'CreateMyPlayer' AND buildchangelist <> 0 AND buildenvironment = 'RELEASE'"
                    } ,
      "dna_login": {
                  "extra_info_1": "isParentAccountDOBVerified", 
                  "extra_info_5": "CASE WHEN APPGROUPID IN ('d3963fb6e4f54a658e1a14846f0c9a8c', '317c0552032c4804bb10d81b89f4c37e') THEN 'full game' WHEN APPGROUPID = '886bcab8848046d0bd89f5f1ce3b057b' THEN 'demo' END"
          }
    
    

}

local_defaults =  {
    'event_trigger': 'eventname',
    'build_changelist': 'buildChangelist',
    "dna_logins_filter": 'appGroupId in ("d3963fb6e4f54a658e1a14846f0c9a8c", "317c0552032c4804bb10d81b89f4c37e","886bcab8848046d0bd89f5f1ce3b057b")'

}

stream_player_activity(database,title,events,local_defaults,data_source,"batch")

# COMMAND ----------


# dbutils.fs.rm("dbfs:/tmp/bluenose/intermediate/streaming/run_dev/fact_player_activity", True)
# spark.sql("drop table bluenose_dev.intermediate.fact_player_activity")
