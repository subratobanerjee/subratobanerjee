# Databricks notebook source
# MAGIC %run ../../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../../gibraltar/intermediate/fact_player_activity

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'wwe2k25'
data_source ='dna'
title = "'WWE 2K25'"

# COMMAND ----------

events = {
    "mycareerprogression": {},
    "careerterminate": {},
    "pinminigame": {},
    "objectivecomplete": {},
    # "chardesync": {},
    "achievementstatus": {},
    "generaltransaction": {},
    "mycareercharstatus": {},
    "loadcareer": {},
    "lobbysearch": {},
    "ssoaccountscreen": {},
    "bookingcard": {},
    "nopurchase": {},
    "mycareerstorystatus": {},
    "careerstart": {},
    "pcdevice": {},
    "matchstatus": {},
    "ftuestatus": {},
    "storeinteraction": {},
    "matchmakeend": {},
    "myfactionprogression": {},
    "gmrosterstatus": {},
    "creationupload": {},
    "sessionstarted": {'language_setting': 'clientlanguage'},
    "matchmake": {},
    # "matchdesync": {"event_trigger": "'matchdesync'"},
    "matchrating": {},
    "matchmakestart": {},
    "universeprogress": {},
    "objectiveavailable": {},
    "careermatch": {},
    "clientdevice": {},
    "matchmakecreate": {},
    "universeplay": {},
    # "newplayer": {},
    "lobbyexit": {},
    "modestatus": {},
    "matchmakingstatus": {},
    "matchroster": {},
    "sessionended": {},
    # "matchoption": {},
    "matchfound": {},
    "lobbycreation": {},
    "lobbyjoin": {},
    "usepowercard": {},
    "gmchallengecomplete": {},
    "ssoaccount": {},
    "orion_mysuperstar_status": {},
    "orion_quest_status": {},
    "myfactionchallengestatus": {}

}

local_defaults =  {
    'event_trigger': 'eventname',
    'events_filter': "UPPER(buildtype) = 'FINAL'"
}
def stream_player_activity(database,title,events,defaults,data_source):
    database = database.lower()
    spark = create_spark_session(name=f"{database}")    
    create_fact_player_activity(spark, database)
    checkpoint_location=f"dbfs:/tmp/{database}/intermediate/streaming/run{environment}/fact_player_activity"
    print(f'checkpoint_location : {checkpoint_location}')
    all_activity_df = activities(database,title,events,defaults,data_source,spark)
    all_activity_df = all_activity_df.dropDuplicates(['player_id', 'platform', 'service', 'received_on', 'source_table', 'event_trigger'])
    print('Started writeStream')
    # Writing the stream output to the title level fact_player_activity table 
    (
        all_activity_df
        .writeStream
        .trigger(processingTime="1 minute")
        .option("checkpointLocation", checkpoint_location)
        .toTable(f"{database}{environment}.intermediate.fact_player_activity")
    )
stream_player_activity(database,title,events,local_defaults,data_source)