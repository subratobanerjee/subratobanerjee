# Databricks notebook source
from pyspark.sql.functions import (expr, when, col, explode, lower, lit)
from functools import reduce

# COMMAND ----------

# MAGIC %run ../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../utils/ddl/intermediate/fact_player_activity

# COMMAND ----------

# Global defaults for columns / filter values can be updated with local defaults
extra_info_field_count = 11
global_defaults = {
    'player_id': 'PLAYERPUBLICID',
    'parent_id': 'Null',
    'received_on': 'receivedOn',
    'event_trigger': 'Null',
    'session_id': 'Null',
    'desc': 'Null',
    'country_code': 'COUNTRYCODE',
    'build_changelist': 'Null',
    'language_setting': 'Null',
    **{f"extra_info_{i}": 'Null' for i in range(1, extra_info_field_count)},
    'app_id': 'APPPUBLICID',
    'platform': 'platform',
    'service': 'service',
    'events_filter': None,
    'dna_logins_filter': None
}

# COMMAND ----------

def activities(database, title, events, local_defaults, data_source, spark):
    """
    Will create read Streams to the given events in the game telemetry and the logins from the coretech logins events

    Parameters:
    input
        database: project database without the enviroment
        title: Title value in the dim_title table
        local_defaults (dict): A dictionary of properties that will be updated with global_defaults
        spark (SparkSession): The Spark session for executing the SQL command.
    output
        activities_df (dataframe)
    """
    #Updating the global_defaults values with local defaults
    global_defaults.update(local_defaults)

    activity_df = []

    for event in events.keys():
        print(f'{event} : started ')

        #if logins config is used ignoring it here
        if event == 'dna_login':
            continue
        
        if data_source == 'gearbox':
            # Prepare fields using expressions with defaults
            game_columns = {
                "player_id": expr(f"{events[event].get('player_id', global_defaults['player_id'])}::STRING as player_id"),
                "parent_id": expr(f"{events[event].get('parent_id', global_defaults['parent_id'])}::STRING as parent_id"),
                "platform": expr(f"{events[event].get('platform', global_defaults['platform'])}::STRING as platform"),
                "service": expr(f"{events[event].get('service', global_defaults['service'])}::STRING as service"),
                "received_on": expr(f"{events[event].get('received_on', global_defaults['received_on'])}::TIMESTAMP as received_on"),
                "source_table": expr(f"'{event}' as source_table"),
                "desc": expr(f"COALESCE({events[event].get('desc', global_defaults['desc'])}, 'GAME TELEMETRY') as desc"),
                "event_trigger": expr(f"{events[event].get('event_trigger', global_defaults['event_trigger'])}::STRING as event_trigger"),
                "session_id": expr(f"{events[event].get('session_id', global_defaults['session_id'])}::STRING as session_id"),
                "country_code": expr(f"{events[event].get('country_code', global_defaults['country_code'])}::STRING as country_code"),
                "build_changelist": expr(f"{events[event].get('build_changelist', global_defaults['build_changelist'])}::STRING as build_changelist"),
                "language_setting": expr(f"{events[event].get('language_setting', global_defaults['language_setting'])}::STRING as language_setting"),
                **{f"extra_info_{i}": expr(f"{events[event].get(f'extra_info_{i}', global_defaults[f'extra_info_{i}'])}::STRING as extra_info_{i}") for i in range(1, extra_info_field_count)}
            }
        else:
            # Prepare fields using expressions with defaults
            game_columns = {
                "player_id": expr(f"{events[event].get('player_id', global_defaults['player_id'])}::STRING as player_id"),
                "parent_id": expr(f"{events[event].get('parent_id', global_defaults['parent_id'])}::STRING as parent_id"),
                "app_id": expr(f"{events[event].get('app_id', global_defaults['app_id'])}::STRING as app_id"),
                "received_on": expr(f"{events[event].get('received_on', global_defaults['received_on'])}::TIMESTAMP as received_on"),
                "source_table": expr(f"'{event}' as source_table"),
                "desc": expr(f"COALESCE({events[event].get('desc', global_defaults['desc'])}, 'GAME TELEMETRY') as desc"),
                "event_trigger": expr(f"{events[event].get('event_trigger', global_defaults['event_trigger'])}::STRING as event_trigger"),
                "session_id": expr(f"{events[event].get('session_id', global_defaults['session_id'])}::STRING as session_id"),
                "country_code": expr(f"{events[event].get('country_code', global_defaults['country_code'])}::STRING as country_code"),
                "build_changelist": expr(f"{events[event].get('build_changelist', global_defaults['build_changelist'])}::STRING as build_changelist"),
                "language_setting": expr(f"{events[event].get('language_setting', global_defaults['language_setting'])}::STRING as language_setting"),
                **{f"extra_info_{i}": expr(f"{events[event].get(f'extra_info_{i}', global_defaults[f'extra_info_{i}'])}::STRING as extra_info_{i}") for i in range(1, extra_info_field_count)}
            }

        # Get the event filter and global filter
        event_filter = events[event].get('filter')
        global_filter = global_defaults['events_filter']

        combined_filter = None
        # Determine the combined filter
        if event_filter and events[event].get('exclude_global_filter', 'n') == 'n':
            combined_filter = expr(f"({event_filter}) AND ({global_filter})") if global_filter else expr(event_filter)
        elif event_filter:
            combined_filter = expr(event_filter)
        elif global_filter and events[event].get('exclude_global_filter', 'n') == 'n':
            combined_filter = expr(global_filter)
        else:
            combined_filter = expr("1 = 1")  # No filters applied
            
        # Read the DataFrame and chain transformations using select
        df = (
                spark
                .readStream
                .option("skipChangeCommits", "true")
                .table(f"{database}{environment}.raw.{event}")
                .select(*game_columns.values()) 
                .where(combined_filter)
        )

        print(f'{event} :readStream Done ')
        activity_df.append(df)

    # Reading the dim_title to get the mapping platform and servic values. Values need to be inserted manually 
    title_df= (
                spark
                .read
                .table(f"reference{environment}.title.dim_title")
                .alias('title')
                .where(expr(f"""title in ({title}) """))
                )

    if data_source=='dna':
        # Getting DNA Logins data
        print('Logins :started ')

        app_ids = ",".join(f"'{row.APP_ID}'" for row in title_df.select('APP_ID').collect())

        login_columns =  {
                "player_id": expr("accountId as player_id"),
                "parent_id": expr("parentAccountId as parent_id"),
                "app_id": expr("CAST(APPPUBLICID AS STRING) as app_id"),
                "received_on": expr("occurredOn::timestamp as received_on"),
                "source_table": expr("'loginevent' as source_table"),
                "desc": expr("'SSO2 Logins' as desc"),
                "event_trigger": expr("'login' as event_trigger"),
                "session_id": expr("sessionId as session_id"),
                "country_code": expr("ifnull(from_json(geoIp, 'countryCode STRING').countryCode, geoip_countrycode) AS countryCode"),
                "build_changelist": expr("NULL::String as build_changelist"),
                "language_setting": expr("language as language_setting"),
                **{f"extra_info_{i}": expr(f"{events.get('dna_login', {}).get(f'extra_info_{i}', 'Null')}::STRING as extra_info_{i} ") for i in range(1, extra_info_field_count)}
        }
                    
        dna_logins_combined_filter = expr(f"""(APPPUBLICID in ({app_ids})) AND ({global_defaults['dna_logins_filter']})""") if global_defaults['dna_logins_filter'] else expr(f"""(APPPUBLICID in ({app_ids}))""")

        logins_df = (
                    spark
                    .readStream
                    .table(f"coretech{environment}.sso.loginevent")
                    .select(*login_columns.values())
                    .where(dna_logins_combined_filter)
        )
        
        activity_df.append(logins_df)
        print('DNA Logins :readStream Done ')

        # Combine all DataFrames into a single DataFrame
        activity = reduce(lambda df1, df2: df1.union(df2), activity_df)

        columns = {
            "player_id": expr("ifnull(player_id, RECEIVED_ON::date::string || '-NULL-Player') AS player_id"),
            "parent_id": expr("ifnull(parent_id, 'Unknown') as parent_id"),
            "platform": expr("ifnull(display_platform, 'Unknown') as platform"),
            "service": expr("ifnull(display_service, 'Unknown') as service"),
            "received_on": "received_on",
            "source_table": "source_table",
            "desc": "desc",
            "event_trigger": "event_trigger",
            "session_id" : "session_id",
            "country_code": "country_code",
            "build_changelist": "build_changelist",
            "language_setting": "language_setting",
            **{f"extra_info_{i}": expr(f"extra_info_{i}") for i in range(1, extra_info_field_count)},
            "dw_insert_date": expr("current_date() as dw_insert_date"),
            "dw_insert_ts": expr("current_timestamp() as dw_insert_ts"),
            "dw_update_ts": expr("current_timestamp() as dw_update_ts")
        }
    
        activities_df = (
            activity.alias('activity')
            .join(title_df, expr("activity.app_id = title.app_id"), 'left')
            .select(*columns.values())
        )
    
    elif data_source=='gearbox':
        # Getting Gearbox Logins data
        print('Logins :started ')

        # platforms = ",".join(f"'{row.platform}'" for row in title_df.select('DISPLAY_PLATFORM').collect())

        login_columns =  {
                "player_id": expr("player_id_platformid as player_id"),
                "parent_id": expr("Null::string as parent_id"),
                "platform": expr("ifnull(iff(display_platform = 'WindowsEpic', 'Windows', display_platform), 'Unknown') as platform"),
                "service": expr("display_service as service"),
                "received_on": expr("maw_time_received::timestamp as received_on"),
                "source_table": expr("'loginevent' as source_table"),
                "desc": expr("'Archway KPI Logins' as desc"),
                "event_trigger": expr("'login' as event_trigger"),
                "session_id": expr("Null::string as session_id"),
                "country_code": expr("(case when player_address_country_string is null then 'ZZ' when player_address_country_string = '--' then 'ZZ' when player_address_country_string = 'CA-QC' then 'ZZ' else player_address_country_string end) as countryCode"),
                "build_changelist": expr("build_tag_string::string as build_changelist"),
                "language_setting": expr("Null::string as language_setting"),
                "extra_info_1": expr("player_id_platformid as extra_info_1"),
                **{f"extra_info_{i}": expr(f"{events.get('dna_login', {}).get(f'extra_info_{i}', 'Null')}::STRING as extra_info_{i} ") for i in range(2, extra_info_field_count)}
        }

        # Read gbx logins
        logins_df = (
            spark.readStream
            .table(f"gbx_prod.raw.logins")
            .where(expr(f"game_title_string = '{database}'"))
        )

        final_logins_df = (
            logins_df.alias('activity')
            .join(title_df.alias("title"),
                    expr("(case when lower(activity.hardware_string) = 'ps5pro' then 'ps5' when lower(activity.hardware_string) = 'xsx' then 'xbsx' when lower(activity.hardware_string) = 'switch2' then 'nsw2' else lower(activity.hardware_string) end) = lower(title.platform) and (case when lower(activity.service_string) = 'nintendo' then 'nso' else lower(activity.service_string) end) = lower(title.service)"),
                    how="left"
                )
            .select(*login_columns.values()).distinct()
        )

        activity_df.append(final_logins_df)
        print('Gearbox Logins :readStream Done ')
        
        # Combine all DataFrames into a single DataFrame
        activity = reduce(lambda df1, df2: df1.union(df2), activity_df)
        columns = {
            "player_id": expr("ifnull(player_id, RECEIVED_ON::date::string || '-NULL-Player') AS player_id"),
            "parent_id": expr("ifnull(parent_id, 'Unknown') as parent_id"),
            "platform": "platform",
            "service": "service",
            "received_on": "received_on",
            "source_table": "source_table",
            "desc": "desc",
            "event_trigger": "event_trigger",
            "session_id" : "session_id",
            "country_code": expr("ifnull(country_code, 'ZZ') as country_code"),
            "build_changelist": "build_changelist",
            "language_setting": "language_setting",
            **{f"extra_info_{i}": expr(f"extra_info_{i}") for i in range(1, extra_info_field_count)},
            "dw_insert_date": expr("current_date() as dw_insert_date"),
            "dw_insert_ts": expr("current_timestamp() as dw_insert_ts"),
            "dw_update_ts": expr("current_timestamp() as dw_update_ts")
        }
    
        activities_df = (
            activity.alias('activity')
            .select(*columns.values()).distinct()
        )

    return activities_df

# COMMAND ----------

def stream_player_activity(database,title,events,defaults,data_source,trigger="streaming"):
    database = database.lower()

    #setting the spark session
    spark = create_spark_session(name=f"{database}")
    
    #Setting the checkpoint_location from the fucntion output
    checkpoint_location = create_fact_player_activity(spark, database)
    print(f'checkpoint_location : {checkpoint_location}')
    
    # Reading the data using readStream
    all_activity_df = activities(database,title,events,defaults,data_source,spark)

    print('Started writeStream')

    # Writing the stream output to the title level fact_player_activity table 
    if trigger == "streaming":
        (
            all_activity_df
            .writeStream
            .trigger(processingTime="1 minute")
            .option("checkpointLocation", checkpoint_location)
            .toTable(f"{database}{environment}.intermediate.fact_player_activity")
        )
    else:
        (
            all_activity_df
            .writeStream
            .trigger(availableNow=True)
            .option("checkpointLocation", checkpoint_location)
            .toTable(f"{database}{environment}.intermediate.fact_player_activity")
        )
