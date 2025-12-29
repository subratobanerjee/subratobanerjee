# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/table_functions

# COMMAND ----------

from pyspark.sql.functions import expr, coalesce
from delta.tables import DeltaTable

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'nero'
data_source ='dna'
title = "'Mafia: The Old Country'"

# COMMAND ----------

def read_mission_complete(environment):
    mission_complete_df = (
        spark
        .read
        .table(f"nero{environment}.intermediate.fact_player_activity")
        .where(expr(
            "source_table = 'missionstatus' and event_trigger = 'mission_complete'"))
        .select(
            "player_id",
            "platform",
            "service",
            expr(
                "CASE WHEN extra_info_1 = 'Plotline.Main.ch_140_showdown' THEN True else False END AS main_campaign_complete"),
            expr(
                "max(extra_info_2::int) OVER (PARTITION BY player_id ORDER BY received_on DESC) AS last_mission_complete_num"),
            expr(
                "last_value(extra_info_1) OVER (PARTITION BY player_id ORDER BY received_on ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS latest_mission_complete_name"),
            expr(
                "max(extra_info_1) OVER (PARTITION BY player_id ORDER BY received_on DESC) AS last_mission_complete_name")
        )
        .distinct()
    )
    return mission_complete_df

# COMMAND ----------

def read_player_metrics(environment):
    prev_max = max_timestamp(spark, f"{database}{environment}.managed.fact_player_summary_ltd", 'dw_insert_ts')

    current_min = (
        spark.read
        .table(f"nero{environment}.intermediate.fact_player_activity")
        .where(expr("dw_insert_ts::date between current_date - 3 and current_date"))
        .select(expr(f"ifnull(min(received_on::timestamp::date),'1999-01-01')::date as min_date"))
    ).collect()[0]['min_date']

    inc_min_date = min(prev_max,current_min)

    player_metrics_df = (
        spark
        .read
        .table(f"nero{environment}.intermediate.fact_player_activity")
        .where(expr(f"dw_insert_ts::date > to_date('{inc_min_date}') - INTERVAL 3 DAY"))
        .select(
            "player_id",
            "platform",
            "service",
            "source_table",
            "received_on",
            "session_id",
            expr(
                "first_value(received_on::date) IGNORE NULLS OVER (PARTITION BY player_id, platform, service ORDER BY received_on ASC) AS first_seen_date"),
            expr(
                "last_value(case when source_table='applicationstatus' then extra_info_1 else null end) IGNORE NULLS OVER (PARTITION BY player_id, platform, service ORDER BY received_on ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  as sku"),
            expr(
                "first_value(country_code) IGNORE NULLS OVER (PARTITION BY player_id, platform, service ORDER BY received_on ASC) AS first_country_code"),
            expr(
                "last_value(received_on::date) IGNORE NULLS OVER (PARTITION BY player_id, platform, service ORDER BY received_on ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_seen_date"),
            expr(
                "last_value(case when source_table='settingsstatus' then language_setting else null end) IGNORE NULLS OVER (PARTITION BY player_id, platform, service ORDER BY received_on, extra_info_10::int  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_language_game"),
            expr(
                "last_value(case when source_table='settingsstatus' then extra_info_1 else null end) IGNORE NULLS OVER (PARTITION BY player_id, platform, service ORDER BY received_on, extra_info_10::int  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  AS last_language_audio"),
            expr(
                "last_value(case when source_table='missionstatus' then extra_info_1 else null end) IGNORE NULLS OVER (PARTITION BY player_id ORDER BY extra_info_9, extra_info_10::int  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_mission_activity"),
            expr(
                "last_value(case when source_table='missionstatus' then extra_info_5 else null end) IGNORE NULLS OVER (PARTITION BY player_id ORDER BY extra_info_9, extra_info_10::int  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_current_checkpoint_name"),
            expr(
                "last_value(case when source_table='missionstatus' then coalesce(extra_info_7, extra_info_8) else null end) IGNORE NULLS OVER (PARTITION BY player_id ORDER BY extra_info_9, extra_info_10::int  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_narrative_objective")
        )
        .distinct()
    )
    return player_metrics_df

# COMMAND ----------

def read_session_time(environment):

    session_time_df =(
        spark
        .read
        .table(f"nero{environment}.managed.fact_player_session_daily")
        .groupby("player_id", "platform", "service", expr("session_start_date::date as date"))
        .agg(
            expr("sum(session_len_sec/60) as total_minutes_session_played"),
            expr("sum(agg_2/60) as total_minutes_background"),
            # expr("count(DISTINCT date) AS total_active_days"),
            # expr("sum(session_count) as total_sessions")
        )
    )
    
    return session_time_df

# COMMAND ----------

def read_mission_time(environment):

    mission_time_df =(
        spark
        .read
        .table(f"nero{environment}.managed.fact_player_mission_daily")
        .groupby("player_id", "platform", "service", "date")
        .agg(
            expr("sum(mission_duration) as total_minutes_mission"),
            expr("sum(net_mission_duration) as total_minutes_net_mission"),
            expr('''
            sum(
            case
            when mission_name ilike '%Main.ch_%' then mission_duration
            else 0
            end
            ) as total_minutes_played_story
            ''')
        )
    )
    return  mission_time_df

# COMMAND ----------

def read_mode_session(environment):

    mode_session_df =(
        spark
        .read
        .table(f"nero{environment}.raw.modesessionstatus")
        .where(expr("gameplay_type ilike '%FreeRide%'"))
        .groupby(expr("playerpublicid as player_id"), expr("receivedon::timestamp::date as received_on"))
        .agg(
            expr("timediff(second, min(receivedon::timestamp), max(receivedon::timestamp))/ 60 as total_minutes_played_freeride")
        )
    )
    return  mode_session_df

# COMMAND ----------

def extract(environment):
    mission_complete_df = read_mission_complete(environment).alias("mc")
    player_metrics_df = read_player_metrics(environment).alias("fpa")
    session_time_df = read_session_time(environment).alias("st")
    mission_time_df= read_mission_time(environment).alias("mt")
    mode_session_df = read_mode_session(environment).alias("ms")
    return mission_complete_df, player_metrics_df,session_time_df,mission_time_df,mode_session_df

# COMMAND ----------

def transform(mission_complete_df, player_metrics_df, session_time_df, mission_time_df, mode_session_df):
    total_time_transformed_df= (
        session_time_df.alias("st")
        .join(mission_time_df.alias("mt"), on=expr("st.player_id = mt.player_id and st.platform = mt.platform and st.date = mt.date and st.service = mt.service"), how="left")
        .join(mode_session_df.alias("ms"), on=expr("st.player_id = ms.player_id and st.date = ms.received_on"), how="left")
        .groupBy("st.player_id","st.platform","st.service")
        .agg(
            expr("sum(total_minutes_session_played) as total_minutes_session"),
            expr("sum(total_minutes_mission) as total_minutes_mission"),
            expr("sum(total_minutes_net_mission) as total_minutes_net_mission"),
            expr("sum(total_minutes_background) as total_minutes_background"),
            expr("sum(total_minutes_played_story) as total_minutes_played_story"),
            expr("sum(total_minutes_played_freeride) as total_minutes_played_freeride")
        )
        .alias("tt")
    )

    final_transform_df = (
        player_metrics_df.alias("fpa")
        .join(mission_complete_df.alias("mc"), on=expr("fpa.player_id = mc.player_id AND fpa.platform = mc.platform AND fpa.service = mc.service"), how="left")
        .join(total_time_transformed_df.alias("tt"), on=expr("fpa.player_id = tt.player_id AND fpa.platform = tt.platform AND fpa.service = tt.service"), how="left")
        .groupBy("fpa.player_id", "fpa.platform", "fpa.service")
        .agg(
            expr("min(first_seen_date) as install_date"),
            expr("max(sku) as sku"),
            expr("first_value(first_country_code) as first_country_code"),
            expr("max(last_seen_date) as last_seen_date"),
            expr("last_value(last_language_game) as last_language_game"),
            expr("last_value(last_language_audio) as last_language_audio"),
            expr("max(COALESCE(main_campaign_complete, False)) AS main_campaign_complete"),
            expr("max(COALESCE(last_mission_complete_num, 0)) AS last_mission_complete_num"),
            expr("max(last_mission_complete_name) AS last_mission_complete_name"),
            expr("max(latest_mission_complete_name) as latest_mission_complete_name"),
            expr("last_value(last_mission_activity) as last_mission_activity"),
            expr("last_value(last_current_checkpoint_name)  as last_checkpoint_name"),
            expr("last_value(last_narrative_objective)  as last_narrative_objective"),
            expr("count(DISTINCT CASE WHEN source_table = 'applicationstatus' THEN received_on::date END) AS total_active_days"),
            expr("count(DISTINCT CASE WHEN source_table = 'applicationstatus' THEN session_id else null END) AS total_sessions"),
            expr("max(total_minutes_session) as total_session_minutes"),
            expr("max(total_minutes_mission) as total_mission_minutes"),
            expr("max(total_minutes_net_mission) as net_mission_minutes"),
            expr("max(total_minutes_background) as total_background_minutes"),
            expr("max(total_minutes_session) / count(DISTINCT CASE WHEN source_table = 'applicationstatus' THEN session_id else null END) as average_session_minutes"),
            expr("max(total_minutes_session) / count(DISTINCT CASE WHEN source_table = 'applicationstatus' THEN received_on::date END) as average_minutes_day"),
            expr("max(total_minutes_mission) / count(DISTINCT CASE WHEN source_table = 'applicationstatus' THEN session_id else null END) as average_mission_minutes"),
            expr("max(total_minutes_background) / count(DISTINCT CASE WHEN source_table = 'applicationstatus' THEN received_on::date END) as average_background_minutes"),
            expr("max(total_minutes_played_story) as total_story_minutes"),
            expr("max(total_minutes_played_freeride) as total_freeride_minutes")
        )
    )
    
    output_df=(
        final_transform_df
        .select(
            "*",
            expr("CURRENT_TIMESTAMP() as dw_insert_ts"),
            expr("CURRENT_TIMESTAMP() as dw_update_ts"),
            expr("sha2(concat_ws('|',player_id,platform, service), 256) as merge_key")
        )
    )


    return output_df

# COMMAND ----------

def load_fact_player_summary_ltd(spark, output_df, database, environment):
    """
    Load the data into the fact_player_summary_ltd table in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        df (DataFrame): The DataFrame to load into the table.
        database (str): The database to load the table into.
        environment (str): The environment to load the table into.
    """

    # Perform the merge operation into the Delta table
    target_table = DeltaTable.forName(spark, f"{database}{environment}.managed.fact_player_summary_ltd")

    (
        target_table
        .alias("target")
        .merge(
            output_df.alias("src"),
            "target.merge_key = src.merge_key")
        .withSchemaEvolution()
        .whenMatchedUpdate(set ={
            "target.install_date" : "least(src.install_date,target.install_date)",
            "target.sku" : "case when src.sku  is null then target.sku else src.sku end",
            "target.first_country_code" : "case when src.install_date < target.install_date then coalesce(src.first_country_code, target.first_country_code) else coalesce(target.first_country_code, src.first_country_code) end",
            "target.last_seen_date" : "greatest(src.last_seen_date,target.last_seen_date)",
            "target.last_language_game" : "case when src.last_language_game is null then target.last_language_game else src.last_language_game end",
            "target.last_language_audio" : "case when src.last_language_audio is null then target.last_language_audio else src.last_language_audio end",
            "target.main_campaign_complete" : "coalesce(src.main_campaign_complete,target.main_campaign_complete)",
            "target.last_mission_complete_num" : "coalesce(src.last_mission_complete_num,target.last_mission_complete_num)",
            "target.last_mission_complete_name" : "coalesce(src.last_mission_complete_name,target.last_mission_complete_name)",
            "target.latest_mission_complete_name" : "coalesce(src.latest_mission_complete_name,target.latest_mission_complete_name)",
            "target.last_mission_activity" : "case when src.last_mission_activity is null then target.last_mission_activity else src.last_mission_activity end",
            "target.last_checkpoint_name" : "case when src.last_checkpoint_name  is null then target.last_checkpoint_name else src.last_checkpoint_name end",
            "target.last_narrative_objective" : "case when src.last_narrative_objective  is null then target.last_narrative_objective else src.last_narrative_objective end",
            "target.total_active_days" : "src.total_active_days",
            "target.total_sessions" : "src.total_sessions",
            "target.total_session_minutes" : "src.total_session_minutes",
            "target.total_mission_minutes" : "src.total_mission_minutes",
            "target.net_mission_minutes" : "src.net_mission_minutes",
            "target.total_background_minutes" : "src.total_background_minutes",
            "target.average_session_minutes" : "src.average_session_minutes",
            "target.average_minutes_day" : "src.average_minutes_day",
            "target.average_mission_minutes" : "src.average_mission_minutes",
            "target.average_background_minutes" : "src.average_background_minutes",
            "target.total_story_minutes" : "src.total_story_minutes",
            "target.total_freeride_minutes" : "src.total_freeride_minutes",
            "target.dw_update_ts": "current_timestamp()"
        })
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

def create_fact_player_summary_ltd_table(spark, database):

    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.fact_player_summary_ltd (
        player_id string NOT NULL,
        platform string NOT NULL,
        service string NOT NULL,
        install_date date,
        sku string,
        first_country_code string,
        last_seen_date date,
        last_language_game string,
        last_language_audio string,
        main_campaign_complete boolean,
        last_mission_complete_num int,
        last_mission_complete_name string,
        latest_mission_complete_name string,
        last_mission_activity string,
        last_checkpoint_name string,
        last_narrative_objective string,
        total_active_days int,
        total_sessions int,
        total_session_minutes decimal(38,2),
        total_mission_minutes decimal(38,2),
        net_mission_minutes decimal(38,2),
        total_background_minutes decimal(38,2),
        average_session_minutes decimal(38,2),
        average_minutes_day decimal(38,2),
        average_mission_minutes decimal(38,2),
        average_background_minutes decimal(38,2),
        total_story_minutes decimal(38,2),
        total_freeride_minutes decimal(38,2),
        dw_insert_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        dw_update_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        merge_key string
    )
        """

    properties = {
        "delta.feature.allowColumnDefaults": "supported" 
    }

    create_table(spark, sql, properties)
    create_fact_player_summary_ltd_view(spark, database)
    return f"Table fact_player_summary_ltd created"


# COMMAND ----------

def create_fact_player_summary_ltd_view(spark, database):
    """
    Create the fact_player_summary_ltd view in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
        mapping (dict): A dictionary of column mappings.
    """

    sql = f"""
    CREATE VIEW IF NOT EXISTS {database}{environment}.managed_view.fact_player_summary_ltd AS (
        SELECT
        *
        from {database}{environment}.managed.fact_player_summary_ltd
    )
    """
    spark.sql(sql)

# COMMAND ----------

def run_batch():
    spark = create_spark_session(name=f"{database}")
    create_fact_player_summary_ltd_table(spark, database)
    df1, df2, df3, df4, df5 = extract(environment)
    output_df = transform(df1, df2, df3, df4, df5)
    load_fact_player_summary_ltd(spark, output_df, database, environment)

# COMMAND ----------

run_batch()
