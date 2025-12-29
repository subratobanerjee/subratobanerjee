# Databricks notebook source
# MAGIC %run ../../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../../utils/ddl/managed/fact_player_game_status_daily

# COMMAND ----------

from pyspark.sql.functions import expr

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment', set_environment())
database = 'wwe2k25'
data_source = 'dna'
view_mapping = {
    'extra_grain_1': 'extra_grain_1 as online',
    'agg_game_status_1': 'agg_game_status_1 as match_start_count',
    'agg_game_status_2': 'agg_game_status_2 as match_end_count',
    'agg_game_status_3': 'agg_game_status_3 as match_win_count',
    'agg_game_status_4': 'agg_game_status_4 as match_lost_count',
    'agg_game_status_5': 'agg_game_status_5 as match_quit_count',
    'agg_gp_1': 'agg_gp_1 as match_count',
    'agg_gp_2': 'agg_gp_2 as match_duration',
    'agg_gp_3': 'agg_gp_3 as mode_enter_count',
    'agg_gp_4': 'agg_gp_4 as mode_duration',
    'agg_gp_5': 'agg_gp_5 as submode_duration'
}

# COMMAND ----------

def get_prev_timestamp(environment, database):
    prev_max_ts = max_timestamp(spark, f"{database}{environment}.managed.fact_player_game_status_daily", "date")
    mode_min_received_on = spark.sql(f"select ifnull(min(receivedOn::timestamp::date), '1970-10-01')::date as min_received_on from {database}{environment}.raw.modestatus where insert_ts >= current_date - interval '2' day").collect()[0]['min_received_on']
    match_min_received_on = spark.sql(f"select ifnull(min(receivedOn::timestamp::date), '1970-10-01')::date as min_received_on from {database}{environment}.raw.matchstatus where insert_ts >= current_date - interval '2' day").collect()[0]['min_received_on']
    min_received_on_inc = min(prev_max_ts, mode_min_received_on, match_min_received_on)
    
    return min_received_on_inc

# COMMAND ----------

def read_mode_status_df(environment):
    # Load source and reference dataframes
    min_received_on = get_prev_timestamp(environment, database)
    return (
        spark
        .read
        .option('maxFilesPerTrigger', 10000)
        .table(f"wwe2k25{environment}.raw.modestatus")
        .select(
            expr("playerPublicId as player_id"),
            expr("gamemode as game_mode"),
            expr("submode as sub_mode"),
            "appPublicId",
            "appgroupid",
            "submodestatus",
            "modestatus",
            "submodetimespent",
            "submodesessionid",
            "modesessionid",
            "modetimespent",
            expr("ifnull(countryCode::string, 'ZZ') AS country_code"),
            expr("receivedOn::timestamp as received_on")
        )
        .where(expr(f"UPPER(buildtype) = 'FINAL' and receivedOn::timestamp::date >= to_date('{min_received_on}') - INTERVAL 2 DAY"))
        .distinct()
    )

# COMMAND ----------

def read_match_status_df(environment):
    # Load source and reference dataframes
    min_received_on = get_prev_timestamp(environment, database)
    return (
        spark
        .read
        .option('maxFilesPerTrigger', 10000)
        .table(f"wwe2k25{environment}.raw.matchstatus")
        .select(
            expr("playerPublicId as player_id"),
            expr("gamemode as game_mode"),
            expr("submode as sub_mode"),
            "matchguid",
            "appPublicId",
            "appgroupid",
            "matchstatus",
            "win",
            "lose",
            "reason",
            expr("ifnull(countryCode::string, 'ZZ') AS country_code"),
            expr("receivedOn::timestamp as received_on")
        )
        .where(expr(f"UPPER(buildtype) = 'FINAL' and matchguid != '00000000-0000-0000-0000-000000000000' and receivedOn::timestamp::date >= to_date('{min_received_on}') - INTERVAL 2 DAY"))
        .distinct()
    )

# COMMAND ----------

def read_match_perf_status_df(environment):
    # Load source and reference dataframes
    min_received_on = get_prev_timestamp(environment, database)
    return (
        spark
        .read
        .option('maxFilesPerTrigger', 10000)
        .table(f"wwe2k25{environment}.raw.matchperfstatus")
        .select(
            expr("playerPublicId as player_id"),
            expr("gamemode as game_mode"),
            expr("submode as sub_mode"),
            "matchguid",
            "appPublicId",
            "appgroupid",
            expr("ifnull(countryCode::string, 'ZZ') AS country_code"),
            expr("receivedOn::timestamp as received_on"),
            "play_time"
        )
        .where(expr(f"UPPER(buildtype) = 'FINAL' and matchguid != '00000000-0000-0000-0000-000000000000' and receivedOn::timestamp::date >= to_date('{min_received_on}') - INTERVAL 2 DAY"))
        .distinct()
    )

def read_match_ros_status_df(environment):
    # Load source and reference dataframes
    min_received_on = get_prev_timestamp(environment, database)
    return (
        spark
        .read
        .option('maxFilesPerTrigger', 10000)
        .table(f"wwe2k25{environment}.raw.matchroster")
        .select(
            expr("playerPublicId as player_id"),
            expr("gamemode as game_mode"),
            expr("submode as sub_mode"),
            "matchguid",
            "appPublicId",
            "appgroupid",
            expr("ifnull(countryCode::string, 'ZZ') AS country_code"),
            expr("receivedOn::timestamp as received_on"),
            "slot"
        )
        .where(expr(f"UPPER(buildtype) = 'FINAL' and AI = 0 and matchguid != '00000000-0000-0000-0000-000000000000' and receivedOn::timestamp::date >= to_date('{min_received_on}') - INTERVAL 2 DAY"))
        .distinct()
    )

# COMMAND ----------

def read_title_df(environment):
    return (
        spark
        .read
        .table(f"reference{environment}.title.dim_title")
        .select(
            "title",
            "app_id",
            "app_group_id",
            expr("display_platform as platform"),
            expr("display_service as service")
        )
    )

# COMMAND ----------

def extract(environment):
    # Join the source and reference tables
    mode_df = read_mode_status_df(environment).alias("mode")
    match_df = read_match_status_df(environment).alias("match")
    match_perf_df = read_match_perf_status_df(environment).alias("match_perf")
    match_ros_df = read_match_ros_status_df(environment).alias("match_ros")
    title_df = read_title_df(environment).alias("title")
    joined_df = (
        match_df
        .join(title_df, on=expr("title.app_id = match.appPublicId"),
            how="inner")
        .join(match_perf_df, on=expr("match.appgroupid = match_perf.appgroupid and match.appPublicId = match_perf.appPublicId and match.matchguid = match_perf.matchguid"),
            how="left")
        .join(match_ros_df, on=expr("match.appgroupid = match_ros.appgroupid and match.appPublicId = match_ros.appPublicId and match.matchguid = match_ros.matchguid and (match.win = match_ros.slot or match.lose = match_ros.slot)"),
            how="left")
        .select(
            expr(f"to_date(match.received_on) as date"),
            expr(f"match.player_id as player_id"),
            "platform",
            "service",
            expr(f"match.game_mode as game_mode"),
            expr(f"match.sub_mode as sub_mode"),
            expr(f"match.country_code as country_code"),
            "matchstatus",
            "win",
            "lose",
            "reason",
            expr("match.matchguid as matchguid"),
            "play_time",
            "slot",
            expr("CASE WHEN match.game_mode = 'Online' THEN 'TRUE' WHEN match.sub_mode LIKE 'MFOnline%' THEN 'TRUE' WHEN match.sub_mode = 'OrionMultiplayer' THEN 'TRUE' ELSE 'FALSE' END AS extra_grain_1")
        )
    )

    mode_joined_df = (
        mode_df
        .join(title_df, on=expr("title.app_id = mode.appPublicId"),
            how="inner")
        .select(
            expr(f"to_date(mode.received_on) as date"),
            expr(f"to_timestamp(mode.received_on) as received_on"),
            expr(f"mode.player_id as player_id"),
            "platform",
            "service",
            expr(f"mode.game_mode as game_mode"),
            expr(f"mode.sub_mode as sub_mode"),
            expr(f"mode.country_code as country_code"),
            "submodestatus",
            "submodesessionid",
            "submodetimespent",
            "modestatus",
            "modesessionid",
            "modetimespent",
            expr("CASE WHEN mode.game_mode = 'Online' THEN 'TRUE' WHEN mode.sub_mode LIKE 'MFOnline%' THEN 'TRUE' WHEN mode.sub_mode = 'OrionMultiplayer' THEN 'TRUE' ELSE 'FALSE' END AS extra_grain_1")
        )
    )

    return joined_df, mode_joined_df

# COMMAND ----------

def transform(df, mode_df, environment):

    match_new_df = (
        df.alias("match")
        .groupBy(
            expr("to_date(match.date) AS date"),
            expr("COALESCE(match.player_id, CONCAT(to_date(match.date), 'Null-Player')) AS player_id"),
            expr("match.platform"),
            expr("match.service"),
            expr("ifnull(match.country_code::string, 'ZZ') AS country_code"),
            expr("match.game_mode"),
            expr("match.sub_mode"),
            expr("match.extra_grain_1"),
            expr("cast(null as varchar(100)) AS extra_grain_2")
        )
        .agg(
            expr("COALESCE(COUNT(DISTINCT CASE WHEN matchstatus = 'Start' THEN match.matchguid ELSE NULL END), 0)").alias("agg_game_status_1"),
            expr("COALESCE(COUNT(DISTINCT CASE WHEN matchstatus = 'End' THEN match.matchguid ELSE NULL END), 0)").alias("agg_game_status_2"),
            expr("COALESCE(COUNT(DISTINCT case when matchstatus = 'End' and reason = 'Match End' and win = slot then matchguid else NULL end), 0)").alias("agg_game_status_3"),
            expr("COALESCE(COUNT(DISTINCT case when matchstatus = 'End' and reason = 'Match End' and lose = slot then matchguid else NULL end), 0)").alias("agg_game_status_4"),
            expr("COALESCE(COUNT(DISTINCT CASE WHEN reason = 'Quit' THEN match.matchguid ELSE NULL END), 0)").alias("agg_game_status_5"),
            expr("COALESCE(COUNT(DISTINCT match.matchguid), 0)").alias("agg_gp_1"),
            expr("cast(round(SUM(COALESCE(cast(match.play_time as float), 0)), 2) as int)").alias("agg_gp_2"),
        ).distinct()
    )

    mode_new_df = (
        mode_df.alias("mode")
        .groupBy(
            expr("to_date(mode.date) AS date"),
            expr("COALESCE(mode.player_id, CONCAT(to_date(mode.date), 'Null-Player')) AS player_id"),
            expr("mode.platform"),
            expr("mode.service"),
            expr("ifnull(mode.country_code::string, 'ZZ') AS country_code"),
            expr("mode.game_mode"),
            expr("mode.sub_mode"),
            expr("mode.extra_grain_1"),
            expr("cast(null as varchar(100)) AS extra_grain_2")
        )
        .agg(
            expr("COALESCE(COUNT(DISTINCT CASE WHEN modestatus = 'ModeEnter' THEN mode.modesessionid ELSE NULL END), 0)").alias("agg_gp_3"),
            expr("cast(round(SUM(COALESCE(cast(mode.modetimespent as float), 0)), 2) as int)").alias("agg_gp_4"),
            expr("cast(round(SUM(COALESCE(cast(mode.submodetimespent as float), 0)), 2) as int)").alias("agg_gp_5")
        ).distinct()
    )

    new_df = (
        mode_new_df.alias("mo")
        .join(match_new_df.alias("ma"), on=expr("mo.date = ma.date and mo.player_id = ma.player_id and mo.platform = ma.platform and mo.service = ma.service and mo.country_code = ma.country_code and mo.game_mode = ma.game_mode and mo.sub_mode = ma.sub_mode and mo.extra_grain_1 = ma.extra_grain_1"), how="full")
        .groupBy(
            expr("coalesce(mo.date, ma.date) as date"), 
            expr("coalesce(mo.player_id, ma.player_id) as player_id"), 
            expr("coalesce(mo.platform, ma.platform) as platform"), 
            expr("coalesce(mo.service, ma.service) as service"), 
            expr("coalesce(mo.country_code, ma.country_code) as country_code"), 
            expr("coalesce(mo.game_mode, ma.game_mode) as game_mode"), 
            expr("coalesce(mo.sub_mode, ma.sub_mode) as sub_mode"), 
            expr("ifnull(coalesce(mo.extra_grain_1, ma.extra_grain_1), 'FALSE') as extra_grain_1"), 
            expr("coalesce(mo.extra_grain_2, ma.extra_grain_2) as extra_grain_2")
        )
        .agg(
            expr("ifnull(max(agg_game_status_1), 0) as agg_game_status_1"),
            expr("ifnull(max(agg_game_status_2), 0) as agg_game_status_2"),
            expr("ifnull(max(agg_game_status_3), 0) as agg_game_status_3"),
            expr("ifnull(max(agg_game_status_4), 0) as agg_game_status_4"),
            expr("ifnull(max(agg_game_status_5), 0) as agg_game_status_5"),
            expr("ifnull(max(agg_gp_1), 0) as agg_gp_1"),
            expr("ifnull(max(agg_gp_2), 0) as agg_gp_2"),
            expr("ifnull(max(agg_gp_3), 0) as agg_gp_3"),
            expr("ifnull(max(agg_gp_4), 0) as agg_gp_4"),
            expr("ifnull(max(agg_gp_5), 0) as agg_gp_5")
        ).distinct()
    )

    return new_df


# COMMAND ----------

def run_batch():
    checkpoint_location = create_fact_player_game_status_daily(spark, database, view_mapping)

    df, mode_df = extract(environment)
    df = transform(df, mode_df, environment)

    load_fact_player_game_status_daily(spark, df, database, environment)

# COMMAND ----------

run_batch()