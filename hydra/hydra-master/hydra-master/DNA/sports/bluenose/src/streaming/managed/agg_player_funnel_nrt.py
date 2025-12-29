# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/table_functions

# COMMAND ----------

from pyspark.sql.functions import expr, coalesce, window
from delta.tables import DeltaTable

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'bluenose'
data_source ='dna'
title = "'PGA Tour 2K25','PGA Tour 2K25: Demo'"

# COMMAND ----------

def create_agg_player_funnel_nrt(spark, database, environment):
    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.agg_player_funnel_nrt (
        timestamp_10min_slice TIMESTAMP,
        platform STRING,
        service STRING,
        total_logins INT,
        unique_players_linked INT,
        unique_players_reached_main_menu INT,
        unique_players_seen_upsell_promo INT,
        unique_players_visited1p_store INT,
        ftue_lessons_completed INT,
        ftue_my_player_created INT,
        ftue_forced_round_played INT,
        welcome_quest_started INT,
        welcome_quest_completed INT,
        dw_insert_ts TIMESTAMP,
        dw_update_ts TIMESTAMP,
        merge_key STRING
    )
    """

    properties = {
        "delta.feature.allowColumnDefaults": "supported" # this won't work until iceberg v3 is released
    }

    create_table(spark,sql, properties)
    create_agg_player_funnel_nrt_view(spark, database, environment)
    return f"dbfs:/tmp/{database}/managed/streaming/run{environment}/agg_player_funnel_nrt"

# COMMAND ----------

def create_agg_player_funnel_nrt_view(spark, database, environment):
    """
    Create the fact_player_game_status_daily view in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
        mapping (dict): A dictionary of column mappings.
    """


    sql = f"""
    CREATE OR REPLACE VIEW {database}{environment}.managed_view.agg_player_funnel_nrt AS (
        SELECT
        *
        from {database}{environment}.managed.agg_player_funnel_nrt
        
    )
    """

    spark.sql(sql)

# COMMAND ----------

def read_player_activity(spark, environment):
  df_player_activity = (
        spark
        .readStream
        .table(f"bluenose{environment}.intermediate.fact_player_activity")
        # .table(f"bluenose_stg.intermediate.fact_player_activity")
        .select(
        "received_on",
        expr("from_unixtime(round(floor(unix_timestamp(received_on) / 600) * 600))::timestamp as received_on_10min_slice"),
        "player_id",
        "platform",
        "service",
        "country_code",
        "source_table",
        "event_trigger",
        expr("extra_info_7 as screen_outcome"),
        expr("extra_info_10 as promoid")
      ).where(
        (expr("extra_info_5 = 'demo'")) &
        (expr("player_id is not null and player_id != 'anonymous'")) 
    )
  )
  return df_player_activity

# COMMAND ----------

def read_player_funnel(spark, environment):
    fact_player_funnel = (
        spark
        .read
        .table(f"bluenose{environment}.managed.fact_player_funnel_nrt")
        .where((expr("game_type = 'demo'")) )
        .alias("lkp")
    ).select(
        'player_id',
        'platform',
        'service',
        'account_linked',
        expr("from_unixtime(round(floor(unix_timestamp(linked_ts) / 600) * 600))").cast("timestamp").alias("linked_ts_10min_slice"),
        'main_menu_seen',
        expr("from_unixtime(round(floor(unix_timestamp(main_menu_ts) / 600) * 600))").cast("timestamp").alias("main_menu_ts_10min_slice"),
        'upsell_promo_seen',
        expr("from_unixtime(round(floor(unix_timestamp(upsell_promo_seen_ts) / 600) * 600))").cast("timestamp").alias("upsell_promo_seen_ts_10min_slice"),
        'players_1p_store_visited',
        expr("from_unixtime(round(floor(unix_timestamp(players_1p_store_visited_ts) / 600) * 600))").cast("timestamp").alias("players_1p_store_visited_ts_10min_slice"),
        'ftue_my_player_created',
        expr("from_unixtime(round(floor(unix_timestamp(ftue_my_player_created_ts) / 600) * 600))").cast("timestamp").alias("ftue_my_player_created_ts_10min_slice"),
        'ftue_forced_round_played',
        expr("from_unixtime(round(floor(unix_timestamp(ftue_forced_round_played_ts) / 600) * 600))").cast("timestamp").alias("ftue_forced_round_played_ts_10min_slice")
    ).where(expr("dw_update_ts::date > CURRENT_DATE() - INTERVAL 2 DAY"))
    return fact_player_funnel

# COMMAND ----------

def read_fact_player_lessons(spark, environment):
    df_player_lessons = (
        spark
        .readStream
        .table(f"bluenose{environment}.managed.fact_player_lesson")
        .select("*",             
                expr("from_unixtime(round(floor(unix_timestamp(received_on) / 600) * 600))::timestamp as received_on_10min_slice"))
        .where( expr("game_type = 'demo'") &
                expr("is_onboarding = True") &
                expr("lesson_name ilike '%SwingBasics%' and lesson_step_name ilike '%ReadingTheGreen%'") &
                expr("is_lesson_step_complete = True") &
                expr("is_ftue = True")
            )
    )
    return df_player_lessons

# COMMAND ----------

def read_quest_lookup(spark, environment):
    quests_df = (
            spark
            .read
            .table(f"bluenose{environment}.reference.quests")
            .select("quest_id")
            .where(
            (expr("quest_type ilike '%onboarding%'")) 
        )
            .distinct()
            .alias("quest_lkp")
        )
    return quests_df

# COMMAND ----------

def read_fact_player_igo(spark, environment):
    onboarding_quests_df = read_quest_lookup(spark, environment)
    onboarding_quests_list = [row.quest_id for row in onboarding_quests_df.collect()]
    onboarding_quests_str = ','.join([f"'{quest_id}'" for quest_id in onboarding_quests_list])

    df_welcome_quests = (
        spark
        .readStream
        .table(f"bluenose{environment}.intermediate.fact_player_igo")
        .select(
            expr("from_unixtime(round(floor(unix_timestamp(received_on) / 600) * 600))::timestamp as received_on_10min_slice"),
            "player_id",
            "platform",
            "service",
            "igo_type",
            "igo_id",
            "status"
        ).where(
            (expr("extra_info_4 = 'demo'")) &
            (expr("player_id is not null and player_id != 'anonymous'")) &
            (expr("igo_type = 'quest'and (status = 'Complete' or status = 'Activated') ")) &
            (expr(f"igo_id in ({onboarding_quests_str})" ))
        )
    )
    return df_welcome_quests

# COMMAND ----------

def read_platform_territory_df(spark, environment):
    return (
        spark
        .readStream
        .table(f"dataanalytics{environment}.standard_metrics.platform_territory_10min_ts")
        .select(
            "timestamp_10min_slice",
            "platform",
            "service"
        )
        .where(expr("timestamp_10min_slice >= '2024-10-01T00:00:00.000'") &
               expr("timestamp_10min_slice < '2025-08-01T00:00:00.000'") &
            (
                (expr("platform = 'Windows' AND service = 'Steam'")) |
                (expr("platform = 'XBSX' AND service = 'Xbox Live'")) |
                (expr("platform = 'PS5' AND service = 'SEN'"))
            )
            )
        .distinct()
    )

# COMMAND ----------

def extract(environment, database):
    player_activity_df = read_player_activity(spark, environment)
    lessons_df = read_fact_player_lessons(spark, environment)
    quests_df = read_fact_player_igo(spark, environment)
    platform_service_timestamp_df = read_platform_territory_df(spark, environment)

    logins_df = (
        player_activity_df.alias('pa')
        .withWatermark("received_on_10min_slice", "15 minutes")
        .groupBy(
            "pa.player_id",
            expr("pa.received_on_10min_slice as timestamp_10min_slice"),
            "pa.platform", 
            "pa.service"
        )
        .agg(
            expr("SUM(CASE WHEN pa.source_table = 'loginevent' AND pa.event_trigger = 'login' THEN 1 ELSE 0 END) AS logins"),
        )
    )

    lessons_agg_df = (
        lessons_df.alias('l')
        .withWatermark("received_on_10min_slice", "15 minutes")
        .groupBy(
            "l.player_id",
            expr("l.received_on_10min_slice as timestamp_10min_slice"),
            "l.platform", 
            "l.service"
        )
        .agg(
            expr("SUM(CASE WHEN l.is_lesson_step_complete = True THEN 1 ELSE 0 END) AS lesson_completed")
        )
    )

    quests_agg_df = (
        quests_df.alias('q')
        .withWatermark("received_on_10min_slice", "15 minutes")
        .groupBy(
            "q.player_id",
            expr("q.received_on_10min_slice as timestamp_10min_slice"),
            "q.platform", 
            "q.service"
        )
        .agg(
            expr("SUM(CASE WHEN q.status = 'Activated' THEN 1 ELSE 0 END) AS quest_started"),
            expr("SUM(CASE WHEN q.status = 'Complete' THEN 1 ELSE 0 END) AS quest_completed")
        )
    )

    combined_df = (
            platform_service_timestamp_df.alias('pt')
            .unionByName(logins_df, True)
            .unionByName(lessons_agg_df, True)
            .unionByName(quests_agg_df, True)
        ).select(
            "timestamp_10min_slice",
            "platform",
            "service",
            "player_id",
            "logins",
            "lesson_completed",
            "quest_started",
            "quest_completed"
        )
    return combined_df

# COMMAND ----------

def transform(df):
    player_funnel_df = read_player_funnel(spark, environment)

    linked_df = player_funnel_df.groupBy(
        'player_id', 'platform', 'service', 'linked_ts_10min_slice'
    ).agg(
        expr("sum(cast(account_linked as int)) as sum_account_linked")
    ).select(
        'player_id', 'platform', 'service',
        expr("linked_ts_10min_slice as timestamp_10min_slice"),
        'sum_account_linked'
    ).where(
    expr("player_id IS NOT NULL AND platform IS NOT NULL AND service IS NOT NULL AND timestamp_10min_slice IS NOT NULL")
    )

    main_menu_df = player_funnel_df.groupBy(
        'player_id', 'platform', 'service', 'main_menu_ts_10min_slice'
    ).agg(
        expr("sum(cast(main_menu_seen as int)) as sum_main_menu_seen")
    ).select(
        'player_id', 'platform', 'service',
        expr("main_menu_ts_10min_slice as timestamp_10min_slice"),
        'sum_main_menu_seen'
    ).where(
    expr("player_id IS NOT NULL AND platform IS NOT NULL AND service IS NOT NULL AND timestamp_10min_slice IS NOT NULL")
    )

    upsell_promo_df = player_funnel_df.groupBy(
        'player_id', 'platform', 'service', 'upsell_promo_seen_ts_10min_slice'
    ).agg(
        expr("sum(cast(upsell_promo_seen as int)) as sum_upsell_promo_seen")
    ).select(
        'player_id', 'platform', 'service',
        expr("upsell_promo_seen_ts_10min_slice as timestamp_10min_slice"),
        'sum_upsell_promo_seen'
    ).where(
    expr("player_id IS NOT NULL AND platform IS NOT NULL AND service IS NOT NULL AND timestamp_10min_slice IS NOT NULL")
    )

    players_1p_store_df = player_funnel_df.groupBy(
        'player_id', 'platform', 'service', 'players_1p_store_visited_ts_10min_slice'
    ).agg(
        expr("sum(cast(players_1p_store_visited as int)) as sum_players_1p_store_visited")
    ).select(
        'player_id', 'platform', 'service',
        expr("players_1p_store_visited_ts_10min_slice as timestamp_10min_slice"),
        'sum_players_1p_store_visited'
    ).where(
    expr("player_id IS NOT NULL AND platform IS NOT NULL AND service IS NOT NULL AND timestamp_10min_slice IS NOT NULL")
    )

    ftue_my_player_df = player_funnel_df.groupBy(
        'player_id', 'platform', 'service', 'ftue_my_player_created_ts_10min_slice'
    ).agg(
        expr("sum(cast(ftue_my_player_created as int)) as sum_ftue_my_player_created")
    ).select(
        'player_id', 'platform', 'service',
        expr("ftue_my_player_created_ts_10min_slice as timestamp_10min_slice"),
        'sum_ftue_my_player_created'
    ).where(
    expr("player_id IS NOT NULL AND platform IS NOT NULL AND service IS NOT NULL AND timestamp_10min_slice IS NOT NULL")
    )

    ftue_forced_round_df = player_funnel_df.groupBy(
        'player_id', 'platform', 'service', 'ftue_forced_round_played_ts_10min_slice'
    ).agg(
        expr("sum(cast(ftue_forced_round_played as int)) as sum_ftue_forced_round_played")
    ).select(
        'player_id', 'platform', 'service',
        expr("ftue_forced_round_played_ts_10min_slice as timestamp_10min_slice"),
        'sum_ftue_forced_round_played'
    ).where(
    expr("player_id IS NOT NULL AND platform IS NOT NULL AND service IS NOT NULL AND timestamp_10min_slice IS NOT NULL")
    )

    df_final = (
        df.alias('p1')
        .unionByName(linked_df, True)
        .unionByName(main_menu_df, True)
        .unionByName(upsell_promo_df, True)
        .unionByName(players_1p_store_df, True)
        .unionByName(ftue_my_player_df, True)
        .unionByName(ftue_forced_round_df, True)
    ).select(
        "timestamp_10min_slice",
        "platform",
        "service",
        "player_id",
        "logins",
        "lesson_completed",
        "quest_started",
        "quest_completed",
        "sum_account_linked",
        "sum_main_menu_seen",
        "sum_upsell_promo_seen",
        "sum_players_1p_store_visited",
        "sum_ftue_my_player_created",
        "sum_ftue_forced_round_played"
    )

    agg_player_funnel_df = df_final.groupBy(
        "timestamp_10min_slice",
        "platform", 
        "service"
    ).agg(
        expr("COUNT(CASE WHEN logins > 0 then player_id END)::int AS total_logins"),
        expr("COUNT(DISTINCT CASE WHEN sum_account_linked > 0 then player_id END)::int as unique_players_linked"),
        expr("COUNT(DISTINCT CASE WHEN sum_main_menu_seen > 0 then player_id END)::int as unique_players_reached_main_menu"),
        expr("COUNT(DISTINCT CASE WHEN sum_upsell_promo_seen > 0 then player_id END)::int as unique_players_seen_upsell_promo"),
        expr("COUNT(DISTINCT CASE WHEN sum_players_1p_store_visited > 0 then player_id END)::int as unique_players_visited1p_store"),
        expr("COUNT(CASE WHEN sum_ftue_my_player_created > 0 then player_id END)::int as ftue_my_player_created"),
        expr("COUNT(CASE WHEN sum_ftue_forced_round_played > 0 then player_id END)::int as ftue_forced_round_played"),
        expr("COUNT(CASE WHEN lesson_completed > 0 then player_id END)::int AS ftue_lessons_completed"),
        expr("COUNT(CASE WHEN quest_started >= 0 then player_id END)::int AS welcome_quest_started"),
        expr("COUNT(CASE WHEN quest_completed > 0 then player_id END)::int AS welcome_quest_completed")
    ).select(
        expr("timestamp_10min_slice::timestamp as timestamp_10min_slice"),
        expr("platform::string as platform"),
        expr("service::string as service"),
        "total_logins",
        "unique_players_linked",
        "unique_players_reached_main_menu",
        "unique_players_seen_upsell_promo",
        "unique_players_visited1p_store",
        "ftue_lessons_completed",
        "ftue_my_player_created",
        "ftue_forced_round_played",
        "welcome_quest_started",
        "welcome_quest_completed"
    )

    return agg_player_funnel_df

# COMMAND ----------

def load_agg_player_funnel_nrt(spark, df, database, environment):
    """
    Load the data into the agg_player_funnel_nrt table in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        df (DataFrame): The DataFrame to load into the table.
        database (str): The database to load the table into.
        environment (str): The environment to load the table into.
    """

    # Perform the merge operation into the Delta table
    final_table = DeltaTable.forName(spark, f"{database}{environment}.managed.agg_player_funnel_nrt")

    # Get schema of the table
    table_schema = spark.read.table(f"{database}{environment}.managed.agg_player_funnel_nrt").schema

    # Create empty dataframe using the schema
    out_df = spark.createDataFrame([], table_schema)

    # Union the incoming dataframe with the empty dataframe, we can set allowMissingColumns to False to be more strict
    df = df.select(
        "*",
        expr("CURRENT_TIMESTAMP() as dw_insert_ts"),
        expr("CURRENT_TIMESTAMP() as dw_update_ts"),
        expr("sha2(concat_ws('|', timestamp_10min_slice, platform, service), 256) as merge_key")
    )
    out_df = out_df.unionByName(df, allowMissingColumns=True)


    merge_condition = """
    old.timestamp_10min_slice = new.timestamp_10min_slice AND
    old.platform = new.platform AND
    old.service = new.service
    """

    update_set = {
        "total_logins": "GREATEST(new.total_logins, old.total_logins)",
        "unique_players_linked": "GREATEST(new.unique_players_linked, old.unique_players_linked)",
        "unique_players_reached_main_menu": "GREATEST(new.unique_players_reached_main_menu, old.unique_players_reached_main_menu)",
        "unique_players_seen_upsell_promo": "GREATEST(new.unique_players_seen_upsell_promo, old.unique_players_seen_upsell_promo)",
        "unique_players_visited1p_store": "GREATEST(new.unique_players_visited1p_store, old.unique_players_visited1p_store)",
        "ftue_lessons_completed": "GREATEST(new.ftue_lessons_completed, old.ftue_lessons_completed)",
        "ftue_my_player_created": "GREATEST(new.ftue_my_player_created, old.ftue_my_player_created)",
        "ftue_forced_round_played": "GREATEST(new.ftue_forced_round_played, old.ftue_forced_round_played)",
        "welcome_quest_started": "GREATEST(new.welcome_quest_started, old.welcome_quest_started)",
        "welcome_quest_completed": "GREATEST(new.welcome_quest_completed, old.welcome_quest_completed)",
        "dw_update_ts": "CURRENT_TIMESTAMP()"
    }

    # Merge the table
    (
        final_table.alias('old')
        .merge(
            out_df.alias('new'),
            "new.merge_key = old.merge_key"
        )
        .whenMatchedUpdate(set=update_set)
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

def proc_batch(df, environment):
    df = transform(df)
    load_agg_player_funnel_nrt(spark, df, database, environment)

# COMMAND ----------

def run_stream():
    spark = create_spark_session(name=f"{database}")
    checkpoint_location = create_agg_player_funnel_nrt(spark, database, environment)
    combined_df = extract(environment, database)
    (
            combined_df
            .writeStream
            .trigger(availableNow=True)
            .outputMode("update")
            .foreachBatch(lambda df, batch_id: proc_batch(df, environment ))
            .option("checkpointLocation", checkpoint_location)
            .start()
    )

# COMMAND ----------

run_stream()

# COMMAND ----------


# dbutils.fs.rm("dbfs:/tmp/bluenose/managed/streaming/run_dev/agg_player_funnel_nrt", True)
# spark.sql("drop table bluenose_dev.managed.agg_player_funnel_nrt")