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
database  = 'bluenose'
data_source ='dna'
title = "'PGA Tour 2K25','PGA Tour 2K25: Demo'"

# COMMAND ----------

def create_fact_player_funnel_nrt(spark, database, environment):
    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.fact_player_funnel_nrt (
        player_id STRING,
        platform STRING,
        service STRING,
        first_seen_country_code STRING,
        game_type STRING,
        installed BOOLEAN,
        install_ts TIMESTAMP,
        account_linked BOOLEAN,
        linked_ts TIMESTAMP,
        main_menu_seen BOOLEAN,
        main_menu_ts TIMESTAMP,
        upsell_promo_seen BOOLEAN,
        upsell_promo_seen_ts TIMESTAMP,
        players_1p_store_visited BOOLEAN,
        players_1p_store_visited_ts TIMESTAMP,
        ftue_my_player_created BOOLEAN,
        ftue_my_player_created_ts TIMESTAMP,
        ftue_forced_round_played boolean,
        ftue_forced_round_played_ts TIMESTAMP,
        ftue_forced_round_score_to_par INT,
        dw_insert_ts TIMESTAMP,
        dw_update_ts TIMESTAMP,
        merge_key STRING
    )
    """

    properties = {
        "delta.feature.allowColumnDefaults": "supported" # this won't work until iceberg v3 is released
    }

    create_table(spark,sql, properties)
    create_fact_player_funnel_nrt_view(spark, database, environment)
    return f"dbfs:/tmp/{database}/managed/streaming/run{environment}/fact_player_funnel_nrt"

# COMMAND ----------

def create_fact_player_funnel_nrt_view(spark, database, environment):
    """
    Create the fact_player_game_status_daily view in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
        mapping (dict): A dictionary of column mappings.
    """


    sql = f"""
    CREATE OR REPLACE VIEW {database}{environment}.managed_view.fact_player_funnel_nrt AS (
        SELECT
          player_id,
          platform,
          service, 
          first_seen_country_code,
          game_type,
          installed,
          install_ts,
          account_linked,
          linked_ts,
          main_menu_seen,
          main_menu_ts,
          upsell_promo_seen,
          upsell_promo_seen_ts,
          players_1p_store_visited,
          players_1p_store_visited_ts,
          ftue_my_player_created,
          ftue_my_player_created_ts,
          ftue_forced_round_played,
          ftue_forced_round_played_ts,
          dw_insert_ts,
          dw_update_ts
        from {database}{environment}.managed.fact_player_funnel_nrt
    )
    """

    spark.sql(sql)

# COMMAND ----------

def read_player_activity(spark, environment):
  df_player_activity = (
        spark
        .readStream
        .table(f"bluenose{environment}.intermediate.fact_player_activity")
        .select(
        "received_on",
        "player_id",
        "platform",
        "service",
        "country_code",
        "source_table",
        "event_trigger",
        expr("case when extra_info_1 in ('True','False') then extra_info_1 else null end as dob_verified"),
        expr("extra_info_2 as mode"),
        expr("extra_info_3 as sub_mode"),
        expr("extra_info_5 as game_type"),
        expr("extra_info_7 as screen_outcome"),
        expr('try_cast(extra_info_8 as int) as score_to_par'),
        expr('extra_info_9 as placement_id'),
        expr("extra_info_10 as promo_id"),
      ).where(
        (expr("player_id is not null and player_id != 'anonymous'")) &
        (expr("extra_info_5 = 'demo'")) 
    )
  )
  return df_player_activity

# COMMAND ----------

def read_sso_mapping_df(environment):
    return (
        spark
        .read
        .table(f"reference{environment}.sso_mapping.dna_obfuscated_id_mapping")
    )

# COMMAND ----------

def read_player_funnel(spark, environment):
    fact_player_funnel = (
        spark
        .read
        .table(f"bluenose{environment}.managed.fact_player_funnel_nrt")
        .select(
            'player_id',
            'platform',
            'service',
            'first_seen_country_code'
        )
    )
    
    if fact_player_funnel is None:
        columns = ["player_id", "platform", "service", "first_seen_country_code"]
        fact_player_funnel = spark.createDataFrame([], columns)
        
    return fact_player_funnel

# COMMAND ----------

def extract(environment, database):
    df = read_player_activity(spark, environment)
    return df

# COMMAND ----------

def transform(df):
    fact_player_funnel = read_player_funnel(spark, environment)
    sso_mapping_df = read_sso_mapping_df(environment)
    if fact_player_funnel is not None:
        df = df.alias('new').join(
            fact_player_funnel.alias('old'),
            on=['player_id', 'platform', 'service'],
            how='left'
        ).select(
            'new.*',
            expr('old.first_seen_country_code as existing_first_seen_country_code')
        )

        df = df.withColumn(
            "first_seen_country_code",
            coalesce(
                'existing_first_seen_country_code',
                expr("FIRST_VALUE(country_code) IGNORE NULLS OVER (PARTITION BY player_id, platform, service ORDER BY received_on)")
            )
        )

    else:
        print("First time funnel not found")
        df = df.withColumn(
        "first_seen_country_code",
        expr("FIRST_VALUE(country_code) IGNORE NULLS OVER (PARTITION BY player_id, platform, service ORDER BY received_on)")
        )

    df = df.alias('act').join(
        sso_mapping_df.alias('sso'),
        df['player_id'] == sso_mapping_df['unobfuscated_platform_id'],
        how='left'
    ).select(
        'act.*', 
        'sso.unobfuscated_platform_id',
        'sso.min_received_on')
    
    promo_views = "('e6b8eda17175436e91718988594daef6', '826e2b15ab304982ad7ba319538fc007')"

    promo_clicks = "('10647a8558874b938de9e97d0206c97d', 'bebf4566bf8a4a0fa7ae461e4bf335e6', '652de0d8fa774d15ac6a6daef4da8d4f', 'df262092c2a2474d99a77e13c2aed3f0', '0ea2ba20065644baa31e2283cfc494b3', '2cec0b1d6d364fe9bde87c18dcce8bc7', '6713057f25bc40c49c7d3855aec5aedc', '91d06350077045eb8f6af02c2b7d1e4e', '8d015684f0e44e0ab02ebe6384f3a577', '397cc88bad3543f9b780784842a3e49b', 'cfa5ecfd3a304f3a904cd3caccf06f07', 'a548a256e0404c309a5d49c04aa54faa', 'd2e6083ad2ef4d618b528bb8564e6b24', 'e6b8eda17175436e91718988594daef6', '826e2b15ab304982ad7ba319538fc007')"

    aggregated_df = df.groupBy(
            "player_id", 
            "platform", 
            "service", 
            "first_seen_country_code",
            "game_type"
    ).agg(
        expr("MAX(CASE WHEN source_table = 'loginevent' AND event_trigger = 'login' THEN 1 ELSE 0 END) AS installed"),
        expr("MIN(CASE WHEN source_table = 'loginevent' AND event_trigger = 'login' THEN received_on END) AS install_ts"),
        expr("MAX(CASE WHEN source_table = 'loginevent' AND event_trigger = 'login' and dob_verified = 'True' THEN 1 ELSE 0 END) AS account_linked"),
        expr("MIN(CASE WHEN source_table = 'loginevent' AND event_trigger = 'login' and dob_verified = 'True' THEN LEAST(received_on, min_received_on) END) AS linked_ts"),
        expr("MAX(CASE WHEN source_table = 'gamemode' AND mode = 'Main Menu' AND sub_mode = 'Main Menu' THEN 1 ELSE 0 END) AS main_menu_seen"),
        expr("MIN(CASE WHEN source_table = 'gamemode' AND mode = 'Main Menu' AND sub_mode = 'Main Menu' THEN received_on END) AS main_menu_ts"),
        expr(f"MAX(CASE WHEN source_table = 'promotion' AND event_trigger = 'Impression' AND promo_id IN {promo_views} THEN 1 ELSE 0 END) AS upsell_promo_seen"),
        expr(f"MIN(CASE WHEN source_table = 'promotion' AND event_trigger = 'Impression' AND promo_id IN {promo_views} THEN received_on END) AS upsell_promo_seen_ts"),
        expr(f"MAX(CASE WHEN source_table = 'promotion' AND event_trigger = 'Click' AND (promo_id IN {promo_clicks} OR placement_id = 'demo_purchasebutton') THEN 1 ELSE 0 END) AS players_1p_store_visited"),
        expr(f"MIN(CASE WHEN source_table = 'promotion' AND event_trigger = 'Click' AND (promo_id IN {promo_clicks} OR placement_id = 'demo_purchasebutton') THEN received_on END) AS players_1p_store_visited_ts"),
        expr("MAX(CASE WHEN source_table = 'characterprogression' AND event_trigger = 'CreateMyPlayer' THEN 1 ELSE 0 END) AS ftue_my_player_created"),
        expr("MIN(CASE WHEN source_table = 'characterprogression' AND event_trigger = 'CreateMyPlayer' then received_on END) AS ftue_my_player_created_ts"),      
        expr("MAX(CASE WHEN source_table = 'roundstatus' AND event_trigger in ('RoundComplete', 'RoundWon') AND mode != 'Training' AND mode != 'Boot' THEN 1 ELSE 0 END) AS ftue_forced_round_played"),
        expr("MIN(CASE WHEN source_table = 'roundstatus' AND event_trigger in ('RoundComplete', 'RoundWon') AND mode != 'Training' AND mode != 'Boot' THEN received_on END) AS ftue_forced_round_played_ts"),
        expr("MIN(CASE WHEN source_table = 'roundstatus' AND event_trigger in ('RoundComplete', 'RoundWon') AND mode != 'Training' AND mode != 'Boot' THEN score_to_par END) AS ftue_forced_round_score_to_par")

    ).select(
        expr("player_id AS player_id"),
        expr("platform AS platform"),
        expr("service AS service"),
        expr("first_seen_country_code AS first_seen_country_code"),
        expr("game_type as game_type"),
        expr("try_cast(installed as boolean) AS installed"),
        expr("install_ts AS install_ts"),
        expr("try_cast((CASE WHEN account_linked = 0 AND (main_menu_seen = 1 OR upsell_promo_seen = 1 OR players_1p_store_visited = 1 OR ftue_my_player_created = 1 OR ftue_forced_round_played = 1) THEN 1 ELSE account_linked END) as boolean) AS account_linked"),
        expr("""case when COALESCE(linked_ts, LEAST(main_menu_ts, upsell_promo_seen_ts, players_1p_store_visited_ts, ftue_forced_round_played_ts, ftue_forced_round_played_ts, ftue_my_player_created_ts)) is null and install_ts is not null then null 
                     when install_ts is not null and COALESCE(linked_ts, LEAST(main_menu_ts, upsell_promo_seen_ts, players_1p_store_visited_ts, ftue_forced_round_played_ts, ftue_forced_round_played_ts, ftue_my_player_created_ts)) between install_ts - INTERVAL '10 MINUTES' and install_ts + INTERVAL '10 MINUTES' then COALESCE(linked_ts, LEAST(main_menu_ts, upsell_promo_seen_ts, players_1p_store_visited_ts, ftue_forced_round_played_ts, ftue_forced_round_played_ts, ftue_my_player_created_ts))
                     when install_ts is null then COALESCE(linked_ts, LEAST(main_menu_ts, upsell_promo_seen_ts, players_1p_store_visited_ts, ftue_forced_round_played_ts, ftue_forced_round_played_ts, ftue_my_player_created_ts)) 
                     ELSE install_ts
                END AS linked_ts"""),
        expr("try_cast(main_menu_seen as boolean) AS main_menu_seen"),
        expr("main_menu_ts AS main_menu_ts"),
        expr("try_cast(upsell_promo_seen as boolean) AS upsell_promo_seen"),
        expr("upsell_promo_seen_ts AS upsell_promo_seen_ts"),
        expr("try_cast(players_1p_store_visited as boolean) AS players_1p_store_visited"),
        expr("players_1p_store_visited_ts AS players_1p_store_visited_ts"),
        expr("try_cast(ftue_my_player_created as boolean) AS ftue_my_player_created"),
        expr("ftue_my_player_created_ts AS ftue_my_player_created_ts"),
        expr("try_cast(ftue_forced_round_played as boolean) AS ftue_forced_round_played"),
        expr("ftue_forced_round_played_ts AS ftue_forced_round_played_ts"),
        expr("ftue_forced_round_score_to_par::int as ftue_forced_round_score_to_par")
    )

    return aggregated_df

# COMMAND ----------


def load_fact_player_funnel_nrt(spark, df, database, environment):
    """
    Load the data into the fact_player_game_status_daily table in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        df (DataFrame): The DataFrame to load into the table.
        database (str): The database to load the table into.
        environment (str): The environment to load the table into.
    """

    # Perform the merge operation into the Delta table
    final_table = DeltaTable.forName(spark, f"{database}{environment}.managed.fact_player_funnel_nrt")

    # get schema of the table
    table_schema = spark.read.table(f"{database}{environment}.managed.fact_player_funnel_nrt").schema

    # create empty dataframe using the schema
    out_df = spark.createDataFrame([], table_schema)

    # union the incoming dataframe with the empty dataframe, we can set allowMissingColumns to False to be more strict
    df = df.select(
            "*",
            expr("CURRENT_TIMESTAMP() as dw_insert_ts"),
            expr("CURRENT_TIMESTAMP() as dw_update_ts"),
            expr("sha2(concat_ws('|', player_id, platform, service, first_seen_country_code, game_type), 256) as merge_key")
        )
    out_df = out_df.unionByName(df, allowMissingColumns=True)

    agg_cols = ['installed', 'install_ts','account_linked','linked_ts','main_menu_seen','main_menu_ts', 'upsell_promo_seen', 'upsell_promo_seen_ts','players_1p_store_visited','players_1p_store_visited_ts', 'ftue_my_player_created', 'ftue_my_player_created_ts', 'ftue_forced_round_played', 'ftue_forced_round_played_ts', 'ftue_forced_round_score_to_par']

    merge_condtion = """
    (old.install_ts IS NULL AND new.install_ts IS NOT NULL) OR (old.install_ts > new.install_ts) OR
    (old.linked_ts IS NULL AND new.linked_ts IS NOT NULL) OR (old.linked_ts > new.linked_ts) OR
    (old.main_menu_ts IS NULL AND new.main_menu_ts IS NOT NULL) OR (old.main_menu_ts > new.main_menu_ts) OR
    (old.upsell_promo_seen_ts IS NULL AND new.upsell_promo_seen_ts IS NOT NULL) OR (old.upsell_promo_seen_ts > new.upsell_promo_seen_ts) OR
    (old.ftue_forced_round_played_ts IS NULL AND new.ftue_forced_round_played_ts IS NOT NULL) OR (old.ftue_forced_round_played_ts > new.ftue_forced_round_played_ts) OR
    (old.ftue_my_player_created_ts IS NULL AND new.ftue_my_player_created_ts IS NOT NULL) OR (old.ftue_my_player_created_ts > new.ftue_my_player_created_ts) OR
    (old.players_1p_store_visited_ts IS NULL AND new.players_1p_store_visited_ts IS NOT NULL) OR (old.players_1p_store_visited_ts > new.players_1p_store_visited_ts)
    """
       # Create the update column dict
    update_set = {
        "installed": "GREATEST(old.installed, new.installed)",
        "install_ts": "COALESCE(LEAST(old.install_ts, new.install_ts), new.install_ts)",
        "account_linked": "GREATEST(old.account_linked, new.account_linked)",
        "linked_ts": "COALESCE(LEAST(old.linked_ts, new.linked_ts), new.linked_ts)",
        "main_menu_seen": "GREATEST(old.main_menu_seen, new.main_menu_seen)",
        "main_menu_ts": "COALESCE(LEAST(old.main_menu_ts, new.main_menu_ts), new.main_menu_ts)",
        "upsell_promo_seen": "GREATEST(old.upsell_promo_seen, new.upsell_promo_seen)",
        "upsell_promo_seen_ts": "COALESCE(LEAST(old.upsell_promo_seen_ts, new.upsell_promo_seen_ts), new.upsell_promo_seen_ts)",
        "players_1p_store_visited": "GREATEST(old.players_1p_store_visited, new.players_1p_store_visited)",
        "players_1p_store_visited_ts": "COALESCE(LEAST(old.players_1p_store_visited_ts, new.players_1p_store_visited_ts), new.players_1p_store_visited_ts)",
        "ftue_my_player_created": "GREATEST(old.ftue_my_player_created, new.ftue_my_player_created)",
        "ftue_my_player_created_ts": "COALESCE(LEAST(old.ftue_my_player_created_ts, new.ftue_my_player_created_ts), new.ftue_my_player_created_ts)",
        "ftue_forced_round_played": "GREATEST(old.ftue_forced_round_played, new.ftue_forced_round_played)",
        "ftue_forced_round_played_ts": "COALESCE(LEAST(old.ftue_forced_round_played_ts, new.ftue_forced_round_played_ts), new.ftue_forced_round_played_ts)",
        "ftue_forced_round_score_to_par": "COALESCE(old.ftue_forced_round_score_to_par, new.ftue_forced_round_score_to_par)", 
        "dw_update_ts": "CURRENT_TIMESTAMP()"
    }
    # Merge the table
    (
        final_table.alias('old')
        .merge(
            out_df.alias('new'),
            "new.merge_key = old.merge_key"
        )
        .whenMatchedUpdate(condition=merge_condtion, set=update_set)
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

def proc_batch(df, environment):
    df = transform(df)
    load_fact_player_funnel_nrt(spark, df, database, environment)

# COMMAND ----------

def run_stream():
    spark = create_spark_session(name=f"{database}")
    checkpoint_location = create_fact_player_funnel_nrt(spark, database, environment)
    df = extract(environment, database)
    (
        df
        .writeStream
        .trigger(availableNow=True)
        .option("checkpointLocation", checkpoint_location)
        .foreachBatch(lambda df, batch_id: proc_batch(df, environment))
        .start()
    )


# COMMAND ----------

run_stream()

# COMMAND ----------

# dbutils.fs.rm("dbfs:/tmp/bluenose/managed/streaming/run_dev/fact_player_funnel_nrt", True)
# spark.sql("drop table bluenose_dev.managed.fact_player_funnel_nrt")
