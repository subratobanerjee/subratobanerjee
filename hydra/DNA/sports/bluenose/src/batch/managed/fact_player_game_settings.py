# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/bluenose_game_specific/fact_player_game_settings

# COMMAND ----------

import json
from pyspark.sql.functions import expr, schema_of_json, explode, from_json, posexplode, col, when, lit, lag, last, first
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import datetime

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'bluenose'
data_source ='dna'
title = 'PGA Tour 2K25'
view_mapping = {}  

# COMMAND ----------

def extract(spark, environment, prev_max_ts):
    
    temp_game_settings_df = (
        spark
        .read
        .table(f"bluenose{environment}.raw.gamesettings")
        .where(f"playerPublicId is not null and playerPublicId not in ('anonymous', 'NULL') and insert_ts::date >= to_date('{prev_max_ts}') - interval '2 day'")
        .select(expr("playerPublicId")).distinct()
    )

    game_settings_df = (
        spark
        .read
        .table(f"bluenose{environment}.raw.gamesettings")
        .alias("src")
        .join(temp_game_settings_df
        .alias("temp"), expr("src.playerPublicId = temp.playerPublicId"), 'inner')
        .select('src.*')
    )

    fact_player_df = (
        spark
        .read
        .table(f"bluenose{environment}.managed.fact_player_summary_ltd")
        .where(f"player_id is not null and player_id not in ('anonymous', 'NULL') and platform is not null and service is not null")
    )

    return game_settings_df, fact_player_df

# COMMAND ----------

def transform(game_settings_df, fact_player_df):
    
    # derive the schema dynamically from the JSON column
    json_schema = schema_of_json(game_settings_df.select(expr("gamesettings:userSettings")).first()[0])
    parsed_df = game_settings_df.withColumn("parsed_gs", from_json(expr("gamesettings:userSettings"), json_schema))
    exploded_df = parsed_df.withColumn("exploded_gs", explode(col("parsed_gs")))
    exploded_df = (
        exploded_df.alias("src").join(fact_player_df.alias("fp"), expr("src.playerPublicId == fp.player_id"), "left")
        .selectExpr("src.*", "fp.platform", "fp.service")
    )
 
    exploded_df = exploded_df.withColumn("game_type", expr("CASE WHEN appGroupId IN ('d3963fb6e4f54a658e1a14846f0c9a8c', '317c0552032c4804bb10d81b89f4c37e') THEN 'full game' WHEN appGroupId = '886bcab8848046d0bd89f5f1ce3b057b' THEN 'demo' ELSE NULL END"))

    window_spec = Window.partitionBy("playerPublicId", "platform", "service").orderBy(expr("CAST(to_timestamp(receivedOn) AS date)"))
    
    # separate columns for each setting and apply the last window function
    exploded_df = exploded_df.withColumn("swing_difficulty",last(col("exploded_gs.difficultySettings.swingDifficulty.val").cast("string")).over(window_spec))
    exploded_df = exploded_df.withColumn("swing_difficulty_reward_weight", last(col("exploded_gs.difficultySettings.swingDifficultyRewardWeight").cast("string")).over(window_spec))
    exploded_df = exploded_df.withColumn("swing_timing_calibration", last(col("exploded_gs.difficultySettings.swingTimingCalibration.val").cast("string")).over(window_spec))
    exploded_df = exploded_df.withColumn("swing_input", last(col("exploded_gs.difficultySettings.swingInput.val").cast("string")).over(window_spec))
    exploded_df = exploded_df.withColumn("swing_bias_input_offset", last(col("exploded_gs.difficultySettings.swingBiasInputOffset.val").cast("string")).over(window_spec))
    exploded_df = exploded_df.withColumn("swing_context_input_offset", last(col("exploded_gs.difficultySettings.swingContextInputOffset.val").cast("string")).over(window_spec))
    exploded_df = exploded_df.withColumn("aim_stick_control_setting", last(col("exploded_gs.controlSettings.switchAimStick.val").cast("string")).over(window_spec))
    exploded_df = exploded_df.withColumn("mouse_sensitivity_control_setting", last(col("exploded_gs.controlSettings.mouseSensitivity.val").cast("string")).over(window_spec))
    exploded_df = exploded_df.withColumn("invert_orbit_control_setting", last(col("exploded_gs.controlSettings.invertOrbitGameplay.val").cast("string")).over(window_spec))

    # separate columns for each setting and apply the lag window function
    exploded_df = exploded_df.withColumn("swing_difficulty_prev", lag(col("exploded_gs.difficultySettings.swingDifficulty.val").cast("string"), 1).over(window_spec))
    exploded_df = exploded_df.withColumn("swing_difficulty_reward_weight_prev", lag(col("exploded_gs.difficultySettings.swingDifficultyRewardWeight").cast("string"), 1).over(window_spec))
    exploded_df = exploded_df.withColumn("swing_timing_calibration_prev", lag(col("exploded_gs.difficultySettings.swingTimingCalibration.val").cast("string"), 1).over(window_spec))
    exploded_df = exploded_df.withColumn("swing_input_prev", lag(col("exploded_gs.difficultySettings.swingInput.val").cast("string"), 1).over(window_spec))
    exploded_df = exploded_df.withColumn("swing_bias_input_offset_prev", lag(col("exploded_gs.difficultySettings.swingBiasInputOffset.val").cast("string"), 1).over(window_spec))
    exploded_df = exploded_df.withColumn("swing_context_input_offset_prev", lag(col("exploded_gs.difficultySettings.swingContextInputOffset.val").cast("string"), 1).over(window_spec))
    exploded_df = exploded_df.withColumn("aim_stick_control_setting_prev", lag(col("exploded_gs.controlSettings.switchAimStick.val").cast("string"), 1).over(window_spec))
    exploded_df = exploded_df.withColumn("mouse_sensitivity_control_setting_prev", lag(col("exploded_gs.controlSettings.mouseSensitivity.val").cast("string"), 1).over(window_spec))
    exploded_df = exploded_df.withColumn("invert_orbit_control_setting_prev", lag(col("exploded_gs.controlSettings.invertOrbitGameplay.val").cast("string"), 1).over(window_spec))

    # map of current value settings
    settings_map = expr("""
        map(
            'swing_difficulty', swing_difficulty,
            'swing_difficulty_reward_weight', swing_difficulty_reward_weight,
            'swing_timing_calibration', swing_timing_calibration,
            'swing_input', swing_input,
            'swing_bias_input_offset', swing_bias_input_offset,
            'swing_context_input_offset', swing_context_input_offset,
            'aim_stick_control_setting', aim_stick_control_setting,
            'mouse_sensitivity_control_setting', mouse_sensitivity_control_setting,
            'invert_orbit_control_setting', invert_orbit_control_setting
        )
    """)

    # map of previous value settings
    settings_map_prev = expr("""
        map(
            'swing_difficulty', swing_difficulty_prev,
            'swing_difficulty_reward_weight', swing_difficulty_reward_weight_prev,
            'swing_timing_calibration', swing_timing_calibration_prev,
            'swing_input', swing_input_prev,
            'swing_bias_input_offset', swing_bias_input_offset_prev,
            'swing_context_input_offset', swing_context_input_offset_prev,
            'aim_stick_control_setting', aim_stick_control_setting_prev,
            'mouse_sensitivity_control_setting', mouse_sensitivity_control_setting_prev,
            'invert_orbit_control_setting', invert_orbit_control_setting_prev
        )
    """)

    # distinct values for the key combination
    result_df = exploded_df.select(
        expr("CAST(to_timestamp(receivedOn) AS string) AS date"),
        expr("playerPublicId AS player_id"),
        expr("platform"),
        expr("service"),
        expr("game_type"),
        expr("coalesce(countryCode, 'ZZ') as country_code"),
        expr("exploded_gs.settingsInstanceId AS setting_instance_id"),
        expr("first_value(to_timestamp(receivedOn)) OVER (PARTITION BY playerPublicId, platform, service, exploded_gs.settingsInstanceId ORDER BY to_timestamp(receivedOn)) AS setting_instance_first_seen_ts"),
        expr("last_value(to_timestamp(receivedOn)) OVER (PARTITION BY playerPublicId, platform, service, exploded_gs.settingsInstanceId ORDER BY to_timestamp(receivedOn)) AS setting_instance_last_seen_ts"),
        expr("CASE WHEN LAG(exploded_gs.settingsInstanceId) OVER (PARTITION BY playerPublicId, platform, service ORDER BY to_timestamp(receivedOn)) IS NULL THEN True ELSE False END AS is_first_seen_player_setting"),
        posexplode(settings_map).alias("pos", "setting_name", "player_current_setting_value"),
        posexplode(settings_map_prev).alias("pos", "setting_name_prev", "player_previous_setting_value")
    ).where(expr("setting_name = setting_name_prev")).drop("setting_name_prev", "pos", "pos_prev").distinct()

    # set player_previous_setting_value only if it is not equal to player_current_setting_value
    result_df = result_df.withColumn("player_previous_setting_value", 
                                     when(col("player_previous_setting_value") != col("player_current_setting_value"), col("player_previous_setting_value"))
                                     .otherwise(lit(None))).distinct()
    result_df = result_df.withColumn("player_previous_setting_value", 
                                 when(col("player_previous_setting_value").isNull(), col("player_current_setting_value"))
                                 .otherwise(col("player_previous_setting_value"))).distinct()
    result_df = result_df.filter(col("setting_instance_id") != '00000000-0000-0000-0000-000000000000')
    
    result_df = result_df.withColumn("is_first_seen_player_setting", when(col("player_previous_setting_value").isNull(), lit(True)).otherwise(col("is_first_seen_player_setting"))).distinct()

    final_result_df = result_df.groupBy(
        "date", "player_id", "platform", "service","game_type", "setting_instance_id", "setting_name","country_code"
    ).agg(
        first("setting_instance_first_seen_ts").alias("setting_instance_first_seen_ts"),
        first("setting_instance_last_seen_ts").alias("setting_instance_last_seen_ts"),
        first("player_current_setting_value").alias("player_current_setting_value"),
        first("player_previous_setting_value").alias("player_previous_setting_value"),
        first("is_first_seen_player_setting").alias("is_first_seen_player_setting")
    )

    return final_result_df


# COMMAND ----------

def process_player_game_settings():
    
    spark = create_spark_session(name=f"{database}")

    checkpoint_location = create_fact_player_game_settings_daily(spark, database, view_mapping)
    prev_max_ts = max_timestamp(spark, f"{database}{environment}.managed.fact_player_game_settings_daily")
    game_settings_df, fact_player_df, = extract(spark, environment, prev_max_ts)
    result_df = transform(game_settings_df, fact_player_df)

    load_fact_player_game_settings_daily(spark, result_df, database, environment)


# COMMAND ----------

process_player_game_settings()
