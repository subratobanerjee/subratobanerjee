# Databricks notebook source
# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/table_functions

# COMMAND ----------

from pyspark.sql.functions import expr
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database = 'oak2'
spark = create_spark_session()

# COMMAND ----------

def create_player_mission_stats_ledger(spark, environment):
    sql = f"""
    CREATE TABLE IF NOT EXISTS oak2{environment}.intermediate.player_mission_stats_ledger (
        player_id STRING,
        title STRING,
        platform STRING,
        first_mission_start_ts TIMESTAMP,
        first_prison_completed_ts TIMESTAMP,
        first_beach_completed_ts TIMESTAMP,
        mission_started BOOLEAN,
        prison_completed BOOLEAN,
        beach_completed BOOLEAN,
        dw_insert_ts TIMESTAMP,
        dw_update_ts TIMESTAMP,
        merge_key STRING
    )
    """
    create_table(spark, sql)


def create_agg_mission_stats_nrt(spark, database, environment):

    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.agg_mission_stats_nrt (
        timestamp_10min_slice TIMESTAMP,
        title STRING,
        platform STRING,
        started_missions BIGINT,
        completed_intro_mission BIGINT,
        completed_first_mission BIGINT,
        merge_key STRING,
        dw_insert_ts TIMESTAMP,
        dw_update_ts TIMESTAMP
    )
    """
    create_table(spark, sql)
    return f"dbfs:/tmp/{database}/managed/streaming/run{environment}/agg_mission_stats_nrt"

# COMMAND ----------

def extract(environment):

    return (
        spark
        .readStream
        .option('maxFilesPerTrigger', 10000)
        .table(f"oak2{environment}.raw.oaktelemetry_mission_state")
        .filter("BuildConfig_string = 'Shipping Game'")
    )

# COMMAND ----------

def transform(mission_df, environment, spark):

    title_df = spark.read.table(f"reference{environment}.title.dim_title").alias("t")
    ledger_df = spark.read.table(f"oak2{environment}.intermediate.player_mission_stats_ledger").alias("lkp")

    platform_join_expr = "(case when m.platform_string = 'XSX' then 'XBSX' when m.platform_string = 'PS5Pro' then 'PS5' when lower(m.platform_string) = 'sage' then 'NSW2' else m.platform_string end) = t.display_platform"
    
    joined_df = (
        mission_df.alias("m")
        .join(title_df, on=expr(platform_join_expr), how="left")
        .filter(expr("t.title = 'Borderlands 4'"))
        .selectExpr(
            "t.title",
            "first_value(t.display_platform) OVER (PARTITION BY m.player_id ORDER BY m.maw_time_received::timestamp) as platform",
            "m.player_id",
            "window(m.maw_time_received::timestamp, '10 minutes').end as mission_start_ts",
            "case when lower(m.event_value_string) like '%mission_main_prisonprologue%' and lower(m.mission_objective_id_string) in ('none','revivearjay') and lower(m.event_key_string) = 'completed' then window(m.maw_time_received::timestamp, '10 minutes').end else null end as prison_completed_ts",
            "case when lower(m.event_value_string) like '%mission_main_beach%' and lower(m.mission_objective_id_string) in ('none','start_broadcast') and lower(m.event_key_string) = 'completed' then window(m.maw_time_received::timestamp, '10 minutes').end else null end as beach_completed_ts"
        ).distinct()
    )
    
    player_df = (
        joined_df
        .select("player_id", "title", "platform", "mission_start_ts", "prison_completed_ts", "beach_completed_ts")
        .distinct()
        .selectExpr("*", "sha2(concat_ws('|', player_id, title, platform), 256) as mission_stats_join_key")
    )

    new_players = (
        player_df.alias("gs")
        .join(ledger_df, on=expr("gs.mission_stats_join_key = lkp.merge_key"), how="left")
        .where(expr("lkp.player_id is null or lkp.prison_completed = false or lkp.beach_completed = false or lkp.mission_started = false"))
    )

    first_events_df = (
        new_players
        .groupBy("gs.player_id", "gs.title", "gs.platform", "mission_stats_join_key")
        .agg(
            expr("min(mission_start_ts) as cur_mission_start_ts"),
            expr("min(prison_completed_ts) as cur_prison_completed_ts"),
            expr("min(beach_completed_ts) as cur_beach_completed_ts")
        )
    )

    mrg_df = (
        first_events_df
        .selectExpr(
            "player_id", "title", "platform",
            "case when cur_mission_start_ts is not null then true else false end as mission_started",
            "case when cur_prison_completed_ts is not null then true else false end as prison_completed",
            "case when cur_beach_completed_ts is not null then true else false end as beach_completed",
            "cur_mission_start_ts as first_mission_start_ts",
            "cur_prison_completed_ts as first_prison_completed_ts",
            "cur_beach_completed_ts as first_beach_completed_ts",
            "current_timestamp() as dw_insert_ts", "current_timestamp() as dw_update_ts",
            "mission_stats_join_key as merge_key"
        )
    )

    mrg_df_with_lkp = mrg_df.alias("m").join(
        ledger_df.alias("lkp"),
        on=expr("m.merge_key = lkp.merge_key"),
        how="left"
    )

    started = (
        mrg_df_with_lkp
        .filter("m.first_mission_start_ts IS NOT NULL AND lkp.first_mission_start_ts IS NULL") # Only count if player has not started
        .withColumn("start_timestamp_10min_slice", expr("window(m.first_mission_start_ts, '10 minutes').end"))
        .groupBy("start_timestamp_10min_slice", "m.title", "m.platform")
        .agg(
            expr("count(distinct m.player_id) as started_missions"), 
            expr("0 as completed_intro_mission"), 
            expr("0 as completed_first_mission")
        ).selectExpr("start_timestamp_10min_slice as timestamp_10min_slice", "title", "platform", "started_missions", "completed_intro_mission", "completed_first_mission")
    )

    intro = (
        mrg_df_with_lkp
        .filter("m.first_prison_completed_ts IS NOT NULL AND lkp.first_prison_completed_ts IS NULL")
        .withColumn("prison_timestamp_10min_slice", expr("window(m.first_prison_completed_ts, '10 minutes').end"))
        .groupBy("prison_timestamp_10min_slice", "m.title", "m.platform")
        .agg(
            expr("0 as started_missions"), 
            expr("count(distinct m.player_id) as completed_intro_mission"), 
            expr("0 as completed_first_mission")
        ).selectExpr("prison_timestamp_10min_slice as timestamp_10min_slice", "title", "platform", "started_missions", "completed_intro_mission", "completed_first_mission")
    )

    beach = (
        mrg_df_with_lkp
        .filter("m.first_beach_completed_ts IS NOT NULL AND lkp.first_beach_completed_ts IS NULL")
        .withColumn("beach_timestamp_10min_slice", expr("window(m.first_beach_completed_ts, '10 minutes').end"))
        .groupBy("beach_timestamp_10min_slice", "m.title", "m.platform")
        .agg(
            expr("0 as started_missions"), 
            expr("0 as completed_intro_mission"), 
            expr("count(distinct m.player_id) as completed_first_mission")
        ).selectExpr("beach_timestamp_10min_slice as timestamp_10min_slice", "title", "platform", "started_missions", "completed_intro_mission", "completed_first_mission")
    )

    mission_transform_df = (
        started.unionByName(intro).unionByName(beach)
        .groupBy("timestamp_10min_slice", "title", "platform")
        .agg(
            expr("coalesce(max(started_missions), 0) as started_missions"),
            expr("coalesce(max(completed_intro_mission), 0) as completed_intro_mission"),
            expr("coalesce(max(completed_first_mission), 0) as completed_first_mission")
        )
    )
    
    mission_transform_df.write.mode("overwrite").saveAsTable(f"oak2{environment}.intermediate.mission_transform_tmp")

    gs_lkp_table = DeltaTable.forName(spark, f"oak2{environment}.intermediate.player_mission_stats_ledger")
    (
        gs_lkp_table.alias("target")
        .merge(mrg_df.alias("source"), "target.merge_key = source.merge_key")
        .whenMatchedUpdate(set={
            "target.first_mission_start_ts": expr("coalesce(target.first_mission_start_ts, source.first_mission_start_ts)"),
            "target.first_prison_completed_ts": expr("coalesce(target.first_prison_completed_ts, source.first_prison_completed_ts)"),
            "target.first_beach_completed_ts": expr("coalesce(target.first_beach_completed_ts, source.first_beach_completed_ts)"),
            "target.mission_started": expr("greatest(source.mission_started, target.mission_started)"),
            "target.prison_completed": expr("greatest(source.prison_completed, target.prison_completed)"),
            "target.beach_completed": expr("greatest(source.beach_completed, target.beach_completed)"),
            "target.dw_update_ts": "current_timestamp()"
        })
        .whenNotMatchedInsertAll()
        .execute()
    )
    
    mission_transform_table = spark.read.table(f"oak2{environment}.intermediate.mission_transform_tmp")

    final_df = (
        mission_transform_table.select(
            "timestamp_10min_slice",
            "platform",
            "title",
            "started_missions",
            "completed_intro_mission",
            "completed_first_mission"
        )
    )
    
    return final_df

# COMMAND ----------

def load(df, environment, spark):

    df_to_load = (
        df.selectExpr(
            "*",
            "current_timestamp() as dw_insert_ts",
            "current_timestamp() as dw_update_ts",
            "sha2(concat_ws('|', timestamp_10min_slice, title, platform), 256) as merge_key"
        )
    )

    target_table_name = f"oak2{environment}.managed.agg_mission_stats_nrt"
    target_agg_table = DeltaTable.forName(spark, target_table_name)
    update_expr = {
        "started_missions": "target.started_missions + source.started_missions",
        "completed_intro_mission": "target.completed_intro_mission + source.completed_intro_mission",
        "completed_first_mission": "target.completed_first_mission + source.completed_first_mission",
        "dw_update_ts": "current_timestamp()"
    }
    (
        target_agg_table.alias("target")
        .merge(df_to_load.alias("source"), "target.merge_key = source.merge_key")
        .whenMatchedUpdate(set=update_expr)
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

def proc_batch(df, environment):

    transformed_df = transform(df, environment, spark)
    load(transformed_df, environment, spark)

# COMMAND ----------

def run_stream():
    """Main function to set up and start the streaming job."""
    checkpoint_location = create_agg_mission_stats_nrt(spark, database, environment)
    create_player_mission_stats_ledger(spark, environment)
    df = extract(environment)

    (
        df
        .writeStream
        .queryName("Oak2 Agg Mission Stats NRT")
        .trigger(processingTime = "10 minutes")
        .option("checkpointLocation", checkpoint_location)
        .foreachBatch(lambda df, batch_id: proc_batch(df, environment))
        .start()
    )

# COMMAND ----------

run_stream()