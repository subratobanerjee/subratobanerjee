# Databricks notebook source
# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/reference/platform_id_mapping

# COMMAND ----------

# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

from pyspark.sql.functions import sha2, concat_ws, expr, first, col
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment', set_environment())
database = 'oak2'
# environment='_stg'
data_source = 'gearbox'
title = 'Borderlands 4'

# COMMAND ----------

def extract(spark, database, environment):
    """
    Loads all necessary source tables from the data warehouse.
    """
    oaktelemetry_mission_state = spark.table(f"{database}_prod.raw.oaktelemetry_mission_state")
    oaktelemetry_game_session = spark.table(f"{database}_prod.raw.oaktelemetry_game_session")
    
    return (
        oaktelemetry_mission_state,
        oaktelemetry_game_session
    )

# COMMAND ----------

def transform_latest_players(mission_state_df, game_session_df):
    """
    Creates the latest_players dataset by unioning mission_state and game_session data
    """
    # First part of union - mission state data
    mission_state_players = (
        mission_state_df
        .filter(
            # expr("lower(event_key_string) = 'active'") &
            # expr("lower(mission_objective_id_string) = 'none'") & -- removed
            expr("buildconfig_string = 'Shipping Game'") &
            expr("host_id = player_platform_id") &
            expr("player_connection = 'local'")
        )
        .groupBy("maw_user_platformid", "player_platform_id")
        .agg(
            expr("min(maw_time_received)").alias("maw_time_received")
        )
        .filter(expr("cast(maw_time_received as date) >= '2025-08-22'"))
        .select(
            col("maw_time_received"),
            col("player_platform_id"),
            col("maw_user_platformid")
        )
    )
    
    # Second part of union - game session data
    game_session_players = (
        game_session_df
        .filter(
            # expr("player_time_played = 0") & -- removed
            expr("buildconfig_string = 'Shipping Game'") &
            expr("host_id = player_platform_id") &
            expr("player_connection = 'local'")
        )
        .groupBy("maw_user_platformid", "player_platform_id")
        .agg(
            expr("min(maw_time_received)").alias("maw_time_received")
        )
        .filter(expr("cast(maw_time_received as date) >= '2025-08-22'"))
        .select(
            col("maw_time_received"),
            col("player_platform_id"),
            col("maw_user_platformid")
        )
    )
    
    # Union the two datasets
    latest_players = mission_state_players.union(game_session_players).distinct()
    
    return latest_players

# COMMAND ----------

def transform_platform_mapping(latest_players_df):
    """
    Creates the platform ID mapping by getting the first platform ID for each user
    and filtering to users with only one unique platform ID
    """
    # Create mapping - get first platform_id for each maw_user_platformid
    window_spec = Window.partitionBy("maw_user_platformid").orderBy("maw_time_received")
    
    mapping = (
        latest_players_df
        .withColumn("player_platform_id_ranked", first("player_platform_id").over(window_spec))
        .select(
            col("maw_user_platformid"),
            col("player_platform_id_ranked").alias("player_platform_id")
        )
        .distinct()
    )
    
    # Create comparison - find users with exactly 1 distinct platform_id
    comparison = (
        mapping
        .groupBy("maw_user_platformid")
        .agg(
            expr("count(distinct player_platform_id)").alias("platform_count")
        )
        .filter(expr("platform_count = 1"))
        .select("maw_user_platformid")
    )
    
    # Final join to get only users with single platform mapping
    final_mapping = (
        mapping.alias("l")
        .join(
            comparison.alias("c"),
            expr("l.maw_user_platformid = c.maw_user_platformid"),
            "inner"
        )
        .select(
            col("l.maw_user_platformid"),
            col("l.player_platform_id")
        )
        .distinct()
    )
    
    return final_mapping

# COMMAND ----------

def load(df, database, environment, spark):
    """
    Loads the transformed data into the target table.
    Excludes maw_user_platformid already present with non-null player_platform_id.
    Never updates existing non-null player_platform_id values.
    """
    target_table_name = f"{database}{environment}.reference.platform_id_mapping"
    
    # Create the table using DDL (assumes the DDL function exists)
    create_platform_id_mapping(spark, database, environment)
    
    # Filter out rows where maw_user_platformid already exists with non-null player_platform_id
    try:
        existing_users_with_mapping = (
            spark.table(target_table_name)
            .filter(expr("player_platform_id is not null"))
            .select("maw_user_platformid")
            .distinct()
        )
        
        # Exclude these users from the source data
        filtered_df = (
            df.alias("source")
            .join(
                existing_users_with_mapping.alias("existing"),
                expr("source.maw_user_platformid = existing.maw_user_platformid"),
                "left_anti"
            )
        )
    except:
        # Table doesn't exist yet, use all data
        filtered_df = df
    
    # Only proceed if we have data to load
    if filtered_df.count() > 0:
        target_df = DeltaTable.forName(spark, target_table_name)
        
        merger_condition = 'target.merge_key = source.merge_key'
        
        # Dynamic update logic - only update if target player_platform_id is null
        update_expr = {col: f"source.{col}" for col in filtered_df.columns if col not in ['dw_insert_ts', 'merge_key']}
        update_expr["dw_update_ts"] = "current_timestamp()"
        
        insert_expr = {col: f"source.{col}" for col in filtered_df.columns}
        
        (target_df.alias("target")
         .merge(filtered_df.alias("source"), merger_condition)
         .whenMatchedUpdate(
             condition="target.player_platform_id is null",
             set=update_expr
         )
         .whenNotMatchedInsert(values=insert_expr)
         .execute())

# COMMAND ----------

def run_batch(database, environment):

    database = database.lower()
    spark = create_spark_session(name=f"{database}_platform_mapping")
    
    oaktelemetry_mission_state, oaktelemetry_game_session = extract(spark, database, environment)
    
    latest_players_df = transform_latest_players(oaktelemetry_mission_state, oaktelemetry_game_session)
    platform_mapping_df = transform_platform_mapping(latest_players_df)
    
    key_cols = ["maw_user_platformid", "player_platform_id"]
    final_df = (
        platform_mapping_df
        .withColumn("dw_update_ts", expr("current_timestamp()"))
        .withColumn("dw_insert_ts", expr("current_timestamp()"))
        .withColumn("merge_key", sha2(concat_ws("||", *key_cols), 256))
    )
    
    load(final_df, database, environment, spark)

# COMMAND ----------

run_batch(database, environment)
