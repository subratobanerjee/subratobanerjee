# Databricks notebook source
from pyspark.sql.functions import (col,session_window,lit,first,coalesce,expr)
from functools import reduce
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run ../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../utils/ddl/intermediate/gbx_fact_player_session

# COMMAND ----------

# Global defaults for columns / filter values can be updated with local defaults

global_defaults = {
    "application_session_id": "execution_guid",
    "player_platform_id": "game_session_primary_player_id_platformid",
    "player_id": "game_session_primary_player_id_string",
    "platform": "platform_string",
    "received_on": "maw_time_received"
}

def withColumnExpr(df: DataFrame, column_mapping: dict) -> DataFrame:
    for new_name, expression in column_mapping.items():
        if new_name == "filter":  # Skip filter as it's not a column transformation
            continue
            
        # Use F.expr() for all column transformations - handles both simple columns and SQL expressions
        print(f"Adding/transforming column '{new_name}' with expression: {expression}")
        df = df.withColumn(new_name, F.expr(expression))
    
    if 'received_on' in df.columns:
        df = df.withColumn("received_on", col("received_on").cast("timestamp"))
        df = df.fillna({'player_id': 'Unknown', 'application_session_id': 'Unknown'})
    return df

def generate_sql_expr(table_columns):
    agg_expr ,group_by_expr = [] , []
    for column, expression in table_columns.items():
        if column == "session_type":
            if "CASE WHEN" in expression:  # Handle dynamic SQL logic for session_type
                group_by_expr.append(F.expr(expression).alias(column))
            else:  # Static logic
                group_by_expr.append(F.lit(expression).alias(column))
        else:
            if expression and expression != "Null":
                agg_expr.append(F.expr(expression).alias(column))
            else:
                agg_expr.append(F.lit(-1).alias(column))
    return agg_expr, group_by_expr

def extract_session(source_tables,title, spark):
    session_df = []

    for source_config in source_tables:
        column_mapping = source_config.get("column_mapping", {})
        merged_column_mapping = {
            key: column_mapping.get(key, global_defaults[key])
            for key in global_defaults
        }
        event_filter = column_mapping.get("filter", "1 = 1")
        combined_filter = F.expr(event_filter)


        table_path = f"{source_config['catalog']}{environment}.{source_config['schema']}.{source_config['table']}"
        print(f"Reading from: {table_path}")

        df = (
            spark
            .readStream
            .option("skipChangeCommits", "true")
            .table(table_path)
            .where(combined_filter)
        )
       
        df = withColumnExpr(df, merged_column_mapping)

        source_columns = df.columns
        needs_dim_title_join = "platform" not in source_columns or "service" not in source_columns or "hardware" not in source_columns
    
        if needs_dim_title_join:
            # Add missing columns to CSS DataFrame
            df = (
                df.withColumn("service", lit(None))
            )
            # Enrich missing columns with title_df
            title_df = (
                spark.read.table(f"reference{environment}.title.dim_title")
                .selectExpr(
                "title",
                "platform as title_platform",
                "service as title_service",
                "display_platform",
                "display_service"
            )
            .where(expr(f"title = '{title}'"))
            )
            # Enrich missing columns with hardware_df
            hardware_df = (
                spark.read.table(f"reference{environment}.platform.dim_platform_hardware")
                .selectExpr(
                "platform as hw_platform",
                "service as hw_service",
                "hardware"
                )
            )
            df = (
                df.join(
                    title_df.alias("title"),
                    F.lower(
                        F.when(F.col("platform") == "PS5Pro", "PS5")
                        .when(F.col("platform") == "XSX", "XBSX")
                        .when(F.col("platform") == "Switch2", "NSW2")
                        .otherwise(F.col("platform"))
                    ) == F.lower(F.col("title.display_platform")),
                    how="left"
                )
                .join(
                    hardware_df.alias("hardware"),
                    F.lower(
                        F.when(F.col("platform") == "XSX", "XBSX")
                        .when(F.col("platform") == "Switch2", "NSW2")
                        .otherwise(F.col("platform"))
                    ) == F.lower(F.col("hardware.hw_platform")),
                    how="left"
                )
                .withColumn("platform", coalesce(F.col("title.display_platform"), F.col("platform")))
                .withColumn("service", coalesce(F.col("title.display_service"), F.col("service")))
                .withColumn("hardware", coalesce(F.col("hardware.hardware"), F.col("platform")))
            )

        session_df.append(df)
    session = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), session_df)
    session = session.fillna({'platform': 'Unknown', 'service': 'Unknown', 'hardware': 'Unknown'})
    return session


def transform_load_session(session,table_columns, database,global_settings,spark):
    agg_expr, group_by_expr = generate_sql_expr(table_columns)
    watermark_duration = global_settings.get("watermark_duration", "30 minutes")
    session_window_duration = global_settings.get("session_window_duration", "10 minutes")
    session_df = session

    window_spec = Window.partitionBy("application_session_id", "player_id", "player_platform_id", "platform", "service", "hardware").orderBy("received_on")

    session_df = (
        session_df
        .withWatermark("received_on", watermark_duration)
        .groupBy(
            session_window("received_on", session_window_duration).alias("window"),
            "application_session_id", 
            "player_id",
            "player_platform_id",
            "platform",
            "service",
            "hardware",
            *group_by_expr
        )
        .agg(*agg_expr,
             F.min("received_on").alias("session_start_ts"),
             F.max ("received_on").alias("session_end_ts")
           )
        .withColumn(
            "merge_key", 
            F.sha2(F.concat_ws("|", "application_session_id", "player_id", "player_platform_id", "platform", "service", "hardware"), 256)
        )
    )

    # Select final columns, handling missing values and applying aliases
    session_df = session_df.select(
        "application_session_id",
        "window",
        "player_id",
        "player_platform_id",
        "service",
        "hardware",
        "session_type",
        "platform",
        "session_start_ts",
        "session_end_ts",
        "local_multiplayer_instances",
        "online_multiplayer_instances",
        "solo_instances",
        *[F.expr(f"agg_{i}").alias(f"agg_{i}") for i in range(1, 6)],
        F.current_timestamp().alias("dw_insert_ts"),
        F.current_timestamp().alias("dw_update_ts"),
        "merge_key"
    )

    session_df = session_df.dropDuplicates(["application_session_id", "player_id", "player_platform_id", "platform", "service", "hardware", "merge_key"])


    update_set = {
        "session_start_ts": "LEAST(old.session_start_ts, new.session_start_ts)",
        "session_end_ts": "GREATEST(old.session_end_ts, new.session_end_ts)",
        "local_multiplayer_instances": "GREATEST(old.local_multiplayer_instances, new.local_multiplayer_instances)",
        "online_multiplayer_instances": "GREATEST(old.online_multiplayer_instances, new.online_multiplayer_instances)",
        "solo_instances": "GREATEST(old.solo_instances, new.solo_instances)",
        "agg_1": "GREATEST(old.agg_1, new.agg_1)",
        "agg_2": "GREATEST(old.agg_2, new.agg_2)",
        "agg_3": "GREATEST(old.agg_3, new.agg_3)",
        "agg_4": "GREATEST(old.agg_4, new.agg_4)",
        "agg_5": "GREATEST(old.agg_4, new.agg_5)",
        "dw_update_ts": "CURRENT_TIMESTAMP()"
    }

    # Perform the merge operation into the Delta table
    final_sessions_table = DeltaTable.forName(spark, f"{database}{environment}.intermediate.fact_player_session")
    (
        final_sessions_table.alias('old')
        .merge(
            session_df.alias('new'),
            "new.merge_key = old.merge_key"
            # merge_condition
        )
        # .whenMatchedUpdate(condition=merge_condition, set=update_set)
        .whenMatchedUpdate(set=update_set)
        .whenNotMatchedInsertAll()
        .execute()
    )

def stream_player_session(source_tables, table_columns,database,title,global_settings,trigger="streaming"):
    database = database.lower()
    spark = create_spark_session(name=f"{database}")
    checkpoint_location = create_fact_player_session(spark, database)

    all_sessions_df = extract_session(source_tables, title,spark)
    print("started writing")

    if trigger == "streaming":
        (
            all_sessions_df
            .writeStream
            .trigger(processingTime="10 minutes")
            .outputMode("update")
            .foreachBatch(lambda all_sessions_df, batch_id: transform_load_session(all_sessions_df,table_columns,database,global_settings, spark))
            .option("checkpointLocation", checkpoint_location)
            .start()
        )
    else:
        (
            all_sessions_df
            .writeStream
            .trigger(availableNow=True)
            .outputMode("update")
            .foreachBatch(lambda all_sessions_df, batch_id: transform_load_session(all_sessions_df,table_columns,database,global_settings, spark))
            .option("checkpointLocation", checkpoint_location)
            .start()
        )
