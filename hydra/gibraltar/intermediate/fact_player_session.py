# Databricks notebook source
from pyspark.sql.functions import (col,session_window,lit,first,coalesce)
from functools import reduce
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run ../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../utils/ddl/intermediate/fact_player_session

# COMMAND ----------

# Global defaults for columns / filter values can be updated with local defaults

global_defaults = {
    "application_session_id": "applicationsessionid",
    "player_id": "playerPublicId",
    "app_public_id": "appPublicId",
    "received_on": "receivedOn",
    "country_code":"CountryCode"
}

def withColumnExpr(df: DataFrame, column_mapping: dict) -> DataFrame:
    normalized_columns = [col.lower() for col in df.columns]
    normalized_mapping = {key.lower(): value.lower() for key, value in column_mapping.items()}

    for new_name, old_name in normalized_mapping.items():
        if old_name in normalized_columns:
            print(f"Renaming column '{old_name}' to '{new_name}'")
            df = df.withColumnRenamed(old_name, new_name)
        else:
            print(f"Column '{old_name}' not found in DataFrame; skipping renaming.")    
    if 'received_on' in df.columns:
        df = df.withColumn("received_on", col("received_on").cast("timestamp"))
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
        needs_dim_title_join = "platform" not in source_columns or "service" not in source_columns
    
        if needs_dim_title_join:
            # Add missing columns to CSS DataFrame
            df = (
                df.withColumn("platform", lit(None))
                .withColumn("service", lit(None))
            )

            # Enrich missing columns with title_df
            title_df = (
                spark.read.table(f"reference{environment}.title.dim_title")
                .select("app_id", "display_platform", "display_service")
            )
            df = (
                df.join(
                    title_df.alias("title"),
                    F.col("app_public_id") == F.col("title.app_id"),
                    how="left"
                )
                .withColumn("platform", coalesce(F.col("platform"), F.col("title.display_platform")))
                .withColumn("service", coalesce(F.col("service"), F.col("title.display_service")))
            )

        session_df.append(df)
    session = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), session_df)
    return session


def transform_load_session(session,table_columns, database,global_settings,spark):
    agg_expr, group_by_expr = generate_sql_expr(table_columns)
    watermark_duration = global_settings.get("watermark_duration", "30 minutes")
    session_window_duration = global_settings.get("session_window_duration", "10 minutes")
    session_df = session
    
    window_spec = Window.partitionBy("application_session_id", "player_id", "platform", "service").orderBy("received_on")
    session_df = session_df.withColumn(
        "country_code",
        first(coalesce(col("country_code"), lit("ZZ")), ignorenulls=True).over(window_spec)
    )

    session_df = (
        session_df
        .withWatermark("received_on", watermark_duration)
        .groupBy(
            session_window("received_on", session_window_duration).alias("window"),
            "application_session_id", 
            "player_id",
            "platform",
            "service",
            "country_code",
            *group_by_expr
        )
        .agg(*agg_expr,
             F.min("received_on").alias("session_start_ts"),
             F.max ("received_on").alias("session_end_ts")
           )
        .withColumn(
            "merge_key", 
            F.sha2(F.concat_ws("|", "application_session_id", "player_id", "platform", "service"), 256)
        )
    )

    # Select final columns, handling missing values and applying aliases
    session_df = session_df.select(
        "application_session_id",
        "window",
        "player_id",
        "service",
        "country_code" ,
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

    # Drop duplicates based on business keys
    session_df = session_df.dropDuplicates(["application_session_id", "player_id", "platform", "service","country_code", "merge_key"])

    update_set = {
    "session_start_ts": "LEAST(old.session_start_ts, new.session_start_ts)",
    "session_end_ts": "GREATEST(old.session_end_ts, new.session_end_ts)",
    "local_multiplayer_instances": "COALESCE(old.local_multiplayer_instances, 0) + COALESCE(new.local_multiplayer_instances, 0)",
    "online_multiplayer_instances": "COALESCE(old.online_multiplayer_instances, 0) + COALESCE(new.online_multiplayer_instances, 0)",
    "solo_instances": "COALESCE(old.solo_instances, 0) + COALESCE(new.solo_instances, 0)",
    "agg_1": "COALESCE(old.agg_1, 0) + COALESCE(new.agg_1, 0)",
    "agg_2": "COALESCE(old.agg_2, 0) + COALESCE(new.agg_2, 0)",
    "dw_update_ts": "CURRENT_TIMESTAMP()"
    }
    # Perform the merge operation into the Delta table
    final_sessions_table = DeltaTable.forName(spark, f"{database}{environment}.intermediate.fact_player_session")
    (
        final_sessions_table.alias('old')
        .merge(
            session_df.alias('new'),
            "new.merge_key = old.merge_key"
        )
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
