# Databricks notebook source
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
from delta.tables import DeltaTable
import argparse
from pyspark.sql import SparkSession

# COMMAND ----------

def arg_parser():
    parser = argparse.ArgumentParser(description="Example script to consume parameters from Databricks job")
    parser.add_argument("--environment", type=str, required=True, help="Description for param1")
    parser.add_argument("--checkpoint_location", type=str, required=True, help="Description for param2")
    
    args = parser.parse_args()
    environment = args.environment
    checkpoint_location = args.checkpoint_location 
    return environment,checkpoint_location

# COMMAND ----------

def extract(environment, spark, title, database):
    login_df = (
        spark
        .read
        .table(f"{database}{environment}.intermediate.fact_player_activity")
        .where(col("source_table")=='loginevent')
        .where((col("platform").isNotNull()) & (col("platform") != 'Unknown'))
        .withColumn("title", lit(title))
        .alias("le")
    )

    return login_df


# COMMAND ----------

def transform(df, environment, spark, column_override={}, agg_override={}):
    base_columns = {
        "title": col("le.title"),
        "platform": col("le.platform"),
        "service": col("le.service"),
        "player_id": col("le.player_id"),
        "login_ts": col("le.received_on"),
        "login_date": col("le.received_on").cast("date"),
        "sessionid": col("le.session_id"),
        "country_code": col("le.country_code"),
        "received_on": col("le.received_on")
    }
    
    base_column_map = {}
    for key, default_col in base_columns.items():
        if key in column_override:
            base_column_map[key] = column_override[key]
        else:
            base_column_map[key] = default_col

    window_spec = Window.partitionBy(
        base_column_map["player_id"],
        base_column_map["title"],
        base_column_map["platform"],
        base_column_map["service"]
    ).orderBy(
        base_column_map["received_on"],
        base_column_map["country_code"]
    ).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    transformed_df = (
        df.withColumns({
            **base_column_map,
            "country_code": coalesce(
                first(
                    when(base_column_map["country_code"] != "ZZ", base_column_map["country_code"]),
                    True
                ).over(window_spec),
                lit("ZZ")
            )
        })
        .groupBy(
            "title",
            "platform",
            "service",
            "player_id",
            "login_date",
            "country_code",
        )
        .agg(
            ifnull(lead(col("login_date"))
                   .over(Window.partitionBy(["title", "player_id", "platform"])
                         .orderBy(desc(col("login_date")))), col("login_date")).alias("last_seen_2"),
            ifnull(lead(col("login_date"), 2)
                   .over(Window.partitionBy(["title", "player_id", "platform"])
                         .orderBy(desc(col("login_date")))), col("last_seen_2")).alias("last_seen_3"),
            agg_override.get("login_count", count_distinct(col("sessionid"))).alias("login_count"),
            dense_rank().over(Window.partitionBy(["title", "player_id", "platform"])
                         .orderBy(col("login_date"))).alias("activity_day")
        )
        .withColumns({
            "dw_insert_ts": current_timestamp(),
            "dw_update_ts": current_timestamp(),
            "merge_key": sha2(concat_ws("", col("title"), col("platform"), col("player_id"), col("login_date")), 256)
        })
    )

    return transformed_df

# COMMAND ----------

def load(df, environment, spark):
    (
        DeltaTable.createIfNotExists(spark)
        .tableName(f"dataanalytics{environment}.standard_metrics.fact_login")
        .addColumn("title", "string")
        .addColumn("platform", "string")
        .addColumn("service", "string")
        .addColumn("player_id", "string")
        .addColumn("login_date", "date")
        .addColumn("country_code", "string")
        .addColumn("last_seen_2", "date")
        .addColumn("last_seen_3", "date")
        .addColumn("login_count", "integer")
        .addColumn("activity_day", "integer")
        .addColumn("dw_insert_ts", "timestamp")
        .addColumn("dw_update_ts", "timestamp")
        .addColumn("merge_key", "string")
        .property('delta.enableIcebergCompatV2', 'true')
        .property('delta.universalFormat.enabledFormats', 'iceberg')
        .execute()
    )

    target_table = DeltaTable.forName(spark, f"dataanalytics{environment}.standard_metrics.fact_login")

    (
        target_table
        .alias("target")
        .merge(
            df.alias("source"),
            "target.merge_key = source.merge_key")
        .withSchemaEvolution()
        .whenMatchedUpdate(
            condition = "(target.login_count != source.login_count and target.activity_day != source.activity_day) or target.country_code != source.country_code",
            set = {
                "target.country_code": "source.country_code",
                "target.login_count": "source.login_count",
                "target.activity_day": "source.activity_day",
                "target.last_seen_2": "source.last_seen_2",
                "target.last_seen_3": "source.last_seen_3",
                "target.dw_update_ts": "current_timestamp()"
        })
        .whenMatchedUpdate(
            condition = "(target.login_count != source.login_count and target.activity_day = source.activity_day) or target.country_code != source.country_code",
            set = {
                "target.country_code": "source.country_code",
                "target.login_count": "source.login_count",
                "target.dw_update_ts": "current_timestamp()"
        })
        .whenMatchedUpdate(
            condition = "(target.login_count = source.login_count and target.activity_day != source.activity_day) or target.country_code != source.country_code",
            set = {
                "target.country_code": "source.country_code",
                "target.activity_day": "source.activity_day",
                "target.last_seen_2": "source.last_seen_2",
                "target.last_seen_3": "source.last_seen_3",
                "target.dw_update_ts": "current_timestamp()"
        })
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

def run_batch():
    environment = dbutils.widgets.get("environment")
    checkpoint_location = dbutils.widgets.get("checkpoint")
    #environment, checkpoint_location = "_dev", "dbfs:/tmp/standard_metrics/managed/batch/run_dev/fact_login"
    spark = SparkSession.builder.appName("Hydra").getOrCreate()
    title_db = {'Civilization VII': 'inverness','Mafia: The Old Country': 'nero','Borderlands 4' : 'oak2'}

    spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
    column_overrides = {'Borderlands 4': {'player_id': col("le.extra_info_1")}}
    agg_overrides = {'Borderlands 4': {'login_count': count(col("player_id"))}}
    
    for title,database in title_db.items():
        df = extract(environment, spark, title, database)
        column_overrides_for_title = column_overrides.get(title, {}) 
        agg_overrides_for_title = agg_overrides.get(title, {}) 
        df = transform(df, environment, spark, column_override=column_overrides_for_title, agg_override=agg_overrides_for_title)

        load(df, environment, spark)

# COMMAND ----------

if __name__ == "__main__":
    run_batch()