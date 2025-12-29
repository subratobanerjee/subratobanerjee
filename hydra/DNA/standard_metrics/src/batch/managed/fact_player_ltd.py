# Databricks notebook source
import json
from pyspark.sql.functions import (lit, col, when, get_json_object, current_timestamp, split, date_trunc, coalesce, least, to_date, row_number, datediff, sha2, concat_ws, countDistinct, first)
from delta.tables import DeltaTable
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
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

def read_logins_df(environment, spark):
    
    logins_df = (
        spark
        .read
        .table(f"dataanalytics{environment}.standard_metrics.fact_login")
        .alias("logins")
        #.where("dw_update_ts::date >= current_date - interval '3 day'")
    )

    window_country = (
        Window.partitionBy("player_id", "platform", "service", "title")
            .orderBy(desc("login_date"))
    )
    logins_df = (
        logins_df.withColumn(
            "country_code",
            first(when(col("country_code") != "ZZ", col("country_code")), ignorenulls=True)
            .over(window_country)
        )
        .withColumn(
            "country_code",
            when(col("country_code").isNull(), "ZZ").otherwise(col("country_code"))
        )
    )

    return logins_df

# COMMAND ----------

def transform(df, environment,spark):
    transformed_df = (
        df
        .where(col("player_id").isNotNull())

        .groupBy(
            "player_id",
            "platform",
            "service",
            "country_code",
            "title",
            "login_date",
            "last_seen_2",
            "last_seen_3",
            "activity_day",
            "login_count"
        )
        .agg(
            min(col("login_date")).over(Window.partitionBy(col("player_id"), col("platform"), col("title"))).alias("install_date"),
            max(col("login_date")).over(Window.partitionBy(col("player_id"), col("platform"), col("title"))).alias("last_seen"),
            max(col("last_seen_2")).over(Window.partitionBy(col("player_id"), col("platform"), col("title"))).alias("last_seen_2_"),
            max(col("last_seen_3")).over(Window.partitionBy(col("player_id"), col("platform"), col("title"))).alias("last_seen_3_"),
            max(col("activity_day")).over(Window.partitionBy(col("player_id"), col("platform"), col("title"))).alias("total_active_days"),
            sum(col("login_count")).over(Window.partitionBy(col("player_id"), col("platform"), col("title"))).alias("total_logins")
        )
        .withColumns({
                    
                    "first_30_days_game_days": when(datediff(col("install_date"), col("login_date")) <= 29 , col("login_date")).otherwise(lit(None)),
                    "Last_30_days_game_days": when(datediff(col("login_date"), current_date()) <= 29 , col("login_date")).otherwise(lit(None)),
                    "first_30_days_logins": when(datediff(col("install_date"), col("login_date")) <= 29 , col("login_count")).otherwise(0),
                    "Last_30_days_logins": when(datediff(col("login_date"), current_date()) <= 29 , col("login_count")).otherwise(0),
                    "first_21_b2b_day": when((datediff(col("install_date"), col("login_date")) <= 20) & (datediff(col("login_date"), col("last_seen_2"))==1) & (datediff(col("login_date"), col("install_date")) > 1),
                     col("login_date")).otherwise(lit(None)),
                    "day_1_": when(datediff(col("login_date"), col("install_date")) == 1 , lit(1)).otherwise(lit(0)),
                    "day_2_": when(datediff(col("login_date"), col("install_date")) == 2 , lit(1)).otherwise(lit(0)),
                    "day_3_": when(datediff(col("login_date"), col("install_date")) == 3 , lit(1)).otherwise(lit(0)),
                    "day_5_": when(datediff(col("login_date"), col("install_date")) == 5 , lit(1)).otherwise(lit(0)),
                    "day_7_": when(datediff(col("login_date"), col("install_date")) == 7 , lit(1)).otherwise(lit(0)),
                    "day_30_": when(datediff(col("login_date"), col("install_date")) == 30 , lit(1)).otherwise(lit(0)),
                    "day_60_": when(datediff(col("login_date"), col("install_date")) == 60 , lit(1)).otherwise(lit(0)),
                    "day_90_": when(datediff(col("login_date"), col("install_date")) == 90 , lit(1)).otherwise(lit(0)),

                    "week_1_": when(ceil(datediff(col("login_date"), col("install_date"))/7) == 1 , lit(1)).otherwise(lit(0)),
                    "week_2_": when(ceil(datediff(col("login_date"), col("install_date"))/7) == 2 , lit(1)).otherwise(lit(0)),
                    "week_3_": when(ceil(datediff(col("login_date"), col("install_date"))/7) == 3 , lit(1)).otherwise(lit(0)),
                    "week_4_": when(ceil(datediff(col("login_date"), col("install_date"))/7) == 4 , lit(1)).otherwise(lit(0)),

                    "month_1_": when(round(months_between(col("login_date"), col("install_date")),0) == 1 , lit(1)).otherwise(lit(0)),
                    "month_2_": when(round(months_between(col("login_date"), col("install_date")),0) == 2 , lit(1)).otherwise(lit(0)),
                    "month_6_": when(round(months_between(col("login_date"), col("install_date")),0) == 6 , lit(1)).otherwise(lit(0)),
                    "month_12_": when(round(months_between(col("login_date"), col("install_date")),0) == 12 , lit(1)).otherwise(lit(0)),
                    "game_day_5th":when((datediff(col("install_date"), col("login_date")) <= 20) & (col("activity_day") == 5), col("login_date")).otherwise(lit(None))
        })
        .select(
        "player_id",
         "title",
         "platform",
         "service",
         "country_code",
         "install_date",
         "login_date",
         "last_seen",
         col("last_seen_2_").alias("last_seen_2"),
         col("last_seen_3_").alias("last_seen_3"),
         "total_active_days",
         "total_logins",
         "game_day_5th",
         "login_count",
         "activity_day",
         "first_30_days_game_days",
         "Last_30_days_game_days",
         "first_30_days_logins",
         "Last_30_days_logins",
         "first_21_b2b_day",
         "day_1_",
         "day_2_",
         "day_3_",
         "day_5_",
         "day_7_",
         "day_30_",
         "day_60_",
         "day_90_",
         "week_1_",
         "week_2_",
         "week_3_",
         "week_4_",
         "month_1_",
         "month_2_",
         "month_6_",
         "month_12_",
        )
        
    )
    final_transformed_df= (
        transformed_df
        .groupBy(
            "player_id",
            "platform",
            "service",
            "title",
            "install_date",
            "last_seen",
            "last_seen_2",
            "last_seen_3",
            "total_logins",
            "total_active_days"
        )
        .agg(
            first(col("country_code")).alias("country_code"),
            sum(col("first_30_days_logins")).alias("first_30_days_logins"),
            sum(col("Last_30_days_logins")).alias("Last_30_days_logins"),
            countDistinct(col("first_30_days_game_days")).alias("first_30_days_active_days"),
            countDistinct(col("Last_30_days_game_days")).alias("Last_30_days_active_days"),
            countDistinct(col("first_21_b2b_day")).alias("first_21_b2b_days"),
            when( (countDistinct(col("first_21_b2b_day")) >= 1) | (max(col("game_day_5th")).isNotNull()), 
                 coalesce( 
                     least( 
                         coalesce(min(col("first_21_b2b_day")), to_date(lit('9999-01-01'))), 
                         coalesce(max(col("game_day_5th")), to_date(lit('9999-01-01'))) 
                     ), 
                     to_date(lit('9999-01-01')) 
                 ) 
            ).alias("viable_date"),
            max(col("day_1_")).alias("day_1"),
            max(col("day_2_")).alias("day_2"),
            max(col("day_3_")).alias("day_3"),
            max(col("day_5_")).alias("day_5"),
            max(col("day_7_")).alias("day_7"),
            max(col("day_30_")).alias("day_30"),
            max(col("day_60_")).alias("day_60"),
            max(col("day_90_")).alias("day_90"),
            max(col("week_1_")).alias("week_1"),
            max(col("week_2_")).alias("week_2"),
            max(col("week_3_")).alias("week_3"),
            max(col("week_4_")).alias("week_4"),
            max(col("month_1_")).alias("month_1"),
            max(col("month_2_")).alias("month_2"),
            max(col("month_6_")).alias("month_6"),
            max(col("month_12_")).alias("month_12"),
            max(col("activity_day")).alias("activity_day")
            
        )
        .withColumns({
                    "is_first_title_install": row_number().over(Window.partitionBy(col("player_id"), col("title"), col("service")).orderBy(col("install_date"),col("platform") ,col("country_code"))) == 1,
                    "days_since_install": datediff(col("last_seen"),col("install_date")),
                    "merge_key": sha2(concat_ws("|", col("player_id"), col("platform"), col("service"),col("title")), 256),
                    "dw_insert_ts": current_timestamp(),
                    "dw_update_ts": current_timestamp()
        })
        .select
        ("player_id",
         "title",
         "platform",
         "service",
         "country_code",
         "install_date",
         "is_first_title_install",
         "days_since_install",
         "last_seen",
         "last_seen_2",
         "last_seen_3",
         "total_active_days",
         "total_logins",
         "first_30_days_active_days",
         "Last_30_days_active_days",
         "first_30_days_logins",
         "Last_30_days_logins",
         "first_21_b2b_days",
         "viable_date",
         "day_1",
         "day_2",
         "day_3",
         "day_5",
         "day_7",
         "day_30",
         "day_60",
         "day_90",
         "week_1",
         "week_2",
         "week_3",
         "week_4",
         "month_1",
         "month_2",
         "month_6",
         "month_12",
         "merge_key",
         "dw_insert_ts",
         "dw_update_ts"

        )
    )

    return final_transformed_df

    
    

# COMMAND ----------

def load(df, environment, spark):
    (
        DeltaTable.createIfNotExists(spark)
        .tableName(f"dataanalytics{environment}.standard_metrics.fact_player_ltd")
        .addColumn("player_id", "string")
        .addColumn("title", "string")
        .addColumn("platform", "string")
        .addColumn("service", "string")
        .addColumn("country_code", "string")
        .addColumn("install_date", "date")
        .addColumn("is_first_title_install", "string")
        .addColumn("days_since_install", "int")
        .addColumn("last_seen", "date")
        .addColumn("last_seen_2", "date")
        .addColumn("last_seen_3", "date")
        .addColumn("total_active_days", "int")
        .addColumn("total_logins", "int")
        .addColumn("first_30_days_active_days", "int")
        .addColumn("Last_30_days_active_days", "int")
        .addColumn("first_30_days_logins", "int")
        .addColumn("Last_30_days_logins", "int")
        .addColumn("first_21_b2b_days", "int")
        .addColumn("viable_date", "date")
        .addColumn("day_1", "int")
        .addColumn("day_2", "int")
        .addColumn("day_3", "int")
        .addColumn("day_5", "int")
        .addColumn("day_7", "int")
        .addColumn("day_30", "int")
        .addColumn("day_60", "int")
        .addColumn("day_90", "int")
        .addColumn("week_1", "int")
        .addColumn("week_2", "int")
        .addColumn("week_3", "int")
        .addColumn("week_4", "int")
        .addColumn("month_1", "int")
        .addColumn("month_2", "int")
        .addColumn("month_6", "int")
        .addColumn("month_12", "int")
        .addColumn("merge_key", "string")
        .addColumn("dw_insert_ts", "timestamp")
        .addColumn("dw_update_ts", "timestamp")
        .property('delta.enableIcebergCompatV2', 'true')
        .property('delta.universalFormat.enabledFormats', 'iceberg')
        .execute()
    )

    target_table = DeltaTable.forName(spark, f"dataanalytics{environment}.standard_metrics.fact_player_ltd")

    (
        target_table
        .alias("target")
        .merge(
            df.alias("source"),
            "target.merge_key = source.merge_key")
        .withSchemaEvolution()
        .whenMatchedUpdate(condition="(ifnull(source.country_code, 'Null') != ifnull(target.country_code, 'Null')) or (ifnull(source.install_date,'9999-01-01'::date) != ifnull(target.install_date, '9999-01-01'::date))  or (source.is_first_title_install!= target.is_first_title_install) or (ifnull(source.days_since_install,0) != ifnull(target.days_since_install,0)) or (ifnull(source.last_seen,'9999-01-01'::date) != ifnull(target.last_seen,'9999-01-01'::date)) or (ifnull(source.last_seen_2, '9999-01-01'::date) != ifnull(target.last_seen_2,'9999-01-01'::date)) or (ifnull(source.last_seen_3, '9999-01-01'::date) != ifnull(target.last_seen_3,'9999-01-01'::date)) or (ifnull(source.total_active_days,0) != ifnull(target.total_active_days,0)) or (ifnull(source.total_logins,0) != ifnull(target.total_logins,0)) or (ifnull(source.first_30_days_active_days,0) != ifnull(target.first_30_days_active_days,0)) or (ifnull(source.Last_30_days_active_days,0) != ifnull(target.Last_30_days_active_days,0)) or (ifnull(source.first_30_days_logins,0) != ifnull(target.first_30_days_logins,0)) or (ifnull(source.Last_30_days_logins,0) != ifnull(target.Last_30_days_logins,0)) or (ifnull(source.first_21_b2b_days,0) != ifnull(target.first_21_b2b_days,0)) or (ifnull(source.viable_date,'9999-01-01'::date) != ifnull(target.viable_date,'9999-01-01'::date)) or (ifnull(source.day_1,0) != ifnull(target.day_1,0)) or (ifnull(source.day_2,0) != ifnull(target.day_2,0)) or (ifnull(source.day_3,0) != ifnull(target.day_3,0)) or (ifnull(source.day_5,0) != ifnull(target.day_5,0)) or (ifnull(source.day_7,0) != ifnull(target.day_7,0)) or (ifnull(source.day_30,0) != ifnull(target.day_30,0)) or (ifnull(source.day_60,0) != ifnull(target.day_60,0)) or (ifnull(source.day_90,0) != ifnull(target.day_90,0)) or (ifnull(source.week_1,0) != ifnull(target.week_1,0)) or (ifnull(source.week_2,0) != ifnull(target.week_2,0)) or (ifnull(source.week_3,0) != ifnull(target.week_3,0)) or (ifnull(source.week_4,0) != ifnull(target.week_4,0)) or (ifnull(source.month_1,0) != ifnull(target.month_1,0)) or (ifnull(source.month_2,0) != ifnull(target.month_2,0)) or (ifnull(source.month_6,0) != ifnull(target.month_6,0)) or (ifnull(source.month_12,0) != ifnull(target.month_12,0))" ,
                            set =
                           {
                                "target.country_code":"source.country_code",
                                "target.install_date":"source.install_date",
                                "target.is_first_title_install":"source.is_first_title_install",
                                "target.days_since_install":"source.days_since_install",
                                "target.last_seen":"source.last_seen",
                                "target.last_seen_2":"source.last_seen_2",
                                "target.last_seen_3":"source.last_seen_3",
                                "target.first_30_days_logins":"source.first_30_days_logins",
                                "target.Last_30_days_logins":"source.Last_30_days_logins",
                                "target.total_logins":"source.total_logins",
                                "target.first_30_days_active_days":"source.first_30_days_active_days",
                                "target.Last_30_days_active_days":"source.Last_30_days_active_days",
                                "target.total_active_days":"source.total_active_days",
                                "target.first_21_b2b_days":"source.first_21_b2b_days",
                                "target.viable_date":"source.viable_date",
                                "target.day_1":"source.day_1",
                                "target.day_2":"source.day_2",
                                "target.day_3":"source.day_3",
                                "target.day_5":"source.day_5",
                                "target.day_7":"source.day_7",
                                "target.day_30":"source.day_30",
                                "target.day_60":"source.day_60",
                                "target.day_90":"source.day_90",
                                "target.week_1":"source.week_1",
                                "target.week_2":"source.week_2",
                                "target.week_3":"source.week_3",
                                "target.week_4":"source.week_4",
                                "target.month_1":"source.month_1",
                                "target.month_2":"source.month_2",
                                "target.month_6":"source.month_6",
                                "target.month_12":"source.month_12",
                                "target.dw_update_ts" : current_timestamp()
                           })
        .whenNotMatchedInsertAll()
        .execute()
    )
    

# COMMAND ----------

def batch_fact_player_ltd():
    environment = dbutils.widgets.get("environment")
    checkpoint_location = dbutils.widgets.get("checkpoint")
    #environment, checkpoint_location = "_dev", "dbfs:/tmp/standard_metrics/managed/batch/run_dev/fact_player_ltd"
    spark = SparkSession.builder.appName("Hydra").getOrCreate()


    spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
    
    df = read_logins_df(environment, spark)
    df = transform(df, environment, spark)

    load(df, environment, spark)

# COMMAND ----------

if __name__ == "__main__":
    batch_fact_player_ltd()
