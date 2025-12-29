# Databricks notebook source
from pyspark.sql.functions import (
    col,
    concat_ws,
    count_distinct,
    current_timestamp,
    ifnull,
    lit,
    sha2,
    datediff,
    lag,
    when,
    count,
    expr
)
from delta.tables import DeltaTable
import argparse
from pyspark.sql import (SparkSession,Window)

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

def read_date_df(environment, spark):
    date_df = (
        spark
        .read
        .table(f"reference{environment}.calendar.dim_date")
        .where("date between current_date() - interval '30 day' and current_date()")
        .alias("dd")
    )

    return date_df

# COMMAND ----------

def read_title_df(environment, spark):
    title_df = (
        spark
        .read
        .table(f"reference{environment}.title.dim_title")
        .select("title", col("display_service").alias("service"), col("display_platform").alias("platform"))
        .alias("dt")
    )

    return title_df

# COMMAND ----------

def read_login_df(environment, spark):
    login_df = (
        spark
        .read
        .table(f"dataanalytics{environment}.standard_metrics.fact_login")
        .where("login_date >= current_date() - interval '30 day'")
        .alias("fl")
    )

    return login_df

# COMMAND ----------

def extract(environment, spark):
    date_df = read_date_df(environment, spark)
    title_df = read_title_df(environment, spark)
    login_df = read_login_df(environment, spark)

    out_df = (
        date_df
        .join(title_df, how="cross")
        .alias("dt")
        .join(login_df, on=[
                            col("fl.login_date") == col("dt.date"),
                            col("fl.title") == col("dt.title"),
                            col("fl.service") == col("dt.service"),
                            col("fl.platform") == col("dt.platform")], how="left")
    )

    return out_df


# COMMAND ----------

def transform(df, environment, spark):
    wmau_df = (
        df
        .select(
            expr("fl.login_date as date"),
            "dt.title",
            "dt.platform",
            "dt.service",
            expr("coalesce(fl.country_code, 'ZZ') as country_code"),
            expr("size(collect_set(player_id) over (partition by dt.title, dt.platform, dt.service, fl.country_code order by login_date::timestamp RANGE BETWEEN INTERVAL 6 DAY PRECEDING AND CURRENT ROW)) as wau"),
            expr("size(collect_set(player_id) over (partition by dt.title, dt.platform, dt.service, fl.country_code order by login_date::timestamp RANGE BETWEEN INTERVAL 29 DAY PRECEDING AND CURRENT ROW)) as mau")
        )
        .distinct()
    )

    mp_df = (
    spark
        .read
        .table(f"dataanalytics{environment}.standard_metrics.fact_multiplayer")
        .groupBy(
            expr("title"),
            expr("platform"),
            expr("service"),
            expr("date")
        )
        .agg(
            expr("sum(multiplayer_instances) as multiplayer_instances")
        )
    )

    transformed_df = (
        df
        .withColumn("winback_status", datediff(col("login_date"), lag(col("login_date"), 1).over(Window.partitionBy(col("player_id"), col("dt.platform"), col("dt.service"),  col("dt.title")).orderBy(col("login_date")))) > 30)
        .groupBy(
            col("dt.title"),
            col("dt.platform"),
            col("dt.service"),
            col("dt.date"),
            ifnull(col("country_code"), lit("ZZ")).alias("country_code")
        )
        .agg(
            count_distinct(col("player_id")).alias("dau"),
            count(when(col("winback_status") == 'true', col("player_id"))).alias("winback")
        )
        .withColumns({
            "merge_key": sha2(concat_ws("", col("dt.title"), col("dt.platform"), col("country_code"), col("dt.service"), col("dt.date")), 256)
        })
        .join(wmau_df, on=["title", "platform", "service", "date", "country_code"], how="left")
        .join(mp_df, on=["title", "platform", "service", "date"], how="left")
        .select(
            col("date").alias("login_date"),
            "title",
            "platform",
            "service",
            "country_code",
            "dau",
            "wau",
            "mau",
            "winback",
            expr("current_timestamp() as dw_insert_ts"),
            expr("current_timestamp() as dw_update_ts"),
            "merge_key",
            "multiplayer_instances"
        )
    )

    return transformed_df

# COMMAND ----------

def load(df, environment, spark):
    (
        DeltaTable.createIfNotExists(spark)
        .tableName(f"dataanalytics{environment}.standard_metrics.fact_active_user")
        .addColumn("title", "string")
        .addColumn("platform", "string")
        .addColumn("service", "string")
        .addColumn("login_date", "date")
        .addColumn("country_code", "string")
        .addColumn("dau", "integer")
        .addColumn("wau", "integer")
        .addColumn("mau", "integer")
        .addColumn("winback", "integer")
        .addColumn("dw_insert_ts", "timestamp")
        .addColumn("dw_update_ts", "timestamp")
        .addColumn("merge_key", "string")
        .addColumn("multiplayer_instances", "bigint")
        .property('delta.enableIcebergCompatV2', 'true')
        .property('delta.universalFormat.enabledFormats', 'iceberg')
        .execute()
    )

    target_table = DeltaTable.forName(spark, f"dataanalytics{environment}.standard_metrics.fact_active_user")

    (
        target_table
        .alias("target")
        .merge(
            df.alias("source"),
            "target.merge_key = source.merge_key")
        .withSchemaEvolution()
        .whenMatchedUpdate(
            condition = "(target.dau < source.dau or target.winback < source.winback or target.wau < source.wau or target.mau < source.mau)",
            set = {
                "target.dau": "source.dau",
                "target.wau": "source.wau",
                "target.mau": "source.mau",
                "target.winback": "source.winback",
                "target.dw_update_ts": "current_timestamp()"
        })
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

def run_batch():
    environment = dbutils.widgets.get("environment")
    checkpoint_location = dbutils.widgets.get("checkpoint")
    # environment, checkpoint_location = "_dev", "dbfs:/tmp/standard_metrics/managed/batch/run_dev/fact_active_user"
    spark = SparkSession.builder.appName("Hydra").getOrCreate()

    spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

    df = extract(environment, spark)
    
    df = transform(df, environment, spark)

    load(df, environment, spark)

# COMMAND ----------

if __name__ == "__main__":
    run_batch()

# COMMAND ----------

# spark.sql("drop table if exists dataanalytics_dev.standard_metrics.fact_active_user")