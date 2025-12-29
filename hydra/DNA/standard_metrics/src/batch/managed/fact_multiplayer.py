# Databricks notebook source
# MAGIC %run ../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../utils/ddl/table_functions

# COMMAND ----------

from pyspark.sql.functions import col, expr
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import date

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment', set_environment())
database = 'dataanalytics'
source_databases = ['bluenose']
spark = create_spark_session()

sources_config = {
    "bluenose": {
        "title": "bluenose",
        "table": "intermediate.fact_player_session"
    },
    "WWE2K25_PROD": {
        "title": "WWE 2K25",
        "match_status_table": "RAW.MATCHSTATUS",
        "match_roster_table": "RAW.MATCHROSTER"
    }
}


# COMMAND ----------

def create_fact_multiplayer_table(target_table):
    sql = f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            title STRING,
            platform STRING,
            service STRING,
            date DATE,
            player STRING,
            multiplayer_instances BIGINT,
            dw_insert_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            dw_update_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            merge_key STRING
        )
    """

    properties = {
        "delta.feature.allowColumnDefaults": "supported"
    }

    create_table(spark, sql, properties)


# COMMAND ----------

def read_title_df(spark, environment):
    return (
        spark.read
        .table(f"reference{environment}.title.dim_title")
        .select(
            "title",
            "display_platform",
            "app_id",
            "display_service"
        )
    )


# COMMAND ----------

def extract(spark, environment):
    prev_max = max_timestamp(spark, f"{database}{environment}.standard_metrics.fact_multiplayer", 'dw_insert_ts')
    if prev_max is None:
        prev_max = date(1999, 1, 1)
    print("prev_max is", prev_max)

    bluenose_df = None
    wwe_df = None

    title_df = read_title_df(spark, environment)

    for source, config in sources_config.items():
        title = config["title"]

        if source == "bluenose":
            bluenose_raw_df = (
                spark.read
                .table(f"{source}{environment}.{config['table']}")
                .where(expr(f"to_date(dw_insert_ts) >= to_date('{prev_max}') - interval 2 days"))
            )
            # bluenose df
            bluenose_df = (
                bluenose_raw_df
                .select(
                    expr("'PGA Tour 2K25' as title"),
                    expr("platform as platform"),
                    expr("service as service"),
                    expr("session_start_ts as session_start_ts"),
                    expr("player_id as player_id"),
                    expr("local_multiplayer_instances as local_multiplayer_instances"),
                    expr("online_multiplayer_instances as online_multiplayer_instances")
                )
            )

        elif source == "WWE2K25_PROD":
            match_status_df = (
                spark.read
                .table(f"{source}.{config['match_status_table']}")
                .where("BUILDTYPE = 'FINAL'")
                .where(expr("RECEIVEDON::TIMESTAMP::DATE >= '2025-03-07'"))
                .where(expr(f"RECEIVEDON::TIMESTAMP::DATE >= to_date('{prev_max}') - interval 2 days"))
            )

            match_roster_df = (
                spark.read
                .table(f"{source}.{config['match_roster_table']}")
                .where("BUILDTYPE = 'FINAL'")
                .where(expr("RECEIVEDON::TIMESTAMP::DATE >= '2025-03-07'"))
                .where(expr(f"RECEIVEDON::TIMESTAMP::DATE >= to_date('{prev_max}') - interval 2 days"))
                .where("AI = 0")
                .where("WRESTLERTYPE <> 'MANAGER'")
            )

            filtered_roster = (
                match_roster_df
                .groupBy(expr("CONCAT(MATCHGUID, PLAYERPUBLICID) as match_player_key"))
                .agg(expr("COUNT(DISTINCT TEAM) as team_count"))
                .where("team_count > 1")
            )

            online_df = (
                match_status_df
                .where("ONLINE = 1")
                .selectExpr(
                    "RECEIVEDON::TIMESTAMP::DATE as date",
                    "PLAYERPUBLICID as player_id",
                    "APPPUBLICID as apppublicid",
                    "'MP_ONLINE' as mp_category",
                    "PLATFORM as platform",
                    "SERVICE as service"
                )
            )

            local_df = (
                match_status_df
                .where("ONLINE <> 1")
                .where("GMONLINESESSIONID IS NULL")
                .withColumn("match_player_key", expr("CONCAT(MATCHGUID, PLAYERPUBLICID)"))
                .join(filtered_roster, "match_player_key", "semi")
                .selectExpr(
                    "RECEIVEDON::TIMESTAMP::DATE as date",
                    "PLAYERPUBLICID as player_id",
                    "APPPUBLICID as apppublicid",
                    "'MP_LOCAL' as mp_category",
                    "PLATFORM as platform",
                    "SERVICE as service"
                )
            )

            combined_raw_wwe_df = online_df.unionByName(local_df)

            # Join with dim_title on apppublicid for WWE
            wwe_df = (
                combined_raw_wwe_df
                .join(title_df, combined_raw_wwe_df["apppublicid"] == title_df["app_id"], "left")
                .select(
                    expr("title as title"),
                    expr("display_platform as platform"),
                    expr("display_service as service"),
                    expr("date as date"),
                    expr("player_id as player_id"),
                    expr("CASE WHEN mp_category IN ('MP_ONLINE', 'MP_LOCAL') THEN 1 ELSE 0 END as multiplayer_flag")
                )
            )

    return bluenose_df, wwe_df


# COMMAND ----------

def transform(bluenose_df, wwe_df):
    bluenose_transformed = bluenose_df.select(
        "title",
        "platform",
        "service",
        expr("to_date(session_start_ts) as date"),
        "player_id",
        expr(
            "CASE WHEN local_multiplayer_instances > 0 OR online_multiplayer_instances > 0 THEN 1 ELSE 0 END as multiplayer_flag")
    )

    wwe_transformed = wwe_df.select(
        "title",
        "platform",
        "service",
        "date",
        "player_id",
        "multiplayer_flag"
    )

    combined_df = bluenose_transformed.unionByName(wwe_transformed)

    mp_df = (
        combined_df.groupBy(
            "title",
            "platform",
            "service",
            "date",
            expr("player_id as player")
        )
        .agg(expr("sum(multiplayer_flag) as multiplayer_instances"))
        .select(
            "*",
            expr("current_timestamp() AS dw_insert_ts"),
            expr("current_timestamp() AS dw_update_ts"),
            expr("sha2(concat_ws('|', title, platform, service, date, player), 256) as merge_key")
        )
    )
    return mp_df


# COMMAND ----------

def load(agg_df, environment):
    # Reference to the target Delta table
    target_df = DeltaTable.forName(spark, f"{database}{environment}.standard_metrics.fact_multiplayer")

    # Merger condition
    # merger_condition = 'target.merge_key = source.merge_key'

    update_cols = [col_name for col_name in agg_df.columns]
    merge_condition = " OR ".join(f"old.{col_name} <> new.{col_name}" for col_name in update_cols)

    update_set = {}
    update_set = {f"old.{col_name}": f"new.{col_name}" for col_name in update_cols}
    update_set[f"old.dw_update_ts"] = "CURRENT_TIMESTAMP()"

    # merge the table
    (
        target_df.alias('old')
        .merge(
            agg_df.alias('new'),
            "new.merge_key = old.merge_key"
        )
        .whenMatchedUpdate(condition=merge_condition, set=update_set)
        .whenNotMatchedInsertAll()
        .execute()
    )


# COMMAND ----------

def run_batch():
    # environment, checkpoint_location = "_dev", "dbfs:/tmp/standard_metrics/managed/batch/run_dev/fact_multiplayer"
    target_table = f"{database}{environment}.standard_metrics.fact_multiplayer"
    create_fact_multiplayer_table(target_table)
    print('Extracting the data')
    bluenose_df, wwe_df = extract(spark, environment)
    agg_df = transform(bluenose_df, wwe_df)
    print('Merge Data')
    load(agg_df, environment)


# COMMAND ----------

if __name__ == "__main__":
    run_batch()

# COMMAND ----------
# spark.sql("drop table if exists dataanalytics_dev.standard_metrics.fact_multiplayer")