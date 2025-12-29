# Databricks notebook source
# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/oak2/fact_player_summary_ltd

# COMMAND ----------

# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import expr
from delta.tables import DeltaTable
from datetime import date
from typing import Tuple

# COMMAND ----------

# Configuration variables
input_param = dbutils_input_params()
ENVIRONMENT = input_param.get('environment', set_environment())
DATABASE = 'oak2'
TITLE = 'Borderlands 4'
SOURCE_TABLE = "gbx_prod.raw.logins"
TARGET_TABLE = f"{DATABASE}{ENVIRONMENT}.managed.fact_player_summary_ltd"
DIM_TITLE_TABLE = f"reference{ENVIRONMENT}.title.dim_title"


def extract(spark: SparkSession, prev_max_ts: date) -> Tuple[DataFrame, DataFrame]:
  
    query = f"""
        SELECT
            player_id_platformid,
            maw_time_received,
            game_title_string,
            CASE
                WHEN player_address_country_string IS NULL THEN 'ZZ'
                WHEN player_address_country_string IN ('--', 'CA-QC') THEN 'CA'
                ELSE player_address_country_string
            END AS player_address_country_string,
            CASE
                WHEN lower(hardware_string) = 'ps5pro' THEN 'ps5'
                WHEN lower(hardware_string) = 'xsx' THEN 'xbsx'
                WHEN lower(hardware_string) = 'switch2' THEN 'nsw2'
                ELSE lower(hardware_string)
            END AS platform,
            CASE
                WHEN lower(service_string) = 'nintendo' THEN 'nso'
                ELSE lower(service_string)
            END AS service
        FROM
            {SOURCE_TABLE}
        WHERE
            to_date(maw_time_received) >= to_date('{prev_max_ts}') - INTERVAL 2 DAYS
            AND game_title_string = 'oak2'
    """
    logins_df = spark.sql(query)
    dim_title_df = spark.table(DIM_TITLE_TABLE)
    return logins_df, dim_title_df

def transform(new_logins_df: DataFrame, dim_title_df: DataFrame) -> DataFrame:

    country_code_window = "PARTITION BY player_id_platformid, platform, service ORDER BY maw_time_received"
    country_codes_df = (
        new_logins_df
        .selectExpr(
            "player_id_platformid as player_id",
            "platform",
            "service",
            f"first_value(player_address_country_string) OVER ({country_code_window}) as first_seen_country_code",
            f"last_value(player_address_country_string) OVER ({country_code_window} ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_seen_country_code",
            f"row_number() OVER ({country_code_window}) as rn"
        )
        .filter("rn = 1")
        .select("player_id", "platform", "service", "first_seen_country_code", "last_seen_country_code")
    )
    
    aggregated_logins = (
        new_logins_df
        .groupBy("player_id_platformid", "platform", "service")
        .agg(
            expr("min(maw_time_received) as first_seen"),
            expr("max(maw_time_received) as last_seen")
        )
        .selectExpr("player_id_platformid as player_id", "platform", "service", "first_seen", "last_seen")
    )
    
    dim_title_filtered = dim_title_df.filter(f"title = '{TITLE}'")

    final_df = (
        aggregated_logins.alias("agg")
        .join(country_codes_df.alias("cc"), ["player_id", "platform", "service"])
        .join(
            dim_title_filtered.alias("t"),
            ["platform", "service"],
            "left"
        )
        .filter("t.display_platform IS NOT NULL AND t.display_service IS NOT NULL")
        .selectExpr(
            "agg.player_id",
            "t.display_platform as platform",
            "t.display_service as service",
            "agg.first_seen",
            "agg.last_seen",
            "cc.first_seen_country_code",
            "cc.last_seen_country_code"
        )
        .withColumn("merge_key", expr("sha2(concat_ws('|', player_id, platform, service), 256)"))
        .withColumn("dw_update_ts", expr("current_timestamp()"))
        .withColumn("dw_insert_ts", expr("current_timestamp()"))
    )
    
    return final_df


def load(df: DataFrame, target_table_name: str, spark: SparkSession):

    target_delta = DeltaTable.forName(spark, target_table_name)

    update_logic = {
        "first_seen": "LEAST(target.first_seen, source.first_seen)",
        "last_seen": "GREATEST(target.last_seen, source.last_seen)",
        "first_seen_country_code": """
            CASE
                WHEN source.first_seen < target.first_seen
                THEN source.first_seen_country_code
                ELSE target.first_seen_country_code
            END
        """,
        "last_seen_country_code": """
            CASE
                WHEN source.last_seen > target.last_seen
                THEN source.last_seen_country_code
                ELSE target.last_seen_country_code
            END
        """,
        "dw_update_ts": "current_timestamp()"
    }

    (
        target_delta.alias("target")
        .merge(df.alias("source"), "target.merge_key = source.merge_key")
        .whenMatchedUpdate(set=update_logic)
        .whenNotMatchedInsertAll()
        .execute()
    )


def run_batch(database: str, environment: str):

    spark = create_spark_session(name=f"{database}_player_summary")

    fact_player_summary_ltd(spark, database, environment)
    create_fact_player_summary_ltd_view(spark, database, environment)

    prev_max = max_timestamp(spark, TARGET_TABLE, 'dw_insert_ts') or date(1999, 1, 1)

    new_logins, dim_title = extract(spark, prev_max)
    
    incremental_transformed_df = transform(new_logins, dim_title)

    load(incremental_transformed_df, TARGET_TABLE, spark)

# COMMAND ----------

run_batch(DATABASE, ENVIRONMENT)