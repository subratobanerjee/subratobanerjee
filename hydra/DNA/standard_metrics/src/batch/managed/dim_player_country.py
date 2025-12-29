# Databricks notebook source
# MAGIC %run ../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../utils/ddl/table_functions

# COMMAND ----------

from datetime import date, datetime
from pyspark.sql.functions import expr, lit
from delta.tables import DeltaTable

# COMMAND ----------

# This configuration drives the entire ETL. It can be loaded from a JSON file.
title_config = {
    "titles_to_process": ["oak2", "nero"], 
    "common_columns": {
        "player_id": "player_id",
        "platform": "platform",
        "service": "service",
        "country_code": "country_code",
        "received_on": "received_on"
    },
    "oak2": {
        "source_table": "intermediate.fact_player_activity",
        "column_mappings": {
            "title": "'Borderlands 4'",
            "player_id": "extra_info_1" 
        }
    },
    "nero": {
        "source_table": "intermediate.fact_player_activity",
        "column_mappings": {
            "title": "'Mafia: The Old Country'"
        }
    }
}


# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment', set_environment())
target_database = 'dataanalytics'

# COMMAND ----------

def extract(spark, title, table_name, prev_max_date, watermark_column='dw_update_ts'):

    extract_df = spark.table(f"{title}{environment}.{table_name}") \
        .filter(expr(f"{watermark_column} >= '{prev_max_date}'")) \
        .filter(expr("player_id is not null and player_id != '' AND upper(player_id) NOT LIKE '%NULL%'"))
        
    return extract_df


# COMMAND ----------

def transform_for_title(source_df, title_name, config):

    common_mappings = config.get("common_columns", {})
    title_specific_mappings = config.get(title_name, {}).get("column_mappings", {})
    final_mappings = {**common_mappings, **title_specific_mappings}

    select_exprs = [f"{expression} as {alias}" for alias, expression in final_mappings.items()]
    
    transformed_df = source_df.selectExpr(*select_exprs)
 
    return transformed_df

# COMMAND ----------

def create_dim_player_country(spark, target_database, environment, properties={}):

    sql = f"""
    CREATE TABLE IF NOT EXISTS {target_database}{environment}.standard_metrics.dim_player_country (
        player_id STRING,
        title STRING,
        platform STRING,
        service STRING,
        country_code STRING,
        received_on TIMESTAMP,
        dw_insert_ts TIMESTAMP,
        dw_update_ts TIMESTAMP
    )
    """
    create_table(spark, sql)

# COMMAND ----------

def load(df, target_database, environment, spark):

    target_table_name = f"{target_database}{environment}.standard_metrics.dim_player_country"
    
    target_df = DeltaTable.forName(spark, target_table_name)
    
    merger_condition = """
        target.player_id = source.player_id AND
        target.title = source.title AND
        target.platform = source.platform AND
        target.service = source.service AND
        target.country_code = source.country_code
    """

    update_expr = {c: f"source.{c}" for c in df.columns if c not in ["player_id", "title", "platform", "service", "country_code","dw_insert_ts"]}
    update_expr["dw_update_ts"] = "current_timestamp()"
    
    insert_expr = {c: f"source.{c}" for c in df.columns}
    insert_expr["dw_insert_ts"] = "current_timestamp()"

    (target_df.alias("target")
        .merge(df.alias("source"), merger_condition)
        .whenMatchedUpdate(set=update_expr)
        .whenNotMatchedInsert(values=insert_expr)
        .execute())

def run_batch(config):
    
    spark = create_spark_session(name=f"{target_database}_dim_player_country")

    target_table = f"{target_database}{environment}.standard_metrics.dim_player_country"
    create_dim_player_country(spark, target_database, environment)

    prev_max_date = max_timestamp(spark, target_table, 'dw_update_ts')
    if prev_max_date is None:
        prev_max_date = datetime(1999, 1, 1)

    final_df = None

    for title_name in config["titles_to_process"]:
        title_config = config[title_name]
        source_df = extract(spark, title_name,title_config["source_table"], prev_max_date)
        result_for_title = transform_for_title(source_df, title_name, config)
        
        if final_df is None:
            final_df = result_for_title
        else:
            final_df = final_df.unionByName(result_for_title)

    if final_df is not None:
        agg_df = (
            final_df
            .groupBy("player_id", "title", "platform", "service","country_code")
            .agg(expr("max(received_on) as received_on"))
        )
    
        load(agg_df, target_database, environment, spark)
    else:
        print("No new data to process.")

run_batch(title_config)