# Databricks notebook source
import json
from pyspark.sql.functions import (
    col,
    current_timestamp,
    expr
    )
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import Window
import argparse
import logging

# COMMAND ----------

# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/table_functions

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'bluenose'
data_source ='dna'
title = "'PGA Tour 2K25','PGA Tour 2K25: Demo'"

# COMMAND ----------

def create_fact_player_lesson(spark, database, environment):
    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.fact_player_lesson (
        received_on TIMESTAMP,
        player_id STRING,
        platform STRING,
        service STRING,
        my_player_id STRING,
        country_code STRING,
        round_instance_id STRING,
        game_type STRING,
        lesson_name STRING,
        lesson_step_name STRING,
        is_lesson_step_complete BOOLEAN,
        is_lesson_complete BOOLEAN,
        lesson_attempt INT,
        is_onboarding BOOLEAN,
        is_ftue BOOLEAN,
        dw_insert_ts TIMESTAMP,
        dw_update_ts TIMESTAMP
    )
    """

    create_table(spark,sql)
    create_fact_player_lesson_view(spark, database, environment)
    return (
        f"dbfs:/tmp/{database}/managed/streaming/run{environment}/fact_player_lesson"
    )

# COMMAND ----------

def create_fact_player_lesson_view(spark, database, environment):
    """
    Create the fact_player_game_status_daily view in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
        mapping (dict): A dictionary of column mappings.
    """


    sql = f"""
    CREATE OR REPLACE VIEW {database}{environment}.managed_view.fact_player_lesson AS (
        SELECT
        *
        from {database}{environment}.managed.fact_player_lesson
    )
    """

    spark.sql(sql)


# COMMAND ----------

def read_title_df(spark, environment,title):
    title_df= (
                spark.
                read.
                table(f"reference{environment}.title.dim_title")
                .select(
                    "display_platform",
                    "app_id",
                    "display_service"
                )
                .where(expr(f"title in ({title}) "))
                .alias("title")
                
                )
    return title_df

# COMMAND ----------

def read_player_lesson(spark,environment, title):
    title_df = read_title_df(spark, environment, title)
    return (
        spark
        .readStream
        .table(f"bluenose{environment}.raw.roundstatus")
        .join(title_df, expr("appPublicId = title.app_id"), "left")
    ).select(
        "receivedOn",
        "playerpublicid",
        "title.display_platform",
        "title.display_service",
        "myplayerid",
        "countryCode",
        "appGroupId",
        "mode",
        "roundinstanceid",
        "lessonname",
        "lessonstepname",
        "lessonstepsuccess",
        "lessoniscomplete",
        "lessonattempt",
        "lessonisonboarding"
    ).where(
        (expr("playerPublicId is not null and playerPublicId != 'anonymous'")) &
        (expr("lessonname is not null"))
    )

# COMMAND ----------

def extract(environmen, title):
    lesson_df = read_player_lesson(spark, environment, title)
    return lesson_df

# COMMAND ----------

def transform(df):
    df = df.select(
        expr("receivedOn::timestamp as received_on"),
        expr("playerpublicid as player_id"),
        expr("display_platform as platform"),
        expr("display_service as service"),
        expr("myplayerid as my_player_id"),
        expr("countryCode as country_code"),
        expr("roundinstanceid as round_instance_id"),
        expr("CASE WHEN appGroupId IN ('d3963fb6e4f54a658e1a14846f0c9a8c', '317c0552032c4804bb10d81b89f4c37e') THEN 'full game' WHEN appGroupId = '886bcab8848046d0bd89f5f1ce3b057b' THEN 'demo' END as game_type"),
        expr("lessonname as lesson_name"),
        expr("lessonstepname as lesson_step_name"),
        expr("lessonstepsuccess::boolean as is_lesson_step_complete"),
        expr("lessoniscomplete::boolean as is_lesson_complete"),
        expr("lessonattempt::int as lesson_attempt"),
        expr("lessonisonboarding::boolean as is_onboarding"),
        expr("case when lessonisonboarding = True and mode = 'Boot' then true else false end as is_ftue")
    )
    return df

# COMMAND ----------

def load_fact_player_lesson(spark, df, database, environment, checkpoint_location):
    """
    Load the data into the fact_player_lesson table in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        df (DataFrame): The DataFrame to load into the table.
        database (str): The database to load the table into.
        environment (str): The environment to load the table into.
    """

    # union the incoming dataframe with the empty dataframe, we can set allowMissingColumns to False to be more strict
    df = df.selectExpr(
        "*",
        "CURRENT_TIMESTAMP() as dw_insert_ts",
        "CURRENT_TIMESTAMP() as dw_update_ts",
    )

    (
        df.writeStream
        .trigger(availableNow=True)  
        .option("checkpointLocation", checkpoint_location)
        .outputMode("append")
        .toTable(f"{database}{environment}.managed.fact_player_lesson")
    )

# COMMAND ----------

def stream_fact_player_lesson():
    spark = create_spark_session(name=f"{database}")
    checkpoint_location = create_fact_player_lesson(spark, database, environment)
    lesson_df= extract(environment,title)
    transformed_lesson_df = transform(lesson_df)
    load_fact_player_lesson(spark, transformed_lesson_df, database, environment, checkpoint_location)
    

# COMMAND ----------

stream_fact_player_lesson()

# COMMAND ----------

# dbutils.fs.rm("dbfs:/tmp/bluenose/managed/streaming/run_dev/fact_player_lesson", True)
# spark.sql("drop table bluenose_dev.managed.fact_player_lesson")
