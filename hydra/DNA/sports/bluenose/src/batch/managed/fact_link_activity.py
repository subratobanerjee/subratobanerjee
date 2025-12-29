# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/managed/fact_link_activity

# COMMAND ----------

from pyspark.sql.functions import expr

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment', set_environment())
database = 'bluenose'
data_source = 'dna'
title = 'bluenose'
view_mapping = {
    'agg_1': 'agg_1 as agg_link_count',
    'agg_2': 'agg_2 as agg_unlink_count',
    'agg_3': 'agg_3 as agg_entering_screen_count',
    'agg_4': 'agg_4 as agg_leaving_screen_count',
    'agg_5': 'agg_5 as agg_attempts_to_link_count'
}

# COMMAND ----------

def read_fact_player_activity(environment, min_received_on):
    return (
        spark
        .read
        .table(f"bluenose{environment}.intermediate.fact_player_activity")
        .select(
            expr("to_date(received_on) as date"),  # Use to_date() to cast to date
            expr("player_id"),
            expr("platform"),
            expr("service"),
            expr("country_code"),
            expr("session_id"),
            expr("event_trigger"),
            expr("extra_info_7")
        )
        .where(expr(f"received_on::date > to_date('{min_received_on}') - INTERVAL 2 DAY and source_table = 'accountlinkstatus'"))
    )

# COMMAND ----------

def read_sso_unlink(environment, min_received_on):
    return (
        spark
        .read
        .table(f"CORETECH{environment}.SSO.UNLINKEVENT")
        .select(
            expr("accountid"),
            expr("Date"),
            expr("get_json_object(geoip, '$.countryCode') as country_code"),  
            expr("sessionID")
        )
        .where(expr(f"Date > to_date('{min_received_on}') - INTERVAL 2 DAY and appgroupid in ('d3963fb6e4f54a658e1a14846f0c9a8c', '317c0552032c4804bb10d81b89f4c37e', '886bcab8848046d0bd89f5f1ce3b057b')"))
        .groupBy(expr("accountid"), expr("Date"), expr("country_code"))  
        .agg(expr("count(distinct sessionID) as unlink_count"))
    )

# COMMAND ----------

def extract(environment):
    prev_max_ts = max_timestamp(spark, f"bluenose{environment}.managed.fact_player_link_activity", "date")
    min_received_on = spark.sql(f"""
    SELECT 
        COALESCE(try_cast(min(to_date(received_on)) AS DATE), DATE('1970-10-01')) AS min_received_on 
    FROM bluenose{environment}.intermediate.fact_player_activity 
    WHERE dw_insert_ts >= CURRENT_DATE - INTERVAL '2' DAY
    """).collect()[0]['min_received_on']
    min_received_on_inc = min(prev_max_ts, min_received_on)
    pla_df = read_fact_player_activity(environment, min_received_on_inc).alias("pla")
    unlinksso_df = read_sso_unlink(environment, min_received_on_inc).alias("sso")

    joined_df = (
        pla_df
        .join(unlinksso_df, on=expr("pla.player_id = sso.accountid and pla.date = sso.date and pla.country_code = sso.country_code"), how="left")
    )

    return joined_df

# COMMAND ----------

def transform(df):
    out_df = (
        df
        .groupBy(
            expr("pla.date"),
            expr("pla.player_id"),
            expr("pla.platform"),
            expr("pla.service"),
            expr("pla.country_code")
        )
        .agg(
            expr("sum(case when extra_info_7 = 'Complete' then 1 else 0 end) as agg_1"),
            expr("first(unlink_count, true) as agg_2"),
            expr("sum(case when event_trigger = 'Entering' then 1 else 0 end) as agg_3"),
            expr("sum(case when event_trigger = 'Leaving' then 1 else 0 end) as agg_4"),
            expr("sum(case when event_trigger = 'Entering' then 1 else 0 end) as agg_5")
        )
    )

    return out_df

# COMMAND ----------

def run_batch():
    spark = create_spark_session(name=f"{database}")
    checkpoint_location = create_fact_player_link_activity(spark, database, view_mapping)

    df = extract(environment)
    df = transform(df)

    load_fact_player_link_activity(spark, df, database, environment)

# COMMAND ----------

run_batch()