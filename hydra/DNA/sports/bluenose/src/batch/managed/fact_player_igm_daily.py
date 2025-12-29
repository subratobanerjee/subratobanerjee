# Databricks notebook source
from pyspark.sql.functions import (expr, when)
from delta.tables import DeltaTable
from functools import reduce

# COMMAND ----------

# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/managed/fact_player_igm_daily

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

database = 'bluenose'
input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
title = "'PGA Tour 2K25','PGA Tour 2K25: Demo'"

view_mapping = {'config_1': 'config_1 as placement_id',
                'config_2': 'config_2 as screen',
                'config_3': 'config_3 as game_type' }

# COMMAND ----------

def read_promotion(database,environment,spark):
    prev_max = max_timestamp(spark, f"{database}{environment}.managed.fact_player_igm_daily", 'date')

    current_min = (
        spark.read
        .table(f"{database}{environment}.raw.promotion")
        .where(expr("(date between current_date - 2 and current_date) and buildenvironment = 'RELEASE'"))
        .select(expr(f"ifnull(min(receivedOn::timestamp),'1999-01-01')::date as min_date"))
    ).collect()[0]['min_date']

    inc_min_date = min(prev_max,current_min)

    inc_filter = f"buildenvironment = 'RELEASE' and receivedOn::timestamp::date >= '{inc_min_date}'::date "

    columns = {
                'player_id': expr('playerPublicId as player_id'),
                'apppublicid': 'appPublicId',
                'date': expr('receivedOn::timestamp::date as date'),
                'country_code': expr('countryCode as country_code'),
                'igm_id': expr('promoid as igm_id'),
                'config_1': expr('placementid as config_1'),
                'config_2': expr('screen as config_2'),
                'config_3': expr("CASE WHEN APPGROUPID IN ('d3963fb6e4f54a658e1a14846f0c9a8c', '317c0552032c4804bb10d81b89f4c37e') THEN 'full game' WHEN APPGROUPID = '886bcab8848046d0bd89f5f1ce3b057b' THEN 'demo' END as config_3"),
                'status': expr('promotionstatus as status'),
                'time_spent': expr('try_cast(timeonscreen::double as int) as time_spent')
            }

    inc_df = (
        spark.read
        .table(f"{database}{environment}.raw.promotion")
        .where(expr(inc_filter))
        .select(*columns.values())
    )
    return inc_df

# COMMAND ----------

def read_dim_title(environment,title,spark):
    title_df =(
                spark
                .read
                .table(f"reference{environment}.title.dim_title")
                .alias('title')
                .where(expr(f"""title in ({title}) """))
                .select('*')
              )
    return title_df

# COMMAND ----------

def extract(database,environment,spark):
    return read_promotion(database,environment,spark)

# COMMAND ----------

def transform(df, database, environment,title,spark):
    title_df = read_dim_title(environment,title,spark)

    groupby_columns = {
            'player_id': 'player_id',
            'DISPLAY_PLATFORM': expr('DISPLAY_PLATFORM as platform'),
            'DISPLAY_SERVICE': expr('DISPLAY_SERVICE as service'),
            'date': 'date',
            'country_code': 'country_code',
            'igm_id': 'igm_id',
            'config_1': 'config_1',
            'config_2': 'config_2',
            'config_3': 'config_3',
            'status': 'status'
        }

    agg_columns = {'time_spent': expr('sum(case when time_spent = 0 then 1 else time_spent end) as time_spent')}

    inc_df = (
                df.alias('igm')
                .join(title_df.alias('title'), expr("igm.appPublicId =title.app_id"), 'inner')
                .groupBy(*groupby_columns)
                .agg(*agg_columns.values())
                .select(
                    *groupby_columns.values(),
                    *agg_columns.keys(),
                    expr("current_timestamp() AS dw_insert_ts"),
                    expr("current_timestamp() AS dw_update_ts"),
                    expr("SHA2(CONCAT_WS('|', player_id, display_platform, display_service, date, country_code, igm_id, config_1, config_2 , config_3, status),256) AS merge_key")
                    )
            )
    return inc_df

# COMMAND ----------

def load(df,database,environment,spark):

    # Merge variables and logic
    target_df = DeltaTable.forName(spark, f"{database}{environment}.managed.fact_player_igm_daily")

    merger_condition = 'target.merge_key = source.merge_key'
    merge_update_conditions = [
                                { 
                                 'condition' : """target.time_spent != source.time_spent""",
                                 'set_fields' : {
                                                    'time_spent': 'greatest(target.time_spent,source.time_spent)'
                                                }
                                }
                        ]

    merge_df = target_df.alias("target").merge(df.alias("source"), f"{merger_condition}")

    merge_df = set_merge_update_condition(merge_df, merge_update_conditions)

    merge_df = set_merge_insert_condition(merge_df, df)

    # Execute the merge operation
    merge_df.execute()

# COMMAND ----------

def run_igm_daily(database,environment,title,view_mapping):
    database = database.lower()

    #setting the spark session
    spark = create_spark_session(name=f"{database}")
    
    #Creating the table and view, checkpoint
    create_fact_player_igm_daily(spark, database ,view_mapping)
    
    # Reading the data using bath data
    df = extract(database,environment,spark)

    #Applying Transformation
    df = transform(df, database, environment,title,spark)

    #Merge data
    load(df,database,environment,spark)

    return 'Merge data completed'

# COMMAND ----------

run_igm_daily(database,environment,title,view_mapping)
