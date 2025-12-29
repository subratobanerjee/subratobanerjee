# Databricks notebook source
from pyspark.sql.functions import (expr, when)
from delta.tables import DeltaTable
from functools import reduce

# COMMAND ----------

# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/managed/agg_igm_daily

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

database = 'bluenose'
input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())

view_mapping = {
                'config_1': 'config_1 as placement_id',
                'config_2': 'config_2 as screen',
                'agg_1' : 'agg_1 as demo_instance_count',
                'agg_2' : 'agg_2 as full_game_instance_count',
                'agg_3' : 'agg_3 as demo_time_spent',
                'agg_4' : 'agg_4 as full_game_time_spent',
                'agg_5' : 'agg_5 as demo_player_count',
                'agg_6' : 'agg_6 as converted_player_count',
                'agg_7' : 'agg_7 as full_game_player_count'
            }

# COMMAND ----------

def extract(database,environment,spark):
    # Selecting the transaction event data
    columns = {
                "date": "date",
                "platform": 'platform',
                "service": 'service',
                "country_code" :'country_code',
                "player_id": 'player_id',
                "igm_id": 'igm_id',
                "config_1": 'config_1',
                "config_2": 'config_2',
                "config_3" : 'config_3',
                "time_spent" : 'time_spent',
                'stauts': 'status'
                }

    prev_max = max_timestamp(spark, f"{database}{environment}.managed.agg_igm_daily", 'date')

    current_min = (
        spark.read
        .table(f"{database}{environment}.managed.fact_player_igm_daily")
        .where(expr("DW_INSERT_TS::date between current_date - 2 and current_date"))
        .select(expr(f"ifnull(min(date),'1999-01-01')::date as min_date"))
    ).collect()[0]['min_date']

    inc_min_date = min(prev_max,current_min)

    inc_df = (
        spark.read
        .table(f"{database}{environment}.managed.fact_player_igm_daily")
        .where(expr(f"date >= '{inc_min_date}'::date"))
        .select(*columns.values())
    )

    return inc_df

# COMMAND ----------

def transform(batch_df, database, environment, spark):
    df_summary = spark.table(f"{database}{environment}.managed.fact_player_summary_ltd").select('*')

    groupby_columns = {'igm.date', 'igm.platform', 'igm.service', 'igm.country_code', 'igm.igm_id', 'igm.config_1', 'igm.config_2', 'igm.status'}

    agg_columns = {
                    'agg_1' : expr("count(distinct case when config_3 = 'demo' then igm_id||igm.player_id else null end) as agg_1"),
                    'agg_2' : expr("count(distinct case when config_3 = 'full game' then igm_id||igm.player_id else null end) as agg_2"),
                    "agg_3": expr("ifnull(sum(case when config_3 = 'demo' then time_spent else null end),0) as agg_3"),
                    "agg_4": expr("ifnull(sum(case when config_3 = 'full game' then time_spent else null end),0) as agg_4"),
                    "agg_5": expr("count(distinct case when ltd_string_1= 'demo' then igm.player_id when ltd_string_1= 'converted' and  ltd_ts_1::date > igm.date then igm.player_id else null end) as agg_5"),
                    'agg_6' : expr("count(distinct case when ltd_string_1= 'converted' and ltd_ts_1::date <= igm.date then igm.player_id else null end) as agg_6"),
                    'agg_7' : expr("count(distinct case when ltd_string_1='full game' then igm.player_id else null end) as agg_7")
                  }

    inc_df = (
            batch_df.alias('igm')
            .join(df_summary.alias('df_summary'),expr("igm.player_id = df_summary.player_id and igm.service = df_summary.service and igm.platform = df_summary.platform"),"left")
            .groupBy(*groupby_columns)
            .agg(*agg_columns.values())
            .select(
                *groupby_columns,
                *agg_columns.keys(),
                expr("current_timestamp() AS dw_insert_ts"),
                expr("current_timestamp() AS dw_update_ts"),
                expr("SHA2(CONCAT_WS('|',igm.date, igm.platform, igm.service, igm.country_code , igm.igm_id, igm.config_1, igm.config_2, igm.status),256) AS merge_key")

            )
        )
    return inc_df

# COMMAND ----------

def load(df,database,environment,spark):

    # Merge variables and logic
    target_df = DeltaTable.forName(spark, f"{database}{environment}.managed.agg_igm_daily")

    merger_condition = 'target.merge_key = source.merge_key'
    merge_update_conditions = [
                                { 
                                 'condition' : """target.agg_1 != source.agg_1 OR
                                                  target.agg_2 != source.agg_2 OR
                                                  target.agg_3 != source.agg_3 OR
                                                  target.agg_4 != source.agg_4 OR
                                                  target.agg_5 != source.agg_5 OR
                                                  target.agg_6 != source.agg_6 OR
                                                  target.agg_7 != source.agg_7
                                                  """,
                                 'set_fields' : {
                                                    'agg_1' : 'greatest(target.agg_1,source.agg_1)',
                                                    'agg_2' : 'greatest(target.agg_2,source.agg_2)',
                                                    'agg_3' : 'greatest(target.agg_3,source.agg_3)',
                                                    'agg_4' : 'greatest(target.agg_4,source.agg_4)',
                                                    'agg_5' : 'greatest(target.agg_5,source.agg_5)',
                                                    'agg_6' : 'greatest(target.agg_6,source.agg_6)',
                                                    'agg_7' : 'greatest(target.agg_7,source.agg_7)',
                                                    'dw_update_ts': 'source.dw_update_ts'
                                                }
                                }
                        ]

    merge_df = target_df.alias("target").merge(df.alias("source"), f"{merger_condition}")

    merge_df = set_merge_update_condition(merge_df, merge_update_conditions)

    merge_df = set_merge_insert_condition(merge_df, df)

    # Execute the merge operation
    merge_df.execute()

# COMMAND ----------

def run_igm_daily(database,environment,view_mapping):
    database = database.lower()

    #setting the spark session
    spark = create_spark_session(name=f"{database}")
    
    #Creating the table and view, checkpoint
    create_agg_igm_daily(spark, database , environment, view_mapping)

    print('Extracting the data')
    
    # Reading the data using bath data
    df = extract(database,environment,spark)

    print('Applying the Transformation')
    #Applying Transformation
    df = transform(df, database, environment, spark)

    print('Merge Data')
    load(df,database,environment,spark)

    return 'Merge data completed'

# COMMAND ----------

run_igm_daily(database,environment,view_mapping)
