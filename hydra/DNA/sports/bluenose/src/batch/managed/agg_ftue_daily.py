# Databricks notebook source
from pyspark.sql.functions import (expr, when)
from delta.tables import DeltaTable
from functools import reduce

# COMMAND ----------

# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/managed/agg_ftue_daily

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

database = 'bluenose'
input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())

view_mapping = {
                'config_1' : 'config_1 as game_type',
                'agg_1' : 'agg_1 as player_count'
            }

# COMMAND ----------

def extract(database,environment,spark):
    # Selecting the transaction event data
    columns = {
                "date": "date",
                "player_id": 'player_id',
                "platform": 'platform',
                "service": 'service',
                'step': 'step',
                'step_name': 'step_name',
                'config_1': 'config_1',
                'status': 'status'
                }

    prev_max = max_timestamp(spark, f"{database}{environment}.managed.agg_ftue_daily", 'date')

    current_min = (
        spark.read
        .table(f"{database}{environment}.managed.fact_player_ftue")
        .where(expr("DW_INSERT_TS::date between current_date - 2 and current_date"))
        .select(expr(f"ifnull(min(date),'1999-01-01')::date as min_date"))
    ).collect()[0]['min_date']

    inc_min_date = min(prev_max,current_min)

    df = (
        spark.read
        .table(f"{database}{environment}.managed.fact_player_ftue")
        .where(expr(f"date >= '{inc_min_date}'::date"))
        .select(*columns.values())
    )

    return df

# COMMAND ----------

def transform(batch_df, database, spark):
    groupby_columns = {'date', 'platform', 'service', 'step', 'step_name', 'config_1', 'status'}

    agg_columns = {
                    'agg_1': expr('count(distinct player_id) as agg_1')
                  }

    df = (
            batch_df
            .groupBy(*groupby_columns)
            .agg(*agg_columns.values())
            .select(
                *groupby_columns,
                *agg_columns.keys(),
                expr("current_timestamp() AS dw_insert_ts"),
                expr("current_timestamp() AS dw_update_ts"),
                expr("SHA2(CONCAT_WS('|',date, platform, service, step , step_name, config_1 , status),256) AS merge_key")

            )
        )
    return df

# COMMAND ----------

def load(df,database,environment,spark):

    # Merge variables and logic
    target_df = DeltaTable.forName(spark, f"{database}{environment}.managed.agg_ftue_daily")

    merger_condition = 'target.merge_key = source.merge_key'
    merge_update_conditions = [
                                { 
                                 'condition' : """target.agg_1 != source.agg_1
                                                  """,
                                 'set_fields' : {
                                                    'agg_1': 'greatest(target.agg_1,source.agg_1)',
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

def run_agg_ftue(database,environment,view_mapping):
    database = database.lower()

    #setting the spark session
    spark = create_spark_session(name=f"{database}")
    
    #Creating the table and view, checkpoint
    create_agg_ftue_daily(spark, database ,view_mapping)

    print('Extracting the data')
    
    # Reading the data using bath data
    df = extract(database,environment,spark)

    print('Applying the Transformation')
    #Applying Transformation
    df = transform(df, database, spark)

    print('Merge Data')
    load(df,database,environment,spark)

    return 'Merge data completed'

# COMMAND ----------

run_agg_ftue(database,environment,view_mapping)
