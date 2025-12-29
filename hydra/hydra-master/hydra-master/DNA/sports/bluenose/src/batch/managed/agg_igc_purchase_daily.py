# Databricks notebook source
from pyspark.sql.functions import (expr, when)
from delta.tables import DeltaTable
from functools import reduce

# COMMAND ----------

# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/managed/agg_igc_purchase_daily

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

database = 'bluenose'
input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())

#For Purchases View has a custom code it need to be created manually
view_mapping = {
                'player_count_1': 'ifnull(player_count_1,0) as converted_player_count',
                'player_count_2': 'ifnull(player_count_2,0) as full_game_player_count',
            }

# COMMAND ----------

def extract(database,environment,spark):
    # Selecting the transaction event data
    columns = {
                "date": "date",
                "player_id": 'player_id',
                "platform": 'platform',
                "service": 'service',
                "country_code" :'country_code',
                "SKU": 'SKU',
                "igc_type": 'igc_type',
                "IGC_TOTAL_AMOUNT": 'IGC_TOTAL_AMOUNT',
                "sku_count" : 'sku_count'
                }

    prev_max = max_timestamp(spark, f"{database}{environment}.managed.agg_igc_purchase_daily", 'date')

    current_min = (
        spark.read
        .table(f"{database}{environment}.managed.fact_player_igc_purchase_daily")
        .where(expr("DW_INSERT_TS::date between current_date - 2 and current_date"))
        .select(expr(f"ifnull(min(date),'1999-01-01')::date as min_date"))
    ).collect()[0]['min_date']

    inc_min_date = min(prev_max,current_min)

    earn_transaction_df = (
        spark.read
        .table(f"{database}{environment}.managed.fact_player_igc_purchase_daily")
        .where(expr(f"date >= '{inc_min_date}'::date"))
        .select(*columns.values())
    )

    return earn_transaction_df

# COMMAND ----------

def transform(batch_df, database, spark):
    df_summary = spark.table(f"{database}{environment}.managed.fact_player_summary_ltd").select('*')

    groupby_columns = {'et.date', 'et.platform', 'et.service', 'et.country_code', 'et.SKU', 'et.igc_type'}



    agg_columns = {
                   
                    "player_count_1": expr("count(distinct case when ltd_string_1= 'converted' then et.player_id else null end) as player_count_1"),
                    "player_count_2": expr("count(distinct case when ltd_string_1!= 'converted' then et.player_id else null end) as player_count_2"),
                    "SKU_COUNT": expr("sum(SKU_COUNT) as SKU_COUNT")
                  }

    earn_transactions = (
            batch_df.alias('et')
            .join(df_summary.alias('df_summary'),expr("et.player_id = df_summary.player_id and et.service = df_summary.service and et.platform = df_summary.platform"),"left")
            .groupBy(*groupby_columns)
            .agg(*agg_columns.values())
            .select(
                *groupby_columns,
                *agg_columns.keys(),
                expr('NULL as IGC_TOTAL_AMOUNT'),
                expr("current_timestamp() AS dw_insert_ts"),
                expr("current_timestamp() AS dw_update_ts"),
                expr("SHA2(CONCAT_WS('|',date, et.platform, et.service, country_code , sku, igc_type),256) AS merge_key")

            )
        )
    return earn_transactions

# COMMAND ----------

def load(earn_transactions,database,environment,spark):

    # Merge variables and logic
    target_df = DeltaTable.forName(spark, f"{database}{environment}.managed.agg_igc_purchase_daily")

    merger_condition = 'target.merge_key = source.merge_key'
    merge_update_conditions = [
                                { 
                                 'condition' : """target.player_count_1 != source.player_count_1 OR
                                                  target.player_count_2 != source.player_count_2 OR
                                                  target.SKU_COUNT != source.SKU_COUNT OR
                                                  """,
                                 'set_fields' : {
                                                
                                                    'player_count_1': 'greatest(target.PLAYER_COUNT_1,source.PLAYER_COUNT_1)',
                                                    'player_count_2': 'greatest(target.PLAYER_COUNT_2,source.PLAYER_COUNT_2)',
                                                    'SKU_COUNT': 'greatest(target.SKU_COUNT,source.SKU_COUNT)',
                                                    'dw_update_ts': 'source.dw_update_ts'
                                                }
                                }
                        ]

    merge_df = target_df.alias("target").merge(earn_transactions.alias("source"), f"{merger_condition}")

    merge_df = set_merge_update_condition(merge_df, merge_update_conditions)

    merge_df = set_merge_insert_condition(merge_df, earn_transactions)

    # Execute the merge operation
    merge_df.execute()

# COMMAND ----------

def run_agg_earns(database,environment,view_mapping):
    database = database.lower()

    #setting the spark session
    spark = create_spark_session(name=f"{database}")
    
    #Creating the table and view, checkpoint
    create_agg_igc_purchase_daily(spark, database ,view_mapping)

    print('Extracting the data')
    
    # Reading the data using bath data
    earn_transaction_df = extract(database,environment,spark)

    print('Applying the Transformation')
    #Applying Transformation
    earn_transactions = transform(earn_transaction_df, database, spark)

    print('Merge Data')
    load(earn_transactions,database,environment,spark)

    return 'Merge data completed'

# COMMAND ----------

run_agg_earns(database,environment,view_mapping)
