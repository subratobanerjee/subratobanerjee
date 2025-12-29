# Databricks notebook source
from pyspark.sql.functions import (expr, when)
from delta.tables import DeltaTable
from functools import reduce

# COMMAND ----------

# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/managed/fact_player_igc_purchase_daily

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

database = 'bluenose'
input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())

#For Purchases View has a custom code it need to be created manually
view_mapping = {    
            }

# COMMAND ----------

def extract(database,environment,spark):
    # Selecting the transaction event data
    columns = {
                "date": expr("received_on::date as date"),
                "received_on": expr("received_on::TIMESTAMP as received_on"),
                "player_id": 'player_id',
                "platform": 'platform',
                "service": 'service',
                "country_code" :'country_code',
        	    "item": expr('item as SKU'),
        	    "currency_type": expr('currency_type as igc_type'),
        	    "currency_amount": 'currency_amount',
        	    "sku_count": expr('extra_info_2 as sku_count')
                }

    transaction_filter = "player_id NOT ILIKE '%Null%' AND player_id is not null AND player_id != 'anonymous' AND action_type = 'Purchase'"

    inc_filter = f"{transaction_filter} and (dw_insert_ts::date between current_date - 2 and current_date)"

    prev_max = max_timestamp(spark, f"{database}{environment}.managed.fact_player_igc_purchase_daily", 'date')

    current_min = (
        spark.read
        .table(f"{database}{environment}.intermediate.fact_player_transaction")
        .where(expr(inc_filter))
        .select(expr(f"ifnull(min(received_on),'1999-01-01')::date as min_date"))
    ).collect()[0]['min_date']

    inc_min_date = min(prev_max,current_min)

    batch_filter = f"({transaction_filter}) and received_on::date >= '{inc_min_date}'::date"

    earn_transaction_df = (
        spark.read
        .table(f"{database}{environment}.intermediate.fact_player_transaction")
        .where(expr(batch_filter))
        .select(*columns.values())
    )

    return earn_transaction_df

# COMMAND ----------

def transform(batch_df, database, spark):

    groupby_columns = {'date', 'player_id', 'platform', 'service', 'country_code', 'sku', 'igc_type'}

    agg_columns = {
                    'sku_count': expr("sum(sku_count) as sku_count"),
                  }

    earn_transactions = (
            batch_df.alias('et')
            .groupBy(*groupby_columns)
            .agg(*agg_columns.values())
            .select(
                *groupby_columns,
                expr('NULL as igc_total_amount'),
                *agg_columns.keys(),
                expr("current_date() AS dw_insert_date"),
                expr("current_timestamp() AS dw_insert_ts"),
                expr("current_timestamp() AS dw_update_ts"),
                expr("SHA2(CONCAT_WS('|',date, player_id, platform, service, country_code , sku, igc_type),256) AS merge_key")

            )
        )
    return earn_transactions

# COMMAND ----------

def load(earn_transactions,database,environment,spark):

    # Merge variables and logic
    target_df = DeltaTable.forName(spark, f"{database}{environment}.managed.fact_player_igc_purchase_daily")

    merger_condition = 'target.merge_key = source.merge_key'
    merge_update_conditions = [
                                { 
                                 'condition' : """target.sku_count != source.sku_count""",
                                 'set_fields' : {   
                                                    'sku_count': 'greatest(target.sku_count,source.sku_count)',
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

def run_batch(database,environment,view_mapping):
    database = database.lower()

    #setting the spark session
    spark = create_spark_session(name=f"{database}")
    
    #Creating the table and view, checkpoint
    create_fact_player_igc_purchase_daily(spark, database ,view_mapping)

    print('Extracting the data')
    
    # Reading the data using bath data
    earn_transaction_df = extract(database,environment,spark)

    #Applying Transformation
    earn_transactions = transform(earn_transaction_df, database, spark)

    #Merge data
    load(earn_transactions,database,environment,spark)

    return 'Merge data completed'

# COMMAND ----------

run_batch(database,environment,view_mapping)
