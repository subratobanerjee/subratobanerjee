# Databricks notebook source
from pyspark.sql.functions import (expr, when)
from delta.tables import DeltaTable
from functools import reduce

# COMMAND ----------

# MAGIC %run ../../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../../utils/ddl/managed/fact_player_igc_earn_daily

# COMMAND ----------

# MAGIC %run ../../../../../../../utils/helpers

# COMMAND ----------

database = 'wwe2k25'
input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())

view_mapping = {
                'game_count_1' : 'game_count_1 as game_count',
                'igc_type_1_earn_count' : 'igc_type_1_earn_count as MFP_count',
                'igc_type_2_earn_count' : 'igc_type_2_earn_count as Store_Credit_count',
                'igc_type_3_earn_count' : 'igc_type_3_earn_count as Tokens_count',
                'igc_type_4_earn_count' : 'igc_type_4_earn_count as AttributePoint_count',
                'igc_type_5_earn_count' : 'igc_type_5_earn_count as VC_count',
                'igc_type_1_earn_amount' : 'igc_type_1_earn_amount as MFP_amount',
                'igc_type_2_earn_amount' : 'igc_type_2_earn_amount as Store_Credit_amount',
                'igc_type_3_earn_amount' : 'igc_type_3_earn_amount as Tokens_amount',
                'igc_type_4_earn_amount' : 'igc_type_4_earn_amount as AttributePoint_amount',
                'igc_type_5_earn_amount' : 'igc_type_5_earn_amount as VC_amount'
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
                "game_mode": 'game_mode',
                "sub_mode": 'sub_mode',
                "transaction_source": expr("source_desc as transaction_source"),
        	    "game_id": 'game_id',
        	    "currency_type": 'currency_type',
        	    "currency_amount": 'currency_amount',
        	    "transaction_id": 'transaction_id',
                "extra_info_1": 'extra_info_1'
                }

    transaction_filter = "player_id NOT ILIKE '%Null%' AND player_id is not null AND player_id != 'anonymous' AND action_type = 'Earn'"

    inc_filter = f"{transaction_filter} and (dw_insert_ts::date between current_date - 2 and current_date)"

    prev_max = max_timestamp(spark, f"{database}{environment}.managed.fact_player_igc_earn_daily", 'date')

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

    groupby_columns = {'et.date', 'et.player_id', 'et.platform', 'et.service', 'et.country_code', 'et.game_mode', 'et.sub_mode', 'et.transaction_source'}

    agg_columns = {
                    'game_count_1': expr("ifnull(count(distinct case when game_id is not null then game_id else null end),0) as game_count_1"),
                    'igc_type_1_earn_count' : expr("ifnull(count(distinct case when currency_type ='MFP' then transaction_id else null end),0) as igc_type_1_earn_count"),
                    'igc_type_2_earn_count' : expr("ifnull(count(distinct case when currency_type ='Store Credit' then transaction_id else null end),0) as igc_type_2_earn_count"),
                    'igc_type_3_earn_count' : expr("ifnull(count(distinct case when currency_type ='Tokens' then transaction_id else null end),0) as igc_type_3_earn_count"),
                    'igc_type_4_earn_count' : expr("ifnull(count(distinct case when currency_type ='AttributePoint' then transaction_id else null end),0) as igc_type_4_earn_count"),
                    'igc_type_5_earn_count' : expr("ifnull(count(distinct case when currency_type ='VC' then transaction_id else null end),0) as igc_type_5_earn_count"),
                    'igc_type_1_earn_amount' : expr("ifnull(sum(case when currency_type ='MFP' then currency_amount else null end),0) as igc_type_1_earn_amount"),
                   'igc_type_2_earn_amount' : expr("ifnull(sum(case when currency_type ='Store Credit' then currency_amount else null end),0) as igc_type_2_earn_amount"),
                    'igc_type_3_earn_amount' : expr("ifnull(sum(case when currency_type ='Tokens' then currency_amount else null end),0) as igc_type_3_earn_amount"),
                    'igc_type_4_earn_amount' : expr("ifnull(sum(case when currency_type ='AttributePoint' then currency_amount else null end),0) as igc_type_4_earn_amount"),
                    'igc_type_5_earn_amount' : expr("ifnull(sum(case when currency_type ='VC' then currency_amount else null end),0) as igc_type_5_earn_amount")
                  }

    earn_transactions = (
            batch_df.alias('et')
            .groupBy(*groupby_columns)
            .agg(*agg_columns.values())
            .select(
                *groupby_columns,
                *agg_columns.keys(),
                expr("current_date() AS dw_insert_date"),
                expr("current_timestamp() AS dw_insert_ts"),
                expr("current_timestamp() AS dw_update_ts"),
                expr("SHA2(CONCAT_WS('|',date, player_id, platform,service, country_code , game_mode, sub_mode ,transaction_source),256) AS merge_key")

            )
        )
    return earn_transactions

# COMMAND ----------

def load(earn_transactions,database,environment,spark):

    # Merge variables and logic
    target_df = DeltaTable.forName(spark, f"{database}{environment}.managed.fact_player_igc_earn_daily")

    merger_condition = 'target.merge_key = source.merge_key'
    merge_update_conditions = [
                                { 
                                 'condition' : """target.GAME_COUNT_1 != source.GAME_COUNT_1 OR
                                                  target.IGC_TYPE_1_EARN_COUNT != source.IGC_TYPE_1_EARN_COUNT OR
                                                  target.IGC_TYPE_1_EARN_AMOUNT != source.IGC_TYPE_1_EARN_AMOUNT OR
                                                  target.IGC_TYPE_2_EARN_COUNT != source.IGC_TYPE_2_EARN_COUNT OR
                                                  target.IGC_TYPE_2_EARN_AMOUNT != source.IGC_TYPE_2_EARN_AMOUNT OR
                                                  target.IGC_TYPE_3_EARN_COUNT != source.IGC_TYPE_3_EARN_COUNT OR
                                                  target.IGC_TYPE_3_EARN_AMOUNT != source.IGC_TYPE_3_EARN_AMOUNT OR
                                                  target.IGC_TYPE_4_EARN_COUNT != source.IGC_TYPE_4_EARN_COUNT OR
                                                  target.IGC_TYPE_4_EARN_AMOUNT != source.IGC_TYPE_4_EARN_AMOUNT
                                                  """,
                                 'set_fields' : {
                                                    'game_count_1': 'greatest(target.GAME_COUNT_1,source.GAME_COUNT_1)',
                                                    'igc_type_1_earn_count': 'greatest(target.IGC_TYPE_1_EARN_COUNT,source.IGC_TYPE_1_EARN_COUNT)',
                                                    'igc_type_1_earn_amount': 'greatest(target.IGC_TYPE_1_EARN_AMOUNT,source.IGC_TYPE_1_EARN_AMOUNT)',
                                                    'igc_type_2_earn_count': 'greatest(target.IGC_TYPE_2_EARN_COUNT,source.IGC_TYPE_2_EARN_COUNT)',
                                                    'igc_type_2_earn_amount': 'greatest(target.IGC_TYPE_2_EARN_AMOUNT,source.IGC_TYPE_2_EARN_AMOUNT)',
                                                    'igc_type_3_earn_count': 'greatest(target.IGC_TYPE_3_EARN_COUNT,source.IGC_TYPE_3_EARN_COUNT)',
                                                    'igc_type_3_earn_amount': 'greatest(target.IGC_TYPE_3_EARN_AMOUNT,source.IGC_TYPE_3_EARN_AMOUNT)',
                                                    'igc_type_4_earn_count': 'greatest(target.IGC_TYPE_4_EARN_COUNT,source.IGC_TYPE_4_EARN_COUNT)',
                                                    'igc_type_4_earn_amount': 'greatest(target.IGC_TYPE_4_EARN_AMOUNT,source.IGC_TYPE_4_EARN_AMOUNT)',
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

def run_earn_transactions(database,environment,view_mapping):
    database = database.lower()

    #setting the spark session
    spark = create_spark_session(name=f"{database}")
    
    #Creating the table and view, checkpoint
    create_fact_player_igc_earn_daily(spark, database ,view_mapping)

    print('Extracting the data')
    
    # Reading the data using bath data
    earn_transaction_df = extract(database,environment,spark)

    #Applying Transformation
    earn_transactions = transform(earn_transaction_df, database, spark)

    #Merge data
    load(earn_transactions,database,environment,spark)

    return 'Merge data completed'

# COMMAND ----------

run_earn_transactions(database,environment,view_mapping)