# Databricks notebook source
from pyspark.sql.functions import (expr, when, col)
from delta.tables import DeltaTable
from functools import reduce
from pyspark.sql.functions import schema_of_json, from_json, explode

# COMMAND ----------

# MAGIC %run ../../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../../utils/ddl/managed/fact_player_igc_purchase_daily

# COMMAND ----------

# MAGIC %run ../../../../../../../utils/helpers

# COMMAND ----------

database = 'wwe2k25'
input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
title = "'WWE 2K25'"

#For Purchases View has a custom code it need to be created manually
view_mapping = {    
            }

# COMMAND ----------

def read_title(environment,title,spark):
    return (
                spark.read
                .table(f"reference{environment}.title.dim_title")
                .alias('title')
                .where(expr(f"title in ({title}) "))
           )

# COMMAND ----------

def extract(database, environment, spark):

    title_df = read_title(environment,title,spark)
    # Selecting the transaction event data
    columns = {
        "date": expr("receivedOn::timestamp::date as date"),
        "player_id": expr("PLAYERPUBLICID as player_id"),
        "platform": expr('display_platform as platform'),
        "service": expr('display_service as service'),
        "country_code": expr('ifnull(countrycode, "ZZ") as country_code'),
        "sku": expr('currencyamount::int as sku'),
        "igc_type": expr("'VC' as igc_type"),
        "currency_amount": expr('currencyamount::int as currency_amount'),
        "sku_count": expr('1 as sku_count')
    }

    orion_columns = {
        "date": expr("receivedOn::timestamp::date as date"),
        "player_id": expr("PLAYERPUBLICID as player_id"),
        "platform": expr('display_platform as platform'),
        "service": expr('display_service as service'),
        "country_code": expr('ifnull(countrycode, "ZZ") as country_code'),
        "sku": expr('E.item_amount::INT as sku'),
        "igc_type": expr("'VC' as igc_type"),
        "currency_amount": expr('E.item_amount::INT as currency_amount'),
        "sku_count": expr('1 as sku_count')
    }
    transaction_filter = f"""
        PLAYERPUBLICID NOT ILIKE '%Null%' AND PLAYERPUBLICID is not null AND PLAYERPUBLICID != 'anonymous'
        and currencyAction = 'Purchase'
        and to_timestamp(receivedon)::date >= '2025-03-06'
        and (itemvaluestr <> 'Not Applicable' or currencyamount in (187500,400000,67500,32500))
        and concat(sessionId,playerpublicid,platform,occurredon) not in
        (
        select concat(sessionId,playerpublicid,platform,occurredon)
        from {database}{environment}.raw.generaltransaction
        where to_timestamp(receivedon)::date >= '2025-03-01'
            and currencyaction = 'Purchase'
            and buildtype = 'FINAL'
        group by all
        having count(*) >= 3
        )
    """

    orion_transaction_filter = f"""
        PLAYERPUBLICID NOT ILIKE '%Null%' AND PLAYERPUBLICID is not null AND PLAYERPUBLICID != 'anonymous'
        and sourceaction = 'Purchase VC' and to_timestamp(receivedon)::date >= '2025-03-06'
        and transactionsource <> 'First Party'
        and earned is not null
        and buildtype = 'FINAL'
    """

    inc_filter = f"{transaction_filter} and (insert_ts::date between current_date - 2 and current_date)"
    orion_inc_filter = f"{orion_transaction_filter} and (insert_ts::date between current_date - 2 and current_date)"

    prev_max = max_timestamp(spark, f"{database}{environment}.managed.fact_player_igc_purchase_daily", 'date')

    current_min = (
        spark.read
        .table(f"{database}{environment}.raw.generaltransaction")
        .where(expr(inc_filter))
        .select(expr(f"ifnull(min(receivedOn::timestamp::date),'1999-01-01')::date as min_date"))
    ).collect()[0]['min_date']

    orion_current_min = (
        spark.read
        .table(f"{database}{environment}.raw.generaltransaction")
        .where(expr(orion_inc_filter))
        .select(expr(f"ifnull(min(receivedOn::timestamp::date),'1999-01-01')::date as min_date"))
    ).collect()[0]['min_date']

    inc_min_date = min(prev_max, current_min)
    orion_inc_min_date = min(prev_max, orion_current_min)

    batch_filter = f"({transaction_filter}) and receivedOn::timestamp::date >= '{inc_min_date}'::date"
    orion_batch_filter = f"({orion_transaction_filter}) and receivedOn::timestamp::date >= '{orion_inc_min_date}'::date"

    temp_df = (
        spark.read
        .table(f"{database}{environment}.raw.generaltransaction")
        .where(expr(batch_filter))
    )

    orion_temp_df = (
        spark.read
        .table(f"{database}{environment}.raw.generaltransaction")
        .where(expr(orion_batch_filter))
    )

    earned_df = orion_temp_df.select("EARNED")
    if earned_df.head(1):
        sample_json = earned_df.first()["EARNED"]
        json_schema = schema_of_json(sample_json)

        orion_flat_df = (
            orion_temp_df
            .withColumn("parsed_earned", from_json(col("EARNED"), json_schema))
            .withColumn("E", explode("parsed_earned"))
        )
    else:
        print("No purchases available for Orion mode")
        orion_flat_df = spark.createDataFrame([], StructType([]))

    # Join with dim_title to get platform and service values
    purchase_df = (
        temp_df.alias('transaction')
        .join(title_df, expr("transaction.apppublicid = title.app_id"), 'left')
        .select(*columns.values())
        # .distinct()
    )

    # Join with dim_title to get platform and service values
    orion_purchase_df = (
        orion_flat_df.alias('transaction')
        .join(title_df, expr("transaction.apppublicid = title.app_id"), 'left')
        .select(*orion_columns.values())
        # .distinct()
    )
    final_purchase_df = purchase_df.unionByName(orion_purchase_df, allowMissingColumns=True)

    return final_purchase_df

# COMMAND ----------

def transform(batch_df, database, spark):

    groupby_columns = {'date', 'player_id', 'platform', 'service', 'country_code', 'sku', 'igc_type'}

    agg_columns = {
                    'igc_total_amount': expr("sum(currency_amount) as igc_total_amount"),
                    'sku_count': expr("sum(sku_count) as sku_count")
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
                                 'condition' : """target.sku_count != source.sku_count OR 
                                 target.igc_total_amount != source.igc_total_amount
                                 """,
                                 'set_fields' : {   
                                                    'sku_count': 'greatest(target.sku_count,source.sku_count)',
                                                    'igc_total_amount': 'greatest(target.igc_total_amount,source.igc_total_amount)',
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