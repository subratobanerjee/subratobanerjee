# Databricks notebook source
from pyspark.sql.functions import (expr, when, from_json, explode)
from functools import reduce

# COMMAND ----------

# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/intermediate/fact_player_transaction

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'bluenose'
title = "'PGA Tour 2K25','PGA Tour 2K25: Demo'"

# COMMAND ----------

def read_transactions(database,environment,spark):
    event_columns = {
                        "transaction_id": expr("ifnull(transactioninstanceid,RECEIVEDON::timestamp||'-Null-Id')::STRING as transaction_id"),
                        "player_id": expr("PLAYERPUBLICID::STRING as player_id"),
                        "app_id": expr("APPPUBLICID::STRING as app_id"),
                        "received_on": expr("receivedon::timestamp::TIMESTAMP as received_on"),
                        "game_id": expr("MODESESSIONID::STRING as game_id"),
                        "game_mode": expr("MODE::STRING as game_mode"),
                        "sub_mode": expr("SUBMODE::STRING as sub_mode"),
                        "source_desc": expr("""action.reason::string as  source_desc"""),
                        "country_code": expr("ifnull(countrycode,'ZZ')::STRING as country_code"),
                        "currency_type": expr("""action.type::string as currency_type"""),
                        "currency_amount": expr("""action.amount::DECIMAL(38,2) as currency_amount"""),
                        "action_type": expr("ifnull(TRANSACTIONTRIGGERTYPE,'Missing')::STRING as action_type"),
                        "item": expr(""" action.itemId::STRING as item"""),
                        "item_type": expr("""
                            CASE 
                                WHEN TRANSACTIONTRIGGERTYPE = 'Spend' THEN action.type
                            END::STRING as item_type
                        """),
                        "item_price": expr("""
                            CASE 
                                WHEN TRANSACTIONTRIGGERTYPE = 'Spend' THEN action.amount
                            END::DECIMAL(38,2) as item_price
                        """),
                        "build_changelist": expr("BUILDCHANGELIST::STRING as build_changelist"),
                        "extra_info_1": expr("""
                            CASE 
                                WHEN APPGROUPID IN ('317c0552032c4804bb10d81b89f4c37e', '317c0552032c4804bb10d81b89f4c37e') THEN 'full game' 
                                WHEN APPGROUPID = '886bcab8848046d0bd89f5f1ce3b057b' THEN 'demo' 
                            END::STRING as extra_info_1
                        """),
                        "extra_info_2": expr('clubpassseasonid as extra_info_2')
                    }

    # Read the event stream
    actions = ['Earn','Spend']
    actions_df = []
    for action in actions:
        if action == 'Spend':
            field = 'spentitems'
        else:
            field = 'earneditems'
        df = (
                spark.readStream
                .table(f"{database}{environment}.raw.transaction")
                .select('*')
                .where(expr(f"""PLAYERPUBLICID is not null and PLAYERPUBLICID != 'anonymous' AND buildenvironment = 'RELEASE'  and TRANSACTIONTRIGGERTYPE ='{action}' and {field} is not null"""))
            )

        schema = "ARRAY<STRUCT<amount: BIGINT, itemId: STRING, reason: STRING, type: STRING>>"
        df_parsed = (df.withColumn("parsed", from_json(expr(f"{field}"), schema)))
        df_exploded = (df_parsed.withColumn("action", explode(expr("parsed"))))
        df = df_exploded.select(*event_columns.values())
        actions_df.append(df)
    return actions_df

def read_title(environment,title,spark):
    return (
                spark.read
                .table(f"reference{environment}.title.dim_title")
                .alias('title')
                .where(expr(f"title in ({title}) "))
           )

def read_reconciliation_fulfilled(title,database,environment,spark):
    event_columns = {
        "transaction_id": expr("transactionid as transaction_id"),
        "player_id": expr("accountid AS player_id"),
        "app_id": expr("APPPUBLICID::STRING as app_id"),
        "received_on": expr("occurredOn::timestamp as received_on"),
        "game_id": expr("Null as game_id"),
        "game_mode": expr("Null as game_mode"),
        "sub_mode": expr("Null as sub_mode"),
        "source_desc": expr("'Reconciliation fulfilled' as source_desc"),
        "country_code": expr("countrycode as country_code"),
        "currency_type": expr("'VC' as currency_type"),
        "currency_amount": expr("NULL as currency_amount"),
        "action_type": expr("'Purchase' AS action_type"),
        "item": expr("items.id as item"),
        "item_type": expr("NULL as item_type"),
        "item_price": expr("NULL as item_price"),
        "build_changelist": expr("Null as build_changelist"),
        "extra_info_1": expr("'full game'  as extra_info_1"),
        "extra_info_2": expr("items.quantity as extra_info_2")
    }

    schema = "ARRAY<STRUCT<id: STRING, quantity: BIGINT>>"

    services = ['xbox' ,'playstation' ,'steam']

    title_df = read_title(environment,title,spark)

    app_ids = ",".join(f"'{row.APP_ID}'" for row in title_df.select('APP_ID').where(expr("title = 'PGA Tour 2K25'")).collect())

    reconciliation_df = []
    for service in services:
        df = (
            spark.readStream
            .table(f"coretech{environment}.ecommercev3.{service}reconciliationfulfilledevent")
            .select('*')
            .where(expr(f"APPPUBLICID in ({app_ids})"))
         )

        df_parsed = df.withColumn("items_parsed", from_json(expr("firstpartydetails:items"), schema))

        df_exploded = df_parsed.withColumn("items", explode(expr("items_parsed")))

        df = df_exploded.select(*event_columns.values())

        reconciliation_df.append(df)

    return reconciliation_df

def extract(database,environment,title,spark):
    # Reading the data using readStream
    transaction_df = read_transactions(database,environment,spark)
    title_df = read_title(environment,title,spark)
    reconciliation_df = read_reconciliation_fulfilled(title,database,environment,spark)
    all_transactions = reconciliation_df + transaction_df
    print('readStream Done')
    return all_transactions, title_df

# COMMAND ----------

def transform(df,title_df,spark):

    transaction_df = reduce(lambda df1, df2: df1.union(df2), df)

    # Columns for final selection
    columns = {
        "transaction_id": expr("ifnull(transaction_id,received_on::timestamp||'-Null-Id') as transaction_id"),
        "player_id": expr("IFNULL(player_id, CONCAT(RECEIVED_ON::date::string, '-NULL-Player')) AS player_id"),
        "platform": expr("IFNULL(display_platform, 'Unknown') AS platform"),
        "service": expr("IFNULL(display_service, 'Unknown') AS service"),
        "received_on": "received_on",
        "game_id": "game_id",
        "game_mode": "game_mode",
        "sub_mode": "sub_mode",
        "source_desc": "source_desc",
        "country_code": "country_code",
        "currency_type": "currency_type",
        "currency_amount": expr("case when currency_amount = 0 then 1 else currency_amount end::DECIMAL(38,2) as currency_amount"),
        "action_type": expr("IFNULL(action_type, 'Unknown') AS action_type"),
        "item": "item",
        "item_type": "item_type",
        "item_price": "item_price",
        "build_changelist": "build_changelist",
        "extra_info_1": "extra_info_1",
        "extra_info_2": "extra_info_2",
        "dw_insert_date": expr("current_date() AS dw_insert_date"),
        "dw_insert_ts": expr("current_timestamp() AS dw_insert_ts"),
        "dw_update_ts": expr("current_timestamp() AS dw_update_ts")
    }

    # Join with dim_title to get platform and service values
    transaction_df = (
        transaction_df.alias('transaction')
        .join(title_df, expr("transaction.app_id = title.app_id"), 'left')
        .select(*columns.values())
    )

    return transaction_df

# COMMAND ----------

def stream_transactions(database,environment,title):
    database = database.lower()

    #setting the spark session
    spark = create_spark_session(name=f"{database}")
    
    #Setting the checkpoint_location from the fucntion output
    checkpoint_location = create_fact_player_transaction(spark, database)
    print(f'checkpoint_location : {checkpoint_location}')
    
    transaction_df, title_df = extract(database,environment,title,spark)

    all_transaction_df = transform(transaction_df,title_df,spark)

    print('Started writeStream')

    #Writing the stream output to the title level fact_player_transaction table 
    (
        all_transaction_df
        .writeStream
        .trigger(availableNow=True)
        .option("checkpointLocation", checkpoint_location)
        .toTable(f"{database}{environment}.intermediate.fact_player_transaction")
    )

# COMMAND ----------

stream_transactions(database,environment,title)
