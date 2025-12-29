# Databricks notebook source
# MAGIC %run ../../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../../utils/ddl/intermediate/fact_player_transaction

# COMMAND ----------

from pyspark.sql.functions import expr, when, col, from_json, explode , explode_outer
from pyspark.sql.functions import col, expr
from functools import reduce
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, DecimalType


# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'wwe2k25'
title = "'WWE 2K25'"

# COMMAND ----------


def read_transactions(database, environment, spark):
    # Define the schema for JSON parsing for both earned and spent
    json_schema = ArrayType(
        StructType([
            StructField("type", StringType(), True),
            StructField("item_type", StringType(), True),
            StructField("item_id", StringType(), True),
            StructField("item_desc", StringType(), True),
            StructField("item_amount", DecimalType(38,2), True),
            StructField("item_amount_spent", DecimalType(38,2), True)
        ])
    )
    
    # Read the raw table
    raw_df = (
        spark.readStream
        .table(f"{database}{environment}.raw.generaltransaction")
    )

    # raw_df.groupBy("gamemode").count().show(truncate=False)
    
    # Parse and explode the earned column
    exploded_df = raw_df.withColumn("earned_json", from_json(col("earned"), json_schema)) \
                        .withColumn("earned_exploded", explode_outer(col("earned_json"))) 
    
                        # .withColumn("spent_json", from_json(col("spent"), json_schema)) \
                        # .withColumn("spent_exploded", explode(col("spent_json")))



    # exploded_df.groupBy("gamemode").count().show(truncate=False)
    # Apply transformations
    final_df = exploded_df.select(
        expr("ifnull(transactionguid, RECEIVEDON::timestamp || '-Null-Id') as transaction_id"),
        expr("PLAYERPUBLICID as player_id"),
        expr("APPPUBLICID as app_id"),
        expr("receivedon::timestamp as received_on"),
        expr("MODESESSIONID as game_id"),
        expr("gamemode as game_mode"),
        expr("SUBMODE as sub_mode"),
        expr("ifnull(countrycode, 'ZZ') as country_code"),
        expr("buildcl as build_changelist"),
        expr("cast(earned_exploded.item_amount as string) as extra_info_1"),
        expr("'N/A' as extra_info_2"),

        # Exploded earned JSON fields
        expr("""
             case when gamemode = 'Orion' then sourceaction else 
             transactionsource end
             """).alias("source_desc"),
        expr("""
             case when spent IS NOT NULL AND sourceaction = 'Purchase VC' then 'VC'
             when spent IS NOT NULL then get_json_object(spent, '$[0].item_type')
             WHEN spent IS NULL AND earned IS NOT NULL then earned_exploded.item_type 
             else currencytype end
             """).alias("currency_type"),
        expr("""
            CASE 
                WHEN spent IS NOT NULL AND sourceaction = 'Purchase VC' THEN 'N/A'
                WHEN (spent IS NOT NULL) or (spent IS NULL AND earned IS NOT NULL) THEN earned_exploded.item_id
                else itemvalue
            END
        """).alias("item"),
        expr("""
             case when (spent IS NOT NULL AND sourceaction = 'Purchase VC') or (spent IS NULL AND earned IS NOT NULL) then cast(earned_exploded.item_amount AS DECIMAL(38,2)) 
             when spent IS NOT NULL and earned_exploded.item_amount_spent <= 0 then cast(get_json_object(spent, '$[0].item_amount') AS DECIMAL(38,2))
             when spent IS NOT NULL and earned_exploded.item_amount_spent >= 0 then cast(earned_exploded.item_amount_spent AS DECIMAL(38,2))
             else cast(currencyAmount AS DECIMAL(38,2)) end
             """).alias("currency_amount"),
        
        # # Exploded spent JSON fields
        # col("spent_exploded.item_desc").alias("spent_desc"),
        # col("spent_exploded.item_type").alias("spent_type"),
        # col("spent_exploded.item_id").alias("spent_item"),
        # col("spent_exploded.item_amount").cast(DecimalType(38, 2)).alias("spent_amount"),  # Cast to DecimalType

        # Logic for action type
        expr("""
            CASE 
                WHEN spent IS NOT NULL AND sourceaction = 'Purchase VC' THEN 'Purchase'
                WHEN spent IS NOT NULL and sourceaction <> 'Purchase VC' THEN 'Spend' 
                WHEN spent IS NULL AND earned IS NOT NULL THEN 'Earn' 
                ELSE ifnull(currencyaction, 'Missing') 
            END
        """).alias("action_type"),

        # Logic for item type and price
        expr("""
            CASE 
                WHEN gamemode = 'Orion' AND spent IS NOT NULL THEN earned_exploded.item_type
                else itemtype 
            END
        """).alias("item_type"),

        col("itemvalue").cast(DecimalType(38, 2)).alias("item_price")
    )
    
    # final_df.groupBy("game_mode").count().show(truncate=False)

    # Apply filtering conditions
    return final_df.where("""
        PLAYERPUBLICID IS NOT NULL 
        AND PLAYERPUBLICID != 'anonymous' 
        AND buildtype = 'FINAL' 
        AND currencyaction IN ('Earn', 'Purchase', 'Not Applicable', 'Spend') 
        AND itemtype IS NOT NULL
    """)


def read_title(environment,title,spark):
    return (
                spark.read
                .table(f"reference{environment}.title.dim_title")
                .alias('title')
                .where(expr(f"title in ({title}) "))
           )

def extract(database,environment,title,spark):
    # Reading the data using readStream
    transaction_df = read_transactions(database,environment,spark)
    title_df = read_title(environment,title,spark)
    # all_transactions = reconciliation_df + transaction_df
    all_transactions =  transaction_df
    print('readStream Done')
    all_transactions.printSchema()

    return all_transactions, title_df
# COMMAND ----------

def transform(df,title_df,spark):

    # transaction_df = reduce(lambda df1, df2: df1.union(df2), df)

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
        df.alias('transaction')
        .join(title_df, expr("transaction.app_id = title.app_id"), 'left')
        .select(*columns.values())
        # .distinct()
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
    all_transaction_df.printSchema()


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

# COMMAND ----------

# dbutils.fs.rm("dbfs:/tmp/wwe2k25/intermediate/streaming/run_dev/fact_player_transaction", True)
# spark.sql("drop table wwe2k25_dev.intermediate.fact_player_transaction")