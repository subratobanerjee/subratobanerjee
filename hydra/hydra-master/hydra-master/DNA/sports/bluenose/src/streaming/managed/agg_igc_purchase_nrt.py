# Databricks notebook source
from pyspark.sql.functions import (expr, when)
from datetime import datetime, timedelta
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/table_functions

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

def create_agg_igc_purchases_nrt_view(spark, database, environment):
    sql = f"""
    CREATE VIEW IF NOT EXISTS {database}{environment}.managed_view.agg_igc_purchase_nrt AS (
        SELECT
            timestamp_10min_slice,
            PLATFORM,
            SERVICE,
            SKU_TYPE,
            SKU_NAME,
            SKU_COUNT,
            IGC_TOTAL_AMOUNT,
            DOLLAR_TOTAL_AMOUNT,
            PLAYER_COUNT,
            DW_INSERT_TS,
            DW_UPDATE_TS
        FROM {database}{environment}.managed.agg_igc_purchase_nrt
    )
    """
    spark.sql(sql)

def create_agg_igc_purchases_nrt(database, environment, spark):
    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.agg_igc_purchase_nrt (
        timestamp_10min_slice timestamp,
        PLATFORM STRING,
        SERVICE STRING,
        SKU_TYPE STRING,
        SKU_NAME STRING,
        SKU_COUNT INT,
        IGC_TOTAL_AMOUNT INT,
        DOLLAR_TOTAL_AMOUNT INT,
        PLAYER_COUNT INT,
        DW_INSERT_TS TIMESTAMP,
        DW_UPDATE_TS TIMESTAMP,
        MERGE_KEY STRING
    )
    COMMENT 'Daily IGM aggregated value'
    PARTITIONED BY (platform, SKU_NAME)
    """
    create_table(spark, sql, properties={})
    create_agg_igc_purchases_nrt_view(spark, database, environment)
    return f"dbfs:/tmp/{database}/managed/streaming/run{environment}/agg_igc_purchase_nrt"

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'bluenose'

# COMMAND ----------

def extract(database, environment, spark):
    # Read the Purchases data
    df = (
        spark.readStream
        .table(f"{database}{environment}.intermediate.fact_player_transaction")
        .where(expr("extra_info_1 = 'full game' AND action_type = 'Purchase'"))
        .select(expr("window(received_on, '10 minute').end::timestamp as timestamp"))
        )

    return df

# COMMAND ----------

def load(df,database,environment,spark):

    # Merge variables and logic
    target_df =  f"{database}{environment}.managed.agg_igc_purchase_nrt"

    source_df = 'agg_igc_p_nrt'
    merge_condition = 'target.merge_key = source.merge_key'

    merge_update_conditions = [
                                { 
                                 'condition' : """target.SKU_COUNT != source.SKU_COUNT OR
                                                  target.IGC_TOTAL_AMOUNT != source.IGC_TOTAL_AMOUNT OR
                                                  target.DOLLAR_TOTAL_AMOUNT != source.DOLLAR_TOTAL_AMOUNT OR
                                                  target.PLAYER_COUNT != source.PLAYER_COUNT
                                                  """,
                                 'set_fields' : {
                                                    'SKU_COUNT': 'greatest(target.SKU_COUNT,source.SKU_COUNT)',
                                                    'IGC_TOTAL_AMOUNT': 'greatest(target.IGC_TOTAL_AMOUNT,source.IGC_TOTAL_AMOUNT)',
                                                    'DOLLAR_TOTAL_AMOUNT': 'greatest(target.DOLLAR_TOTAL_AMOUNT,source.DOLLAR_TOTAL_AMOUNT)',
                                                    'PLAYER_COUNT': 'greatest(target.PLAYER_COUNT,source.PLAYER_COUNT)',                                                    
                                                    'dw_update_ts': 'source.dw_update_ts'
                                                }
                                }
                        ]

    merge_data(target_df, source_df, merge_condition, df , merge_update_conditions)


# COMMAND ----------

def transform(df, batch_id, database, environment, spark):
    lkup_skus = spark.table(f"{database}{environment}.reference.lkup_skus").select('*')
    df_summary = spark.table(f"{database}{environment}.managed.fact_player_summary_ltd").select('*')


    min_time = spark.sql(f"select ifnull(max(timestamp_10min_slice) , '2025-02-21 00:00:00') as min_time from {database}{environment}.managed.agg_igc_purchase_nrt where PLAYER_COUNT > 0").collect()[0]['min_time']
    
    dim_table_columns = {
        "timestamp_10min_slice": "timestamp_10min_slice",
        "platform": "platform",
        "service": "service"
    }

    dim_table_df = (
        spark.read
        .table(f"dataanalytics{environment}.standard_metrics.platform_territory_10min_ts")
        .where(expr(f"""timestamp_10min_slice BETWEEN '{min_time}'::timestamp-interval'12 hours' AND window(current_timestamp(), '10 minutes').end 
                        and (service, platform) IN (('Steam', 'Windows'), ('SEN', 'PS5'), ('Xbox Live', 'XBSX'))"""))
        .select(*dim_table_columns.values())
        .distinct()
    )


    event_columns = {
        "timeslice_10_mins": expr("window(received_on, '10 minute').end as timestamp_10min_slice"),
        "platform": "platform",
        "service": "p.service",
        'SKU_TYPE': expr("CASE WHEN lower(NAME) LIKE '%pack%' THEN 'Pack' ELSE 'VC' END as SKU_TYPE"),
        'SKU_NAME':  expr('NAME as SKU_NAME'),
        'IGC_TOTAL_AMOUNT': expr("vc_amount as IGC_TOTAL_AMOUNT"),
        'DOLLAR_TOTAL_AMOUNT': expr("dollar_value as DOLLAR_TOTAL_AMOUNT"),
        "SKU_COUNT": expr("extra_info_2 as SKU_COUNT"),
        "player_id": 'player_id',
        "transaction_id": "transaction_id",
        "item": expr('item as SKU')
    }

    df = (
        spark.read
        .table(f"{database}{environment}.intermediate.fact_player_transaction").alias('p')
        .join(lkup_skus.alias('s'), expr("p.item = s.sku_id"), "left")
        .where(expr(f"""extra_info_1 = 'full game' AND action_type = 'Purchase' AND (window(received_on, '10 minute').end BETWEEN '{min_time}'::timestamp-interval'12 hours' and current_timestamp())"""))
        .select(*event_columns.values())
    )


    birdie_pack_players = (
        spark.read.table(f"{database}{environment}.intermediate.fact_player_entitlement").alias('e')
        .where(expr("""entitlement_id ='BirdiePack' AND (e.dw_insert_ts::date BETWEEN CURRENT_DATE() - INTERVAL 7 DAYS AND CURRENT_DATE())"""))
        .selectExpr("player_id")
    ).distinct()

    remove_skus = lkup_skus.where(expr("vc_amount in (500, 1300, 1800)")).select("service","sku_id","vc_amount")


    starter_pack_transactions = (
        df.alias("t")
        .join(lkup_skus.alias("s"), expr("s.SKU_ID = t.sku"), "left")
        .join(birdie_pack_players.alias("b"), expr("b.player_id = t.player_id"), "left")
        .join(remove_skus.alias("rs"), expr("rs.service = t.service"), "left")
        .where(expr("name ilike '%starter%'"))
        .select("transaction_id",
                 "t.player_id",
                 "rs.sku_id",
                 "rs.vc_amount",
                 expr("case when b.player_id is  null then True else False end as is_purchased")))


    df = (
            df.alias('p')
            .join(starter_pack_transactions.alias('s'), expr("p.transaction_id = s.transaction_id and p.player_id=s.player_id and p.sku = s.sku_id"), 'left')
            .join(df_summary.alias('df_summary'),expr("p.player_id = df_summary.player_id and p.service = df_summary.service and p.platform = df_summary.platform"),"inner")
            .where(expr("s.vc_amount not in (500, 1300) or (is_purchased = True and vc_amount = 1800) or s.vc_amount is null"))
            .select("p.*")
            )

    groupby_columns = {'timestamp_10min_slice', 'platform', 'service', "SKU_TYPE", 'SKU_NAME'}

    agg_columns = {
        "SKU_COUNT": expr("sum(SKU_COUNT) as SKU_COUNT"),
        "IGC_TOTAL_AMOUNT": expr("min(IGC_TOTAL_AMOUNT) * sum(SKU_COUNT) as IGC_TOTAL_AMOUNT"),
        "DOLLAR_TOTAL_AMOUNT": expr("min(DOLLAR_TOTAL_AMOUNT) * sum(SKU_COUNT) as DOLLAR_TOTAL_AMOUNT"),
        "PLAYER_COUNT": expr("count(distinct player_id) as PLAYER_COUNT")
    }

    df = (
        df
        .groupBy(*groupby_columns)
        .agg(*agg_columns.values())
        .select(
            *groupby_columns,
            *agg_columns.keys()
        )
    )

    finial_columns = {
        "timeslice_10_mins": "d.timestamp_10min_slice",
        "platform": "d.platform",
        "service": "d.service",
        'SKU_TYPE': expr("ifnull(SKU_TYPE,'default') as SKU_TYPE"),
        'SKU_NAME':  expr("ifnull(SKU_NAME,'default') as SKU_NAME"),
        'IGC_TOTAL_AMOUNT': expr("ifnull(IGC_TOTAL_AMOUNT,0) as IGC_TOTAL_AMOUNT"),
        'DOLLAR_TOTAL_AMOUNT': expr("ifnull(DOLLAR_TOTAL_AMOUNT,0) as DOLLAR_TOTAL_AMOUNT"),
        "SKU_COUNT": expr("ifnull(SKU_COUNT,0) as SKU_COUNT"),
        "PLAYER_COUNT": expr("ifnull(PLAYER_COUNT,0) as PLAYER_COUNT")

    }

    df = (
        dim_table_df.alias('d')
        .join(df.alias('e'), expr("d.timestamp_10min_slice = e.timestamp_10min_slice and d.platform = e.platform and d.service = e.service"), "left")
        .select(
            *finial_columns.values(),
            expr("current_timestamp() AS dw_insert_ts"),
            expr("current_timestamp() AS dw_update_ts"),
            expr("SHA2(CONCAT_WS('|',d.timestamp_10min_slice, d.platform, d.service, ifnull(SKU_TYPE,'default') , ifnull(SKU_NAME,'default')),256) AS merge_key")
        )
    )
    load(df,database,environment,spark)

# COMMAND ----------

def stream_job(database,environment):
    database = database.lower()

    #setting the spark session
    spark = create_spark_session(name=f"{database}")
    
    #Setting the checkpoint_location from the fucntion output
    checkpoint_location = create_agg_igc_purchases_nrt(database,environment,spark)
    print(f'checkpoint_location : {checkpoint_location}')
    
    df = extract(database,environment,spark)

    print('Started writeStream')

    #Writing the stream output to the table 
    (
        df
        .writeStream
        .trigger(availableNow=True)
        .foreachBatch(lambda df, batch_id: transform(df, batch_id, database,environment, spark))
        .option("checkpointLocation", checkpoint_location)
        .outputMode("append")
        .queryName(f'{database}_agg_igc_purchase_nrt')
        .start()
    )


# COMMAND ----------

stream_job(database, environment)
