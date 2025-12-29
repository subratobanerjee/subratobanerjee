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

def create_agg_igc_earn_spend_nrt_view(spark, database, environment):
    sql = f"""
    CREATE VIEW IF NOT EXISTS {database}{environment}.managed_view.agg_igc_earn_spend_nrt AS (
        SELECT
            timestamp_10min_slice,
            PLATFORM,
            SERVICE,
            action_type,
            source_desc,
            IGC_type,
            IGC_TOTAL_AMOUNT,
            PLAYER_COUNT,
            DW_INSERT_TS,
            DW_UPDATE_TS
        FROM {database}{environment}.managed.agg_igc_earn_spend_nrt
    )
    """
    spark.sql(sql)

def create_agg_igc_earn_spend_nrt(database, environment, spark):
    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.agg_igc_earn_spend_nrt (
        timestamp_10min_slice timestamp,
        PLATFORM STRING,
        SERVICE STRING,
        action_type STRING,
        source_desc STRING,
        IGC_type string,
        IGC_TOTAL_AMOUNT INT,
        PLAYER_COUNT INT,
        DW_INSERT_TS TIMESTAMP,
        DW_UPDATE_TS TIMESTAMP,
        MERGE_KEY STRING
    )
    COMMENT 'Daily IGM aggregated value'
    PARTITIONED BY (platform, source_desc)
    """
    create_table(spark, sql, properties={})
    create_agg_igc_earn_spend_nrt_view(spark, database, environment)
    return f"dbfs:/tmp/{database}/managed/streaming/run{environment}/agg_igc_earn_spend_nrt"

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
        .where(expr("extra_info_1 = 'full game' AND action_type in('Earn', 'Spend')"))
        .select(expr("window(received_on, '10 minute').end::timestamp as timestamp"))
        )

    return df

# COMMAND ----------

def load(df,database,environment,spark):

    # Merge variables and logic
    target_df = f"{database}{environment}.managed.agg_igc_earn_spend_nrt"
    source_df = 'agg_igc_es_nrt'
    merge_condition = 'target.merge_key = source.merge_key'
    merge_update_conditions = [
                                { 
                                 'condition' : """target.IGC_TOTAL_AMOUNT != source.IGC_TOTAL_AMOUNT OR
                                                  target.PLAYER_COUNT != source.PLAYER_COUNT
                                                  """,
                                 'set_fields' : {
                                                'IGC_TOTAL_AMOUNT': 'greatest(target.IGC_TOTAL_AMOUNT,source.IGC_TOTAL_AMOUNT)',
                                                'PLAYER_COUNT': 'greatest(target.PLAYER_COUNT,source.PLAYER_COUNT)',                                                    
                                                'dw_update_ts': 'source.dw_update_ts'
                                                }
                                }
                        ]

    merge_data(target_df, source_df, merge_condition, df , merge_update_conditions)


# COMMAND ----------

def transform(df, batch_id, database, environment, spark):

    min_time = spark.sql(f"select ifnull(max(timestamp_10min_slice) , '2025-02-21 00:00:00') as min_time from {database}{environment}.managed.agg_igc_earn_spend_nrt where PLAYER_COUNT > 0").collect()[0]['min_time']
    
    dim_table_columns = {
        "timestamp_10min_slice": "timestamp_10min_slice",
        "platform": "platform",
        "service": "service"
    }

    dim_table_df = (
        spark.read
        .table(f"dataanalytics{environment}.standard_metrics.platform_territory_10min_ts")
        .where(expr(f"""timestamp_10min_slice BETWEEN '{min_time}'::timestamp AND window(current_timestamp(), '10 minutes').end 
                        and (service, platform) IN (('Steam', 'Windows'), ('SEN', 'PS5'), ('Xbox Live', 'XBSX'))"""))
        .select(*dim_table_columns.values())
        .distinct()
    )


    event_columns = {
        "timeslice_10_mins": expr("window(received_on, '10 minute').end as timestamp_10min_slice"),
        "platform": "platform",
        "service": "service",
        'action_type': 'action_type',
        'source_desc':  'source_desc',
        'igc_type': expr('currency_type as igc_type'),
        'currency_amount': 'currency_amount',
        "player_id": 'player_id'
    }

    df = (
        spark.read
        .table(f"{database}{environment}.intermediate.fact_player_transaction").alias('p')
        .where(expr(f"""extra_info_1 = 'full game' AND action_type in ('Earn', 'Spend') AND (window(received_on, '10 minute').end BETWEEN '{min_time}'::timestamp and current_timestamp())"""))
        .select(*event_columns.values())
    )

    groupby_columns = {'timestamp_10min_slice', 'platform', 'service', "action_type", 'source_desc', 'igc_type'}

    agg_columns = {
        "IGC_TOTAL_AMOUNT": expr("sum(currency_amount) as IGC_TOTAL_AMOUNT"),
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
        'action_type': expr("ifnull(action_type,'default') as action_type"),
        'source_desc':  expr("ifnull(source_desc,'default') as source_desc"),
        'igc_type': expr("ifnull(igc_type,'default') as igc_type"),
        "IGC_TOTAL_AMOUNT": expr("ifnull(IGC_TOTAL_AMOUNT,0) as IGC_TOTAL_AMOUNT"),
        "PLAYER_COUNT": expr("ifnull(PLAYER_COUNT,0) as PLAYER_COUNT")

    }

    df = (
        dim_table_df.alias('d')
        .join(df.alias('e'), expr("d.timestamp_10min_slice = e.timestamp_10min_slice and d.platform = e.platform and d.service = e.service"), "left")
        .select(
            *finial_columns.values(),
            expr("current_timestamp() AS dw_insert_ts"),
            expr("current_timestamp() AS dw_update_ts"),
            expr("SHA2(CONCAT_WS('|',d.timestamp_10min_slice, d.platform, d.service, ifnull(action_type,'default') , ifnull(source_desc,'default'), ifnull(igc_type,'default')),256) AS merge_key")
        )
    )
    load(df,database,environment,spark)

# COMMAND ----------

def stream_job(database,environment):
    database = database.lower()

    #setting the spark session
    spark = create_spark_session(name=f"{database}")
    
    #Setting the checkpoint_location from the fucntion output
    checkpoint_location = create_agg_igc_earn_spend_nrt(database,environment,spark)
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
        .queryName(f'{database}_agg_igc_earn_spend_nrt')
        .start()
    )


# COMMAND ----------

stream_job(database, environment)
