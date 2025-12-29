# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/table_functions

# COMMAND ----------

from pyspark.sql.functions import expr, window
from datetime import datetime, timedelta
from delta.tables import DeltaTable

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'bluenose'
data_source ='dna'
title = "'PGA Tour 2K25','PGA Tour 2K25: Demo'"

# COMMAND ----------

def create_agg_seasonpass_purchase_nrt(database,environment,spark):
    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.agg_seasonpass_purchase_nrt (
        timestamp_10min_slice TIMESTAMP,
        platform STRING,
        service STRING,
        player_type STRING,
        pass_type string,
        country_code STRING,
        purchase_count INT,
        dw_insert_ts TIMESTAMP,
        dw_update_ts TIMESTAMP,
        merge_key STRING
    )
    """
    create_table(spark,sql, properties={})
    create_agg_seasonpass_purchase_nrt_view(database,environment,spark)
    return f"dbfs:/tmp/{database}/managed/streaming/run{environment}/agg_seasonpass_purchase_nrt"

# COMMAND ----------

def create_agg_seasonpass_purchase_nrt_view(database,environment,spark):
    """
    Create the fact_player_game_status_daily view in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
        mapping (dict): A dictionary of column mappings.
    """

    sql = f"""
    CREATE VIEW IF NOT EXISTS {database}{environment}.managed_view.agg_seasonpass_purchase_nrt AS (
        SELECT 
                timestamp_10min_slice,
                platform,
                service,
                player_type,
                pass_type,
                country_code,
                purchase_count,
                dw_insert_ts,
                dw_update_ts
        from {database}{environment}.managed.agg_seasonpass_purchase_nrt
        where timestamp_10min_slice <= window(current_timestamp(),'10 minutes').end
    )
    """
    spark.sql(sql)

# COMMAND ----------

def extract(spark, database, environment):
    df = (
            spark
            .readStream
            .table(f"{database}{environment}.raw.applicationsession")
            .select(expr('receivedOn::timestamp as received_on'))
         )
    return df

# COMMAND ----------

def load(df,database,environment,spark):
    # Merge variables and logic
    target_df = f"{database}{environment}.managed.agg_seasonpass_purchase_nrt"
    source_df = 'agg_spp_df'
    merge_condition = 'target.merge_key = source.merge_key'
    merge_update_conditions = [
                                { 
                                 'condition' : """target.purchase_count != source.purchase_count""",
                                 'set_fields' : {
                                                    'purchase_count': 'source.purchase_count',
                                                    'dw_update_ts': 'source.dw_update_ts'
                                                }
                                }
                        ]

    merge_data(target_df, source_df, merge_condition, df , merge_update_conditions)
# COMMAND ----------

def transform(df, batch_id, database,environment, spark):
    min_time = spark.sql(f"select ifnull(max(timestamp_10min_slice) , '2025-02-21 00:00:00') as min_time from {database}{environment}.managed.agg_seasonpass_purchase_nrt where purchase_count > 0").collect()[0]['min_time']

    dim_table_columns = {
        "timestamp_10min_slice": "timestamp_10min_slice",
        "platform": "platform",
        "service": "service"
    }

    dim_table_df = (
        spark.read
        .table(f"dataanalytics{environment}.standard_metrics.platform_territory_10min_ts")
        .where(expr(f"""timestamp_10min_slice BETWEEN '{min_time}'::timestamp-interval'12 hours' AND current_timestamp()
                        and (service, platform) IN (('Steam', 'Windows'), ('SEN', 'PS5'), ('Xbox Live', 'XBSX'))"""))
        .select(*dim_table_columns.values())
        .distinct()
    )
    
    event_columns = {
        "timestamp_10min_slice": "timestamp_10min_slice",
        "platform": "platform",
        "service": "service",
        "player_type": "player_type",
        "country_code": "country_code",
        "purchase_count": "purchase_count"
    }

    pass_df = (
        spark.read
        .table(f"{database}{environment}.intermediate.fact_player_entitlement").alias('e')
        .join(spark.read.table(f"{database}{environment}.managed_view.fact_player_summary_ltd").alias('p'), expr("e.player_id = p.player_id and e.service = p.service and e.platform = p.platform"), "inner")
        .where(expr(f""" e.game_type='full game' and p.player_type != 'demo' 
                        and entitlement_id in ('LegendsChoicePack', 'SundayRedPack', 'MembersPass', 'ClubhousePassSeason1', 'ClubhousePassSeason2' ,'ClubhousePassSeason3' ,'ClubhousePassSeason4' ,'ClubhousePassSeason5' ,'ClubhousePassSeason6' )
                        and (window(e.first_seen, '10 minutes').end between '{min_time}'-interval'12 hours' AND current_timestamp())"""))
        .select(
            expr("window(e.first_seen, '10 minutes').end as timestamp_10min_slice"),
            "e.platform",
            "e.service",
            "p.player_type",
            expr("""case when entitlement_id in ('LegendsChoicePack','SundayRedPack') then 8
                         when entitlement_id in ('MembersPass', 'ClubhousePassSeason2' ,'ClubhousePassSeason3' ,'ClubhousePassSeason4' ,'ClubhousePassSeason5' ,'ClubhousePassSeason6' ) then 7 
                         when entitlement_id in ('ClubhousePassSeason1' ) then 1
                    end as pass_type"""),
            expr("ifnull(e.first_seen_country_code, 'ZZ') as country_code"),
            "e.player_id"
        )
    )

    groupby_columns = {'platform' ,'service' , 'player_type' , 'country_code'}

    agg_columns = { "pass_type": expr("max(pass_type)  as pass_type"),
                   "purchase_count": expr("count(distinct player_id) as purchase_count"),
                   "timestamp_10min_slice": expr("min(timestamp_10min_slice) as timestamp_10min_slice")
    }

    df = (
           pass_df
           .groupBy(*groupby_columns)
           .agg(*agg_columns.values())
           .select(
                *groupby_columns,
                *agg_columns.keys()
                )
        )
    
    df = ( df.where(expr("pass_type is not null and pass_type != 8")) ) 

    finial_columns = {
        "timestamp_10min_slice": "d.timestamp_10min_slice",
        "platform": "d.platform",
        "service": "d.service",
        'player_type': expr("ifnull(player_type,'default') as player_type"),
        'pass_type': expr("ifnull(case when pass_type = 1 then 'ClubhousePassSeason1' when pass_type = 7 then 'MembersPass' end,'default') as pass_type"),
        'country_code':  expr("ifnull(country_code,'default') as country_code"),
        "purchase_count": expr("ifnull(purchase_count,0) as purchase_count")

    }

    df = (
        dim_table_df.alias('d')
        .join(df.alias('e'), expr("d.timestamp_10min_slice = e.timestamp_10min_slice and d.platform = e.platform and d.service = e.service"), "left")
        .select(
            *finial_columns.values(),
            expr("current_timestamp() AS dw_insert_ts"),
            expr("current_timestamp() AS dw_update_ts"),
            expr("SHA2(CONCAT_WS('|',d.timestamp_10min_slice, d.platform, d.service, ifnull(player_type,'default'),ifnull(pass_type,'default'), ifnull(country_code,'default')),256) AS merge_key")
        )
    )

    load(df,database,environment,spark)

# COMMAND ----------

def stream_job(database,environment):
    database = database.lower()

    #setting the spark session
    spark = create_spark_session(name=f"{database}")
    
    #Setting the checkpoint_location from the fucntion output
    checkpoint_location = create_agg_seasonpass_purchase_nrt(database,environment,spark)
    print(f'checkpoint_location : {checkpoint_location}')
    
    df = extract(spark, database, environment)
    
    print('Started writeStream')

    #Writing the stream output to the table 
    (
        df
        .writeStream
        .trigger(availableNow=True)
        .foreachBatch(lambda df, batch_id: transform(df, batch_id, database,environment, spark))
        .option("checkpointLocation", checkpoint_location)
        .outputMode("append")
        .queryName(f'{database}_agg_seasonpass_purchase_nrt')
        .start()
    )

# COMMAND ----------

stream_job(database, environment)
