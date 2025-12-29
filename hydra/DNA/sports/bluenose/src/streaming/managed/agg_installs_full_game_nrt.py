# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/table_functions

# COMMAND ----------

from pyspark.sql.functions import expr, window
from delta.tables import DeltaTable

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'bluenose'
data_source ='dna'
title = "'PGA Tour 2K25','PGA Tour 2K25: Demo'"

# COMMAND ----------

def create_agg_installs_full_game_nrt(database,environment,spark):
    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.agg_installs_full_game_nrt (
        title STRING,
        timestamp_10min_slice TIMESTAMP,
        platform STRING,
        service STRING,
        player_type STRING,
        segment STRING,
        country_code STRING,
        installs_count INT,
        dw_insert_ts TIMESTAMP,
        dw_update_ts TIMESTAMP,
        merge_key STRING
    )
    """
    create_table(spark,sql, properties={})
    create_agg_installs_full_game_nrt_view(database,environment,spark)
    return f"dbfs:/tmp/{database}/managed/streaming/run{environment}/agg_installs_full_game_nrt"

# COMMAND ----------

def create_agg_installs_full_game_nrt_view(database,environment,spark):
    """
    Create the fact_player_game_status_daily view in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
        mapping (dict): A dictionary of column mappings.
    """

    sql = f"""
    CREATE OR REPLACE VIEW {database}{environment}.managed_view.agg_installs_full_game_nrt AS (
        SELECT
        *
        from {database}{environment}.managed.agg_installs_full_game_nrt
        where timestamp_10min_slice <= window(current_timestamp(),'10 minutes').end
    )
    """
    spark.sql(sql)

# COMMAND ----------

def extract(spark, database, environment):
    df = (
            spark
            .readStream
            .table(f"{database}{environment}.intermediate.fact_player_activity")
            .select(expr('received_on as timestamp'))
         )
    return df

# COMMAND ----------

def load(df,database,environment,spark):

    #Merge variables and logic
    target_df = f"{database}{environment}.managed.agg_installs_full_game_nrt"
    source_df = 'agg_install_fg_df'
    merge_condition = 'target.merge_key = source.merge_key'
    merge_update_conditions = [
                                { 
                                 'condition' : """target.installs_count != source.installs_count""",
                                 'set_fields' : {
                                                    'installs_count': 'greatest(target.installs_count,source.installs_count)',
                                                    'dw_update_ts': 'source.dw_update_ts'
                                                }
                                }
                        ]

    merge_data(target_df, source_df, merge_condition, df , merge_update_conditions)

# COMMAND ----------

def transform(df, batch_id, database,environment, spark):
    min_time = spark.sql(f"select ifnull(max(timestamp_10min_slice) , '2025-02-21 00:00:00') as min_time from {database}{environment}.managed.agg_installs_full_game_nrt where installs_count > 0 and title = 'PGA Tour 2K25'").collect()[0]['min_time']
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
        "timeslice_10_mins": expr("window_end as timestamp_10min_slice"),
        "platform": "p.platform",
        "service": "p.service",
        "player_type": "p.player_type",
        "country_code": "country_code",
        "segment": "segment",
        "installs_count": "installs_count"
    }

    df = (
        spark.read
        .table(f"{database}{environment}.managed_view.fact_player_summary_ltd").alias('p')
        .join(spark.read.table(f"{database}{environment}.reference.lkup_cohorts").alias('c'), expr("p.player_id = c.player_id"), "left")
        .where(expr(f"""p.player_type != 'demo' AND p.is_linked != False and  window(p.full_game_first_seen, '10 minutes').end between '{min_time}'-interval'12 hours' AND current_timestamp()
        """))
        .select(
            expr("window(p.full_game_first_seen, '10 minutes').end as window_end"),
            "p.platform",
            "p.service",
            "p.player_type",
            expr("ifnull(p.first_seen_country_code, 'ZZ') as country_code"),
            "p.player_id",
            expr("ifnull(combined_cohort,'New/Undefined') as segment")
        )
        .groupBy("window_end", "p.platform", "p.service", "p.player_type", "country_code", "segment")
        .agg(expr("count(distinct p.player_id) as installs_count"))
        .select(*event_columns.values())
    )

    finial_columns = {
        'title': expr("'PGA Tour 2K25' as title"),
        "timeslice_10_mins": "d.timestamp_10min_slice",
        "platform": "d.platform",
        "service": "d.service",
        'player_type': expr("ifnull(player_type,'default') as player_type"),
        'segment': expr("ifnull(segment,'default') as segment"),
        'country_code':  expr("ifnull(country_code,'default') as country_code"),
        "installs_count": expr("ifnull(installs_count,0) as installs_count")

    }

    df = (
        dim_table_df.alias('d')
        .join(df.alias('e'), expr("d.timestamp_10min_slice = e.timestamp_10min_slice and d.platform = e.platform and d.service = e.service"), "left")
        .select(
            *finial_columns.values(),
            expr("current_timestamp() AS dw_insert_ts"),
            expr("current_timestamp() AS dw_update_ts"),
            expr("SHA2(CONCAT_WS('|',d.timestamp_10min_slice, d.platform, d.service, ifnull(player_type,'default') , ifnull(segment,'default'), ifnull(country_code,'default')),256) AS merge_key")
        )
    )

    load(df,database,environment,spark)

# COMMAND ----------

def stream_job(database,environment):
    database = database.lower()

    #setting the spark session
    spark = create_spark_session(name=f"{database}")
    
    #Setting the checkpoint_location from the fucntion output
    checkpoint_location = create_agg_installs_full_game_nrt(database,environment,spark)
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
        .queryName(f'{database}_agg_installs_full_game_nrt')
        .start()
    )

# COMMAND ----------

stream_job(database, environment)
