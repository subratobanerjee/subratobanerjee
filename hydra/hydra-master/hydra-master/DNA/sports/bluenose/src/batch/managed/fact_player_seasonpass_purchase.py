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

def create_fact_player_seasonpass_purchase(database,environment,spark):
    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.fact_player_seasonpass_purchase (
        player_id string,
        platform STRING,
        service STRING,
        player_type STRING,
        pass_type STRING,
        country_code STRING,
        timestamp TIMESTAMP,
        dw_insert_ts TIMESTAMP,
        dw_update_ts TIMESTAMP,
        merge_key STRING
    )
    """
    create_table(spark,sql, properties={})
    create_fact_player_seasonpass_purchase_view(database,environment,spark)
    return f"dbfs:/tmp/{database}/managed/batch/run{environment}/fact_player_seasonpass_purchase"

# COMMAND ----------

def create_fact_player_seasonpass_purchase_view(database,environment,spark):
    """
    Create the fact_player_game_status_daily view in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
        mapping (dict): A dictionary of column mappings.
    """

    sql = f"""
    CREATE VIEW IF NOT EXISTS {database}{environment}.managed_view.fact_player_seasonpass_purchase AS (
        SELECT 
                player_id,
                platform,
                service,
                player_type,
                pass_type,
                country_code,
                timestamp
        from {database}{environment}.managed.fact_player_seasonpass_purchase
    )
    """
    spark.sql(sql)

# COMMAND ----------

def extract(spark, database, environment):
    prev_max = max_timestamp(spark, f"{database}{environment}.managed.fact_player_seasonpass_purchase", 'timestamp')

    inc_min = min(prev_max, datetime.now().date() - timedelta(days=2))

    df = (
            spark
            .read
            .table(f"{database}{environment}.intermediate.fact_player_entitlement").alias('e')
            .where(expr(f"""e.game_type='full game' 
                        and entitlement_id in ('LegendsChoicePack', 'SundayRedPack', 'MembersPass', 'ClubhousePassSeason1', 'ClubhousePassSeason2' ,'ClubhousePassSeason3' ,'ClubhousePassSeason4' ,'ClubhousePassSeason5' ,'ClubhousePassSeason6' )
                        and (first_seen::date between '{inc_min}'::Date AND current_date())"""))
            .select('*')
         )
    return df

# COMMAND ----------

def load(df, database, environment, spark):
    target_df = DeltaTable.forName(spark, f"{database}{environment}.managed.fact_player_seasonpass_purchase")
    
    merger_condition = 'target.merge_key = source.merge_key'
    merge_df = target_df.alias("target").merge(df.alias("source"), merger_condition)
    
    merge_df = set_merge_insert_condition(merge_df, df)
    
    # Execute the merge operation
    merge_df.execute()

    print('Merge completed')

# COMMAND ----------

def transform(df, database,environment, spark):

    pass_df = (
        df.alias('e')
        .join(spark.read.table(f"{database}{environment}.managed_view.fact_player_summary_ltd").alias('p'), expr("e.player_id = p.player_id and e.service = p.service and e.platform = p.platform"), "inner")
        .where(expr("""p.player_type != 'demo'"""))
        .select(
            expr("e.first_seen as timestamp"),
            "e.platform",
            "e.service",
            "p.player_type",
            expr("""case when entitlement_id in ('LegendsChoicePack','SundayRedPack') then 8
                         when entitlement_id in ('MembersPass') then 7
                         when entitlement_id in ('ClubhousePassSeason1', 'ClubhousePassSeason2' ,'ClubhousePassSeason3' ,'ClubhousePassSeason4' ,'ClubhousePassSeason5' ,'ClubhousePassSeason6' ) then  right(entitlement_id,1)::int
                    end as pass_type"""),
            expr("ifnull(e.first_seen_country_code, 'ZZ') as country_code"),
            "e.player_id"
        )
    )

    groupby_columns = {'platform' ,'service' , 'player_type' , 'country_code' , 'player_id'}

    agg_columns = { "pass_type": expr("max(pass_type)  as pass_type"),
                    "timestamp": expr("min(timestamp) as timestamp")
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
        "player_id": "player_id",
        "platform": "platform",
        "service": "service",
        'player_type': "player_type",
        'pass_type': expr("case when pass_type between 1 and 6 then 'ClubhousePassSeason'||pass_type when pass_type = 7 then 'MembersPass' end as pass_type"),
        'country_code': "country_code",
        "timestamp": "timestamp"
    }
    

    df = (
        df
        .select(
            *finial_columns.values(),
            expr("current_timestamp() AS dw_insert_ts"),
            expr("current_timestamp() AS dw_update_ts"),
            expr("SHA2(CONCAT_WS('|',player_id, timestamp, platform, service, player_type, pass_type, country_code), 256) AS merge_key")
        )
    )

    return df

# COMMAND ----------

def run_job(database,environment):
    database = database.lower()

    #setting the spark session
    spark = create_spark_session(name=f"{database}")
    
    #Setting the checkpoint_location from the fucntion output
    checkpoint_location = create_fact_player_seasonpass_purchase(database,environment,spark)
    print('Started extract')

    df = extract(spark, database, environment)

    print('Started transform')

    df = transform(df, database,environment, spark)
    
    print('Started Load')
    
    load(df,database,environment,spark)


# COMMAND ----------

run_job(database, environment)
