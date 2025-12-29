# Databricks notebook source
from pyspark.sql.functions import (expr, when, sha2, concat_ws)
from delta.tables import DeltaTable
from functools import reduce

# COMMAND ----------

# MAGIC %run ../../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../../utils/ddl/wwe_game_specific/agg_store_interactions_daily

# COMMAND ----------

# MAGIC %run ../../../../../../../utils/helpers

# COMMAND ----------
database = 'wwe2k25'
input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
target_schema = 'managed'
data_source ='dna'
title = 'WWE2K25'

view_mapping = {
            }

# COMMAND ----------
def extract(database,environment,spark):
    # Selecting the transaction event data
    transaction_filter = "player_id NOT ILIKE '%Null%' AND player_id is not null AND player_id != 'anonymous'"
    inc_filter = f"{transaction_filter} and (dw_insert_ts::date between current_date - 2 and current_date)"
    prev_max = max_timestamp(spark, f"{database}{environment}.managed.agg_store_interactions_daily", 'date')

    current_min = (
        spark.read
        .table(f"{database}{environment}.managed.fact_player_store_interactions_daily")
        .where(expr(inc_filter))
        .select(expr(f"ifnull(min(date),'1999-01-01')::date as min_date"))
    ).collect()[0]['min_date']

    inc_min_date = min(prev_max,current_min)
    batch_filter = f"({transaction_filter}) and date::date >= '{inc_min_date}'::date"

    extract_df = (
        spark.read
        .table(f"{database}{environment}.managed.fact_player_store_interactions_daily")
        .where(expr(batch_filter))
        .select(
            'player_id',
            'platform',
            'service',
            'date',
            'game_mode',
            'sub_mode',
            'store_action',
            'item',
            'item_name',
            'item_type',
            'time_spent',
            'items_purchased',
            'items_viewed',
            'agg_si_1',
            'agg_si_2',
            'agg_si_3',
            'agg_si_4',
            'agg_si_5'
        )
    )

    return extract_df

# COMMAND ----------
def transform(batch_df, database, environment, spark):
    groupby_columns = {'et.date', 'et.platform', 'et.service', 'et.game_mode', 'et.sub_mode', 'et.store_action', 'et.item', 'et.item_name', 'et.item_type'}

    agg_columns = {
        'player_count': expr("COUNT(DISTINCT et.player_id) AS player_count"),
        'time_spent': expr("sum(et.time_spent) AS time_spent"),
        'items_purchased': expr("sum(et.items_purchased) AS items_purchased"),
        'items_viewed': expr("sum(et.items_viewed) AS items_viewed"),
        'agg_si_1': expr("sum(et.agg_si_1) AS agg_si_1"),
        'agg_si_2': expr("sum(et.agg_si_2) AS agg_si_2"),
        'agg_si_3': expr("sum(et.agg_si_3) AS agg_si_3"),
        'agg_si_4': expr("sum(et.agg_si_4) AS agg_si_4"),
        'agg_si_5': expr("sum(et.agg_si_5) AS agg_si_5")
    }

    transform_df = (
        batch_df.alias('et')
        .groupBy(*groupby_columns)
        .agg(*agg_columns.values())
    )
    
    return transform_df

# COMMAND ----------

def load(df,database,environment,spark):

    # Perform the merge operation into the Delta table
    final_table = DeltaTable.forName(spark, f"{database}{environment}.{target_schema}.agg_store_interactions_daily")

    # get schema of the table
    table_schema = spark.read.table(f"{database}{environment}.{target_schema}.agg_store_interactions_daily").schema

    # create empty dataframe using the schema
    out_df = spark.createDataFrame([], table_schema)

    # union the incoming dataframe with the empty dataframe, we can set allowMissingColumns to False to be more strict
    df = df.selectExpr(
            "*",
            "CURRENT_TIMESTAMP() as dw_insert_ts",
            "CURRENT_TIMESTAMP() as dw_update_ts",
            "SHA2(CONCAT_WS('|',date, platform, service, game_mode, sub_mode, store_action, item, item_name, item_type),256) as merge_key"
        )
    out_df = out_df.unionByName(df, allowMissingColumns=True)

    # create the update column dict
    agg_cols = ['player_count', 'time_spent', 'items_purchased', 'items_viewed', 'agg_si_1', 'agg_si_2', 'agg_si_3', 'agg_si_4', 'agg_si_5']
    
    merge_condition = " OR ".join(f"old.{col_name} <> new.{col_name}" for col_name in agg_cols)
    
    update_set = {}
    for col_name in agg_cols:
        update_set[f"old.{col_name}"] = f"greatest(new.{col_name}, old.{col_name})"
    
    update_set[f"old.dw_update_ts"] = "CURRENT_TIMESTAMP()"
    
    # merge the table
    (
        final_table.alias('old')
        .merge(
            out_df.alias('new'),
            "new.merge_key = old.merge_key"
        )
        .whenMatchedUpdate(condition=merge_condition, set=update_set)
        .whenNotMatchedInsertAll()
        .execute()
    )


# COMMAND ----------

def run_batch_agg_store_interactions_daily(database,environment,view_mapping):
    database = database.lower()

    #setting the spark session
    spark = create_spark_session(name=f"{database}")
    
    #Creating the table and view, checkpoint
    create_agg_store_interactions_daily(spark, database , environment, view_mapping)

    print('Extracting the data')
    
    # Reading the data using bath data
    df_extract = extract(database,environment,spark)

    print('Applying the Transformation')
    
    #Applying Transformation
    df_transform = transform(df_extract, database, environment, spark)

    print('Merge Data')
    load(df_transform,database,environment,spark)

    return 'Merge data completed'

# COMMAND ----------

run_batch_agg_store_interactions_daily(database,environment,view_mapping)
