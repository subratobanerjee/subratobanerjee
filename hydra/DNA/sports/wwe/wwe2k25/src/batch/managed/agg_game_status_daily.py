# Databricks notebook source
from pyspark.sql.functions import (expr, when, sha2, concat_ws)
from delta.tables import DeltaTable
from functools import reduce

# COMMAND ----------

# MAGIC %run ../../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../../utils/ddl/wwe_game_specific/agg_game_status_daily

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
    'extra_grain_1': 'extra_grain_1 as online',
    'agg_game_status_1': 'agg_game_status_1 as match_start_count',
    'agg_game_status_2': 'agg_game_status_2 as match_end_count',
    'agg_game_status_3': 'agg_game_status_3 as match_win_count',
    'agg_game_status_4': 'agg_game_status_4 as match_lost_count',
    'agg_game_status_5': 'agg_game_status_5 as match_quit_count',
    'agg_gp_1': 'agg_gp_1 as match_count',
    'agg_gp_2': 'agg_gp_2 as match_duration',
    'agg_gp_3': 'agg_gp_3 as mode_enter_count',
    'agg_gp_4': 'agg_gp_4 as mode_duration',
    'agg_gp_5': 'agg_gp_5 as submode_duration'
}

# COMMAND ----------
def extract(database,environment,spark):
    # Selecting the transaction event data
    transaction_filter = "player_id NOT ILIKE '%Null%' AND player_id is not null AND player_id != 'anonymous'"
    inc_filter = f"{transaction_filter} and (dw_insert_ts::date between current_date - 2 and current_date)"
    prev_max = max_timestamp(spark, f"{database}{environment}.managed.agg_game_status_daily", 'date')

    current_min = (
        spark.read
        .table(f"{database}{environment}.managed.fact_player_game_status_daily")
        .where(expr(inc_filter))
        .select(expr(f"ifnull(min(date),'1999-01-01')::date as min_date"))
    ).collect()[0]['min_date']

    inc_min_date = min(prev_max,current_min)
    batch_filter = f"({transaction_filter}) and date::date >= '{inc_min_date}'::date"

    extract_df = (
        spark.read
        .table(f"{database}{environment}.managed.fact_player_game_status_daily")
        .where(expr(batch_filter))
        .select(
            'DATE'
            ,'PLAYER_ID'
            ,'PLATFORM'
            ,'SERVICE'
            ,'GAME_MODE'
            ,'SUB_MODE'
            ,'COUNTRY_CODE'
            ,'EXTRA_GRAIN_1'
            ,'AGG_GAME_STATUS_1'
            ,'AGG_GAME_STATUS_2'
            ,'AGG_GAME_STATUS_3'
            ,'AGG_GAME_STATUS_4'
            ,'AGG_GAME_STATUS_5'
            ,'AGG_GP_1'
            ,'AGG_GP_2'
            ,'AGG_GP_3'
            ,'AGG_GP_4'
            ,'AGG_GP_5'
            ,'AGG_GP_6'
            ,'AGG_GP_7'
            ,'AGG_GP_8'
            ,'AGG_GP_9'
            ,'AGG_GP_10'
            ,'AGG_GP_11'
            ,'AGG_GP_12'
            ,'AGG_GP_13'
            ,'AGG_GP_14'
            ,'AGG_GP_15'
        )
    )

    return extract_df

# COMMAND ----------
def transform(batch_df, database, environment, spark):
    groupby_columns = {'et.date', 'et.platform', 'et.service', 'et.game_mode', 'et.sub_mode', 'et.country_code', 'et.extra_grain_1'}

    agg_columns = {
        'agg_game_status_1': expr("sum(et.agg_game_status_1) AS agg_game_status_1"),
        'agg_game_status_2': expr("sum(et.agg_game_status_2) AS agg_game_status_2"),
        'agg_game_status_3': expr("sum(et.agg_game_status_3) AS agg_game_status_3"),
        'agg_game_status_4': expr("sum(et.agg_game_status_4) AS agg_game_status_4"),
        'agg_game_status_5': expr("sum(et.agg_game_status_5) AS agg_game_status_5"),
        'player_count': expr("count(distinct et.player_id) AS player_count"),
        'agg_gp_1': expr("sum(et.agg_gp_1) AS agg_gp_1"),
        'agg_gp_2': expr("sum(et.agg_gp_2) AS agg_gp_2"),
        'agg_gp_3': expr("sum(et.agg_gp_3) AS agg_gp_3"),
        'agg_gp_4': expr("sum(et.agg_gp_4) AS agg_gp_4"),
        'agg_gp_5': expr("sum(et.agg_gp_5) AS agg_gp_5"),
        'agg_gp_6': expr("sum(et.agg_gp_6) AS agg_gp_6"),
        'agg_gp_7': expr("sum(et.agg_gp_7) AS agg_gp_7"),
        'agg_gp_8': expr("sum(et.agg_gp_8) AS agg_gp_8"),
        'agg_gp_9': expr("sum(et.agg_gp_9) AS agg_gp_9"),
        'agg_gp_10': expr("sum(et.agg_gp_10) AS agg_gp_10"),
        'agg_gp_11': expr("sum(et.agg_gp_11) AS agg_gp_11"),
        'agg_gp_12': expr("sum(et.agg_gp_12) AS agg_gp_12"),
        'agg_gp_13': expr("sum(et.agg_gp_13) AS agg_gp_13"),
        'agg_gp_14': expr("sum(et.agg_gp_14) AS agg_gp_14"),
        'agg_gp_15': expr("sum(et.agg_gp_15) AS agg_gp_15")
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
    final_table = DeltaTable.forName(spark, f"{database}{environment}.{target_schema}.agg_game_status_daily")

    # get schema of the table
    table_schema = spark.read.table(f"{database}{environment}.{target_schema}.agg_game_status_daily").schema

    # create empty dataframe using the schema
    out_df = spark.createDataFrame([], table_schema)

    # union the incoming dataframe with the empty dataframe, we can set allowMissingColumns to False to be more strict
    df = df.selectExpr(
            "*",
            "CURRENT_TIMESTAMP() as dw_insert_ts",
            "CURRENT_TIMESTAMP() as dw_update_ts",
            "SHA2(CONCAT_WS('|',date, platform, service, game_mode, sub_mode, country_code, extra_grain_1),256) as merge_key"
        )
    out_df = out_df.unionByName(df, allowMissingColumns=True)

    # create the update column dict
    agg_cols = ['agg_game_status_1', 'agg_game_status_2', 'agg_game_status_3', 'agg_game_status_4', 'agg_game_status_5', 'player_count', 'agg_gp_1', 'agg_gp_2', 'agg_gp_3', 'agg_gp_4', 'agg_gp_5', 'agg_gp_6', 'agg_gp_7', 'agg_gp_8', 'agg_gp_9', 'agg_gp_10', 'agg_gp_11', 'agg_gp_12', 'agg_gp_13', 'agg_gp_14', 'agg_gp_15']
    
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

def run_batch_agg_game_status_daily(database,environment,view_mapping):
    database = database.lower()

    #setting the spark session
    spark = create_spark_session(name=f"{database}")
    
    #Creating the table and view, checkpoint
    create_agg_game_status_daily(spark, database, view_mapping)

    #Reading the data using bath data
    print('Extracting the data')
    df_extract = extract(database,environment,spark)

    #Applying Transformation
    print('Applying the Transformation')
    df_transform = transform(df_extract, database, environment, spark)

    #Loading data into the table uning Merge
    print('Merge Data')
    load(df_transform,database,environment,spark)

    return 'Merge data completed'

# COMMAND ----------
run_batch_agg_game_status_daily(database,environment,view_mapping)
