# Databricks notebook source
from pyspark.sql.functions import (expr, when, sha2, concat_ws)
from delta.tables import DeltaTable
from functools import reduce

# COMMAND ----------

# MAGIC %run ../../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../../utils/ddl/wwe_game_specific/fact_player_store_interactions_daily

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
def read_raw_storeinteraction(environment,min_received_on):  
    transaction_filter = "buildtype ilike 'final'"
    raw_storeinteraction_df = (
        spark.read.table(f"{database}{environment}.raw.storeinteraction")
        .where((expr(f"receivedOn::timestamp::date > to_date('{min_received_on}') - INTERVAL 2 DAY and {transaction_filter}")))
        .select(
            'playerPublicId',
            'receivedOn',
            'gamemode',
            'submode',
            'storeaction',
            expr("ifnull(itemid::int,-1)::string as item"),
            'numitemspurchased',
            'numitemsviewed',
            'appPublicId',
            'submodesessionid'
            )
        )

    return raw_storeinteraction_df

# COMMAND ----------
def read_title_df(environment):
    title_df= (
        spark.read.table(f"reference{environment}.title.dim_title")
        .select(
            'app_id',
            'display_platform',
            'display_service'
            )
        )

    return title_df

# COMMAND ----------
def extract(environment, database):
    transaction_filter = "buildtype ilike 'final'"
    inc_filter = f"{transaction_filter} and (receivedOn::timestamp::date between current_date - 2 and current_date)"
    prev_max_ts = max_timestamp(spark, f"{database}{environment}.managed.fact_player_store_interactions_daily", 'date')
        
    current_min = (
        spark.read
        .table(f"{database}{environment}.raw.storeinteraction")
        .where(expr(inc_filter))
        .select(expr(f"ifnull(min(receivedOn::timestamp::date),'1999-01-01')::timestamp::date as min_date"))
    ).collect()[0]['min_date']
    inc_min_date = min(prev_max_ts,current_min)

    store_interactions_df = read_raw_storeinteraction(environment,inc_min_date)
    dim_title_df = read_title_df(environment)

    return store_interactions_df,dim_title_df

# COMMAND ----------
def transform(store_interactions_df,title_df):
    agg_cols = {
        'items_purchased': expr("sum(ifnull(si.numitemspurchased::int,0)) as items_purchased"),
        'items_viewed': expr("sum(ifnull(si.numitemsviewed::int,0)) as items_viewed")
    }

    # Step 1: Calculate secondsfloat
    secondsfloat_calc_df = (
        store_interactions_df.alias('sc')
        .groupBy("sc.playerPublicId", "sc.gamemode", "sc.submode", "sc.storeaction", "sc.item", "sc.submodesessionid")
        .agg(expr("timestampdiff(SECOND, min(sc.receivedOn::timestamp), max(sc.receivedOn::timestamp)) as secondsfloat"))
    )

    # Step 2: Aggregate secondsfloat
    agg_seconds_df = (
        secondsfloat_calc_df.alias('ag')
        .groupBy("ag.playerPublicId", "ag.gamemode", "ag.submode", "ag.storeaction", "ag.item")
        .agg(expr("sum(ag.secondsfloat) as secondsfloat"))
    )

    # Step 3: Join raw to dim for Platform and Service and compute values for items_purchased and items_viewed
    dim_details_df = (
        store_interactions_df.alias('si')
        .join(title_df.alias('d'),
            on = expr("si.appPublicId == d.app_id"),
            how="left")
        .groupBy(
            expr("ifnull(si.playerPublicId,receivedOn::timestamp::date::string||'-NULL-Player') as player_id"), 
            "d.display_platform",
            "d.display_service", 
            expr("si.receivedOn::timestamp::date as date"),
            "si.gamemode", "si.submode", "si.storeaction", "si.item"
            )
        .agg(*agg_cols.values())
        .select(
            *agg_cols.keys()
            ,expr("date")
            ,"player_id"
            ,expr("d.display_platform as platform")
            ,expr("d.display_service as service")
            ,expr("gamemode as game_mode")
            ,expr("si.submode as sub_mode")
            ,expr("si.storeaction as store_action")
            ,expr("item as item")
            ,expr("'Default' as item_name")
            ,expr("'Default' as item_type")
        )
    )

    # Step 4: Join dim_details_df with agg_seconds to get the seconsfloat value
    final_df = (
        dim_details_df.alias('m')
        .join(agg_seconds_df.alias('a'),
              on= expr("m.player_id = a.playerPublicId and m.game_mode = a.gamemode and m.sub_mode = a.submode and m.store_action = a.storeaction and m.item = a.item"),
              how="left")
        .select(
            expr("m.*")
            ,expr("ifnull(a.secondsfloat,0) as time_spent")
            )
        )

    return final_df

# COMMAND ----------
def load_fact_player_store_interactions_daily(df, database, environment):
    """
    Load the data into the fact_player_store_interactions_daily table in the specified environment.

    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        df (DataFrame): The DataFrame to load into the table.
        database (str): The database to load the table into.
        environment (str): The environment to load the table into.
    """

    # Perform the merge operation into the Delta table
    final_table = DeltaTable.forName(spark, f"{database}{environment}.{target_schema}.fact_player_store_interactions_daily")

    # get schema of the table
    table_schema = spark.read.table(f"{database}{environment}.{target_schema}.fact_player_store_interactions_daily").schema

    # create empty dataframe using the schema
    out_df = spark.createDataFrame([], table_schema)

    # union the incoming dataframe with the empty dataframe, we can set allowMissingColumns to False to be more strict
    df = df.selectExpr(
            "*",
            "CURRENT_TIMESTAMP() as dw_insert_ts",
            "CURRENT_TIMESTAMP() as dw_update_ts",
            "SHA2(CONCAT_WS('|',player_id, platform, service, date, game_mode, sub_mode, store_action, item, item_name, item_type),256) as merge_key"
        )
    out_df = out_df.unionByName(df, allowMissingColumns=True)

    # create the update column dict
    agg_cols = ['time_spent','items_purchased','items_viewed']
    #agg_cols = ['items_purchased','items_viewed']
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
def run_batch_fact_player_store_interactions_daily(database,environment,view_mapping):
    database = database.lower()

    #setting the spark session
    spark = create_spark_session(name=f"{database}")
    
    #Creating the table and view, checkpoint
    create_fact_player_store_interactions_daily(spark, database ,view_mapping)

    print('Extracting the data')
    
    # Reading the data using bath data
    raw_storeinteraction_df,title_df = extract(environment, database)

    #Applying Transformation
    df = transform(raw_storeinteraction_df,title_df)

    #Merge data
    load_fact_player_store_interactions_daily(df, database, environment)

    return 'Merge data completed'

# COMMAND ----------
run_batch_fact_player_store_interactions_daily(database,environment,view_mapping)
