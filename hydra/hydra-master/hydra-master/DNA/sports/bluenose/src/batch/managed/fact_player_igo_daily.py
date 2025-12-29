# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/managed/fact_player_igo_daily

# COMMAND ----------

from pyspark.sql.functions import (expr,when)

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment', set_environment())
database = 'bluenose'
data_source = 'dna'
view_mapping = {
    'primary_instance_id': 'primary_instance_id as myplayer_id',
    'igo_id_1': 'IGO_ID_1 as QUEST_ID',
    'igo_id_2': 'IGO_ID_2 as OBJECTIVE_ID',
    'igo_id_3': 'IGO_ID_3 as CHAPTER_ID',
    'agg_1': 'AGG_1 as DEMO_GAME_COUNT',
    'agg_2': 'AGG_2 as FULL_GAME_COUNT'
}

# COMMAND ----------

def extract(database,environment,spark):
    prev_max = max_timestamp(spark, f"{database}{environment}.managed.fact_player_igo_daily", 'date')

    current_min = (
        spark.read
        .table(f"{database}{environment}.intermediate.fact_player_igo")
        .where(expr("dw_insert_ts::date >= current_date - interval '2 day'"))
        .select(expr(f"ifnull(min(received_on),'1999-01-01')::date as min_date"))
    ).collect()[0]['min_date']

    inc_min_date = min(prev_max,current_min)

    columns = {
                "date": expr("received_on::date as date"),
                "player_id": 'player_id',
                "platform": 'platform',
                "service": 'service',
                "country_code" :'country_code',
                "igo_type": 'igo_type',
                "status": "status",
                "igo_id_1": expr('extra_info_1 as igo_id_1'),
                "igo_id_2": expr('extra_info_2 as igo_id_2'),
                "igo_id_3": expr('extra_info_3 as igo_id_3'),
                "game_type": expr('extra_info_4 as game_type')
                }

    igo_df =  (
                spark.read
                .table(f"{database}{environment}.intermediate.fact_player_igo")
                .where(expr(f"received_on::date >= '{inc_min_date}'::date"))
                .select(*columns.values())
             )

    return igo_df

# COMMAND ----------

def transform(batch_df,database, spark):
    groupby_columns = {'date', 'player_id', 'platform', 'service', 'country_code', 'igo_type','status','igo_id_1', 'igo_id_2' , 'igo_id_3'}  

    agg_columns = {
                    'AGG_1': expr("sum(case when game_type = 'demo' then 1 else 0 end) as AGG_1"),
                    'AGG_2': expr("sum(case when game_type = 'full game' then 1 else 0 end) as AGG_2")
                     }

    igo = (
         batch_df
        .groupBy(*groupby_columns)
        .agg(*agg_columns.values())
        .select(
            *groupby_columns,
            *agg_columns.keys(),
            expr("current_timestamp() AS dw_insert_ts"),
            expr("current_timestamp() AS dw_update_ts"),
            expr("SHA2(CONCAT_WS('|',date, player_id, platform,service, country_code , igo_type, status, igo_id_1 ,igo_id_2, igo_id_3),256) AS merge_key")

        )
    )

    return igo

def load(batch_df,database,environment,spark):
    target_df = DeltaTable.forName(spark, f"{database}{environment}.managed.fact_player_igo_daily")

    merger_condition = 'target.merge_key = source.merge_key'

    merge_update_conditions = [
                                { 
                                 'condition' : """target.AGG_1 != source.AGG_1 OR
                                                  target.AGG_2 != source.AGG_2 
                                                  """,
                                 'set_fields' : {
                                                    'AGG_1': 'greatest(target.AGG_1,source.AGG_1)',
                                                    'AGG_2': 'greatest(target.AGG_2,source.AGG_2)',
                                                    'dw_update_ts': 'source.dw_update_ts'
                                                }
                                }
                        ]

    merge_df = target_df.alias("target").merge(batch_df.alias("source"), f"{merger_condition}")

    merge_df = set_merge_update_condition(merge_df, merge_update_conditions)

    merge_df = set_merge_insert_condition(merge_df, batch_df)

    # Execute the merge operation
    merge_df.execute()

# COMMAND ----------

def run_batch(database,environment,view_mapping):
    database = database.lower()

    #setting the spark session
    spark = create_spark_session(name=f"{database}")
    
    #Creating the table and view, checkpoint
    create_fact_player_igo_daily(spark, database ,environment, view_mapping)

    print('Extracting the data')
    
    # Reading the data using bath data
    df = extract(database,environment,spark)

    #Applying Transformation
    df = transform(df, database, spark)

    #Merge data
    load(df,database,environment,spark)

    return 'Merge data completed'

# COMMAND ----------

run_batch(database,environment,view_mapping)
