# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/managed/agg_igo_daily

# COMMAND ----------

from pyspark.sql.functions import expr

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment', set_environment())
database = 'bluenose'
data_source = 'dna'
view_mapping = {
    'IGO_ID_1': 'IGO_ID_1 as QUEST_ID',
    'IGO_ID_2': 'IGO_ID_2 as OBJECTIVE_ID',
    'IGO_ID_3': 'IGO_ID_3 as CHAPTER_ID',
    'AGG_1': 'AGG_1 as DEMO_GAME_COUNT',
    'AGG_2': 'AGG_2 as FULL_GAME_COUNT',
    'AGG_3': 'AGG_3 as DEMO_PLAYER_COUNT',
    'AGG_4': 'AGG_4 as CONVERTED_PLAYER_COUNT',
    'AGG_5': 'AGG_5 as FULL_GAME_PLAYER_COUNT',
    'AGG_6': 'AGG_6 as DEMO_MYPLAYER_COUNT',
    'AGG_7': 'AGG_7 as CONVERTED_MYPLAYER_COUNT',
    'AGG_8': 'AGG_8 as FULL_GAME_MYPLAYER_COUNT',
}

# COMMAND ----------

def extract(database,environment,spark):
    # Selecting the  event data
    columns = {
                'DATE': 'DATE',
                'PLAYER_ID': 'PLAYER_ID',
                'PRIMARY_INSTANCE_ID': expr('ifnull(PRIMARY_INSTANCE_ID,-1) as PRIMARY_INSTANCE_ID'),
                'PLATFORM': 'PLATFORM',
                'SERVICE': 'SERVICE',
                'COUNTRY_CODE': 'COUNTRY_CODE',
                'IGO_ID_1': 'IGO_ID_1',
                'IGO_ID_2': 'IGO_ID_2',
                'IGO_ID_3': 'IGO_ID_3',
                'IGO_TYPE': 'IGO_TYPE',
                'STATUS': 'STATUS',
                'AGG_1': 'AGG_1',
                'AGG_2': 'AGG_2'
                }

    prev_max = max_timestamp(spark, f"{database}{environment}.managed.agg_igo_daily", 'date')

    current_min = (
        spark.read
        .table(f"{database}{environment}.managed.fact_player_igo_daily")
        .where(expr("DW_INSERT_TS::date between current_date - 2 and current_date"))
        .select(expr(f"ifnull(min(date),'1999-01-01')::date as min_date"))
    ).collect()[0]['min_date']

    inc_min_date = min(prev_max,current_min)

    df = (
        spark.read
        .table(f"{database}{environment}.managed.fact_player_igo_daily")
        .where(expr(f"date >= '{inc_min_date}'::date"))
        .select(*columns.values())
    )

    return df

# COMMAND ----------

def transform(batch_df, database, spark):
    df_summary = spark.table(f"{database}{environment}.managed.fact_player_summary_ltd").select('*')

    groupby_columns = {'igo.date', 'igo.platform', 'igo.service', 'igo.country_code', 'igo.IGO_ID_1', 'igo.IGO_ID_2', 'igo.IGO_ID_3', 'igo.IGO_TYPE', 'igo.status'}



    agg_columns = {
                    'AGG_1' : expr("sum(AGG_1) as AGG_1"),
                    'AGG_2' : expr("sum(AGG_2) as AGG_2"),
                    "AGG_3": expr("count(distinct case when ltd_string_1= 'demo' then igo.player_id when ltd_string_1= 'converted' and  ltd_ts_1::date > igo.date then igo.player_id else null end) as agg_3"),
                    "AGG_4": expr("count(distinct case when ltd_string_1= 'converted' and ltd_ts_1::date <= igo.date then igo.player_id else null end) as agg_4"),
                    "AGG_5": expr("count(distinct case when ltd_string_1='full game' then igo.player_id else null end) as agg_5"),
                    "AGG_6": expr("count(distinct case when ltd_string_1= 'demo' then igo.player_id when ltd_string_1= 'converted' and  ltd_ts_1::date > igo.date then igo.player_id||igo.PRIMARY_INSTANCE_ID else null end) as agg_6"),
                    "AGG_7": expr("count(distinct case when ltd_string_1= 'converted' and ltd_ts_1::date <= igo.date then igo.player_id||igo.PRIMARY_INSTANCE_ID else null end) as agg_7"),
                    "AGG_8": expr("count(distinct case when ltd_string_1='full game' then igo.player_id||igo.PRIMARY_INSTANCE_ID else null end) as agg_8")                
                  }

    df = (
            batch_df.alias('igo')
            .join(df_summary.alias('df_summary'),expr("igo.player_id = df_summary.player_id and igo.service = df_summary.service and igo.platform = df_summary.platform"),"left")
            .groupBy(*groupby_columns)
            .agg(*agg_columns.values())
            .select(
                *groupby_columns,
                *agg_columns.keys(),
                expr("current_timestamp() AS dw_insert_ts"),
                expr("current_timestamp() AS dw_update_ts"),
                expr("SHA2(CONCAT_WS('|',date, igo.platform, igo.service, country_code , IGO_ID_1, IGO_ID_2 ,IGO_ID_3, IGO_TYPE, status),256) AS merge_key")

            )
        )
    return df

# COMMAND ----------

def load(df,database,environment,spark):

    # Merge variables and logic
    target_df = DeltaTable.forName(spark, f"{database}{environment}.managed.agg_igo_daily")

    merger_condition = 'target.merge_key = source.merge_key'
    merge_update_conditions = [
                                { 
                                 'condition' : """target.AGG_1 != source.AGG_1 OR
                                                  target.AGG_2 != source.AGG_2 OR
                                                  target.AGG_3 != source.AGG_3 OR
                                                  target.AGG_4 != source.AGG_4 OR
                                                  target.AGG_5 != source.AGG_5 OR
                                                  target.AGG_6 != source.AGG_6 OR
                                                  target.AGG_7 != source.AGG_7 OR
                                                  target.AGG_8 != source.AGG_8
                                                  """,
                                 'set_fields' : {
                                                    'AGG_1': 'greatest(target.AGG_1,source.AGG_1)',
                                                    'AGG_2': 'greatest(target.AGG_2,source.AGG_2)',
                                                    'AGG_3': 'greatest(target.AGG_3,source.AGG_3)',
                                                    'AGG_4': 'greatest(target.AGG_4,source.AGG_4)',
                                                    'AGG_5': 'greatest(target.AGG_5,source.AGG_5)',
                                                    'AGG_6': 'greatest(target.AGG_6,source.AGG_6)',
                                                    'AGG_7': 'greatest(target.AGG_7,source.AGG_7)',
                                                    'AGG_8': 'greatest(target.AGG_8,source.AGG_8)',
                                                    'dw_update_ts': 'source.dw_update_ts'
                                                }
                                }
                        ]

    merge_df = target_df.alias("target").merge(df.alias("source"), f"{merger_condition}")

    merge_df = set_merge_update_condition(merge_df, merge_update_conditions)

    merge_df = set_merge_insert_condition(merge_df, df)

    # Execute the merge operation
    merge_df.execute()

# COMMAND ----------

def run_batch(database,environment,view_mapping):
    database = database.lower()

    #setting the spark session
    spark = create_spark_session(name=f"{database}")
    
    #Creating the table and view, checkpoint
    create_agg_igo_daily(spark, database,environment, view_mapping)

    print('Extracting the data')
    
    # Reading the data using bath data
    df = extract(database,environment,spark)

    print('Applying the Transformation')
    #Applying Transformation
    df = transform(df, database, spark)

    print('Merge Data')
    load(df,database,environment,spark)

    return 'Merge data completed'

# COMMAND ----------

run_batch(database,environment,view_mapping)
