# Databricks notebook source
from pyspark.sql.functions import (expr, when)
from delta.tables import DeltaTable
from functools import reduce

# COMMAND ----------

# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/managed/fact_player_ftue

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

database = 'bluenose'
input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())

view_mapping = {'config_1': 'config_1 as game_type'}

# COMMAND ----------

def read_fact_player_lesson(database,environment,spark):
    prev_max = max_timestamp(spark, f"{database}{environment}.managed.fact_player_ftue", 'date','step = 1')

    current_min = (
        spark.read
        .table(f"{database}{environment}.managed.fact_player_lesson")
        .where(expr("dw_insert_ts::date between current_date - 2 and current_date"))
        .select(expr(f"ifnull(min(received_on),'1999-01-01')::date as min_date"))
    ).collect()[0]['min_date']

    inc_min_date = min(prev_max,current_min)

    inc_filter = f"is_onboarding = True and received_on::date >= '{inc_min_date}'::date and lesson_step_name = 'ReadingTheGreen' and is_ftue = True"

    columns = {
                'player_id': 'player_id',
                'platform': 'platform',
                'service' : 'service',
                'step': expr('1 as step'),
                'step_name': expr("""'Lessons'::string as step_name"""),
                'status': expr("""'Completed' as status"""),
                'received_on': 'received_on',
                'game_type': expr('game_type as config_1')
            }

    fact_player_lesson_df = (
        spark.read
        .table(f"{database}{environment}.managed.fact_player_lesson")
        .where(expr(inc_filter))
        .select(*columns.values())
    )
    return fact_player_lesson_df

# COMMAND ----------

def read_fact_player_progression(database,environment,spark):
    prev_max = max_timestamp(spark, f"{database}{environment}.managed.fact_player_ftue", 'date','step = 2')

    current_min = (
        spark.read
        .table(f"{database}{environment}.intermediate.fact_player_progression")
        .where(expr("dw_insert_ts::date between current_date - 2 and current_date"))
        .select(expr(f"ifnull(min(received_on),'1999-01-01')::date as min_date"))
    ).collect()[0]['min_date']

    inc_min_date = min(prev_max,current_min)

    inc_filter = f"""action = 'CreateMyPlayer' and received_on >= '{inc_min_date}'::date """

    columns = {
                'player_id': 'player_id',
                'platform': 'platform',
                'service' : 'service',
                'step': expr('2 as step'),
                'step_name': expr("""'MyPlayer Creation'::string as step_name"""),
                'status': expr("""'Completed' as status"""),
                'received_on': 'received_on',
                'game_type': expr('extra_info_8 as config_1')
            }

    fact_player_progression_df = (
        spark.read
        .table(f"{database}{environment}.intermediate.fact_player_progression")
        .where(expr(inc_filter))
        .select(*columns.values())
    )
    return fact_player_progression_df

# COMMAND ----------

def read_fact_player_game_status_daily(database,environment,spark):
    prev_max = max_timestamp(spark, f"{database}{environment}.managed.fact_player_ftue", 'date','step = 3')

    current_min = (
        spark.read
        .table(f"{database}{environment}.managed.fact_player_game_status_daily")
        .where(expr("dw_insert_ts::date between current_date - 2 and current_date"))
        .select(expr(f"ifnull(min(date),'1999-01-01')::date as min_date"))
    ).collect()[0]['min_date']

    inc_min_date = min(prev_max,current_min)
    
    # added filter to exclude onbaording = true
    inc_filter = f"date >= '{inc_min_date}'::date and extra_grain_2 = False and agg_game_status_1 > 0"


    columns = {
                'player_id': 'player_id',
                'platform': 'platform',
                'service' : 'service',
                'step': expr('3 as step'),
                'step_name': expr("""'Forced Round of Play'::string as step_name"""),
                'status': expr("""'Completed' as status"""),
                'received_on': expr('date as received_on'),
                'game_type': expr('extra_grain_1 as config_1')
            }
    fact_player_game_status_daily_df = (
        spark.read
        .table(f"{database}{environment}.managed.fact_player_game_status_daily")
        .where(expr(inc_filter))
        .select(*columns.values())
    )
    return fact_player_game_status_daily_df    

# COMMAND ----------

def extract(database,environment,spark):
    step_1_df = read_fact_player_lesson(database,environment,spark)
    step_2_df = read_fact_player_progression(database,environment,spark)
    step_3_df = read_fact_player_game_status_daily(database,environment,spark)
    return [step_1_df,step_2_df,step_3_df]

# COMMAND ----------

def transform(batch_df, database, spark):
    batch_df = reduce(lambda df1, df2: df1.union(df2), batch_df)

    groupby_columns = {'player_id', 'platform', 'service', 'step', 'step_name', 'status' ,'config_1'}

    agg_columns = {
                    'date': expr("min(received_on::date) as date")
                    }

    ftue = (
            batch_df.alias('et')
            .groupBy(*groupby_columns)
            .agg(*agg_columns.values())
            .select(
                *groupby_columns,
                *agg_columns.keys(),
                expr("current_timestamp() AS dw_insert_ts"),
                expr("current_timestamp() AS dw_update_ts"),
                expr("SHA2(CONCAT_WS('|',date, player_id, platform, service, step, step_name, status, config_1),256) AS merge_key")

            )
        )
    return ftue

# COMMAND ----------

def load(df,database,environment,spark):

    # Merge variables and logic
    target_df = DeltaTable.forName(spark, f"{database}{environment}.managed.fact_player_ftue")

    merger_condition = 'target.merge_key = source.merge_key'
    merge_update_conditions = [
                                { 
                                 'condition' : """target.date != source.date""",
                                 'set_fields' : {
                                                    'date': 'least(target.date,source.date)'
                                                }
                                }
                        ]

    merge_df = target_df.alias("target").merge(df.alias("source"), f"{merger_condition}")

    merge_df = set_merge_update_condition(merge_df, merge_update_conditions)

    merge_df = set_merge_insert_condition(merge_df, df)

    # Execute the merge operation
    merge_df.execute()

# COMMAND ----------

def run_ftue(database,environment,view_mapping):
    database = database.lower()

    #setting the spark session
    spark = create_spark_session(name=f"{database}")
    
    #Creating the table and view, checkpoint
    create_fact_player_ftue(spark, database ,view_mapping)
    
    # Reading the data using bath data
    ftue_df = extract(database,environment,spark)

    #Applying Transformation
    ftue_df = transform(ftue_df, database, spark)

    #Merge data
    load(ftue_df,database,environment,spark)

    return 'Merge data completed'

# COMMAND ----------

run_ftue(database,environment,view_mapping)
