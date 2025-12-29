# Databricks notebook source
from pyspark.sql.functions import (expr, when)
from delta.tables import DeltaTable
from functools import reduce

# COMMAND ----------

# MAGIC %run ../../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../../utils/ddl/managed/agg_igc_earn_daily

# COMMAND ----------

# MAGIC %run ../../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../../utils/ddl/table_functions

# COMMAND ----------
database = 'wwe2k25'
input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())


view_mapping = {
                'game_count' : 'game_count as game_count',
                'player_count' : 'player_count as player_count',
                'igc_type_1_earn_count' : 'igc_type_1_earn_count as igc_type_1_earn_count',
                'igc_type_2_earn_count' : 'igc_type_2_earn_count as igc_type_2_earn_count',
                'igc_type_3_earn_count' : 'igc_type_3_earn_count as igc_type_3_earn_count',
                'igc_type_4_earn_count' : 'igc_type_4_earn_count as igc_type_4_earn_count',
                'igc_type_5_earn_count' : 'igc_type_5_earn_count as igc_type_5_earn_count',
                'igc_type_1_earn_amount' : 'igc_type_1_earn_amount as igc_type_1_earn_amount',
                'igc_type_2_earn_amount' : 'igc_type_2_earn_amount as igc_type_2_earn_amount',
                'igc_type_3_earn_amount' : 'igc_type_3_earn_amount as igc_type_3_earn_amount',
                'igc_type_4_earn_amount' : 'igc_type_4_earn_amount as igc_type_4_earn_amount',
                'igc_type_5_earn_amount' : 'igc_type_5_earn_amount as igc_type_5_earn_amount'
            }

# COMMAND ----------

def extract(database,environment,spark):
    # Selecting the transaction event data
    columns = {
                "date": 'date',
                "player_id": 'player_id',
                "platform": 'platform',
                "service": 'service',
                "country_code" :'country_code',
                "game_mode": 'game_mode',
                "sub_mode": 'sub_mode',
                "transaction_source": "transaction_source",
                "game_count": expr("game_count_1 as game_count"),
                'igc_type_1_earn_count' : 'igc_type_1_earn_count',
                'igc_type_2_earn_count' : 'igc_type_2_earn_count',
                'igc_type_3_earn_count' : 'igc_type_3_earn_count',
                'igc_type_4_earn_count' : 'igc_type_4_earn_count',
                'igc_type_5_earn_count' : 'igc_type_5_earn_count',
                'igc_type_1_earn_amount' : 'igc_type_1_earn_amount',
                'igc_type_2_earn_amount' : 'igc_type_2_earn_amount',
                'igc_type_3_earn_amount' : 'igc_type_3_earn_amount',
                'igc_type_4_earn_amount' : 'igc_type_4_earn_amount',
                'igc_type_5_earn_amount' : 'igc_type_5_earn_amount'
                }

    transaction_filter = "player_id NOT ILIKE '%Null%' AND player_id is not null AND player_id != 'anonymous'"

    inc_filter = f"{transaction_filter} and (dw_insert_ts::date between current_date - 2 and current_date)"

    prev_max = max_timestamp(spark, f"{database}{environment}.managed.agg_igc_earn_daily", 'date')

    current_min = (
        spark.read
        .table(f"{database}{environment}.managed.fact_player_igc_earn_daily")
        .where(expr(inc_filter))
        .select(expr(f"ifnull(min(date),'1999-01-01')::date as min_date"))
    ).collect()[0]['min_date']

    inc_min_date = min(prev_max,current_min)

    batch_filter = f"({transaction_filter}) and date::date >= '{inc_min_date}'::date"

    Spend_transaction_df = (
        spark.read
        .table(f"{database}{environment}.managed.fact_player_igc_earn_daily")
        .where(expr(batch_filter))
        .select(*columns.values())
    )

    return Spend_transaction_df

# COMMAND ----------

def transform(batch_df, database, spark):
    groupby_columns = {'et.date', 'et.platform', 'et.service', 'et.country_code', 'et.game_mode', 'et.sub_mode'}

    agg_columns = {
        'player_count': expr("COUNT(DISTINCT player_id) AS player_count"),
        'game_count': expr("SUM(game_count) AS game_count"),

        'igc_type_1_earn_count': expr("SUM(igc_type_1_earn_count) AS igc_type_1_earn_count"),
        'igc_type_2_earn_count': expr("SUM(igc_type_2_earn_count) AS igc_type_2_earn_count"),
        'igc_type_3_earn_count': expr("SUM(igc_type_3_earn_count) AS igc_type_3_earn_count"),
        'igc_type_4_earn_count': expr("SUM(igc_type_4_earn_count) AS igc_type_4_earn_count"),
        'igc_type_5_earn_count': expr("SUM(igc_type_5_earn_count) AS igc_type_5_earn_count"),


        'igc_type_1_earn_amount': expr("SUM(igc_type_1_earn_amount) AS igc_type_1_earn_amount"),
        'igc_type_2_earn_amount': expr("SUM(igc_type_2_earn_amount) AS igc_type_2_earn_amount"),
        'igc_type_3_earn_amount': expr("SUM(igc_type_3_earn_amount) AS igc_type_3_earn_amount"),
        'igc_type_4_earn_amount': expr("SUM(igc_type_4_earn_amount) AS igc_type_4_earn_amount"),
        'igc_type_5_earn_amount': expr("SUM(igc_type_5_earn_amount) AS igc_type_5_earn_amount"),

    }

    Spend_transactions = (
        batch_df.alias('et')
        .groupBy(*groupby_columns)
        .agg(*agg_columns.values())
        .select(
            *groupby_columns,
            *agg_columns.keys(),
            # expr("current_date() AS dw_insert_date"),
            expr("current_timestamp() AS dw_insert_ts"),
            expr("current_timestamp() AS dw_update_ts"),
            expr("SHA2(CONCAT_WS('|',date, platform,service, country_code , game_mode, sub_mode ),256) AS merge_key")
        )
    )

    return Spend_transactions


# COMMAND ----------

def load(Spend_transactions,database,environment,spark):

    # Merge variables and logic
    target_df = DeltaTable.forName(spark, f"{database}{environment}.managed.agg_igc_earn_daily")

    merger_condition = 'target.merge_key = source.merge_key'
    merge_update_conditions = [
                                { 
                                 'condition' : """target.player_count != source.player_count OR
                                                  target.igc_type_1_earn_count != source.igc_type_1_earn_count OR
                                                  target.igc_type_1_earn_amount != source.igc_type_1_earn_amount OR
                                                  target.igc_type_2_earn_count != source.igc_type_2_earn_count OR
                                                  target.igc_type_2_earn_amount != source.igc_type_2_earn_amount OR
                                                  target.igc_type_3_earn_count != source.igc_type_3_earn_count OR
                                                  target.igc_type_3_earn_amount != source.igc_type_3_earn_amount OR
                                                  target.igc_type_4_earn_count != source.igc_type_4_earn_count OR
                                                  target.igc_type_4_earn_amount != source.igc_type_4_earn_amount OR
                                                  target.igc_type_5_earn_amount != source.igc_type_5_earn_amount OR
                                                  target.igc_type_5_earn_count != source.igc_type_5_earn_count
                                                  """,
                                 'set_fields' : {
                                                    'player_count': 'greatest(target.player_count,source.player_count)',
                                                    'igc_type_1_earn_count': 'greatest(target.igc_type_1_earn_count,source.igc_type_1_earn_count)',
                                                    'igc_type_1_earn_amount': 'greatest(target.igc_type_1_earn_amount,source.igc_type_1_earn_amount)',
                                                    'igc_type_2_earn_count': 'greatest(target.igc_type_2_earn_count,source.igc_type_2_earn_count)',
                                                    'igc_type_2_earn_amount': 'greatest(target.igc_type_2_earn_amount,source.igc_type_2_earn_amount)',
                                                    'igc_type_3_earn_count': 'greatest(target.igc_type_3_earn_count,source.igc_type_3_earn_count)',
                                                    'igc_type_3_earn_amount': 'greatest(target.igc_type_3_earn_amount,source.igc_type_3_earn_amount)',
                                                    'igc_type_4_earn_count': 'greatest(target.igc_type_4_earn_count,source.igc_type_4_earn_count)',
                                                    'igc_type_4_earn_amount': 'greatest(target.igc_type_4_earn_amount,source.igc_type_4_earn_amount)',
                                                    'igc_type_5_earn_amount': 'greatest(target.igc_type_5_earn_amount,source.igc_type_5_earn_amount)',
                                                    'igc_type_5_earn_count': 'greatest(target.igc_type_5_earn_count,source.igc_type_5_earn_count)',
                                                    'dw_update_ts': 'source.dw_update_ts'
                                                }
                                }
                        ]

    merge_df = target_df.alias("target").merge(Spend_transactions.alias("source"), f"{merger_condition}")

    merge_df = set_merge_update_condition(merge_df, merge_update_conditions)

    merge_df = set_merge_insert_condition(merge_df, Spend_transactions)

    # Execute the merge operation
    merge_df.execute()

# COMMAND ----------

def create_wwe_agg_igc_earn_daily(spark, database,view_mapping, properties={}):
    sql = f"""
            CREATE TABLE IF NOT EXISTS {database}{environment}.managed.AGG_IGC_EARN_DAILY 
                    (
                        DATE DATE  COMMENT 'Received date from the source',
                        PLATFORM VARCHAR(16777216)  COMMENT 'Transformed platform value which can be used for reporting',
                        SERVICE VARCHAR(16777216)  COMMENT 'Transformed service value which can be used for reporting',
                        COUNTRY_CODE VARCHAR(16777216)  COMMENT 'Country code from the source',
                        GAME_MODE VARCHAR(16777216) ,
                        SUB_MODE VARCHAR(16777216) ,
                        TRANSACTION_SOURCE VARCHAR(16777216)  COMMENT 'Source of the earned transaction',
                        game_count decimal(38,0) comment 'game counts from the earned action',
                        PLAYER_COUNT decimal(38,0) COMMENT 'Player distinct counts',
                        IGC_TYPE_1_EARN_COUNT decimal(38,0) COMMENT 'AGG Earns Placeholder',
                        IGC_TYPE_1_EARN_AMOUNT decimal(38,2) COMMENT 'AGG Earns Placeholder',
                        IGC_TYPE_2_EARN_COUNT decimal(38,0) COMMENT 'AGG Earns Placeholder',
                        IGC_TYPE_2_EARN_AMOUNT decimal(38,2) COMMENT 'AGG Earns Placeholder',
                        IGC_TYPE_3_EARN_COUNT decimal(38,0) COMMENT 'AGG Earns Placeholder',
                        IGC_TYPE_3_EARN_AMOUNT decimal(38,2) COMMENT 'AGG Earns Placeholder',
                        IGC_TYPE_4_EARN_COUNT decimal(38,0) COMMENT 'AGG Earns Placeholder',
                        IGC_TYPE_4_EARN_AMOUNT decimal(38,2) COMMENT 'AGG Earns Placeholder',
                        IGC_TYPE_5_EARN_COUNT decimal(38,0) COMMENT 'AGG Earns Placeholder',
                        IGC_TYPE_5_EARN_AMOUNT decimal(38,2) COMMENT 'AGG Earns Placeholder',
                        dw_insert_ts timestamp comment 'data warehouse audit field for records inserted timestamp',
                        dw_update_ts timestamp comment 'data warehouse audit field for records updated timestamp',
                        merge_key string comment 'unique id generated by the hash of the grain of the table '
                    )
                    COMMENT 'The table will have Ingame currency earned aggregated data'
                    partitioned by (date,platform)
        """
    create_table(spark, sql, properties)
    create_wwe_agg_igc_earn_daily_view(spark, database, view_mapping)
    return f"dbfs:/tmp/{database}/managed/streaming/run{environment}/agg_igc_earn_daily"


# COMMAND ----------

def create_wwe_agg_igc_earn_daily_view(spark, database, mapping):
    """
    Create the fact_player_igc_earn_nrt view in the specified environment.

    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
        mapping (dict): A dictionary of column mappings.
    """
    sql = f"""
    CREATE OR REPLACE VIEW {database}{environment}.managed_view.agg_igc_earn_daily AS (
        SELECT
            date,
            platform,
            service,
            country_code, 
            game_mode,
            sub_mode,
            transaction_source, 
            {','.join(str(mapping[key]) for key in mapping)},
            dw_insert_ts,
            dw_update_ts
        from {database}{environment}.managed.agg_igc_earn_daily
    )
    """
    spark.sql(sql)


def run_earn_transactions(database,environment,view_mapping):
    database = database.lower()

    #setting the spark session
    spark = create_spark_session(name=f"{database}")
    
    #Creating the table and view, checkpoint
    create_wwe_agg_igc_earn_daily(spark, database ,view_mapping)

    print('Extracting the data')
    
    # Reading the data using bath data
    Spend_transaction_df = extract(database,environment,spark)

    #Applying Transformation
    Spend_transactions = transform(Spend_transaction_df, database, spark)

    #Merge data
    load(Spend_transactions,database,environment,spark)

    return 'Merge data completed'

# COMMAND ----------

run_earn_transactions(database,environment,view_mapping)