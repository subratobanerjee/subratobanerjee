# Databricks notebook source
from pyspark.sql.functions import (expr, when)
from delta.tables import DeltaTable
from functools import reduce

# COMMAND ----------

from pyspark.sql import SparkSession

def create_spark_session(name='Hydra',local_config={}):

    spark = SparkSession.builder.appName(f"{name}").getOrCreate()

    spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "false")
    spark.conf.set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")
    spark.conf.set("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")
    spark.conf.set("spark.sql.streaming.stateStore.stateSchemaCheck", "false")
    return spark


# COMMAND ----------

global_properties = {
        'delta.enableIcebergCompatV2': 'true',
        'delta.universalFormat.enabledFormats': 'iceberg',
        'write.metadata.delete.current.file.enabled': 'true',
        'write.metadata.check.interval-ms': '10000',
        'snapshot.expiration.interval-ms': '604800000',
        'write.target.file.size': '52428800',
        'delta.autoOptimize.optimizeWrite': 'true',
        'delta.autoOptimize.autoCompact': 'true'
    }

# COMMAND ----------

def create_table(spark, sql, properties={}):
    
    global_properties.update(properties)

    sql =  sql+'\nTBLPROPERTIES ( \n'

    for key, value in global_properties.items():
        sql += f"    '{key}' = '{value}',\n"
    
    sql = sql.rstrip(',\n') + "\n    );"

    return spark.sql(sql)


# COMMAND ----------

def create_fact_player_code_redemption(spark, database, environment, view_mapping, properties={}):

    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.fact_player_code_redemption (
        title STRING,
        platform STRING,
        service STRING,
        country_code STRING,
        player_id STRING,
        timestamp timestamp,
        campaign_id STRING,
        code_id STRING,
        code STRING,
        reason STRING,
        status STRING,
        dw_insert_ts TIMESTAMP,
        dw_update_ts TIMESTAMP,
        merge_key STRING
    )
    comment 'the table will have code redemptions from the ecommerce'
    partitioned by (platform)
    """
    create_table(spark, sql, properties)
    create_fact_player_code_redemption_view(spark, database,environment, view_mapping)
    return f"dbfs:/tmp/{database}/managed/batch/run{environment}/fact_player_code_redemption"

# COMMAND ----------

def create_fact_player_code_redemption_view(spark, database, environment, mapping):
    sql = f"""
    CREATE VIEW IF NOT EXISTS {database}{environment}.managed_view.fact_player_code_redemption AS (
        SELECT
            title ,
            platform ,
            service ,
            country_code ,
            player_id,
            timestamp ,
            campaign_id ,
            code_id ,
            code ,
            reason ,
            status
        from {database}{environment}.managed.fact_player_code_redemption
    )
    """
    spark.sql(sql)

# COMMAND ----------

def max_timestamp(spark, table_name, column_name='dw_insert_ts', filters='1=1'):
    latest_ts_value = spark.sql(f"select ifnull(max({column_name}) , '1999-01-01')::date as latest_value from {table_name} where {filters}").collect()[0]['latest_value']
    return latest_ts_value

# COMMAND ----------

import argparse
def arg_parser():
    parser = argparse.ArgumentParser(description="Consume parameters from Databricks job")
    parser.add_argument("--environment", type=str, required=True, help="Description for environment")
    args = parser.parse_args()
    environment = args.environment
    return environment

# COMMAND ----------

def set_merge_insert_condition (merge_df,df):
    # Dynamically generate the insert columns based on micro_batch_df columns
    insert_columns = {f"target.{col}": f"source.{col}" for col in df.columns}
    return merge_df.whenNotMatchedInsert(values=insert_columns)

# COMMAND ----------

def read_coderedemptionsuccessevent(database,environment,spark):
    prev_max = max_timestamp(spark, f"{database}{environment}.managed.fact_player_code_redemption", 'timestamp' ,"status = 'Success'")

    current_min = (
        spark.read
        .table(f"{database}{environment}.ecommercev3.coderedemptionsuccessevent")
        .where(expr("occurredOn::timestamp::date between current_date - 2 and current_date"))
        .select(expr(f"ifnull(min(occurredOn::timestamp),'1999-01-01')::date as min_date"))
    ).collect()[0]['min_date']

    inc_min_date = min(prev_max,current_min)

    inc_filter = f"occurredOn::timestamp::date >= '{inc_min_date}'::date"

    columns = {
                'player_id': expr('accountid as player_id'),
                'campaign_id': expr('campaignid as campaign_id'),
                'status': expr("""'Completed' as status"""),
                'code_id': expr('codeid as code_id'),
                'code': 'code',
                'timestamp': expr('occurredOn::timestamp as timestamp'),
                'reason': expr("'Redeemed' as reason"),
                'status': expr("'Success' as status"),
                'country_code': expr("ifnull(countrycode,'zz') as country_code"),
                'appPublicId': 'appPublicId'

            }


    df = (
        spark.read
        .table(f"{database}{environment}.ecommercev3.coderedemptionsuccessevent")
        .where(expr(inc_filter))
        .select(*columns.values())
    )
    return df

# COMMAND ----------

def read_coderedemptionfailedevent(database,environment,spark):
    prev_max = max_timestamp(spark, f"{database}{environment}.managed.fact_player_code_redemption", 'timestamp',"status = 'Failed'")

    current_min = (
        spark.read
        .table(f"{database}{environment}.ecommercev3.coderedemptionfailedevent")
        .where(expr("occurredOn::timestamp::date between current_date - 2 and current_date"))
        .select(expr(f"ifnull(min(occurredOn::timestamp),'1999-01-01')::date as min_date"))
    ).collect()[0]['min_date']

    inc_min_date = min(prev_max,current_min)

    inc_filter = f"occurredOn::timestamp::date >= '{inc_min_date}'::date"

    columns = {
                'player_id': expr('accountid as player_id'),
                'campaign_id': expr('campaignid as campaign_id'),
                'status': expr("""'Completed' as status"""),
                'code_id': expr('codeid as code_id'),
                'code': 'code',
                'timestamp': expr('occurredOn::timestamp as timestamp'),
                'reason': 'reason',
                'status': expr("'Failed' as status"),
                'country_code': expr("ifnull(countrycode,'zz') as country_code"),
                'appPublicId': 'appPublicId'
            }


    df = (
        spark.read
        .table(f"{database}{environment}.ecommercev3.coderedemptionfailedevent")
        .where(expr(inc_filter))
        .select(*columns.values())
    )
    return df


# COMMAND ----------

def extract(database,environment,spark):
    df_1 = read_coderedemptionsuccessevent(database,environment,spark)
    df_2 = read_coderedemptionfailedevent(database,environment,spark)
    return [df_1, df_2]

# COMMAND ----------

def transform(batch_df, database, environment, spark):
    batch_df = reduce(lambda df1, df2: df1.union(df2), batch_df)

    columns = {  
                'title': 'title',
                'platform': expr('display_platform as platform'),
                'service': expr('display_service as service'),
                'country_code': 'country_code',
                'player_id': 'player_id',
                'campaign_id': 'campaign_id',
                'code_id': 'code_id',
                'code': 'code',
                'timestamp': 'timestamp',
                'reason': 'reason',
                'status': 'status'
            }

    dim_title = spark.table(f"reference{environment}.title.dim_title").select('title','display_service','display_platform','app_id')

    df = (
            batch_df.alias('e')
            .join(dim_title.alias('t'),expr("e.appPublicId = t.app_id"),"inner")
            .select(
                *columns.values(),
                expr("current_timestamp() AS dw_insert_ts"),
                expr("current_timestamp() AS dw_update_ts"),
                expr("SHA2(CONCAT_WS('|',title, platform, service, country_code, player_id , campaign_id, code_id, code, reason, status),256) AS merge_key")

            )
        )
    return df

# COMMAND ----------

def load(df,database,environment,spark):

    # Merge variables and logic
    target_df = DeltaTable.forName(spark, f"{database}{environment}.managed.fact_player_code_redemption")

    merger_condition = 'target.merge_key = source.merge_key'

    merge_df = target_df.alias("target").merge(df.alias("source"), f"{merger_condition}")

    merge_df = set_merge_insert_condition(merge_df, df)

    # Execute the merge operation
    merge_df.execute()

# COMMAND ----------

def run_batch():
    database = 'coretech'
    environment = arg_parser()
    view_mapping = {}

    database = database.lower()

    #setting the spark session
    spark = create_spark_session(name=f"{database}")
    
    #Creating the table and view, checkpoint
    create_fact_player_code_redemption(spark, database ,environment, view_mapping)
    
    print('Extracting the data')
    # Reading the data using bath data
    df = extract(database,environment,spark)

    print('Transforming the data')
    #Applying Transformation
    df = transform(df,database,environment,spark)
    
    print('Merging the data')
    #Merge data
    load(df,database,environment,spark)

    return 'Merge data completed'

# COMMAND ----------

run_batch()
