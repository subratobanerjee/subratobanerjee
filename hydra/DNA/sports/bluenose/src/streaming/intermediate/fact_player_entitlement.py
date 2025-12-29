# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/intermediate/fact_player_entitlement

# COMMAND ----------

from pyspark.sql.functions import expr, when

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'bluenose'
title = "'PGA Tour 2K25','PGA Tour 2K25: Demo'"
    
def read_dim_title_df(title, environment, spark):
    title_df = (
        spark
        .read
        .table(f"reference{environment}.title.dim_title")
        .where(f"title in ({title})")
        .selectExpr(
            "app_id::string AS app_id",
            "display_platform::string AS platform",
            "display_service::string AS service"
        )
    )

    return title_df

def extract(database, environment, spark):
    transaction_filter = "playerPublicId NOT ILIKE '%Null%' AND playerPublicId is not null AND playerPublicId != 'anonymous'"

    event_columns = {
        "player_id": expr("playerPublicId as player_id"),
        "app_public_id": expr("appPublicId::string as app_id"),
        "activedlc_value": expr("explode(from_json(activedlc, 'array<string>')) as activedlc_value"),
        "country_code": expr("countryCode::string as country_code"),
        "received_on": expr("receivedOn::timestamp as received_on"),
        "game_type": expr("""CASE WHEN appGroupId IN ('d3963fb6e4f54a658e1a14846f0c9a8c', '317c0552032c4804bb10d81b89f4c37e') THEN 'full game'
                                  WHEN appGroupId = '886bcab8848046d0bd89f5f1ce3b057b' THEN 'demo' ELSE NULL END as game_type""")
        }

    df = (
            spark.readStream
            .table(f"{database}{environment}.raw.applicationsession")
            .where(expr("""activedlc IS NOT NULL AND buildenvironment = 'RELEASE'AND playerPublicId IS NOT NULL 
                            AND playerPublicId is not null AND playerPublicId != 'anonymous'"""))
            .select(*event_columns.values())
        )

    return df


def load(df,database,environment,spark):

    # Merge variables and logic
    target_df = DeltaTable.forName(spark, f"{database}{environment}.intermediate.fact_player_entitlement")

    merger_condition = 'target.merge_key = source.merge_key'

    merge_update_conditions = [
                                    { 
                                         'condition' : """target.last_seen < source.last_seen
                                                          or target.first_seen > source.first_seen
                                                         """,
                                         'set_fields' : {
                                                            'first_seen' : 'least(target.first_seen,source.first_seen)',
                                                            'last_seen' : 'greatest(target.last_seen,source.last_seen)',
                                                            'first_seen_country_code':"""case 
                                                                                            when target.first_seen > source.first_seen and source.first_seen_country_code != 'ZZ' then source.first_seen_country_code
                                                                                            when target.first_seen < source.first_seen and source.first_seen_country_code != 'ZZ' and target.first_seen_country_code = 'ZZ' then source.first_seen_country_code
                                                                                         end""",
                                                            'dw_update_ts': 'source.dw_update_ts'
                                                        }
                                        }                        
                                ]

    merge_df = target_df.alias("target").merge(df.alias("source"), f"{merger_condition}")

    merge_df = set_merge_update_condition(merge_df, merge_update_conditions)

    merge_df = set_merge_insert_condition(merge_df, df)

    # Execute the merge operation
    merge_df.execute()

def transform(df,batch_id, database, environment, spark):
    title_df = read_dim_title_df(title, environment, spark)

    transformed_columns = { 
             "player_id" : "player_id",
             "platform" : "platform",
             "service": 'service',
             "game_type": 'game_type',
             "country_code": "country_code", 
             "entitlement_id": expr('activedlc_value as entitlement_id'),
             "received_on": 'received_on'
            }
    
    df = (
        df.alias('d')
        .join(title_df.alias('t'),expr('d.app_id = t.app_id'),'left')
        .select(*transformed_columns.values())
        )

    df = (
        df.selectExpr(
            "player_id",
            "platform",
            "service",
            "game_type",
            "entitlement_id",
            "ifnull(FIRST(country_code,True) OVER (PARTITION BY player_id, entitlement_id, platform, service ORDER BY received_on ASC),'ZZ') as first_seen_country_code",
            "FIRST(received_on,True) OVER (PARTITION BY player_id, entitlement_id, platform, service ORDER BY received_on ASC) as first_seen",
            "FIRST(received_on,True) OVER (PARTITION BY player_id, entitlement_id, platform, service ORDER BY received_on DESC) as last_seen",
            "current_timestamp() as dw_insert_ts",
            "current_timestamp() as dw_update_ts",
            "sha2(concat_ws('|', player_id, platform, service, entitlement_id), 256) as merge_key"
        ).distinct()
    )

    load(df,database,environment,spark)

# COMMAND ----------

def stream_transactions(database,environment,title):
    database = database.lower()

    #setting the spark session
    spark = create_spark_session(name=f"{database}")
    
    #Setting the checkpoint_location from the fucntion output
    checkpoint_location = create_fact_player_entitlement(spark, database, environment)
    print(f'checkpoint_location : {checkpoint_location}')
    
    df = extract(database,environment,spark)

    print('Started writeStream')

    #Writing the stream output to the table 
    (
        df
        .writeStream
        .trigger(availableNow=True)
        .foreachBatch(lambda df, batch_id: transform(df, batch_id, database,environment, spark))
        .option("checkpointLocation", checkpoint_location)
        .outputMode("append")
        .start()
    )

# COMMAND ----------

stream_transactions(database,environment,title)
