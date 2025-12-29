# Databricks notebook source
# MAGIC %run ../../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../../utils/ddl/table_functions


# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import expr
# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'wwe2k25'
title = "'WWE 2K25'"
    
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

def read_dim_entitlement_df(environment, spark):
    dim_entitlement_df =  (
        spark.read.table(f"wwe2k25{environment}.reference.dim_entitlement")
        .selectExpr(
            "sku::string AS entitlement_sku",
            "name::string AS entitlement_name",
            "type::string AS entitlement_type",
            "price::double AS purchase_price"
        )
    )
    return dim_entitlement_df


def extract(database, environment, spark):
    dlc_columns = [
        "auxawesome", "auxbeautiful", "auxcool", "auxdynamite", "auxexcellent", "auxfantastic", "auxgreat", "auxheavenly",
        "basegame", "deluxeedition", "dlc10", "dlc20", "dlc30", "dlc40", "dlc50", "dlc60",
        "mycareerbonus", "preorderbonus", "seasonpass", "storeunlockbonus", "superdeluxe"
    ]
    
    df = (
            spark.readStream
            .table(f"{database}{environment}.raw.dlclist")
            .selectExpr(
            "playerPublicId as player_id",
            "appPublicId as app_id",
            "receivedOn as received_on",
            "countryCode as country_code",
            f"stack({len(dlc_columns)}, " + ", ".join([f"'{dlc}', {dlc}" for dlc in dlc_columns]) + ") as (entitlement_id, entitlement_value)"
        )
        .filter("entitlement_value IS NOT NULL")
        .filter("playerPublicId IS NOT NULL AND countryCode IS NOT NULL")
    )
    return df

# COMMAND ----------

def transform(df, batch_id, database, environment, spark):
    title_df = read_dim_title_df(title, environment, spark)
    entitlement_df = read_dim_entitlement_df(environment, spark)

    # Read sku_lookup for seasonpass_flag and preorder_flag
    sku_lookup_df = (
        spark.read.table(f"wwe2k25{environment}.managed.sku_lookup")
        .selectExpr(
            "player_id as sku_player_id",
            "platform as sku_platform",
            "service as sku_service",
            "sku as entitlement_id",
            "preorder_flag",
            "seasonpass_flag"
        )
    )

    df = (
        df
        .withColumn("first_seen_country_code", expr("FIRST(country_code, TRUE) OVER (PARTITION BY player_id, app_id, entitlement_id ORDER BY received_on)"))
        .withColumn("first_seen", expr("FIRST(received_on, TRUE) OVER (PARTITION BY player_id, app_id, entitlement_id ORDER BY received_on)"))
        .withColumn("last_seen", expr("LAST(received_on, TRUE) OVER (PARTITION BY player_id, app_id, entitlement_id ORDER BY received_on)"))
    )

    df = (
        df.alias('d')
        .join(title_df.alias('t'), expr('d.app_id = t.app_id'), 'left')
        .join(entitlement_df.alias("e"), expr("d.entitlement_id = e.entitlement_sku"), "left")
        .join(sku_lookup_df.alias('s'), 
              (expr('d.player_id = s.sku_player_id') & expr('t.platform = s.sku_platform') & expr('t.service = s.sku_service')), 
              'left')
        .withColumn(
        "season_pass_entitlement", 
        expr("CASE WHEN s.seasonpass_flag = true THEN true ELSE false END")  
            )
        .withColumn(
        "full_game_entitlement", 
        expr("CASE WHEN s.preorder_flag = false THEN true ELSE false END")  
        )
    )
    df = df.filter(
    (df.entitlement_name.isNotNull()) |
    (df.entitlement_type.isNotNull()) |
    (df.purchase_price.isNotNull())
    )

    df = (
        df
        .selectExpr(
            "IFNULL(d.player_id, DATE(FROM_UNIXTIME(d.first_seen))) AS player_id",
            "t.platform",
            "t.service",
            "d.entitlement_id", 
            "d.first_seen_country_code",
            "d.first_seen",
            "d.last_seen",
            "e.entitlement_name",
            "e.entitlement_type",
            "e.purchase_price",
            "season_pass_entitlement",
            "full_game_entitlement",
            "current_timestamp() as dw_insert_ts",
            "current_timestamp() as dw_update_ts",
            "sha2(concat_ws('|', d.player_id, t.platform, t.service, d.entitlement_id), 256) AS merge_key",
        ).distinct()
    )

    # Load the transformed data
    load(df, database, environment, spark)

# COMMAND ----------

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
                                                            'season_pass_entitlement': 'source.season_pass_entitlement',
                                                            'full_game_entitlement': 'source.full_game_entitlement',
                                                            
                                                            'dw_update_ts': 'source.dw_update_ts'
                                                        }
                                        }                        
                                ]

    merge_df = target_df.alias("target").merge(df.alias("source"), f"{merger_condition}")

    merge_df = set_merge_update_condition(merge_df, merge_update_conditions)

    merge_df = set_merge_insert_condition(merge_df, df)

    # Execute the merge operation
    merge_df.execute()


def stream_transactions(database,environment,title):
    database = database.lower()

    #setting the spark session
    spark = create_spark_session(name=f"{database}")
    
    #Setting the checkpoint_location from the fucntion output
    checkpoint_location = create_wwe_fact_player_entitlement(spark, database, environment)
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

def create_wwe_fact_player_entitlement(spark, database, environment, properties={}):
    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.intermediate.fact_player_entitlement (
        player_id STRING,
        platform STRING,
        service STRING,
        entitlement_id STRING,
        first_seen_country_code STRING,
        first_seen TIMESTAMP,
        last_seen TIMESTAMP,
        entitlement_name STRING,
        entitlement_type STRING,
        purchase_price DOUBLE,
        season_pass_entitlement BOOLEAN,
        full_game_entitlement BOOLEAN,
        dw_insert_ts TIMESTAMP,
        dw_update_ts TIMESTAMP,
        merge_key STRING
    )
    """

    properties = {
        "delta.feature.allowColumnDefaults": "supported"  # this won't work until iceberg v3 is released
    }

    create_table(spark, sql, properties)
    return f"dbfs:/tmp/{database}/intermediate/streaming/run{environment}/fact_player_entitlements"


# COMMAND ----------

stream_transactions(database,environment,title)