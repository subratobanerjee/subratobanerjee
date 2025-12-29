# Databricks notebook source
# MAGIC %run ../../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../../utils/ddl/table_functions

# COMMAND ----------

from pyspark.sql.functions import expr,sha2,concat_ws
from delta.tables import DeltaTable


# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'wwe2k25'
target_schema = 'intermediate'
data_source ='dna'
title = 'WWE2K25'
spark = create_spark_session()
key_cols = [
    "player_id",
    "received_on",
    "platform",
    "service",
    "country_code",
    "game_mode",
    "sub_mode",
    "status",
    "config_1",
    "config_2",
    "config_3",
    "matchmaking_id"
    ]

# COMMAND ----------

def create_fact_player_matchmaking(database):

    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.{target_schema}.fact_player_matchmaking (
        player_id STRING,
        received_on TIMESTAMP,
        platform STRING,
        service STRING,
        country_code STRING,
        game_mode STRING,
        sub_mode STRING,
        status STRING,
        config_1 STRING,
        config_2 STRING,
        config_3 STRING,
        matchmaking_id STRING,
        wait_time DECIMAL(18,2),
        dw_insert_ts TIMESTAMP,
        dw_update_ts TIMESTAMP,
        merge_key STRING
    )
    """

    properties = {
        "delta.feature.allowColumnDefaults": "supported" # this won't work until iceberg v3 is released
    }

    create_table(spark, sql, properties)
    return f"dbfs:/tmp/{database}/managed/batch/run{environment}/fact_player_matchmaking"

# COMMAND ----------

def read_matchmaking_status(environment,min_received_on):
    
    matchmaking_df = (
                        spark
                         .read
                         .table(f"{database}{environment}.raw.matchmakingstatus")
                         .where((expr(f"receivedOn::timestamp::date > to_date('{min_received_on}') - INTERVAL 2 DAY")))
                         )

    return matchmaking_df


# COMMAND ----------
def read_title_df(environment):
    title_df= (
                spark
                .read
                .table(f"reference{environment}.title.dim_title")
                .alias("title")
                )
    return title_df

# COMMAND ----------

def extract(environment, database):
    prev_max_ts = max_timestamp(spark, f"{database}{environment}.{target_schema}.fact_player_matchmaking", "received_on::timestamp")
    
    matchmaking_df = read_matchmaking_status(environment, prev_max_ts)
    title_df = read_title_df(environment)
    return matchmaking_df,title_df

def transform(matchmaking_df,title_df):
        
    
    final_df =  (
                    matchmaking_df.alias("mks").join(
                    title_df.alias("title"),
                    on = expr("mks.appPublicId == title.APP_ID"),
                    how = "left"
                    ).groupBy(
                        expr("coalesce(playerPublicId, concat_ws('-', receivedOn::timestamp::date, 'NULL-Player'))").alias("player_id"),
                        expr("receivedOn::timestamp AS received_on"),
                        expr("title.DISPLAY_PLATFORM AS platform"),
                        expr("title.DISPLAY_SERVICE as service"),
                        expr("countryCode AS country_code"),
                        expr("gamemode AS game_mode"),
                        expr("submode AS sub_mode"),
                        expr("status"),
                        expr("matchtype AS config_1"),
                        expr("matchgender AS config_2"),
                        expr("null::string AS config_3"),
                        expr("matchmakingguid AS matchmaking_id")
                    ).agg(
                        expr("max(waittime) as wait_time")
                    ).withColumn("merge_key", sha2(concat_ws("||", *key_cols), 256))
    )

    
    return final_df


# COMMAND ----------

def load_fact_player_matchmaking(df, database, environment):
    """
    Load the data into the fact_player_game_status_daily table in the specified environment.

    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        df (DataFrame): The DataFrame to load into the table.
        database (str): The database to load the table into.
        environment (str): The environment to load the table into.
    """

    # Perform the merge operation into the Delta table
    final_table = DeltaTable.forName(spark, f"{database}{environment}.{target_schema}.fact_player_matchmaking")

    # get schema of the table
    table_schema = spark.read.table(f"{database}{environment}.{target_schema}.fact_player_matchmaking").schema

    # create empty dataframe using the schema
    out_df = spark.createDataFrame([], table_schema)

    # union the incoming dataframe with the empty dataframe, we can set allowMissingColumns to False to be more strict
    df = df.select(
            "*",
            expr("CURRENT_TIMESTAMP() as dw_insert_ts"),
            expr("CURRENT_TIMESTAMP() as dw_update_ts")
        )
    out_df = out_df.unionByName(df, allowMissingColumns=True)

   
    # create the update column dict
    insert_cols = key_cols + ["dw_insert_ts", "merge_key"]
    update_cols = [col for col in df.columns if col not in insert_cols]
    update_set = {col: "old." + col for col in update_cols}
    merge_condition = " OR ".join(f"old.{col_name} <> new.{col_name}" for col_name in update_cols)
    
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

def run_batch():
    checkpoint_location = create_fact_player_matchmaking(database)
    matchmaking_df,title_df = extract(environment, database)
    df = transform(matchmaking_df,title_df)
    load_fact_player_matchmaking(df, database, environment)

# COMMAND ----------

run_batch()