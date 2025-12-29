# Databricks notebook source
# MAGIC %run ../../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../../utils/ddl/managed/fact_player_summary_ltd

# COMMAND ----------

from pyspark.sql import functions as f
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from pyspark.sql.functions import expr

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'wwe2k25'
execution_date = f.current_timestamp()
spark = create_spark_session()
interval = 2 # Use this parameter to go back for multiple days, to fix attributes for historical data
view_mapping = { 'ltd_string_2' : 'ltd_string_2 as first_seen_country_code',
                 'ltd_string_3' : 'ltd_string_3 as last_seen_country_code'
}

# COMMAND ----------

def read_player_activity(database,environment):
    # Selecting player activity incremental data based on the in-built read checkpoint/marker
    activity_df = (
        spark
        .readStream
        .table(f"{database}{environment}.intermediate.fact_player_activity")
        .filter((f.col('player_id').isNotNull()) & (f.lower('platform') != 'unknown'))
        #& (f.col("dw_insert_ts").between(f.lit(execution_date) - f.expr(f"interval {interval} days"), f.lit(execution_date))))
        #.where(expr(batch_filter))
        .alias("activity")
    )
    return activity_df

# COMMAND ----------

def load_dimplayer(batch_df, batch_id, database,environment,spark):
  
    # Group by player_id, platform, service, and window
    windowSpec = Window.partitionBy("player_id", "platform", "service").orderBy("received_on")

    window_df = batch_df.withColumns({
                                    "player_id" : f.col("player_id"),
                                    "platform" : f.col("platform"),
                                    "service": f.col("service"),
                                    "first_seen": f.min("received_on").over(windowSpec),
                                    "last_seen": f.max("received_on").over(windowSpec),
                                    "first_seen_country_code" : f.first("country_code", ignorenulls=True).over(windowSpec),
                                    "last_seen_country_code" : f.last("country_code", ignorenulls=True).over(windowSpec),
                                    "first_seen_lang": f.first(
                                    f.when(f.col("language_setting").isNotNull() & (f.col("language_setting") != "None"), f.col("language_setting"))
                                     .otherwise(None),
                                         ignorenulls=True
                                            ).over(windowSpec),
                                    "last_seen_lang": f.last(
                                    f.when(f.col("language_setting").isNotNull() & (f.col("language_setting") != "None"), f.col("language_setting"))
                                     .otherwise(None),
                                         ignorenulls=True
                                            ).over(windowSpec),
                                    "dw_insert_ts" : f.lit(execution_date),
                                    "dw_update_ts" : f.lit(execution_date)

    }
    ).select(
    "player_id",
    "platform",
    "service",
    "first_seen",
    "last_seen",
    "first_seen_country_code",
    "last_seen_country_code",
    "first_seen_lang",
    "last_seen_lang",
    "dw_insert_ts",
    "dw_update_ts"
    ).distinct()


    player_df = window_df.groupBy(
                                            f.col("player_id"),
                                            f.col("platform"),
                                            f.col("service")
                                    ).agg(
                                            f.max("first_seen").alias("first_seen"),
                                            f.max("last_seen").alias("last_seen"),
                                            f.max("first_seen_country_code").alias("ltd_string_2"),
                                            f.max("last_seen_country_code").alias("ltd_string_3"),
                                            f.max("first_seen_lang").alias("first_seen_lang"),
                                            f.max("last_seen_lang").alias("last_seen_lang"),
                                            f.max("dw_insert_ts").alias("dw_insert_ts"),
                                            f.max("dw_update_ts").alias("dw_update_ts")
                                    ).distinct()
    player_df = player_df.select('*',
    expr("sha2(concat_ws('|', player_id, platform, service), 256) as merge_key")
    )
    # Load the Delta table
    delta_table = DeltaTable.forName(spark, f"{database}{environment}.managed.fact_player_summary_ltd")

    (delta_table.alias("old")
     .merge(
         player_df.alias("new"),
         "new.merge_key = old.merge_key"
     )
     .whenNotMatchedInsert(values=
         {
             "player_id": "new.player_id",
             "platform": "new.platform",
             "service": "new.service",
             "first_seen": "new.first_seen",
             "last_seen": "new.last_seen",
             "ltd_string_2": "new.ltd_string_2",
             "ltd_string_3": "new.ltd_string_3",
             "first_seen_lang": "new.first_seen_lang",
             "last_seen_lang": "new.last_seen_lang",
             "dw_insert_ts": "new.dw_insert_ts",
             "dw_update_ts": "new.dw_update_ts",
             "merge_key": "new.merge_key"
         }
     )
     .whenMatchedUpdate(condition="new.first_seen < old.first_seen",
                        set=
                        {
                            "first_seen": "new.first_seen",
                            "ltd_string_2": "ifnull(new.ltd_string_2, old.ltd_string_2)",
                            "first_seen_lang": "ifnull(new.first_seen_lang, old.first_seen_lang)",
                            "dw_update_ts": "new.dw_update_ts"
                        }
     )
     .whenMatchedUpdate(condition="new.last_seen > old.last_seen",
                        set=
                        {
                            "last_seen": "new.last_seen",
                            "ltd_string_3": "ifnull(new.ltd_string_3, old.ltd_string_3)",
                            "last_seen_lang": "ifnull(new.last_seen_lang, old.last_seen_lang)",
                            "dw_update_ts": "new.dw_update_ts"
                        }
     )
     .execute()
    )

  

# COMMAND ----------

def process_data():
    checkpoint_location = create_fact_player_summary_ltd(spark,database,view_mapping)
    activity_df = read_player_activity(database,environment)
    (
        activity_df
        .writeStream
        .trigger(availableNow=True)
        .foreachBatch(lambda batch_df, batch_id: load_dimplayer(batch_df, batch_id, database,environment,spark))
        .option("checkpointLocation", checkpoint_location)
        .outputMode("append")
        .start()
    )



# COMMAND ----------

process_data()
