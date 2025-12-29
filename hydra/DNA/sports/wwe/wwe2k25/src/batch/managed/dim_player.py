# Databricks notebook source
# MAGIC %run ../../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../../utils/helpers

# COMMAND ----------

from pyspark.sql import functions as f
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'wwe2k25'
execution_date = f.current_timestamp()
spark = create_spark_session()
interval = 2 # Use this parameter to go back for multiple days, to fix attributes for historical data
view_mapping = { 

}

# COMMAND ----------

def read_player_activity(database,environment):
    activity_df = (
        spark
        .readStream
        .table(f"{database}{environment}.intermediate.fact_player_activity")
        .withWatermark("received_on", '1 day')
        .filter((f.col('player_id').isNotNull()) & (f.lower('platform') != 'unknown') &                              
                  (f.col("dw_insert_ts").between(f.lit(execution_date) - f.expr(f"interval {interval} days"), f.lit(execution_date))))
        .alias("activity")
    )
    return activity_df

# COMMAND ----------

def create_dimplayer(database,environment):

    DeltaTable.createIfNotExists(spark
                            ).tableName(f"{database}{environment}.managed.fact_player_summary_ltd"
                            ).addColumn("player_id", "STRING"
                            ).addColumn("platform", "STRING"
                            ).addColumn("service", "STRING"
                            ).addColumn("first_seen", "TIMESTAMP"
                            ).addColumn("last_seen", "TIMESTAMP"
                            ).addColumn("first_seen_country_code", "STRING"
                            ).addColumn("last_seen_country_code", "STRING"
                            ).addColumn("first_seen_lang", "STRING"
                            ).addColumn("last_seen_lang", "STRING"
                            ).addColumn("dw_inserted_ts", "TIMESTAMP"
                            ).addColumn("dw_updated_ts", "TIMESTAMP"
                            ).execute()
    create_dimplayer_view(spark, database,environment, view_mapping)       
    return f"dbfs:/tmp/{database}/managed/streaming/run{environment}/fact_player_summary_ltd"                  

def create_dimplayer_view(spark, database,environment, view_mapping):
    """
    Create the fact_player_summary_ltd view in the specified environment.

    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
        mapping (dict): A dictionary of column mappings.
    """
    sql = f"""
    CREATE OR REPLACE VIEW {database}{environment}.managed_view.fact_player_summary_ltd AS (
        SELECT
            player_id,
            platform,
            service,
            first_seen,
            last_seen,
            first_seen_country_code,
            last_seen_country_code,
            first_seen_lang,
            last_seen_lang,
            dw_inserted_ts,
            dw_updated_ts
        from {database}{environment}.managed.fact_player_summary_ltd
    )
    """

    spark.sql(sql)

# COMMAND ----------

def load_dimplayer(batch_df, batch_id, database,environment,spark):
  
    # Group by player_id, platform, service, and window
    windowSpec = Window.partitionBy("player_id", "platform", "service").orderBy("received_on")

    window_df = batch_df.withColumns({
                                    "player_id" : f.col("player_id"),
                                    "platform" : f.col("platform"),
                                    "service": f.col("service"),
                                    "first_seen" : f.min("received_on").over(windowSpec),
                                    "last_seen" : f.max("received_on").over(windowSpec),
                                    "first_seen_country_code" : f.first("country_code", ignorenulls=True).over(windowSpec),
                                    "last_seen_country_code" : f.last("country_code", ignorenulls=True).over(windowSpec),
                                    "first_seen_lang" : f.first("language_setting", ignorenulls=True).over(windowSpec),
                                    "last_seen_lang" : f.last("language_setting", ignorenulls=True).over(windowSpec),
                                    "dw_inserted_ts" : f.lit(execution_date),
                                    "dw_updated_ts" : f.lit(execution_date)

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
    "dw_inserted_ts",
    "dw_updated_ts"
    ).distinct()


    player_df = window_df.groupBy(
                                            f.col("player_id"),
                                            f.col("platform"),
                                            f.col("service")
                                    ).agg(
                                            f.max("first_seen").alias("first_seen"),
                                            f.max("last_seen").alias("last_seen"),
                                            f.max("first_seen_country_code").alias("first_seen_country_code"),
                                            f.max("last_seen_country_code").alias("last_seen_country_code"),
                                            f.max("first_seen_lang").alias("first_seen_lang"),
                                            f.max("last_seen_lang").alias("last_seen_lang"),
                                            f.max("dw_inserted_ts").alias("dw_inserted_ts"),
                                            f.max("dw_updated_ts").alias("dw_updated_ts")
                                    ).distinct()
    
    # Load the Delta table
    delta_table = DeltaTable.forName(spark, f"{database}{environment}.managed.fact_player_summary_ltd")

    (delta_table.alias("old")
     .merge(
         player_df.alias("new"),
         "old.player_id = new.player_id and old.platform = new.platform and old.service = new.service"
     )
     .whenNotMatchedInsert(values=
         {
             "player_id": "new.player_id",
             "platform": "new.platform",
             "service": "new.service",
             "first_seen": "new.first_seen",
             "last_seen": "new.last_seen",
             "first_seen_country_code": "new.first_seen_country_code",
             "last_seen_country_code": "new.last_seen_country_code",
             "first_seen_lang": "new.first_seen_lang",
             "last_seen_lang": "new.last_seen_lang",
             "dw_inserted_ts": "new.dw_inserted_ts",
             "dw_updated_ts": "new.dw_updated_ts"
         }
     )
     .whenMatchedUpdate(condition="new.first_seen < old.first_seen",
                        set=
                        {
                            "first_seen": "new.first_seen",
                            "first_seen_country_code": "ifnull(new.first_seen_country_code, old.first_seen_country_code)",
                            "first_seen_lang": "ifnull(new.first_seen_lang, old.first_seen_lang)",
                            "dw_updated_ts": "new.dw_updated_ts"
                        }
     )
     .whenMatchedUpdate(condition="new.last_seen > old.last_seen",
                        set=
                        {
                            "last_seen": "new.last_seen",
                            "last_seen_country_code": "ifnull(new.last_seen_country_code, old.last_seen_country_code)",
                            "last_seen_lang": "ifnull(new.last_seen_lang, old.last_seen_lang)",
                            "dw_updated_ts": "new.dw_updated_ts"
                        }
     )
     .execute()
    )

  

# COMMAND ----------

def process_data():
    checkpoint_location=create_dimplayer(database,environment)
    activity_df = read_player_activity(database,environment)
    (
        activity_df
        .writeStream
        .foreachBatch(lambda batch_df, batch_id: load_dimplayer(batch_df, batch_id, database,environment,spark))
        .option("checkpointLocation", checkpoint_location)
        .outputMode("append")
        .start()
    )



# COMMAND ----------

process_data()
