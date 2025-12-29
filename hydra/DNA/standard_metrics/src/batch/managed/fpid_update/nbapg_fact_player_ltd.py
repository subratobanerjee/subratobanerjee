# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment', set_environment())
database = 'dataanalytics'


# COMMAND ----------

def extract(spark):
    target_table = f"{database}{environment}.standard_metrics.fact_player_ltd"
    target_df = spark.table(target_table)

    # Add fpid_hashed if missing
    if 'fpid_hashed' not in target_df.columns:
        print("Adding missing column 'fpid_hashed' to target table")
        target_df = target_df.withColumn("fpid_hashed", expr("NULL").cast("string"))
        target_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)
        print('fpid_hashed column added')
    return target_table, target_df

# COMMAND ----------

def transform(spark, target_df):
    updated_df = (
        target_df
        .where(expr("fpid_hashed IS NULL AND title = 'NBA 2K Playgrounds 2'"))
        .withColumn("fpid_hashed", expr(f"{database}{environment}.cdp_ng.salted_puid(split_part(player_id,'-',-1))"))
        .withColumn("dw_update_ts", current_timestamp())
    )
    return updated_df

# COMMAND ----------

def load(spark, target_table, updated_df):
    delta_table = DeltaTable.forName(spark, target_table)

    delta_table.alias("old").merge(
        updated_df.alias("new"),
        "old.player_id = new.player_id AND old.title = new.title"
    ).whenMatchedUpdate(
        condition="old.fpid_hashed IS NULL AND old.title = 'NBA 2K Playgrounds 2'",
        set={
            "fpid_hashed": "new.fpid_hashed",
            "dw_update_ts": "new.dw_update_ts"
        }
    ).execute()

def run_batch():
    checkpoint_location = dbutils.widgets.get("checkpoint")
    print(checkpoint_location)
    spark = create_spark_session()
    
    target_table, target_df = extract(spark)
    df = transform(spark, target_df)
    load(spark, target_table, df)

# COMMAND ----------

if __name__ == "__main__":
    run_batch()