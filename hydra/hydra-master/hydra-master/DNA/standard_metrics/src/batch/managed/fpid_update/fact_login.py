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
    login_df = (
        spark
        .read
        .table(f"{database}{environment}.standard_metrics.fact_player_ltd")
        .where(expr(f"dw_update_ts::date = current_date() and fpid_hashed is not null"))
    )

    return login_df

# COMMAND ----------

def transform(login_df, spark):
    target_table = f"{database}{environment}.standard_metrics.fact_login"

    target_df = spark.table(target_table)

  # Add fpid_hashed if missing
    if 'fpid_hashed' not in target_df.columns:
        print("Adding missing column 'fpid_hashed' to target table")
        target_df = target_df.withColumn("fpid_hashed", expr("NULL").cast("string"))
        target_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)
        print('fpid_hashed column added')

    login_df_filtered = login_df.where(expr("fpid_hashed IS NOT NULL")).alias("new").selectExpr("player_id", "title", "platform", "service", "fpid_hashed").distinct()

    return target_table, login_df_filtered

# COMMAND ----------

def load(spark, target_table, login_df_filtered):
    delta_table = DeltaTable.forName(spark, target_table)
    delta_table.alias("old").merge(
        login_df_filtered.alias("new"),
        "old.player_id = new.player_id and old.title = new.title and old.platform = new.platform and old.service = new.service"
    ).whenMatchedUpdate(
        condition="old.fpid_hashed IS NULL",
        set={"fpid_hashed": "new.fpid_hashed"}
    ).execute()

# COMMAND ----------

def run_batch():
    checkpoint_location = dbutils.widgets.get("checkpoint")
    print(checkpoint_location)
    spark = create_spark_session()

    df = extract(spark)

    target_table, df = transform(df, spark)

    load(spark, target_table, df)

# COMMAND ----------

if __name__ == "__main__":
    run_batch()