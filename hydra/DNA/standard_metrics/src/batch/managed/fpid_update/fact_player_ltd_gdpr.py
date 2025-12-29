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
    core_df = (
        spark.read.table(f"coretech_prod.sso.accountdeleteevent")
        .where(
            expr(f"date BETWEEN date_sub(current_date(), 3) AND current_date()")
        )
        .selectExpr(f"accountId as account_id", "date")
        .dropDuplicates(["account_id"])
    )
    return core_df

# COMMAND ----------

def transform(core_df, spark):
    dm_df = (
        spark.read.table(f"{database}{environment}.standard_metrics.fact_player_ltd")
        .alias("dm")
    )

    joined_df = dm_df.join(core_df.alias("ad"), expr("dm.player_id = ad.account_id"), "inner")

    # Update fpid_hashed and DW_UPDATE_TS
    updated_df = (
        joined_df
        .withColumn(
            "fpid_hashed",
            expr("CASE WHEN ad.account_id IS NOT NULL AND fpid_hashed IS NOT NULL THEN NULL ELSE fpid_hashed END")
        )
    ).select(
        "player_id",
        "fpid_hashed",
    ).distinct()

    return updated_df

# COMMAND ----------

def load(updated_df, spark):
    delta_table = DeltaTable.forName(spark, f"{database}{environment}.standard_metrics.fact_player_ltd")

    delta_table.alias("old").merge(
        updated_df.alias("new"),
        "old.player_id = new.player_id"
    ).whenMatchedUpdate(
        condition="old.fpid_hashed IS NOT NULL AND new.fpid_hashed IS NULL",
        set={
            "fpid_hashed": "new.fpid_hashed",
            "dw_update_ts": current_timestamp()
        }
    ).execute()

# COMMAND ----------

def run_batch():
    checkpoint_location = dbutils.widgets.get("checkpoint")
    print(checkpoint_location)
    spark = create_spark_session()

    df = extract(spark)
    
    df = transform(df, spark)

    load(df, spark)

# COMMAND ----------

if __name__ == "__main__":
    run_batch()
