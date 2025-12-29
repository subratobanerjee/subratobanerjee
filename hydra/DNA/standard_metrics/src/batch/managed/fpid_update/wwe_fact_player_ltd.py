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
    fpid_all = (
        spark
        .read
        .table(f"reference_customer.customer.vw_fpid_all")
        .where(expr(f"src='WWE'"))
    )

    fpid_all = fpid_all.withColumn("fpid_hashed", expr(f"{database}{environment}.cdp_ng.salted_puid(fpid)"))

    target_table = f"{database}{environment}.standard_metrics.fact_player_ltd"
    target_df = spark.table(target_table)

    # Add fpid_hashed if missing
    if 'fpid_hashed' not in target_df.columns:
        print("Adding missing column 'fpid_hashed' to target table")
        target_df = target_df.withColumn("fpid_hashed", expr("NULL").cast("string"))
        target_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)
        print('fpid_hashed column added')
    return target_table, fpid_all

# COMMAND ----------

def load(spark, target_table, fpid_all):
    delta_table = DeltaTable.forName(spark, target_table)

    delta_table.alias("old").merge(
        fpid_all.alias("new"),
        "lower(old.player_id) = lower(new.player_id)"
    ).whenMatchedUpdate(
        condition="old.fpid_hashed IS NULL and old.title ilike 'WWE%' and old.title not in ('WWE 2K Battlegrounds', 'WWE 2K23','WWE 2K24','WWE 2K25')",
        set={
            "fpid_hashed": "new.fpid_hashed",
            "dw_update_ts": current_timestamp()
        }
    ).execute()

def run_batch():
    checkpoint_location = dbutils.widgets.get("checkpoint")
    print(checkpoint_location)
    spark = create_spark_session()
    
    target_table, fpid_all = extract(spark)
    load(spark, target_table, fpid_all)

# COMMAND ----------

if __name__ == "__main__":
    run_batch()