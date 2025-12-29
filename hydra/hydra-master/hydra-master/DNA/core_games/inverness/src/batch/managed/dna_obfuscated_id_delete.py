# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

from pyspark.sql.functions import expr
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException
import time

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
spark = create_spark_session()

# COMMAND ----------

def read_unlinkevent(environment):
    return (
        spark
        .read
        .table(f"coretech{environment}.sso.unlinkevent").select("accountId").distinct()
    )

# COMMAND ----------

def extract(environment):
    unlink_df = read_unlinkevent(environment)

    return unlink_df

# COMMAND ----------

def delete(unlink_df, environment):

    max_retries = 5
    retries = 0
    success = False

    src_df = DeltaTable.forName(spark, f"reference{environment}.sso_mapping.dna_obfuscated_id_mapping")
    
    while not success and retries < max_retries:
        try:
            src_df.alias("t").merge(
                unlink_df.alias("u"),
                "t.unobfuscated_platform_id = u.accountId"
            ).whenMatchedDelete().execute()

            success = True
        except AnalysisException as e:
            if "ConcurrentAppendException" in str(e):
                print("ConcurrentAppendException occurred. Retrying...")
                retries += 1
                time.sleep(5)  # Wait for 5 seconds before retrying
            else:
                raise e

    if not success:
        print("Operation failed after maximum retries.")


def run_batch():

    df = extract(environment)
    delete(df,environment)


# COMMAND ----------

run_batch()