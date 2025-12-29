# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../gibraltar/managed/dna_agg_login_nrt

# COMMAND ----------

from pyspark.sql.functions import expr, coalesce, window
from delta.tables import DeltaTable

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'nero'
data_source ='dna'
title = 'Mafia: The Old Country'
app_group_ids = ['c818ad1d80dd4638961b95d154151402', '22ac082d3b8e49219094611b85bda868']
view_mapping = {
    'agg_gp_1': 'agg_gp_1 as login_count'
}

# COMMAND ----------

stream_agg_logins(data_source, database, title, view_mapping, app_group_ids)

# COMMAND ----------

# dbutils.fs.rm("dbfs:/tmp/nero/managed/streaming/run_dev/agg_logins_nrt", True)
# spark.sql("DROP TABLE IF EXISTS nero_dev.managed.agg_logins_nrt")
