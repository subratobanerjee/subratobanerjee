# Databricks notebook source
# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../gibraltar/managed/gbx_agg_installs_nrt

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'oak2'
data_source ='gearbox'
title = 'Borderlands 4'
view_mapping = {
    'agg_gp_1': 'agg_gp_1 as install_count'
}

# COMMAND ----------

stream_agg_installs(data_source, database, title, view_mapping)

# COMMAND ----------

# dbutils.fs.rm("dbfs:/tmp/oak2/managed/streaming/run_dev/agg_installs_nrt", True)
# spark.sql("DROP TABLE IF EXISTS oak2_dev.managed.agg_installs_nrt")
