# Databricks notebook source
# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../gibraltar/managed/dna_opt_link_agg_installs_nrt

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'nero'
data_source ='dna'
title = 'Mafia: The Old Country'
view_mapping = {
    'agg_gp_1': 'agg_gp_1 as install_count'
}

# COMMAND ----------

# dbutils.fs.rm("dbfs:/tmp/nero/managed/streaming/run_dev/agg_installs_nrt", True)
# spark.sql("DROP TABLE IF EXISTS nero_dev.managed.agg_installs_nrt")

# dbutils.fs.rm("dbfs:/tmp/nero/intermediate/streaming/run_dev/player_install_ledger", True)
# spark.sql("DROP TABLE IF EXISTS nero_dev.intermediate.player_install_ledger")


# COMMAND ----------

stream_agg_installs(data_source, database, title, view_mapping)


