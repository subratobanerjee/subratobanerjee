# Databricks notebook source
# MAGIC %md
# MAGIC # Table Compaction Job
# MAGIC
# MAGIC This job is run on a schedule to compact (`optimize`) tables.
# MAGIC
# MAGIC 1. Define a list of schemas containing tables to compact
# MAGIC 2. Get list of all tables in those schemas
# MAGIC 3. Run `OPTIMIZE` on all tables

# COMMAND ----------

from pyspark.sql.functions import concat, lit, col
import json

# COMMAND ----------

dbutils.widgets.text(name="environment", defaultValue="_dev")

environment = dbutils.widgets.get("environment")

# load in catalog & schema mapping
with open("config/schema_config.json") as f:
    schema_config = json.load(f)

catalog_list = schema_config["catalog_list"]
schema_list = schema_config["schema_list"]

# COMMAND ----------

df = []

for catalog in catalog_list:
    for i in range(0, len(schema_list[catalog])):
        if not df:
            df = (
                spark
                .sql(f"show tables in {catalog}{environment}.{schema_list[catalog][i]}")
                .withColumn("schema_table", concat(lit(catalog + environment), lit("."), col("database"), lit("."), col("tableName")))
            )
        else:
            df = (
                df
                .union(spark
                       .sql(f"show tables in {catalog}{environment}.{schema_list[catalog][i]}")
                       .withColumn("schema_table", concat(lit(catalog + environment), lit("."), col("database"), lit("."), col("tableName"))))
            )


# COMMAND ----------

display(df)

# COMMAND ----------

tables = df.select("schema_table").collect()

for table in tables:
    spark.sql(f"OPTIMIZE {table[0]}")
