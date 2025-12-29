# Databricks notebook source
import json
from delta.tables import DeltaTable
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window


# COMMAND ----------

dbutils.widgets.text(name="environment", defaultValue="_dev")


environment = dbutils.widgets.get("environment")



(
    DeltaTable.createIfNotExists(spark)
    .tableName(f"dataanalytics{environment}.standard_metrics.platform_territory_10min_ts")
    .addColumn("timestamp_10min_slice", "timestamp")
    .addColumn("platform", "string")
    .addColumn("service", "string")
    .addColumn("territory_name", "string")
    .property('delta.enableIcebergCompatV2', 'true')
    .property('delta.universalFormat.enabledFormats', 'iceberg')
    .execute()
)






# COMMAND ----------

min_timestamp = '2024-08-20 00:00:00'
max_timestamp = '2025-08-25 00:00:00'


#10 minute time slice data
df1 = spark.sql(f"SELECT explode(sequence(timestamp('{min_timestamp}'), timestamp('{max_timestamp}'), interval 10 minutes))")
df1_10min_timeslice = df1.withColumnRenamed('col', 'timestamp_10min_slice').alias("timeslice")








# COMMAND ----------

title_df= (
            spark.
            read.
            table(f"reference{environment}.title.dim_title")
            .alias("title")
            #.where(col("title").isin("Horizon"))
            )



country_df = (
            spark.
            read.
            table(f"reference{environment}.location.dim_country")
            .alias("country")
            )



platform_territory_df= df1_10min_timeslice.crossJoin(title_df).crossJoin(country_df).select("timeslice.timestamp_10min_slice", col("title.display_platform").alias("platform"),col("title.display_service").alias("service"),col("country.territory_name").alias("territory_name")).distinct().orderBy("timeslice.timestamp_10min_slice")





# load to table
(platform_territory_df.write
 .mode("overwrite")
 .format("delta")
 .saveAsTable(f"dataanalytics{environment}.standard_metrics.platform_territory_10min_ts")
)
