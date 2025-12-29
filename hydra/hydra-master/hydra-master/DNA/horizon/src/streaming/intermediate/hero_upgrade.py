# Databricks notebook source
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

from pyspark.sql.functions import col, from_json, when, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# COMMAND ----------

spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "false")

dbutils.widgets.text(name="environment", defaultValue="_dev")
environment = dbutils.widgets.get("environment")

dbutils.widgets.text(name="checkpoint_location", defaultValue=f"dbfs:/tmp/horizon/intermediate/nrt/run{environment}")
checkpoint_location = dbutils.widgets.get("checkpoint_location") + "/hero_upgrade"


# COMMAND ----------

# Define the schema for the JSON string
upgrade_payload_schema = StructType([
    StructField("UpgradeChoices", ArrayType(StringType()), True),
    StructField("Selected Upgrade", StringType(), True)
])

# COMMAND ----------

player_action_df = (
    spark
    .readStream
    .table(f"horizon{environment}.raw.match_hero_progression")
    .withColumn("receivedOn", col("receivedOn").cast("timestamp"))
    .withColumn("upgrade_occurred_on", col("occurredon").cast("timestamp"))
    .withColumn("player_id", col("player.dna_account_id"))
    .withColumn("player_name", col("player.name"))

    .withColumn(
        "match_id", 
        when(
            (col("gmi.game_flavor").isin("GM_Trials", "Trials")) | 
            (col("extra_details.type") == "Trials"), 
            col("group.id")
        ).otherwise(col("event_defaults.match_id"))
    )
    .withColumn("upgrade_payload", from_json(col("extra_details.UpgradePayload"), upgrade_payload_schema))
    .withColumn("upgrades_offered", col("upgrade_payload.UpgradeChoices"))
    .withColumn("upgrade_chosen", col("upgrade_payload.Selected Upgrade"))
    .withColumn("gameplay_instance_id", col("gmi.gameplay_instance_id"))
    #.withColumn("upg_rank", row_number().over(Window.partitionBy("player_id").orderBy(col("occurredon"))))
    .where(col("player_id").isNotNull())
    .where(col("event_trigger") == "UpgradeSelection")

    .select(col("player_id").alias("player_id"),
             col("player_name").alias("player_name"),
             col("match_id").alias("match_id"),
             col("upgrades_offered").alias("upgrades_offered"),  
             col("upgrade_chosen").alias("upgrade_chosen"),
             col("gameplay_instance_id").alias("gameplay_instance_id"),
             col("upgrade_occurred_on").alias("upgrade_occurred_on"),
             col("receivedOn").alias("receivedOn")
             
             )
)



# COMMAND ----------

(
    DeltaTable.createIfNotExists(spark)
    .tableName(f"horizon{environment}.intermediate.hero_upgrade")
    .addColumns(player_action_df.schema)
    .property('delta.enableIcebergCompatV2', 'true')
    .property('delta.universalFormat.enabledFormats', 'iceberg')
    .execute()
)

# COMMAND ----------

(
player_action_df
.writeStream
.option("mergeSchema", "true")
.option("checkpointLocation", checkpoint_location)
.format("delta")
.outputMode("append")
.toTable(f"horizon{environment}.intermediate.hero_upgrade")
)
