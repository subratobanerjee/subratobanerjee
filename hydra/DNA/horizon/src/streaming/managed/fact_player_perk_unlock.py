# Databricks notebook source
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

# COMMAND ----------

spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "false")

dbutils.widgets.text(name="environment", defaultValue="_dev")
dbutils.widgets.text(name="checkpoint_location", defaultValue="dbfs:/tmp/horizon/checkpoint")

environment = dbutils.widgets.get("environment")
checkpoint_location = dbutils.widgets.get("checkpoint_location")

# COMMAND ----------

spa_df = (
    spark
    .readStream
    .table(f"horizon{environment}.raw.player_action_event")
    .withColumn("receivedOn", col("receivedOn").cast("timestamp"))
    .withColumn("match_id",
                when(col("event_defaults.match_type") == "Trials", col("group.id"))
                .otherwise(col("event_defaults.match_id")))
    .withWatermark("receivedOn", "10 minutes")
    .groupBy(
        session_window("receivedOn", "3 minutes"),
        col("player.dna_account_id").alias("player_id"),
        col("player.name").alias("player_name"),
        col("group.id").alias("group_id"),
        col("match_id"),
        col("event_defaults.build_environment"),
        col("event_defaults.cl").cast('int').alias("build_changelist"),
        col("event_defaults.match_type"),
        col("event_defaults.matchmaker_environment"))
    .agg(
        count(when(col("event_trigger") == "Kill", True)).alias("kills"),
        count(when(col("event_trigger") == "Death", True)).alias("deaths"),
        count(when(col("event_trigger") == "Assist", True)).alias("assists"),
        count(when(col("event_trigger") == "ReviveFromDowned_Reviver", True)).alias("revives"),
        count(when(col("event_trigger") == "ReviveFromKilled_Rescuer", True)).alias("rescues"),
        count(when(col("event_trigger") == "ChestOpened", True)).alias("chests_opened"),
        count(when((col("event_trigger") == "item_used") & (col("extra_details.item_type") == "Healing"), True)).alias("healing_items_used"),
        count(when((col("event_trigger") == "item_used") & (col("extra_details.item_type") == "Gadget"), True)).alias("gadgets_used"),
        max(when(col("event_trigger") == "InteractablePickup_ArmorUpgrade", col("extra_details.item_level").cast('int')).otherwise(1)).alias("max_armor_tier"),
        max(col("receivedOn")).alias("latest_ts"))
    .where(col("player_id").isNotNull())
    .where(col("group_id").isNotNull())
    .where(col("match_id").isNotNull())
    .where(col("build_environment") != "development_editor")
    .where(col("player_name").ilike("%2kgsv4drone%") == False)
    .withColumn("latency", extract(lit('S'), current_timestamp() - col("latest_ts")))
    .withColumn("merge_key", sha2(concat_ws("|", col("player_id"), col("group_id"), col("match_id")), 256))
    .alias("spa")
)

# COMMAND ----------

mhp_df = (
    spark
    .readStream
    .table(f"horizon{environment}.raw.match_hero_progression")
    .withColumn("receivedOn", col("receivedOn").cast("timestamp"))
    .withColumn("match_id",
                when(col("event_defaults.match_type") == "Trials", col("group.id"))
                .otherwise(col("event_defaults.match_id")))
    .withWatermark("receivedOn", "10 minutes")
    .groupBy(
        session_window("receivedOn", "3 minutes"),
        col("player.dna_account_id").alias("player_id"),
        col("player.name").alias("player_name"),
        col("group.id").alias("group_id"),
        col("match_id"),
        col("event_defaults.build_environment"),
        col("event_defaults.cl").cast('int').alias("build_changelist"),
        col("event_defaults.match_type"),
        col("event_defaults.matchmaker_environment"))
    .agg(
        count(when(col("event_trigger") == "HeroLevelUp", True)).alias("levelups"),
        count(when(col("event_trigger") == "UpgradeSelection", True)).alias("upgrades"),
        max(col("receivedOn")).alias("latest_ts"))
    .where(col("player_id").isNotNull())
    .where(col("group_id").isNotNull())
    .where(col("match_id").isNotNull())
    .where(col("build_environment") != "development_editor")
    .where(col("player_name").ilike("%2kgsv4drone%") == False)
    .withColumn("latency", round(extract(lit('S'), current_timestamp() - col("latest_ts")), 2))
    .withColumn("merge_key", sha2(concat_ws("|", col("player_id"), col("group_id"), col("match_id")), 256))
    .alias("mhp")
)

# COMMAND ----------

pms_df = (
    spa_df
    .withWatermark("latest_ts", "3 minutes")
    .join(mhp_df.withWatermark("latest_ts", "3 minutes"), expr("""spa.merge_key = mhp.merge_key 
                       and mhp.latest_ts BETWEEN spa.latest_ts - INTERVAL 3 minutes and spa.latest_ts"""), "left")
    .select("spa.*", "mhp.levelups", "mhp.upgrades")
)

# COMMAND ----------

def proc_batch(df, batch_id):
    (
        DeltaTable.createIfNotExists(spark)
        .tableName(f"horizon{environment}.managed.fact_player_match_summary")
        .addColumns(df.schema)
        .property('delta.enableIcebergCompatV2', 'true')
        .property('delta.universalFormat.enabledFormats', 'iceberg')
        .execute()
    )
    
    target_table = DeltaTable.forName(spark, f"horizon{environment}.managed.fact_player_match_summary")

    (
        target_table
        .alias("target")
        .merge(
            df.alias("source"),
            "target.merge_key = source.merge_key")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .withSchemaEvolution()
        .execute()
    )

# COMMAND ----------

(
    spa_df
    .writeStream
    .foreachBatch(proc_batch)
    .option("checkpointLocation", checkpoint_location)
    .start()
)
