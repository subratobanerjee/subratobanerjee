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
checkpoint_location = dbutils.widgets.get("checkpoint_location") + "/hero_level_up"


# COMMAND ----------

match_start_df = (
    spark
    .readStream
    .table(f"horizon{environment}.raw.match_status_event")
    .withColumn("receivedOn", col("receivedOn").cast("timestamp"))
    .withColumn(
        "ms_match_id", 
        when(
            col("extra_details.type") == "Trials",
            col("group.id")
        ).otherwise(col("event_defaults.match_id"))
    )
    .withWatermark("receivedOn", "5 minutes")
    .where(col("event_trigger") == "PlayerMatchStart")
    .groupBy(
        session_window("receivedOn", "10 minutes"),
        col("ms_match_id"),
        col("extra_details.type").alias("match_type")
    )
    .agg(
        min(col("occurredon").cast("timestamp")).alias("match_start_ts"),
        max(col("receivedOn")).alias("latest_ts")
        )
    .alias("match_start")
)


# COMMAND ----------

# Define the schema for the JSON string
level_up_sources_schema = ArrayType(StructType([
    StructField("PickupCommon", ArrayType(StringType()), True),
    StructField("PickupRare", StringType(), True),
    StructField("PickupLegendary", StringType(), True),
    StructField("Kill", StringType(), True),    
    StructField("BountyKill", StringType(), True),    
    StructField("BountySurvive", StringType(), True),    
    StructField("GauntletRoundBasic", StringType(), True),    
    StructField("GauntletRoundWin", StringType(), True),    
    StructField("GauntletRoundLoss", StringType(), True),    
    StructField("RingClose", StringType(), True),    
    StructField("Debug", StringType(), True)
]))

# COMMAND ----------

spa_df = (
    spark
    .readStream
    #.table(f"horizon{environment}.raw.player_action_event")
    .table(f"horizon{environment}.raw.match_hero_progression")
     .withColumns({"match_id": when((col("gmi.game_flavor").isin("GM_Trials", "Trials")) | (col("extra_details.type") == "Trials"), 
                                    col("group.id")).otherwise(col("event_defaults.match_id")),
                    "receivedOn": col("receivedOn").cast("timestamp"),
                    "date": col("receivedOn").cast("timestamp").cast("date"),
                    "levelup_occurred_on": col("occurredon").cast("timestamp"),
                    "event_trigger": col("event_trigger"),
                    "player_id": col("player.dna_account_id"),
                    "player_name": col("player.name"),
                    "group_id": col("group.id"),
                    "build_changelist": col("event_defaults.cl"),
                    "mm_environment": col("event_defaults.matchmaker_environment"),
                    "gameplay_instance_id": col("gmi.gameplay_instance_id"),
                    "old_level": col("extra_details.HeroLevelPrevious"),
                    "new_level": col("extra_details.HeroLevelCurrent"),
                    "levelup_payload": explode(from_json(col("extra_details.LevelUpSources"), level_up_sources_schema)),
                    "pickup_common_xp": col("levelup_payload.PickupCommon"),
                    "pickup_rare_xp": col("levelup_payload.PickupRare"),
                    "pickup_legendary_xp": col("levelup_payload.PickupLegendary"),
                    "kill_xp": col("levelup_payload.Kill"),
                    "bounty_kill_xp": col("levelup_payload.BountyKill"),
                    "bounty_survive_xp": col("levelup_payload.BountySurvive"),
                    "gauntlet_round_basic_xp": col("levelup_payload.GauntletRoundBasic"),
                    "gauntlet_round_win_xp": col("levelup_payload.GauntletRoundWin"),
                    "gauntlet_round_loss_xp": col("levelup_payload.GauntletRoundLoss"),
                    "ring_close_xp": col("levelup_payload.RingClose"),
                    "debug_xp": col("levelup_payload.Debug"),
                    "hero_type": col("hero.type")                  
                })
  
    .withWatermark("receivedOn", "5 minutes")
    .where(col("player_id").isNotNull())
    .where(col("event_trigger") == "HeroLevelUp")
    .select(
            "player_id",
            "group_id",
            "player_name",
            "build_changelist",
            "mm_environment",
            "date",
            "hero_type",
            "gameplay_instance_id",
            "old_level",
            "new_level",
            "levelup_occurred_on",
            "match_id",
            "pickup_common_xp",
            "pickup_rare_xp",
            "pickup_legendary_xp",
            "kill_xp",
            "bounty_kill_xp",
            "bounty_survive_xp",
            "gauntlet_round_basic_xp",
            "gauntlet_round_win_xp",
            "gauntlet_round_loss_xp",
            "ring_close_xp",
            "debug_xp",
            "receivedOn"
             )     
    .alias("spa")
)

#display(spa_df)

# COMMAND ----------

from pyspark.sql.functions import expr, col, max

hero_lvl_up_df = (
    spa_df.alias("spa")
    .join(
        match_start_df.alias("match_start").withWatermark("latest_ts", "3 minutes"),
        expr("""spa.match_id = match_start.ms_match_id 
                and match_start.latest_ts BETWEEN spa.receivedOn - INTERVAL 3 minutes and spa.receivedOn"""),
        "left"
    )
    .withColumn("time_to_level_up", 
                when(col("spa.old_level") == 0, unix_timestamp(col("match_start.match_start_ts")) - unix_timestamp(col("spa.levelup_occurred_on")))
                .otherwise(0)
                #add window function : LAG(levelup_occurred_on) OVER (PARTITION BY spa.player_dna_account_id, match_id ORDER BY levelup_occurred_on)
    )
    .withColumn("merge_key", sha2(concat_ws("|", col("spa.player_id"), col("spa.group_id"), col("spa.match_id")), 256))
    .select("spa.*",
             col("match_start.match_type").alias("match_type"),
             col("match_start.match_start_ts").alias("match_start_ts"),
             col("time_to_level_up").alias("time_to_level_up")
    )
)



# COMMAND ----------

(
    DeltaTable.createIfNotExists(spark)
    .tableName(f"horizon{environment}.intermediate.hero_level_up")
    .addColumns(hero_lvl_up_df.schema)
    .property('delta.enableIcebergCompatV2', 'true')
    .property('delta.universalFormat.enabledFormats', 'iceberg')
    .execute()
)

# COMMAND ----------

(
    hero_lvl_up_df
    .writeStream
    .option("mergeSchema", "true")
    .option("checkpointLocation", checkpoint_location)
    .format("delta")
    .outputMode("append")
    .toTable(f"horizon{environment}.intermediate.hero_level_up")
)
