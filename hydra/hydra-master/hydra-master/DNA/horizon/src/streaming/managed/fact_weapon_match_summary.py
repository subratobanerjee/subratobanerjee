# Databricks notebook source
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

# COMMAND ----------

spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "false")
spark.conf.set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")
spark.conf.set("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")

dbutils.widgets.text(name="environment", defaultValue="_dev")
environment = dbutils.widgets.get("environment")

dbutils.widgets.text(name="checkpoint_location", defaultValue=f"dbfs:/tmp/horizon/managed/nrt/run{environment}")
checkpoint_location = dbutils.widgets.get("checkpoint_location") + "/fact_weapon_match_summary"

# COMMAND ----------

wep_lkp_df = (
    spark
    .read
    .table(f"horizon{environment}.reference.hero_weapon_ref")
    .alias("lkp")
)

# COMMAND ----------

fwms_df = (
    spark
    .readStream
    .table(f"horizon{environment}.raw.weapon_usage_event")
    .alias("wu")
    .withColumn("receivedOn", col("receivedOn").cast("timestamp"))
    .withColumn("match_id",
                when(col("event_defaults.match_type") == "Trials", col("group.id"))
                .otherwise(col("event_defaults.match_id")))
    .withColumn("weapon", 
                when(col("event_trigger") == "weapon_swap", col("inventory.current_weapon"))
                .otherwise(col("inventory.current_weapon")))
    .join(wep_lkp_df, expr("weapon = lkp.weapon_name"), "left")
    .withWatermark("receivedOn", "5 minutes")
    .groupBy(
        session_window("receivedOn", "10 minutes"),
        col("player.dna_account_id").alias("player_id"),
        col("player.name").alias("player_name"),
        col("group.id").alias("group_id"),
        col("match_id"),
        col("hero.type").alias("hero_class_name"),
        col("weapon"),
        col("event_defaults.build_environment"),
        col("event_defaults.cl").cast('int').alias("build_changelist"),
        col("event_defaults.match_type"),
        col("event_defaults.matchmaker_environment"))
    .agg(
        max(ifnull(col("extra_details.new_weapon_tier").cast("int"), lit(1))).alias("max_weapon_tier"),
        sum(when(col("lkp.ammo_type") == "Pellet", ifnull(col("weaponaggregates.pelletstotaldamage").cast("int"), lit(0)))
            .when(col("lkp.ammo_type") == "Projectile", ifnull(col("weaponaggregates.projectilestotaldamage").cast("int"), lit(0)))
            .when(col("lkp.ammo_type") == "Hitscan", ifnull(col("weaponaggregates.hitscantotaldamage").cast("int"), lit(0)))
            .otherwise(lit(0))).alias("total_direct_damage"),
        sum(when(col("lkp.ammo_type") == "Projectile", ifnull(col("weaponaggregates.totaldot").cast("int"), lit(0)))
            .otherwise(lit(0))).alias("total_dot_damage"),
        sum(when(col("lkp.ammo_type") == "Projectile", ifnull(col("weaponaggregates.projectiledamagefalloff").cast("int"), lit(0)))
            .when(col("lkp.ammo_type") == "Hitscan", ifnull(col("weaponaggregates.hitscandamagefalloff").cast("int"), lit(0)))
            .otherwise(lit(0))).alias("total_falloff_damage"),
        (col("total_direct_damage") + col("total_dot_damage")).alias("total_damage"),
        sum(col("weaponaggregates.totalhealingprovided").cast("int")).alias("total_healing"),
        sum(when(col("lkp.ammo_type") == "Pellet", ifnull(col("weaponaggregates.numpelletsfired").cast("int"), lit(0)))
            .when(col("lkp.ammo_type") == "Projectile", ifnull(col("weaponaggregates.numprojectilesfired").cast("int"), lit(0)))
            .when(col("lkp.ammo_type") == "Hitscan", ifnull(col("weaponaggregates.numhitscanfired").cast("int"), lit(0)))
            .otherwise(lit(0))).alias("num_fired"),
        sum(when(col("lkp.ammo_type") == "Pellet", ifnull(col("weaponaggregates.numpellethitplayers").cast("int"), lit(0)))
            .when(col("lkp.ammo_type") == "Projectile", ifnull(col("weaponaggregates.numprojectilehitplayers").cast("int"), lit(0)))
            .when(col("lkp.ammo_type") == "Hitscan", ifnull(col("weaponaggregates.numhitscanhitplayers").cast("int"), lit(0)))
            .otherwise(lit(0))).alias("num_hit_players"),
        sum(when(col("lkp.ammo_type") == "Pellet", ifnull(col("weaponaggregates.numpelletcrithitplayers").cast("int"), lit(0)))
            .when(col("lkp.ammo_type") == "Projectile", ifnull(col("weaponaggregates.numprojectilecrithitplayers").cast("int"), lit(0)))
            .when(col("lkp.ammo_type") == "Hitscan", ifnull(col("weaponaggregates.numhitscancrithitplayers").cast("int"), lit(0)))
            .otherwise(lit(0))).alias("num_crit_hit_players"),
        sum(when(col("lkp.ammo_type") == "Pellet", ifnull(col("weaponaggregates.numpellethitbots").cast("int"), lit(0)))
            .when(col("lkp.ammo_type") == "Projectile", ifnull(col("weaponaggregates.numprojectilehitbots").cast("int"), lit(0)))
            .when(col("lkp.ammo_type") == "Hitscan", ifnull(col("weaponaggregates.numhitscanhitbots").cast("int"), lit(0)))
            .otherwise(lit(0))).alias("num_hit_bots"),
        sum(when(col("lkp.ammo_type") == "Pellet", ifnull(col("weaponaggregates.numpelletcrithitbots").cast("int"), lit(0)))
            .when(col("lkp.ammo_type") == "Projectile", ifnull(col("weaponaggregates.numprojectilecrithitbots").cast("int"), lit(0)))
            .when(col("lkp.ammo_type") == "Hitscan", ifnull(col("weaponaggregates.numhitscancrithitbots").cast("int"), lit(0)))
            .otherwise(lit(0))).alias("num_crit_hit_bots"),
        sum(when(col("lkp.ammo_type") == "Pellet", ifnull(col("weaponaggregates.numpellethitdamageables").cast("int"), lit(0)))
            .when(col("lkp.ammo_type") == "Projectile", ifnull(col("weaponaggregates.numprojectilehitdamageables").cast("int"), lit(0)))
            .when(col("lkp.ammo_type") == "Hitscan", ifnull(col("weaponaggregates.numhitscanhitdamageables").cast("int"), lit(0)))
            .otherwise(lit(0))).alias("num_hit_damageables"),
        sum(when(col("lkp.ammo_type") == "Pellet", ifnull(col("weaponaggregates.numpelletcrithitdamageables").cast("int"), lit(0)))
            .when(col("lkp.ammo_type") == "Projectile", ifnull(col("weaponaggregates.numprojectilecrithitdamageables").cast("int"), lit(0)))
            .when(col("lkp.ammo_type") == "Hitscan", ifnull(col("weaponaggregates.numhitscancrithitdamageables").cast("int"), lit(0)))
            .otherwise(lit(0))).alias("num_crit_hit_damageables"),
        (col("num_hit_players") + col("num_hit_bots") + col("num_hit_damageables")).alias("total_num_hits"),
        (col("num_crit_hit_players") + col("num_crit_hit_bots") + col("num_crit_hit_damageables")).alias("total_num_crit_hits"),
        round(col("total_num_hits") / when(col("num_fired") == 0, lit(1)).otherwise(col("num_fired")), 2)
            .cast("float").alias("pct_weapon_accuracy"),
        round(col("total_num_crit_hits") / when(col("total_num_hits") == 0, lit(1)).otherwise(col("total_num_hits")) * 100, 2)
            .cast("float").alias("pct_crit_hits"),
        ifnull(max(when(col("lkp.weapon_type") == "Primary", get_json_object("inventory.weapon_0", "$.current_ammo").cast("int") + get_json_object("inventory.itemslot_1", "$.count").cast("int"))
            .when(col("lkp.weapon_type") == "Secondary", get_json_object("inventory.weapon_1", "$.current_ammo").cast("int") + get_json_object("inventory.itemslot_0", "$.count").cast("int"))), lit(0)).alias("max_held_ammo"),
        ifnull(min(when(col("lkp.weapon_type") == "Primary", get_json_object("inventory.weapon_0", "$.current_ammo").cast("int") + get_json_object("inventory.itemslot_1", "$.count").cast("int"))
            .when(col("lkp.weapon_type") == "Secondary", get_json_object("inventory.weapon_1", "$.current_ammo").cast("int") + get_json_object("inventory.itemslot_0", "$.count").cast("int"))), lit(0)).alias("min_held_ammo"),
        ifnull(sum(when((col("event_trigger") == "PickUpAmmo") & (col("extra_details.ammotype") == "Primary"), col("extra_details.ammoamount").cast("int"))), lit(0)).alias("total_primary_ammo_picked_up"),
        ifnull(sum(when((col("event_trigger") == "PickUpAmmo") & (col("extra_details.ammotype") == "Secondary"), col("extra_details.ammoamount").cast("int"))), lit(0)).alias("total_secondary_ammo_picked_up"),
        count(when(col("event_trigger") == "weapon_reload", True)).cast("int").alias("num_reloads"),
        max(col("receivedon")).alias("latest_event_ts"),
        current_timestamp().alias("dw_insert_ts"),
        current_timestamp().alias("dw_update_ts"))
    .where(col("player_id").isNotNull())
    .where(col("group_id").isNotNull())
    .where(col("match_id").isNotNull())
    .where(col("build_environment") != "development_editor")
    .where(col("player_name").ilike("%2kgsv4drone%") == False)
    .where(col("weapon").isNotNull())
    .withColumn("merge_key", sha2(concat_ws("|", col("player_id"), col("group_id"), col("match_id"), col("weapon")), 256))
)

# COMMAND ----------

display(fwms_df)

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
        .tableName(f"horizon{environment}.managed.fact_weapon_match_summary")
        .addColumns(df.schema)
        .property('delta.enableIcebergCompatV2', 'true')
        .property('delta.universalFormat.enabledFormats', 'iceberg')
        .execute()
    )
    
    target_table = DeltaTable.forName(spark, f"horizon{environment}.managed.fact_weapon_match_summary")

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
