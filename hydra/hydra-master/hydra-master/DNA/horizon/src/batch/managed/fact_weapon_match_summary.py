# Databricks notebook source
from utils.helpers import (
    arg_parser,
    setup_spark
)

import json
from pyspark.sql.functions import (
    col, 
    when, 
    round, 
    sha2, 
    concat_ws, 
    current_timestamp, 
    min,
    max,
    count,
    sum,
    ifnull,
    lit,
    get_json_object,
    expr,
    ilike
    )
from delta.tables import DeltaTable


# COMMAND ----------

def read_reference_hero_weapon(spark,environment):

    wep_lkp_df = (
        spark
        .read
        .table(f"horizon{environment}.reference.hero_weapon_ref")
        .alias("lkp")
    )

    return wep_lkp_df

# COMMAND ----------

def readstream_raw_weapon_usage(spark,environment):
    fwms_df = (
        spark
        .readStream
        .table(f"horizon{environment}.raw.weapon_usage_event")
    )

    return fwms_df

# COMMAND ----------

def proc_batch(df,ref_df, batch_id, environment,spark):
    df = (
        df
        .withColumns({
            "receivedOn": col("receivedOn").cast("timestamp"),
            "match_id": when(col("event_defaults.match_type") == "Trials", col("group.id")).otherwise(col("event_defaults.match_id")),
            "weapon": col("inventory.current_weapon")
        })
        .join(ref_df, expr("weapon = lkp.weapon_name"), "left")
        .groupBy(
            col("player.dna_account_id").alias("player_id"),
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
            ifnull(sum(col("weaponaggregates.totalhealingprovided").cast("int")), lit(0)).alias("total_healing"),
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
                # .when(col("lkp.ammo_type") == "Projectile", ifnull(col("weaponaggregates.numprojectilecrithitbots").cast("int"), lit(0)))
                .when(col("lkp.ammo_type") == "Hitscan", ifnull(col("weaponaggregates.numhitscancrithitbots").cast("int"), lit(0)))
                .otherwise(lit(0))).alias("num_crit_hit_bots"),
            sum(when(col("lkp.ammo_type") == "Pellet", ifnull(col("weaponaggregates.numpellethitdamageables").cast("int"), lit(0)))
                .when(col("lkp.ammo_type") == "Projectile", ifnull(col("weaponaggregates.numprojectilehitdamageables").cast("int"), lit(0)))
                .when(col("lkp.ammo_type") == "Hitscan", ifnull(col("weaponaggregates.numhitscanhitdamageables").cast("int"), lit(0)))
                .otherwise(lit(0))).alias("num_hit_damageables"),
            # sum(
            #     when(col("lkp.ammo_type") == "Pellet", ifnull(col("weaponaggregates.numpelletcrithitdamageables").cast("int"), lit(0)))
            #     when(col("lkp.ammo_type") == "Projectile", ifnull(col("weaponaggregates.numprojectilecrithitdamageables").cast("int"), lit(0)))
            #     when(col("lkp.ammo_type") == "Hitscan", ifnull(col("weaponaggregates.numhitscancrithitdamageables").cast("int"), lit(0)))
            #     .otherwise(lit(0))).alias("num_crit_hit_damageables"),
            (col("num_hit_players") + col("num_hit_bots") + col("num_hit_damageables")).alias("total_num_hits"),
            (col("num_crit_hit_players") + col("num_crit_hit_bots")).alias("total_num_crit_hits"),
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
            max(col("receivedOn")).alias("latest_event_ts"),
            current_timestamp().alias("dw_insert_ts"),
            current_timestamp().alias("dw_update_ts"))
        .where(col("player_id").isNotNull())
        .where(col("group_id").isNotNull())
        .where(col("match_id").isNotNull())
        .where(col("build_environment") != "development_editor")
        .where(col("weapon").isNotNull())
        .withColumn("merge_key", sha2(concat_ws("|", col("player_id"), col("group_id"), col("match_id"), col("weapon")), 256))
    )

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
        .withSchemaEvolution()
        .whenMatchedUpdate(set = 
                           {
                                "target.max_weapon_tier": "greatest(source.max_weapon_tier, target.max_weapon_tier)",
                                "target.total_direct_damage": "source.total_direct_damage + target.total_direct_damage",
                                "target.total_dot_damage": "source.total_dot_damage + target.total_dot_damage",
                                "target.total_falloff_damage": "source.total_falloff_damage + target.total_falloff_damage",
                                "target.total_damage": "source.total_damage + target.total_damage",
                                "target.total_healing": "source.total_healing + target.total_healing",
                                "target.num_fired": "source.num_fired + target.num_fired",
                                "target.num_hit_players": "source.num_hit_players + target.num_hit_players",
                                "target.num_crit_hit_players": "source.num_crit_hit_players + target.num_crit_hit_players",
                                "target.num_hit_bots": "source.num_hit_bots + target.num_hit_bots",
                                "target.num_crit_hit_bots": "source.num_crit_hit_bots + target.num_crit_hit_bots",
                                "target.num_hit_damageables": "source.num_hit_damageables + target.num_hit_damageables",
                                # "target.num_crit_hit_damageables": "source.num_crit_hit_damageables + target.num_crit_hit_damageables",
                                "target.total_num_hits": "source.total_num_hits + target.total_num_hits",
                                "target.total_num_crit_hits": "source.total_num_crit_hits + target.total_num_crit_hits",
                                "target.pct_weapon_accuracy": "round((source.total_num_hits + target.total_num_hits) / (source.num_fired + target.num_fired) * 100, 2)",
                                "target.pct_crit_hits": "round((source.total_num_crit_hits + target.total_num_crit_hits) / (source.total_num_hits + target.total_num_hits) * 100, 2)",
                                "target.max_held_ammo": "greatest(source.max_held_ammo, target.max_held_ammo)",                                
                                "target.min_held_ammo": "least(source.min_held_ammo, target.min_held_ammo)",
                                "target.total_primary_ammo_picked_up": "source.total_primary_ammo_picked_up + target.total_primary_ammo_picked_up",
                                "target.total_secondary_ammo_picked_up": "source.total_secondary_ammo_picked_up + target.total_secondary_ammo_picked_up",
                                "target.num_reloads": "source.num_reloads + target.num_reloads",
                                "target.dw_update_ts": "current_timestamp()"
                           })
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

def write_fact_weapon_match(fwms_df,ref_df,checkpoint_location,environment,spark):

    (
        fwms_df
        .writeStream
        .outputMode("update")
        .foreachBatch(lambda df, batch_id: proc_batch(df, ref_df,batch_id, environment,spark))
        .option("checkpointLocation", checkpoint_location)
        .trigger(availableNow=True)
        .start()
    )

# COMMAND ----------

def process_fact_weapon_match_summary():

    spark = setup_spark("batch")

    environment,checkpoint_location = arg_parser()
    # environment, checkpoint_location = "_dev", "dbfs:/tmp/horizon/managed/batch/run_dev/fact_weapon_match_summary"

    checkpoint_location = checkpoint_location + "/fact_weapon_match_summary"

    fct_match_lvl_ability_df = readstream_raw_weapon_usage(spark,environment)

    ref_df = read_reference_hero_weapon(spark,environment)

    write_fact_weapon_match(fct_match_lvl_ability_df,ref_df,checkpoint_location,environment,spark)

# COMMAND ----------

if __name__ == "__main__":
    process_fact_weapon_match_summary()
