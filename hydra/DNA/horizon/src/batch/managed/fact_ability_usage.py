# Databricks notebook source
import json
from utils.helpers import (
    arg_parser,
    setup_spark,
    setup_logger
)
from pyspark.sql.functions import (
    col, 
    when, 
    coalesce, 
    round, 
    sha2, 
    concat_ws, 
    current_timestamp, 
    max
    )

logger = setup_logger()

# COMMAND ----------

def read_reference_data(environment,spark): 
    hero_lkp_df = (
        spark
        .read
        .table(f"horizon{environment}.reference.ref_hero")
        .alias("lkp")
    )

    return hero_lkp_df

# COMMAND ----------

def transform_fact_ability_usage(environment,spark):

    fact_ability_usage_df = (
        spark
        .readStream
        .table(f"horizon{environment}.raw.ability_usage_event") 
        .alias("ability_usage")
        .withColumn("received_on", col("receivedOn").cast("timestamp"))
        .withColumn("occurred_on", col("occurredon").cast("timestamp"))
        .withColumn("hero_class_name",col("hero.type"))
        .withColumn("player_id", col("player.dna_account_id"))
        .withColumn("group_id", col("group.id"))
        .withColumn("build_environment", col("event_defaults.build_environment"))
        .withColumn("build_changelist", col("event_defaults.cl").cast('int'))
        .withColumn("mm_environment", col("event_defaults.matchmaker_environment"))
        .withColumn("match_id",
                    when(col("event_defaults.match_type").isin("Trials", "GM_Trials"), col("group.id"))
                    .otherwise(col("event_defaults.match_id")))
        .withColumn("gameplay_instance_id", col("gmi.gameplay_instance_id"))
        .withColumn("match_type", 
                    coalesce(when(col("gmi.game_flavor").isin("Trials", "GM_Trials"), 'Trials'),
                            when(col("gmi.game_experience_type") == "ArenaElim", 'Gauntlet')
                            .when(col("gmi.game_experience_type") == "BR", 'BR')
                    ))
        .withColumn("ability_name", coalesce(col("extra_details.ability_name"), 
                                            #col("extra_details.name"), 
                                            #col("extra_details.AbilityGUID"),
                                            col("extra_details.AbilityName")))
        .withColumn("ability_slot", 
                    coalesce(
                        #col("extra_details.slot"),
                                            col("extra_details.ability_slot"), col("extra_details.AbilitySlot")))
        .withColumn("ability_duration", round(coalesce(col("extra_details.ability_duration"), col("extra_details.AbilityDuration")), 2))
        .withColumn("num_targets_hit", coalesce(
                                                #col("extra_details.AbilityNumTargetsHit"), 
                                                col("extra_details.num_targets_hit"), col("extra_details.NumTargetsHit")))
        .withColumn("damage_done", coalesce(col("extra_details.ability_damage_done"), col("extra_details.AbilityDamageDone")))
        .withColumn("dot_damage_done", col("extra_details.damage_over_time_done"))
        #.withColumn("hero_stance", coalesce(col("extra_details.herostance"), col("extra_details.hero_stance")))
        #.withColumn("previous_hero_stance", col("extra_details.ability_previous_stance"))
                    #coalesce(col("extra_details.old_hero_stance"), col("extra_details.ability_previous_stance"))) old_hero_stance not present
        .withColumn("initial_resource", col("extra_details.initial_hero_resource")
                                                )
        .withColumn("final_resource", col("extra_details.final_hero_resource"))
        .withColumn("ability_distance_to_target", round(coalesce(col("extra_details.ability_distance_to_target")
                                                                ), 2))
        .withColumn("ability_end_coords_x", round(col("extra_details.ability_end_coord_x"), 2))
                                                           
        .withColumn("ability_end_coords_y", round(col("extra_details.ability_end_coord_y"),2))
                                                            
        .withColumn("ability_end_coords_z", round(col("extra_details.ability_end_coord_z"),2)) 

        #.withColumn("hero_ability_resource_amount", coalesce(col("extra_details.HeroAbilityResourceAmount"), col("extra_details.heroabilityresourceamount")))
        .withColumn("ability_exit_reason", coalesce(col("extra_details.ability_reason_exit"), col("extra_details.AbilityReasonExit")))
        .withColumn("num_bots_hit", col("extra_details.num_bots_hit"))
        .withColumn("num_destructibles_hit", col("extra_details.num_destructibles_hit"))
        .withColumn("num_players_hit", col("extra_details.num_players_hit"))
        .withColumn("healing_done", col("extra_details.healing_done"))
        .withColumn("num_targets_healed", col("extra_details.num_players_healed"))
        .withColumn("self_heal_amount", col("extra_details.ability_self_heal_amount"))
        .withColumn("ability_damage_amp_amount", col("extra_details.ability_damage_amp_amount"))
        .withColumn("num_hits_while_amped", col("extra_details.num_hit_targets_while_amped"))
        #.withColumn("new_hero_stance", col("extra_details.new_hero_stance")) #new_hero_stance not present
        .withColumn("absorb_amount", col("extra_details.ability_absorb_amount"))
        .withColumn("ability_projectile_type", col("extra_details.ability_projectile_type"))
        .withColumn("ability_stance", col("extra_details.ability_stance"))
        .withColumn("pet_state", col("extra_details.pet_state"))
        .withColumn("targeted_ally", col("extra_details.targeted_ally_GUID"))
        .withColumn("has_stuck", col("extra_details.has_stuck"))
        .withColumn("dw_insert_ts",current_timestamp())
        .withColumn("dw_update_ts",current_timestamp())

    
        .where(col("player_id").isNotNull())
        .where(col("match_id").isNotNull())
        .where(col("group_id").isNotNull())
        .where(col("build_environment") != "development_editor")
        .withColumn("merge_key", sha2(concat_ws("|", col("player_id"), col("match_id"), col("group_id")), 256))

        .select("match_id",
                "gameplay_instance_id",
                "group_id",
                "match_type",
                "player_id",
                "hero_class_name",
                "build_changelist",
                "build_environment",
                "mm_environment",
                "event_trigger",
                "occurred_on",
                "received_on",
                "ability_name",
                "ability_slot",
                "ability_duration",
                "num_targets_hit",
                "num_bots_hit",
                "num_destructibles_hit",
                "num_players_hit",
                "damage_done",
                "dot_damage_done",
                "healing_done",
                "num_targets_healed",
                "self_heal_amount",
                "ability_damage_amp_amount",
                "num_hits_while_amped",
                #"hero_stance",
                #"previous_hero_stance",
                #"new_hero_stance",
                "initial_resource",
                "final_resource",
                "ability_distance_to_target",
                "ability_end_coords_x",
                "ability_end_coords_y",
                "ability_end_coords_z",
                "absorb_amount",
                "ability_projectile_type",
                "ability_stance",
                #"hero_ability_resource_amount",
                "ability_exit_reason",
                "pet_state",
                "targeted_ally",
                "has_stuck",    
                "dw_insert_ts",    
                "dw_update_ts",        
                "merge_key"
    )
    )

    return fact_ability_usage_df



# COMMAND ----------

def write_fact_ability(fact_ability_usage_df,checkpoint_location,environment):

    (
    fact_ability_usage_df
    .writeStream
    .option("mergeSchema", "true")
    .option("checkpointLocation", checkpoint_location)
    .trigger(availableNow=True)
    .format("delta")
    .outputMode("append")
    .toTable(f"horizon{environment}.managed.fact_ability_usage")
    )

    logger.info(f"Finished Write function")


# COMMAND ----------

def process_fact_ability_usage():

    spark = setup_spark("batch")

    environment,checkpoint_location = arg_parser()

    #environment, checkpoint_location = "_dev", "dbfs:/tmp/horizon/managed/run_dev/fact_ability_usage"

    checkpoint_location = checkpoint_location + "/fact_ability_usage"

    fact_ability_usage_df = transform_fact_ability_usage(environment,spark)

    write_fact_ability(fact_ability_usage_df,checkpoint_location,environment)


# COMMAND ----------

if __name__ == "__main__":
    process_fact_ability_usage()
