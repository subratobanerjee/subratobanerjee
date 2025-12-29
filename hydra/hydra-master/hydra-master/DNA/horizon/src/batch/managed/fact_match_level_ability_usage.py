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
    avg
    )
from delta.tables import DeltaTable

# COMMAND ----------

def readstream_match_lvl_ability(spark,environment):

    fct_match_lvl_ability_df = (
        spark
        .readStream
        .table(f"horizon{environment}.managed.fact_ability_usage")
    )

    return fct_match_lvl_ability_df

# COMMAND ----------

def proc_batch(df, batch_id, checkpoint_path,environment,spark):
    df = (
        df
        .alias("match_level_ability")
        .groupBy(
            col("match_id"),
            col("match_type"),
            col("player_id"),
            col("group_id"),
            col("hero_class_name"),
            col("build_changelist"),
            col("build_environment"),
            col("mm_environment"),
            col("ability_name"),
            col("ability_slot")
        )
        .agg(
            min(col("received_on").cast("date")).alias("date"),
            count(col("event_trigger")).alias("num_ability_casts"),
            min(col("ability_duration")).alias("min_ability_duration"),
            max(col("ability_duration")).alias("max_ability_duration"),
            round(avg(col("ability_duration")), 2).alias("avg_ability_duration"),
            round(avg(col("num_targets_hit")), 2).alias("avg_num_targets_hit"),
            max(col("num_targets_hit")).alias("max_num_targets_hit"),
            sum(col("damage_done")).alias("total_damage_done"),
            sum(col("dot_damage_done")).alias("total_dot_damage_done"),
            sum(col("ability_damage_amp_amount")).alias("total_amped_damage"),
            round(avg(col("num_hits_while_amped")), 2).alias("avg_num_targets_hit_amped"),
            sum(col("healing_done")).alias("total_healing_done"),
            sum(col("self_heal_amount")).alias("total_self_healing_done"),
            round(avg(col("num_targets_healed")), 2).alias("avg_num_targets_healed"),
            max(col("num_targets_healed")).alias("max_num_targets_healed"),
            round(avg(col("initial_resource")-col("final_resource")), 2).alias("avg_resource_used"),
            round(avg(col("initial_resource")), 2).alias("avg_starting_resource"),
            round(avg(col("final_resource")), 2).alias("avg_ending_resource"),
            min(col("initial_resource")).alias("min_initial_resource"),
            max(col("final_resource")).alias("max_final_resouce"),
            round(avg(col("ability_distance_to_target")), 2).alias("avg_distance_traveled"),
            round(avg(col("absorb_amount")), 2).alias("avg_absorb_amount"),
            count(when(col("has_stuck") == True, 1).otherwise(0)).alias("num_successful_sticks"),
            sum(col("ability_damage_amp_amount")).alias("total_boosted_damage"),
            sum(col("num_hits_while_amped")).alias("total_boosted_hits"),
            round(sum(when((col("ability_name") == 'Headbutt Charge') & (col("damage_done") > 0), 1))/col("num_ability_casts")*100, 1).alias("pct_successful_hit"),
            round((col("num_successful_sticks") / when(col("num_ability_casts") == 0, 1).otherwise(col("num_ability_casts")))*100, 2).alias("pct_successful_sticks"),
            sum(when(col("ability_exit_reason").isNotNull(), 1)).alias("num_ally_casts"),
            count(when(col("ability_exit_reason").isin('Expired', 'expired', 'State.Ability.TimeOut'), 1)).alias("num_times_expired"),
            count(when(col("ability_exit_reason").isin('Destroyed', 'destroyed'), 1)).alias("num_times_destroyed"),
            count(when(col("ability_exit_reason").isin('Recalled'), 1)).alias("num_times_recalled"),
            count(when(col("ability_exit_reason").isin('Repair Cancelled', 'Killed', 'Cancel due to Pet Attaching', 'Cancelled by Pet Attaching', 'Cacell due to Pet Attaching', 'Ability Cancelled'), 1)).alias("num_times_cancelled"),
            count(when(col("ability_exit_reason").isin('Success'), 1)).alias("num_times_successful"),
            count(when(col("ability_exit_reason").isin('State.Weapon.Firing'), 1)).alias("num_times_fired"),
            count(when(col("ability_exit_reason").isin('State.Interacting.Hold'), 1)).alias("num_times_interact"),
            count(when(col("ability_stance").isin('homing'), 1)).alias("num_homing_projectiles"),
            count(when(col("ability_stance").isin('ground'), 1)).alias("num_ground_projectiles"),
            count(when(col("ability_projectile_type").isin('self_heal'), 1)).alias("num_self_heals"),
            count(when(col("ability_projectile_type").isin('target_heal'), 1)).alias("num_targeted_heals"),
            count(when(col("pet_state").isin('Attached'), 1)).alias("num_times_cast_attached"),
            count(when(col("pet_state").isin('Deployed'), 1)).alias("num_times_cast_deployed"),
            count(when(col("pet_state").isin('Chest'), 1)).alias("num_times_opened_chest"),
            count(when(col("pet_state").isin('Attack'), 1)).alias("num_times_attacked"),
            count(when(col("pet_state").isin('Guard'), 1)).alias("num_times_guarded"),
            count(when(col("pet_state").isin('Returning'), 1)).alias("num_times_returned"),
            count(when(col("pet_state").isin('Revive'), 1)).alias("num_times_revive"),
            count(when(col("pet_state").isin('Disabled'), 1)).alias("num_times_elim"),
            #coalesce(sum(when(col("pet_state") == 'Idle', col("ability_duration"))),0).alias("time_spent_idle"),
            max(col("received_on")).alias("latest_event_ts"),
            current_timestamp().alias("dw_insert_ts"),
            current_timestamp().alias("dw_update_ts")
        )
        .where(col("player_id").isNotNull())
        .where(col("match_type").isNotNull())
        .withColumn("merge_key", sha2(concat_ws("|", col("player_id"), col("group_id"), col("match_id"), col("ability_name"), col("hero_class_name"), col("match_type"),col("mm_environment")), 256))
    
    )
    (
        DeltaTable.createIfNotExists(spark)
        .tableName(f"horizon{environment}.managed.fact_match_level_ability_usage")
        .addColumns(df.schema)
        .property('delta.enableIcebergCompatV2', 'true')
        .property('delta.universalFormat.enabledFormats', 'iceberg')
        .execute()
    )
    
    target_table = DeltaTable.forName(spark, f"horizon{environment}.managed.fact_match_level_ability_usage")
    (
        target_table
        .alias("target")
        .merge(
            df.alias("source"),
            "target.merge_key = source.merge_key")
        .withSchemaEvolution()
        .whenMatchedUpdate(set = 
                           {
                                "target.num_ability_casts": "source.num_ability_casts + target.num_ability_casts",
                                "target.min_ability_duration": "least(source.min_ability_duration, target.min_ability_duration)",
                                "target.max_ability_duration": "greatest(source.max_ability_duration, target.max_ability_duration)",
                                "target.avg_ability_duration": "source.avg_ability_duration + target.avg_ability_duration",
                                "target.avg_num_targets_hit": "source.avg_num_targets_hit + target.avg_num_targets_hit",
                                "target.max_num_targets_hit": "greatest(source.max_num_targets_hit, target.max_num_targets_hit)",
                                "target.total_damage_done": "source.total_damage_done + target.total_damage_done",
                                "target.total_dot_damage_done": "source.total_dot_damage_done + target.total_dot_damage_done",
                                "target.total_amped_damage": "source.total_amped_damage + target.total_amped_damage",
                                "target.avg_num_targets_hit_amped": "source.avg_num_targets_hit_amped + target.avg_num_targets_hit_amped",
                                "target.total_healing_done": "source.total_healing_done + target.total_healing_done",
                                "target.total_self_healing_done": "source.total_self_healing_done + target.total_self_healing_done",
                                "target.avg_num_targets_healed": "source.avg_num_targets_healed + target.avg_num_targets_healed",
                                "target.max_num_targets_healed": "greatest(source.max_num_targets_healed, target.max_num_targets_healed)",
                                "target.avg_resource_used": "source.avg_resource_used + target.avg_resource_used",
                                "target.avg_starting_resource": "source.avg_starting_resource + target.avg_starting_resource",
                                "target.avg_ending_resource": "source.avg_ending_resource + target.avg_ending_resource",
                                "target.max_final_resouce": "greatest(source.max_final_resouce, target.max_final_resouce)",
                                "target.min_initial_resource": "least(source.min_initial_resource, target.min_initial_resource)",
                                "target.avg_distance_traveled": "source.avg_distance_traveled + target.avg_distance_traveled",
                                "target.avg_absorb_amount": "source.avg_absorb_amount + target.avg_absorb_amount",
                                "target.num_successful_sticks": "source.num_successful_sticks + target.num_successful_sticks",
                                "target.total_boosted_damage": "source.total_boosted_damage + target.total_boosted_damage",
                                "target.total_boosted_hits": "source.total_boosted_hits + target.total_boosted_hits",
                                "target.pct_successful_sticks": "round((source.num_successful_sticks + target.num_successful_sticks) / (source.num_ability_casts + target.num_ability_casts) * 100, 2)",
                                "target.num_ally_casts": "source.num_ally_casts + target.num_ally_casts",
                                "target.num_times_expired": "source.num_times_expired + target.num_times_expired",
                                "target.num_times_destroyed": "source.num_times_destroyed + target.num_times_destroyed",
                                "target.num_times_recalled": "source.num_times_recalled + target.num_times_recalled",
                                "target.num_times_cancelled": "source.num_times_cancelled + target.num_times_cancelled",
                                "target.num_times_successful": "source.num_times_successful + target.num_times_successful",
                                "target.num_times_fired": "source.num_times_fired + target.num_times_fired",
                                 "target.num_times_interact": "source.num_times_interact + target.num_times_interact",
                                "target.num_homing_projectiles": "source.num_homing_projectiles + target.num_homing_projectiles",
                                "target.num_ground_projectiles": "source.num_ground_projectiles + target.num_ground_projectiles",
                                "target.num_self_heals": "source.num_self_heals + target.num_self_heals",
                                "target.num_targeted_heals": "source.num_targeted_heals + target.num_targeted_heals",
                                "target.num_times_cast_attached": "source.num_times_cast_attached + target.num_times_cast_attached",
                                "target.num_times_cast_deployed": "source.num_times_cast_deployed + target.num_times_cast_deployed",
                                "target.num_times_opened_chest": "source.num_times_opened_chest + target.num_times_opened_chest",
                                "target.num_times_attacked": "source.num_times_attacked + target.num_times_attacked",
                                "target.num_times_guarded": "source.num_times_guarded + target.num_times_guarded",
                                "target.num_times_returned": "source.num_times_returned + target.num_times_returned",
                                "target.num_times_revive": "source.num_times_revive + target.num_times_revive",
                                "target.num_times_elim": "source.num_times_elim + target.num_times_elim",
                                "target.dw_update_ts": "current_timestamp()"
                           })
        .whenNotMatchedInsertAll()
        .execute()
    )


# COMMAND ----------

def write_fct_match_lvl_ability(checkpoint_location,environment,spark,fct_match_lvl_ability_df):

    (
        fct_match_lvl_ability_df
        .writeStream
        .outputMode("update")
        .foreachBatch(lambda df, batch_id: proc_batch(df, batch_id, checkpoint_location,environment,spark))
        .option("checkpointLocation", checkpoint_location)
        .trigger(availableNow=True)
        .start()
    )

# COMMAND ----------

def process_fact_match_lvl_ability_usage():

    spark = setup_spark("batch")

    environment,checkpoint_location = arg_parser()

    #environment, checkpoint_location = "_dev", "dbfs:/tmp/horizon/raw/run_dev/fact_match_level_ability_usage"

    checkpoint_location = checkpoint_location + "/fact_match_level_ability_usage"

    fct_match_lvl_ability_df = readstream_match_lvl_ability(spark,environment)

    write_fct_match_lvl_ability(checkpoint_location,environment,spark,fct_match_lvl_ability_df)

# COMMAND ----------

if __name__ == "__main__":
    process_fact_match_lvl_ability_usage()
