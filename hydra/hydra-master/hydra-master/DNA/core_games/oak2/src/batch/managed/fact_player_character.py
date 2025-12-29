# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/table_functions

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/oak2/fact_player_character

# COMMAND ----------

from pyspark.sql.functions import col, expr, row_number, sha2
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import date
from pyspark.sql.window import Window
from pyspark.sql.types import LongType, DoubleType, IntegerType, TimestampType

input_param = dbutils_input_params()
environment = input_param.get('environment', set_environment())
database = 'oak2'
# environment='_stg' # Note : stg has latest data with latest new columns added which are not present on dev
data_source = 'gearbox'
title = 'Borderlands 4'

# For dev environment, we want to read from stg but write to dev
# source_environment = '_stg' if environment == '_dev' else environment

def extract(spark, environment, database, prev_max):
    # Raw table data load and filter - using source_environment for reading
    df_mission_state = (
    spark.table(f"{database}{environment}.raw.oaktelemetry_mission_state")
    .filter(
        expr(f"""
            to_date(maw_time_received) >= to_date('{prev_max}') - interval 2 days
            AND event_value_string NOT IN (
                'Mission_GbxMoment_FactsLevelSequence',
                'displaytestmission',
                'DisplayTextMission'
            )
        """)
    )
    .filter("buildconfig_string = 'Shipping Game'")
)

    df_poa_state = spark.table(f"{database}{environment}.raw.oaktelemetry_poa_state") \
        .filter(expr(f"to_date(maw_time_received) >= to_date('{prev_max}') - interval 2 days")) \
        .filter("buildconfig_string = 'Shipping Game'")

    return df_mission_state, df_poa_state

def transform(spark, database, environment, df_mission_state, df_poa_state):
    # aggregations from mission state table
    mission_state = (
        df_mission_state
        .filter(
            expr("""
                    player_platform_id is not null and 
                    player_character not in ('OakCharacter', 'OakPlayerState', 'HexPlayerState', 'ozonecharacter', 'Player_Hex')
                """)
        )
        .groupBy(
             expr("coalesce(player_id, player_platform_id) as player_id"),
            "player_platform_id",
            "player_character_id",
            "player_character"
        )
        .agg(
            expr("max(player_level) as player_level"),
            expr("""
                    min(
                        case
                            when lower(event_value_string) = 'mission_main_prisonprologue'
                                and lower(event_key_string) = 'completed'
                            then maw_time_received
                        end
                    ) as prison_complete_ts
                """),
            expr("""
                    min(
                        case
                            when lower(event_value_string) = 'mission_main_beach'
                                and lower(event_key_string) = 'completed'
                            then maw_time_received
                        end
                    ) as beach_complete_ts
                """),
            expr("""
                    max(
                        case 
                            when lower(event_value_string) = 'mission_main_beach'
                            then maw_time_received 
                        end
                    ) as beach_max_ts
                """),
            expr("""
                    max(
                        case
                            when lower(event_value_string) = 'mission_main_beach'
                            then player_time_played
                        end
                    ) as beach_complete_time_played
                """),
            expr("""
                    min(
                        case
                            when lower(event_value_string) = 'mission_main_grasslands1'
                                and lower(mission_objective_id_string) = 'scan_vehicle'
                                and lower(event_key_string) = 'completed'
                            then maw_time_received
                        end
                    ) as vehicle_scan_ts
                """),
            expr("min(maw_time_received) as player_character_create_ts")
        )
    )

    # 2. Assign character number per player_id based on create_ts
    window_spec = Window.partitionBy("player_id").orderBy("player_character_create_ts")

    playerchar_df = (
        mission_state
        .withColumn("player_character_num", row_number().over(window_spec))
    )

    # 3. Aggregate poa_state
    poa_state = (
        df_poa_state
        .filter("""player_platform_id is not null""")
        .groupBy(expr("coalesce(player_id, player_platform_id) as player_id"),
                 "player_platform_id",
                 "player_character_id")
        .agg(
            expr("""
                    min(
                        case 
                            when lower(poa_type_string) = 'activity_silo' and lower(poa_status_string) = 'claimed' 
                            then maw_time_received
                    end) as silo_claim_ts
                """)
        )
    )

    # 4. Join everything
    join_df = playerchar_df.alias("pc").join(
        poa_state.alias("ps"),
        on=["player_id", "player_character_id", "player_platform_id"],
        how="left"
    )

    # 5. Final projection
    final_df = (
        join_df
        .select(
            expr("coalesce(player_id, pc.player_platform_id) as player_id"),
            "pc.player_platform_id",
            "player_character_id",
            "player_character",
            "player_character_num",
            expr("player_character_create_ts"),
            "player_level",
            "prison_complete_ts",
            "beach_complete_ts",
            "beach_complete_time_played",
            "beach_max_ts",
            "silo_claim_ts",
            "vehicle_scan_ts"
        )
        .withColumn("dw_insert_ts", expr("current_timestamp()"))
        .withColumn("dw_update_ts", expr("current_timestamp()"))
        .withColumn(
            "merge_key",
            expr("sha2(concat_ws('|', player_id, player_platform_id, player_character_num), 256)")
        )
    )
    return final_df

# COMMAND ----------

def load(df, database, environment, spark):
    # Reference to the target Delta table
    target_df = DeltaTable.forName(spark, f"{database}{environment}.managed.fact_player_character")

    # Merge condition
    merger_condition = 'target.merge_key = source.merge_key'

    # Extract all update fields
    update_types = (LongType, DoubleType, IntegerType, TimestampType)
    update_fields = [
        field.name for field in df.schema.fields if isinstance(field.dataType, update_types)
    ]

    # Timestamp fields to preserve earliest or latest values
    least_fields = ["prison_complete_ts", "beach_complete_ts", "vehicle_scan_ts", "player_character_create_ts", "silo_claim_ts"]

    # Dynamically build set_fields dict based on field type
    set_fields = {
        field: f"least(target.{field}, source.{field})" if field in least_fields
        else f"greatest(target.{field}, source.{field})"
        for field in update_fields
    }
    # set_fields["dw_update_ts"] = "source.dw_update_ts"

    # Optional: construct a condition string (can be improved or removed)
    update_condition = " OR ".join([f"target.{field} != source.{field}" for field in update_fields])

    merge_update_conditions = [
        {
            'condition': update_condition,
            'set_fields': set_fields
        }
    ]

    # Begin merge operation
    merge_df = target_df.alias("target").merge(df.alias("source"), merger_condition)
    merge_df = set_merge_update_condition(merge_df, merge_update_conditions)
    merge_df = set_merge_insert_condition(merge_df, df)

    merge_df.execute()

# COMMAND ----------

def run_batch(database, environment):
    database = database.lower()

    # Setting the spark session
    spark = SparkSession.builder.appName(f"{database}").getOrCreate()

    # Creating the table structure - using target environment
    create_fact_player_character(spark, database, environment)
    create_fact_player_character_view(spark, database, environment)

    print('Extracting the data')

    prev_max = max_timestamp(spark, f"{database}{environment}.managed.fact_player_character", 'dw_insert_ts')
    if prev_max is None:
        prev_max = date(1999, 1, 1)
    print(prev_max)

    # extract - passing environment for logging purposes
    df_mission_state, df_poa_state = extract(spark, environment, database, prev_max)

    # transform
    df = transform(spark, database, environment, df_mission_state, df_poa_state)
    df.display()

    print('Merge Data')
    load(df, database, environment, spark)

# COMMAND ----------

run_batch(database, environment)
