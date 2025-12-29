# Databricks notebook source
# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/managed/fact_player_ftue_steps

# COMMAND ----------

# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

from datetime import date
from pyspark.sql.types import LongType, DoubleType, IntegerType, TimestampType
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lower, when, min, max, round as pyspark_round, first, row_number, current_timestamp, sha2, concat_ws, lit, expr, to_date, trim, coalesce



input_param = dbutils_input_params()
environment = input_param.get('environment', set_environment())
database = 'oak2'
# environment='_stg' # Note : stg has latest data with latest new columns added which might not present on dev
data_source = 'gearbox'
title = 'Borderlands 4'




def extract(spark, database, environment, prev_max):
    # Load and filter raw tables
    mission_state = (
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

    mission_info = spark.table(f"{database}{environment}.reference.mission_info")

    poa_state = spark.table(f"{database}{environment}.raw.oaktelemetry_poa_state") \
        .filter(expr(f"to_date(maw_time_received) >= to_date('{prev_max}') - interval 2 days")) \
        .filter("buildconfig_string = 'Shipping Game'")

    fact_player_char = spark.table(f"{database}{environment}.managed_view.fact_player_character")

    dim_title_df = spark.table(f"reference_stg.title.dim_title")
    return mission_state, mission_info, poa_state, fact_player_char ,dim_title_df


def transformation_1(mission_state, mission_info, dim_title_df,fact_player_char):
    session_players = (
        mission_state
        .where(expr("""
            lower(event_value_string) in ('mission_main_prisonprologue', 'mission_main_beach')
            and player_character_id is not null
            and player_platform_id is not null
        """))
        .groupBy(
            expr("execution_guid"), 
            expr("maw_time_received"), 
            expr("lower(event_value_string) as mission_name")
        )
        .agg(
            expr("max(session_players) as max_session_players"),
            expr("count(distinct case when player_connection = 'local' then coalesce(player_id,player_platform_id) end) as local_players"),
            expr("count(distinct case when player_connection = 'online' then coalesce(player_id,player_platform_id) end) as online_players")
        )
        .groupBy(
            expr("execution_guid"), 
            expr("mission_name")
        )
        .agg(
            expr("max(max_session_players) as max_session_players"),
            expr("ceil(avg(local_players)) as avg_local_players"),
            expr("ceil(avg(online_players)) as avg_online_players"),
            expr("ceil(avg(max_session_players)) as avg_session_players"),
            expr("max(local_players) as max_local_players"),
            expr("max(online_players) as max_online_players")
        )
    )

    # Main query
    main_query = (
        mission_state.alias("ms")
        .join(
            mission_info.alias("mi"),
            expr("lower(event_value_string) = mi.mission_name and lower(mission_objective_id_string) = lower(mi.mission_objective_id)"),
            "left"
        )
        .join(
            session_players.alias("sesh"),
            expr("ms.execution_guid = sesh.execution_guid and lower(ms.event_value_string) = sesh.mission_name"),
            "left"
        )
        .join(
            dim_title_df.alias("dt"),
            expr("""
                dt.title = 'Borderlands 4'
                and (case 
                        when ms.platform_string = 'XSX' then 'XBSX' 
                        when ms.platform_string = 'PS5Pro' then 'PS5' 
                        when lower(ms.platform_string) = 'sage' then 'NSW2'
                        else ms.platform_string 
                     end) = dt.display_platform
            """),
            "left"
        )
        .where(expr("""
            player_platform_id is not null
            and player_character_id is not null
            and (
                lower(event_value_string) in ('mission_main_prisonprologue', 'mission_main_beach') or
                (lower(event_value_string) = 'mission_main_grasslands1' and lower(mission_objective_id_string) = 'scan_vehicle')
            )
        """))
    )

    main_query = main_query.withColumn(
        "ftue_step_num",
        expr("""
            CASE 
                WHEN lower(event_value_string) = 'mission_main_grasslands1' 
                     AND lower(mission_objective_id_string) = 'scan_vehicle' THEN 2000
                WHEN lower(mission_objective_id_string) = 'none' THEN 0
                ELSE mi.mission_step_num
            END
        """)
    )

    main_query1 = main_query.groupBy(
        expr("coalesce(player_id,player_platform_id) as player_id"),
        expr("player_platform_id"), 
        expr("player_character_id"), 
        expr("player_character"), 
        expr("dt.display_platform").alias("platform"),
        expr("dt.display_service").alias("service"), 
        expr("lower(event_value_string) as mission_name"),
        expr("lower(mission_objective_id_string) as mission_objective_id"),
        expr("ftue_step_num")
    ).agg(
        expr("coalesce(max(max_session_players), max(session_players)) as max_session_players"),
        expr("coalesce(max(avg_session_players), avg(session_players)) as avg_session_players"),
        expr("coalesce(max(max_local_players), 1) as max_local_players"),
        expr("coalesce(max(avg_local_players), 1) as avg_local_players"),
        expr("coalesce(max(max_online_players), 0) as max_online_players"),
        expr("coalesce(max(avg_online_players), 0) as avg_online_players"),

        expr("max(case when lower(event_key_string) = 'completed' or (lower(mission_objective_id_string) = 'call_the_elevator_fake' and lower(event_key_string) = 'failed') then ms.execution_guid else null end) as completed_session_id"),       
        expr("min(case when lower(event_key_string) = 'active' then maw_time_received end) as start_ts"),
        expr("min(case when lower(event_key_string) = 'completed' or (lower(mission_objective_id_string) = 'call_the_elevator_fake' and lower(event_key_string) = 'failed') then maw_time_received end) as complete_ts"),
        expr("min(case when lower(event_key_string) = 'active' then player_level end) as starting_level"),
        expr("min(case when lower(event_key_string) = 'completed' or (lower(mission_objective_id_string) = 'call_the_elevator_fake' and lower(event_key_string) = 'failed') then player_level end) as ending_level"),
        expr("round(min(case when lower(event_key_string) = 'active' then player_time_played end), 2) as starting_time_played"),
        expr("round(min(case when lower(event_key_string) = 'completed' or (lower(mission_objective_id_string) = 'call_the_elevator_fake' and lower(event_key_string) = 'failed') then player_time_played end), 2) as ending_time_played"),
        expr("max(case when lower(event_key_string) = 'completed' or (lower(mission_objective_id_string) = 'call_the_elevator_fake' and lower(event_key_string) = 'failed') then true else false end) as mission_step_completed"),     
        expr("max(case when lower(event_key_string) = 'failed' and not (lower(mission_objective_id_string) = 'call_the_elevator_fake' and lower(event_key_string) = 'failed') then true else false end) as mission_step_failed")
    )



    mission_agg = (
    main_query1.alias("ms")
    .join(
        fact_player_char.alias("fpc"),
        on=(
            (col("ms.player_platform_id") == col("fpc.player_platform_id")) &
            (col("ms.player_character_id") == col("fpc.player_character_id")) &
            (col("ms.player_id") == expr("coalesce(fpc.player_id,fpc.player_platform_id)")) &
            (col("ms.player_character") == col("fpc.player_character"))
        ),
        how="left"
    )
    .selectExpr(
        "ms.*",
        "fpc.beach_complete_time_played"
    )
    )

    mission_agg = mission_agg.withColumn(
        "time_to_complete_mins",
        expr("""
            round(
                CASE 
                    WHEN ftue_step_num = 2000 
                    THEN (ending_time_played - beach_complete_time_played) / 60 
                    ELSE (ending_time_played - starting_time_played) / 60 
                END,
            2)
        """)
    )

    mission_agg = mission_agg.select(expr("coalesce(player_id,player_platform_id) as player_id"), "player_platform_id", "player_character_id", "player_character", "platform", "service", "completed_session_id", "max_session_players", "avg_session_players", "max_local_players", "avg_local_players", "max_online_players", "avg_online_players", "mission_name", "mission_objective_id", "ftue_step_num", "start_ts", "complete_ts", "starting_level", "ending_level", "starting_time_played", "ending_time_played", "mission_step_completed", "mission_step_failed", "time_to_complete_mins")


    return mission_agg



def transformation_2(poa_state, fact_player_char, dim_title_df):
    session_players = (
        poa_state.alias("ps")
        .where(expr("""
            ps.player_platform_id is not null
            and ps.player_character_id is not null
            and lower(ps.poa_type_string) = 'activity_silo'
            and lower(ps.poa_status_string) = 'claimed'
        """))
        .groupBy(
            expr("execution_guid"),
            expr("maw_time_received"),
            expr("lower(event_value_string) as mission_objective_id")
        )
        .agg(
            expr("count(distinct case when player_connection = 'local' then coalesce(player_id,player_platform_id) end) as local_players"),
            expr("count(distinct case when player_connection = 'online' then coalesce(player_id,player_platform_id) end) as online_players")
        )
    )

    first_silo_df = (
        poa_state.alias("ps")
        .join(
            fact_player_char.alias("fpc"),
            expr("""
                ps.player_platform_id = fpc.player_platform_id
                and ps.player_character_id = fpc.player_character_id
            """),
            "left"
        )
        .join(
            session_players.alias("players"),
            expr("""
                ps.execution_guid = players.execution_guid
                AND ps.maw_time_received = players.maw_time_received
                AND lower(event_value_string) = players.mission_objective_id
            """),
            "left"
        )
        .join(
            dim_title_df.alias("dt"),
            expr("""
                dt.title = 'Borderlands 4'
                and (
                    case
                        when ps.platform_string = 'XSX' then 'XBSX'
                        when ps.platform_string = 'PS5Pro' then 'PS5'
                        when lower(ps.platform_string) = 'sage' then 'NSW2'
                        else ps.platform_string
                    end
                ) = dt.display_platform
            """),
            "left"
        )
        .where(expr("""
            ps.player_platform_id is not null
            AND ps.player_character_id is not null
            AND lower(ps.poa_type_string) = 'activity_silo'
            AND lower(ps.poa_status_string) = 'claimed'
        """))
    )

    first_silo_df = first_silo_df.select(
        expr("coalesce(ps.player_id,ps.player_platform_id) as player_id"),
        expr("ps.player_platform_id").alias("player_platform_id"),
        expr("ps.player_character_id").alias("player_character_id"),
        expr("ps.player_character").alias("player_character"),
        expr("dt.display_platform").alias("platform"),
        expr("dt.display_service").alias("service"),
        expr("coalesce(ps.execution_guid, 'Unknown')").alias("completed_session_id"),
        expr("session_players").alias("avg_session_players"),
        expr("session_players").alias("max_session_players"),
        expr("local_players").alias("avg_local_players"),
        expr("local_players").alias("max_local_players"),
        expr("online_players").alias("avg_online_players"),
        expr("online_players").alias("max_online_players"),
        expr("'first_silo'").alias("mission_name"),
        expr("lower(event_value_string)").alias("mission_objective_id"),
        expr("1000").alias("ftue_step_num"),
        expr("""
            first_value(ps.maw_time_received) over (
                partition by ps.player_platform_id, ps.player_character_id
                order by ps.maw_time_received
            )
        """).alias("start_ts"),
        expr("""
            first_value(ps.maw_time_received) over (
                partition by ps.player_platform_id, ps.player_character_id
                order by ps.maw_time_received
            )
        """).alias("complete_ts"),
        expr("""
            first_value(ps.player_level) over (
                partition by ps.player_platform_id, ps.player_character_id
                order by ps.maw_time_received
            )
        """).alias("starting_level"),
        expr("""
            first_value(ps.player_level) over (
                partition by ps.player_platform_id, ps.player_character_id
                order by ps.maw_time_received
            )
        """).alias("ending_level"),
        expr("""
            first_value(round(player_time_played, 2)) over (partition by ps.player_platform_id, ps.player_character_id order by ps.maw_time_received)
        """).alias("starting_time_played"),
        expr("""
            first_value(round(player_time_played, 2)) over (partition by ps.player_platform_id, ps.player_character_id order by ps.maw_time_received)
        """).alias("ending_time_played"),
        expr("true").alias("mission_step_completed"),
        expr("false").alias("mission_step_failed"),
        expr("round((player_time_played - fpc.beach_complete_time_played) / 60, 2)").alias("time_to_complete_mins"),
        expr("""
            row_number() OVER (
                PARTITION BY ps.player_platform_id, ps.player_character_id
                ORDER BY ps.maw_time_received
            )
        """).alias("rn")
    )

    first_silo_df = first_silo_df.filter(expr("rn = 1")).drop("rn")

    first_silo_df = first_silo_df.groupBy(
    "player_id",
    "player_platform_id",
    "player_character_id",
    "player_character",
    "platform",
    "service",
    "completed_session_id",
    "mission_name",
    "mission_objective_id",
    "ftue_step_num",
    "start_ts",
    "complete_ts",
    "starting_level",
    "ending_level",
    "starting_time_played",
    "ending_time_played",
    "mission_step_completed",
    "mission_step_failed",
    "time_to_complete_mins"
    ).agg(
    expr("coalesce(max(max_session_players), 1)").alias("max_session_players"),
    expr("coalesce(avg(avg_session_players), 1)").alias("avg_session_players"),
    expr("coalesce(max(max_local_players), 1)").alias("max_local_players"),
    expr("coalesce(avg(avg_local_players), 1)").alias("avg_local_players"),
    expr("coalesce(max(max_online_players), 0)").alias("max_online_players"),
    expr("coalesce(avg(avg_online_players), 0)").alias("avg_online_players")
    )
    first_silo_df = first_silo_df.select("player_id", "player_platform_id", "player_character_id", "player_character", "platform", "service", "completed_session_id", "max_session_players", "avg_session_players", "max_local_players", "avg_local_players", "max_online_players", "avg_online_players", "mission_name", "mission_objective_id", "ftue_step_num", "start_ts", "complete_ts", "starting_level", "ending_level", "starting_time_played", "ending_time_played", "mission_step_completed", "mission_step_failed", "time_to_complete_mins")


    return first_silo_df




def load(df, database, environment, spark):
    # Reference to the target Delta table
    target_df = DeltaTable.forName(spark, f"{database}{environment}.managed.fact_player_ftue_step")

    # Merge condition
    merger_condition = 'target.merge_key = source.merge_key'

    # Generate update conditions for numeric fields
    update_types = (LongType, DoubleType, IntegerType,TimestampType)
    update_fields = [
        field.name for field in df.schema.fields if isinstance(field.dataType, update_types)
    ]

    # Dynamically build set_fields dict
    set_fields = {
        field: f"greatest(target.{field}, source.{field})" for field in update_fields
    }
    set_fields["dw_update_ts"] = "source.dw_update_ts"

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


def run_batch(database, environment):
    database = database.lower()

    # Setting the spark session
    spark = SparkSession.builder.appName(f"{database}").getOrCreate()

    create_fact_player_ftue_step(spark, database, environment)
    create_fact_player_ftue_step_view(spark, database, environment)
    prev_max = max_timestamp(spark, f"{database}{environment}.managed.fact_player_ftue_step", 'dw_insert_ts')
    if prev_max is None:
        prev_max = date(1999, 1, 1)
    print(prev_max)

    #extract_tables
    mission_state, mission_info, poa_state, fact_player_char ,dim_title_df = extract(spark, database, environment,prev_max)
    print('Extracting the data')

    #transformation
    mission_agg_df = transformation_1(mission_state, mission_info, dim_title_df,fact_player_char)
    first_silo_df = transformation_2(poa_state, fact_player_char, dim_title_df)
    # Union
    final_df = mission_agg_df.unionAll(first_silo_df).distinct()     

    df = final_df.withColumn("dw_update_ts", expr("current_timestamp()")) \
    .withColumn("dw_insert_ts", expr("current_timestamp()")) \
    .withColumn("merge_key", expr("sha2(concat_ws('|', player_id, platform, service, player_platform_id, player_character_id, player_character,mission_objective_id, mission_name, completed_session_id), 256)"))

    print('Merge Data')
    
    load(df, database, environment, spark)               


run_batch(database, environment)