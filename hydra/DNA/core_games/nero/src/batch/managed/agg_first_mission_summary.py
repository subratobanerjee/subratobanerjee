# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/table_functions

# COMMAND ----------

from pyspark.sql.functions import expr
from delta.tables import DeltaTable

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'nero'
data_source ='dna'
title = "'Mafia: The Old Country'"

# COMMAND ----------

def read_fact_player_mission_daily(spark, environment):
    # Start missions
    prev_max = max_timestamp(spark, f"{database}{environment}.managed.agg_first_mission_summary", 'dw_insert_ts')
    if prev_max is None:
        prev_max = date(1999, 1, 1)
    else:
        prev_max

    start_missions = (
        spark
        .read
        .table(f"nero{environment}.managed.fact_player_mission_daily")
        .where(
            expr("start_mission_status = 'new_mission'") &
            expr("mission_name NOT ILIKE '%FreeRide%'") & 
            expr(f"dw_insert_ts::date > to_date('{prev_max}') - INTERVAL 3 DAY")
    ).select(
        "player_id",
        "mission_num",
        "mission_name",
        "mission_run_id",
        expr("mission_start_ts AS first_mission_start_ts"),
        expr("start_difficulty AS initial_difficulty"),
        expr("ROW_NUMBER() OVER (PARTITION BY player_id, mission_num, mission_name ORDER BY mission_start_ts) AS rn")

    ).where(expr("rn = 1"))
    )

    # End missions
    end_missions = (
        spark
        .read
        .table(f"nero{environment}.managed.fact_player_mission_daily")
        .where(
        expr("end_mission_status = 'mission_complete'") &
        expr("mission_name NOT ILIKE '%FreeRide%'") &
        expr(f"dw_insert_ts::date > to_date('{prev_max}') - INTERVAL 3 DAY")
    ).select(
        "player_id",
        "mission_num",
        "mission_name",
        expr("mission_end_ts AS first_mission_complete_ts"),
        "end_difficulty",
        expr("ROW_NUMBER() OVER (PARTITION BY player_id, mission_num, mission_name ORDER BY mission_end_ts) AS rn")
    ).where(expr("rn = 1"))
    )

    # Mission difficulties
    mission_difficulties = (
        spark
        .read
        .table(f"nero{environment}.managed.fact_player_mission_daily").alias("d")
        .join(
        start_missions.alias("s"),
        expr("d.player_id = s.player_id AND d.mission_num = s.mission_num AND d.mission_name = s.mission_name"),
        "left"
    ).join(
        end_missions.alias("e"),
        expr("d.player_id = e.player_id AND d.mission_num = e.mission_num AND d.mission_name = e.mission_name"),
        "left"
    ).where(
        expr("d.mission_start_ts >= s.first_mission_start_ts") &
        (expr("d.mission_end_ts <= e.first_mission_complete_ts")) & 
        expr("d.mission_name NOT ILIKE '%FreeRide%'")
    ).select(
        expr("d.player_id"),
        expr("d.mission_num"),
        expr("d.mission_name"),
        expr("d.mission_run_id"),
        expr("s.initial_difficulty"),
        expr("e.end_difficulty"),
        expr("d.start_difficulty"),
        expr("d.mission_start_ts"),
        expr("d.mission_end_ts"),
        expr("s.first_mission_start_ts"),
        expr("e.first_mission_complete_ts"),
        expr("d.lowest_difficulty"),
        expr("d.highest_difficulty"),
        expr("d.difficulty_changes"),
        expr("LAG(d.end_difficulty) OVER (PARTITION BY d.player_id, d.mission_num, d.mission_name ORDER BY d.mission_start_ts) AS prev_end_difficulty")
    )

    )

    return mission_difficulties

# COMMAND ----------

def extract(environment, database):
    mission_difficulties = read_fact_player_mission_daily(spark, environment)
    return mission_difficulties

# COMMAND ----------

def transform(mission_difficulties):
    transformed_df = mission_difficulties.groupBy(
        "player_id",
        "mission_num",
        "mission_name",
        "initial_difficulty",
        "end_difficulty",
        "first_mission_start_ts",
        "first_mission_complete_ts"
    ).agg(
        expr("MAX(highest_difficulty) AS highest_difficulty"),
        expr("MIN(lowest_difficulty) AS lowest_difficulty"),
        expr("SUM(CASE WHEN prev_end_difficulty IS NOT NULL AND prev_end_difficulty <> start_difficulty THEN 1 ELSE 0 END) AS diff_change_outside_missions"),
        expr("SUM(difficulty_changes) AS diff_change_within_missions")
    ).select(
        expr("COALESCE(player_id, 'ZZ') as player_id"),
        expr("mission_num as mission_num"),
        expr("mission_name as mission_name"),
        "initial_difficulty",
        "end_difficulty",
        "first_mission_start_ts",
        "first_mission_complete_ts",
        "highest_difficulty",
        "lowest_difficulty",
        expr("diff_change_outside_missions + diff_change_within_missions AS difficulty_change_qty")
    )

    return transformed_df



# COMMAND ----------

def load_agg_first_mission_summary(spark, df, database, environment):
    """
    Load the data into the agg_first_mission_summary table in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        df (DataFrame): The DataFrame to load into the table.
        database (str): The database to load the table into.
        environment (str): The environment to load the table into.
    """

    # Perform the merge operation into the Delta table
    final_table = DeltaTable.forName(spark, f"{database}{environment}.managed.agg_first_mission_summary")

    # get schema of the table
    table_schema = spark.read.table(f"{database}{environment}.managed.agg_first_mission_summary").schema

    # create empty dataframe using the schema
    out_df = spark.createDataFrame([], table_schema)

    # union the incoming dataframe with the empty dataframe, we can set allowMissingColumns to False to be more strict
    df = df.select(
            "*",
            expr("CURRENT_TIMESTAMP() as dw_insert_ts"),
            expr("CURRENT_TIMESTAMP() as dw_update_ts"),
            expr("sha2(concat_ws('|', player_id,mission_num, mission_name), 256) as merge_key")
        )
    out_df = out_df.unionByName(df, allowMissingColumns=True)

    # dynamically setup merge condition based on the aggregate columns in this database's fact_igm_promos_daily table
    
    agg_cols = ['initial_difficulty', 'end_difficulty', 'first_mission_start_ts', 'first_mission_complete_ts', 'highest_difficulty', 'lowest_difficulty', 'difficulty_change_qty']

    merge_condition = " OR ".join(f"old.{col_name} <> new.{col_name}" for col_name in agg_cols)

    # create the update column dict
    update_set = {
        "initial_difficulty": "COALESCE(old.initial_difficulty, new.initial_difficulty)",
        "end_difficulty": "COALESCE(new.end_difficulty, old.end_difficulty)",
        "first_mission_start_ts": "COALESCE(LEAST(old.first_mission_start_ts, new.first_mission_start_ts), new.first_mission_start_ts)",
        "first_mission_complete_ts": "COALESCE(LEAST(old.first_mission_complete_ts, new.first_mission_complete_ts), new.first_mission_complete_ts)",
        "highest_difficulty": "COALESCE(GREATEST(old.highest_difficulty, new.highest_difficulty), new.highest_difficulty)",
        "lowest_difficulty": "COALESCE(LEAST(old.lowest_difficulty, new.lowest_difficulty), new.lowest_difficulty)",
        "difficulty_change_qty": "COALESCE(old.difficulty_change_qty, 0) + COALESCE(new.difficulty_change_qty, 0)",
        "dw_update_ts": "CURRENT_TIMESTAMP()"
    }

    # merge the table
    (
        final_table.alias('old')
        .merge(
            out_df.alias('new'),
            "new.merge_key = old.merge_key"
        )
        .whenMatchedUpdate(
            condition="old.first_mission_complete_ts IS NULL OR new.first_mission_complete_ts < old.first_mission_complete_ts",
            set=update_set
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

def create_agg_first_mission_summary(spark, database):

    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.agg_first_mission_summary (
        player_id STRING NOT NULL,
        mission_num STRING NOT NULL,
        mission_name STRING NOT NULL,
        initial_difficulty INT,
        end_difficulty INT,
        first_mission_start_ts TIMESTAMP,
        first_mission_complete_ts TIMESTAMP,
        highest_difficulty INT,
        lowest_difficulty INT,
        difficulty_change_qty INT,
        dw_insert_ts TIMESTAMP,
        dw_update_ts TIMESTAMP,
        merge_key STRING
    )
        """

    properties = {
        "delta.feature.allowColumnDefaults": "supported" 
    }

    create_table(spark, sql, properties)
    create_agg_first_mission_summary_view(spark, database)
    return f"Table agg_first_mission_summary created"


# COMMAND ----------

def create_agg_first_mission_summary_view(spark, database):
    """
    Create the fact_player_game_status_daily view in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
        mapping (dict): A dictionary of column mappings.
    """

    sql = f"""
    CREATE VIEW IF NOT EXISTS {database}{environment}.managed_view.agg_first_mission_summary AS (
        SELECT
        *
        from {database}{environment}.managed.agg_first_mission_summary
    )
    """
    spark.sql(sql)

# COMMAND ----------

def run_batch():
    spark = create_spark_session(name=f"{database}")
    create_agg_first_mission_summary(spark, database)
    df1 = extract(environment, database)
    df = transform(df1)
    load_agg_first_mission_summary(spark, df, database, environment)

# COMMAND ----------

run_batch()

# COMMAND ----------

# MAGIC %md
# MAGIC
