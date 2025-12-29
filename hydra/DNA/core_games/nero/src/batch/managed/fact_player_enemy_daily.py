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

def read_enemy_status(spark,environment):
    prev_max = max_timestamp(spark, f"{database}{environment}.managed.fact_player_enemy_daily", 'date')

    current_min = (
        spark.read
        .table(f"nero{environment}.raw.enemystatus")
        .where(expr("insert_ts::date between current_date - 3 and current_date"))
        .select(expr(f"ifnull(min(receivedOn::timestamp::date),'1999-01-01')::date as min_date"))
    ).collect()[0]['min_date']

    inc_min_date = min(prev_max,current_min)
    
    return (
        spark
        .read
        .table(f"nero{environment}.raw.enemystatus")
        .where(
               (expr("mission_run_id IS NOT NULL AND mission_run_id != '' and mission_run_id is not null")) &
               (expr(f"receivedOn::timestamp::date >= '{inc_min_date}'::date"))
        )
        .select(
            expr("receivedOn::timestamp::date as date"),
            expr("playerPublicId as player_id"),
            expr("sessionId as session_id"),
            expr("mission_run_id"),
            expr("enemy_type"),
            expr("enemy_status"),
            expr("play_state"),
            expr("enemy_killed_by_weapon"),
            expr("enemy_killed_by_damage"),
            expr("enemy_killed_by_agent"),
            expr("enemy_guid")
    )
 )
    

# COMMAND ----------

def extract(environment, database):
    enemy_status_df = read_enemy_status(spark, environment)
    return enemy_status_df

# COMMAND ----------

def transform(enemy_status_df):
    return (
        enemy_status_df
        .groupBy(
            "date",
            "player_id",
            "session_id",
            "mission_run_id",
            "enemy_type",
            "enemy_status",
            "play_state",
            expr(
                """
                CASE 
                    WHEN LEFT(enemy_killed_by_weapon, 3) = 'BP_'
                    THEN
                        substr(LEFT(enemy_killed_by_weapon, instr(enemy_killed_by_weapon, '_C') - 1), 4)
                    ELSE enemy_killed_by_weapon
                END AS enemy_killed_weapon_name
                """
            ),
            "enemy_killed_by_damage",
            "enemy_killed_by_agent"
        )
        .agg(
            expr("COUNT(DISTINCT enemy_guid) as enemy_qty"),
            expr(
                "COUNT(DISTINCT CASE WHEN enemy_status = 'enemy_killed' "
                "AND enemy_killed_by_weapon IS NOT NULL THEN enemy_guid END) as enemy_qty_weapon"
            ),
            expr(
                "COUNT(DISTINCT CASE WHEN enemy_status = 'enemy_killed' "
                "AND enemy_killed_by_damage IS NOT NULL THEN enemy_guid END) as enemy_qty_damage"
            )
        )
        .select(
            expr("date"),
            expr("COALESCE(player_id, 'ZZ') as player_id"),
            expr("COALESCE(session_id, 'ZZ') as session_id"),
            expr("COALESCE(mission_run_id, 'ZZ') as mission_run_id"),
            expr("enemy_type as enemy_type"),
            expr("enemy_status as enemy_status"),
            expr("play_state as play_state"),
            expr("enemy_killed_weapon_name as enemy_killed_weapon_name"),
            expr("enemy_killed_by_damage as enemy_killed_by_damage"),
            expr("enemy_killed_by_agent as enemy_killed_by_agent"),
            expr("enemy_qty"),
            expr("enemy_qty_weapon"),
            expr("enemy_qty_damage")
        )
    )

# COMMAND ----------

def load_fact_player_enemy_daily(spark, df, database, environment):
    """
    Load the data into the fact_player_enemy_daily table in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        df (DataFrame): The DataFrame to load into the table.
        database (str): The database to load the table into.
        environment (str): The environment to load the table into.
    """

    # Perform the merge operation into the Delta table
    final_table = DeltaTable.forName(spark, f"{database}{environment}.managed.fact_player_enemy_daily")

    # get schema of the table
    table_schema = spark.read.table(f"{database}{environment}.managed.fact_player_enemy_daily").schema

    # create empty dataframe using the schema
    out_df = spark.createDataFrame([], table_schema)

    # union the incoming dataframe with the empty dataframe, we can set allowMissingColumns to False to be more strict
    df = df.select(
            "*",
            expr("CURRENT_TIMESTAMP() as dw_insert_ts"),
            expr("CURRENT_TIMESTAMP() as dw_update_ts"),
            expr("sha2(concat_ws('|', date, player_id, session_id, mission_run_id, enemy_type, enemy_status, play_state, enemy_killed_weapon_name, enemy_killed_by_damage, enemy_killed_by_agent), 256) as merge_key")
        )
    out_df = out_df.unionByName(df, allowMissingColumns=True)

    # dynamically setup merge condition based on the aggregate columns in this database's fact_igm_promos_daily table
    
    agg_cols = ['enemy_qty', 'enemy_qty_weapon', 'enemy_qty_damage']

    merge_condition = " OR ".join(f"old.{col_name} <> new.{col_name}" for col_name in agg_cols)

    update_set = {}
    for col_name in agg_cols:
        update_set[f"old.{col_name}"] = f"new.{col_name}"

    update_set[f"old.dw_update_ts"] = "CURRENT_TIMESTAMP()"

    # merge the table
    (
        final_table.alias('old')
        .merge(
            out_df.alias('new'),
            "new.merge_key = old.merge_key"
        )
        .whenMatchedUpdate(condition=merge_condition, set=update_set)
        .whenNotMatchedInsertAll()
        .execute()
    )


# COMMAND ----------

def create_fact_player_enemy_daily(spark, database):

    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.fact_player_enemy_daily (
        date DATE NOT NULL,
        player_id STRING NOT NULL,
        session_id STRING NOT NULL,
        mission_run_id STRING NOT NULL,
        enemy_type STRING ,
        enemy_status STRING ,
        play_state STRING  ,
        enemy_killed_weapon_name STRING ,
        enemy_killed_by_damage STRING ,
        enemy_killed_by_agent STRING ,
        enemy_qty INT,
        enemy_qty_weapon INT,
        enemy_qty_damage INT,
        dw_insert_ts TIMESTAMP,
        dw_update_ts TIMESTAMP,
        merge_key STRING
    )
        """

    properties = {
        "delta.feature.allowColumnDefaults": "supported" 
    }

    create_table(spark, sql, properties)
    create_fact_player_enemy_daily_view(spark, database)
    return f"Table fact_player_enemy_daily created"


# COMMAND ----------

def create_fact_player_enemy_daily_view(spark, database):
    """
    Create the fact_player_game_status_daily view in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
        mapping (dict): A dictionary of column mappings.
    """

    sql = f"""
    CREATE VIEW IF NOT EXISTS {database}{environment}.managed_view.fact_player_enemy_daily AS (
        SELECT
        *
        from {database}{environment}.managed.fact_player_enemy_daily
    )
    """
    spark.sql(sql)

# COMMAND ----------

def run_batch():
    spark = create_spark_session(name=f"{database}")
    create_fact_player_enemy_daily(spark, database)
    enemy_status_df = extract(environment, database)
    transformed_df = transform(enemy_status_df)
    load_fact_player_enemy_daily(spark, transformed_df, database, environment)

# COMMAND ----------

run_batch()