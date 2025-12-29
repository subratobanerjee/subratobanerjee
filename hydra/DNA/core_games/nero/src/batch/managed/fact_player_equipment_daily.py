# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/table_functions

# COMMAND ----------

from pyspark.sql.functions import expr, explode, split
from delta.tables import DeltaTable

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'nero'
data_source ='dna'
title = "'Mafia: The Old Country'"

# COMMAND ----------

def read_player_status(spark,environment):
    prev_max = max_timestamp(spark, f"{database}{environment}.managed.fact_player_equipment_daily", 'date')

    current_min = (
        spark.read
        .table(f"nero{environment}.raw.playerstatus")
        .where(expr("insert_ts::date between current_date - 3 and current_date"))
        .select(expr(f"ifnull(min(receivedOn::timestamp::date),'1999-01-01')::date as min_date"))
    ).collect()[0]['min_date']

    inc_min_date = min(prev_max,current_min)

    equipments_df = (
        spark
        .read
        .table(f"nero{environment}.raw.playerstatus")
        .where(
            expr(
                "player_status = 'heartbeat' "
                "and playerpublicid != 'anonymous' "
                "and playerpublicid is not null "
                "and mission_run_id is not null"
            ) & expr(f"receivedOn::timestamp::date >= '{inc_min_date}'::date")
        )
        .select(
            expr("receivedOn::timestamp::date as date"),
            expr("playerPublicId as player_id"),
            expr("sessionId as session_id"),
            "mission_run_id",
            "equipped_outfit",
            "equipped_charms",
            "owned_charms",
            "player_status",
            expr("occurredOn::timestamp as occurred_on")
        )
    )
    return equipments_df

# COMMAND ----------

def extract(environment, database):
    equipment_df = read_player_status(spark,environment)
    return equipment_df

# COMMAND ----------

def transform(equipments_df):
    # Equipped Outfits
    equipped_outfits = (
        equipments_df
        .where(expr("equipped_outfit is not null"))
        .groupBy(
            expr("date"),
            expr("player_id"),
            expr("session_id"),
            expr("mission_run_id"),
            expr("equipped_outfit as equipment_name")
        )
        .agg(
            expr("count(distinct occurred_on) as equipment_heartbeat_qty")
        )
        .select(
            expr("date"),
            expr("player_id"),
            expr("session_id"),
            expr("mission_run_id"),
            expr("'outfit' as equipment_type"),
            expr("equipment_name"),
            expr("'equipped' as equipment_status"),
            expr("equipment_heartbeat_qty")
        )
    )

    # Equipped Charms
    equipped_charms_split = (
        equipments_df
        .filter(expr("equipped_charms is not null"))
        .select(
            expr("date"),
            expr("player_id"),
            expr("session_id"),
            expr("mission_run_id"),
            explode(split(expr("equipped_charms"), ",")).alias("charms"),
            expr("occurred_on")
        )
        .groupBy(
            expr("date"),
            expr("player_id"),
            expr("session_id"),
            expr("mission_run_id"),
            expr("charms")
        )
        .agg(
            expr("count(distinct occurred_on) as heartbeats")
        )
        .filter(expr("trim(charms) != 'None'"))
    )

    equipped_charms = (
        equipped_charms_split
        .groupBy(
            expr("date"),
            expr("player_id"),
            expr("session_id"),
            expr("mission_run_id"),
            expr("'charm' as equipment_type"),
            expr("right(trim(charms), length(trim(charms)) - 11) as equipment_name"),
            expr("'equipped' as equipment_status")
        )
        .agg(
            expr("sum(heartbeats) as equipment_heartbeat_qty")
        )
    )

    # # Owned Charms
    owned_charms_split = (
        equipments_df
        .filter(expr("owned_charms is not null"))
        .select(
            expr("date"),
            expr("player_id"),
            expr("session_id"),
            expr("mission_run_id"),
            explode(split(expr("owned_charms"), ",")).alias("charms"),
            expr("occurred_on")
        )
        .groupBy(
            expr("date"),
            expr("player_id"),
            expr("session_id"),
            expr("mission_run_id"),
            expr("charms")
        )
        .agg(
            expr("count(distinct occurred_on) as heartbeats")
        )
    )

    owned_charms = (
        owned_charms_split
        .groupBy(
            expr("date"),
            expr("player_id"),
            expr("session_id"),
            expr("mission_run_id"),
            expr("'charm' as equipment_type"),
            expr("right(trim(charms), length(trim(charms)) - 11) as equipment_name"),
            expr("'owned' as equipment_status")

        )
        .agg(
            expr("sum(heartbeats) as equipment_heartbeat_qty")
        )
    )

    # Union all dataframes
    all_equips = equipped_outfits.unionByName(equipped_charms).unionByName(owned_charms)
    
    return all_equips    

# COMMAND ----------

def load_fact_player_equipment_daily(spark, df, database, environment):
    """
    Load the data into the fact_player_mission_daily table in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        df (DataFrame): The DataFrame to load into the table.
        database (str): The database to load the table into.
        environment (str): The environment to load the table into.
    """

    # Perform the merge operation into the Delta table
    final_table = DeltaTable.forName(spark, f"{database}{environment}.managed.fact_player_equipment_daily")

    # get schema of the table
    table_schema = spark.read.table(f"{database}{environment}.managed.fact_player_equipment_daily").schema

    # create empty dataframe using the schema
    out_df = spark.createDataFrame([], table_schema)

    # union the incoming dataframe with the empty dataframe, we can set allowMissingColumns to False to be more strict
    df = df.select(
            "*",
            expr("CURRENT_TIMESTAMP() as dw_insert_ts"),
            expr("CURRENT_TIMESTAMP() as dw_update_ts"),
            expr("sha2(concat_ws('|', date, player_id, session_id, mission_run_id, equipment_type, equipment_name, equipment_status), 256) as merge_key")
        )
    out_df = out_df.unionByName(df, allowMissingColumns=True)

    # dynamically setup merge condition based on the aggregate columns in this database's fact_igm_promos_daily table
    
    agg_cols = ['equipment_heartbeat_qty']

    merge_condition = " OR ".join(f"old.{col_name} <> new.{col_name}" for col_name in agg_cols)

    # create the update column dict

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

def create_fact_player_mission_daily(spark, database):

    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.fact_player_equipment_daily (
        date DATE,
        player_id STRING,
        session_id STRING,
        mission_run_id STRING,
        equipment_type STRING,
        equipment_name STRING,
        equipment_status STRING,
        equipment_heartbeat_qty INT,
        dw_insert_ts TIMESTAMP,
        dw_update_ts TIMESTAMP,
        merge_key STRING
    )
        """

    properties = {
        "delta.feature.allowColumnDefaults": "supported" 
    }

    create_table(spark, sql, properties)
    create_fact_player_equipment_daily_view(spark, database)
    return f"Table fact_player_equipment_daily created"


# COMMAND ----------

def create_fact_player_equipment_daily_view(spark, database):
    """
    Create the fact_player_game_status_daily view in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
        mapping (dict): A dictionary of column mappings.
    """

    sql = f"""
    CREATE VIEW IF NOT EXISTS {database}{environment}.managed_view.fact_player_equipment_daily AS (
        SELECT
        *
        from {database}{environment}.managed.fact_player_equipment_daily
    )
    """
    spark.sql(sql)

# COMMAND ----------

def run_batch():
    spark = create_spark_session(name=f"{database}")
    create_fact_player_mission_daily(spark, database)
    df1 = extract(environment, database)
    df = transform(df1)
    load_fact_player_equipment_daily(spark, df, database, environment)

# COMMAND ----------

run_batch()
