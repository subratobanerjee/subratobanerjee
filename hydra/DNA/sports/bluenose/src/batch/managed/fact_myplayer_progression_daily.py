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
database  = 'bluenose'
data_source ='dna'
title = 'PGA Tour 2K25'

# COMMAND ----------

def create_fact_myplayer_progression_daily(spark, database):

    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.fact_myplayer_progression_daily (
        date DATE,
        player_id STRING,
        platform STRING,
        service STRING,
        my_player_id STRING,
        game_type STRING,
        total_ap_spent DOUBLE,
        avg_ap_spent DOUBLE,
        total_sp_spent DOUBLE,
        avg_sp_spent DOUBLE,
        ap_earned DOUBLE,
        sp_earned DOUBLE,
        max_level_reached INT,
        num_level_ups INT,
        remaning_ap_balance DOUBLE,
        remaining_sp_balance DOUBLE,
        avg_sessions_to_spent DOUBLE,
        dw_insert_ts TIMESTAMP,
        dw_update_ts TIMESTAMP,
        merge_key STRING
    )
        """

    properties = {
        "delta.feature.allowColumnDefaults": "supported" # this won't work until iceberg v3 is released
    }

    create_table(spark, sql, properties)
    create_fact_myplayer_progression_daily_view(spark, database, environment)
    return f"dbfs:/tmp/{database}/managed/batch/run{environment}/create_fact_myplayer_progression_daily"


# COMMAND ----------

def create_fact_myplayer_progression_daily_view(spark, database, environment):
    """
    Create the fact_myplayer_progression_daily view in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
        mapping (dict): A dictionary of column mappings.
    """


    sql = f"""
    CREATE OR REPLACE VIEW {database}{environment}.managed_view.fact_myplayer_progression_daily AS (
        SELECT
        *
        from {database}{environment}.managed.fact_myplayer_progression_daily
    )
    """

    spark.sql(sql)

# COMMAND ----------

def read_player_session(environment,min_received_on):
    
    player_session_df = (spark
                         .read
                         .table(f"bluenose{environment}.intermediate.fact_player_session")
                         .where((expr(f"dw_insert_ts::timestamp::date > to_date('{min_received_on}') - INTERVAL 2 DAY"))))

    return player_session_df


# COMMAND ----------

def read_player_progression(environment, min_received_on):
    player_prog_df = (
    spark
    .read
    .table(f"bluenose{environment}.intermediate.fact_player_progression")
    .select(
        "received_on",
        expr("received_on::date as date"),
        "player_id",
        "platform",
        "service",
        "primary_instance_id",
        "secondary_instance_id",
        "subject_id",
        "action",
        expr("extra_info_2 as current_subject_value"),
        expr("extra_info_3 as prev_subject_value"),
        expr("extra_info_4 as attribute_points_earned"),
        expr("extra_info_5 as skill_points_earned"),
        expr("extra_info_8 as game_type"),
        expr("ROW_NUMBER() OVER (PARTITION BY received_on::date, player_id, platform, service, primary_instance_id,extra_info_8, subject_id ORDER BY received_on DESC) as rn")
    ).where((expr(f"received_on::timestamp::date > to_date('{min_received_on}') - INTERVAL 2 DAY")) & 
            (expr("current_subject_value is not null"))
            )
    )
    return player_prog_df

# COMMAND ----------

def extract(environment, database):
    prev_max_ts = max_timestamp(spark, f"{database}{environment}.managed.fact_myplayer_progression_daily", "date")
    min_received_on = spark.sql(f"select ifnull(min(received_on), '1970-10-01')::date as min_received_on from {database}{environment}.intermediate.fact_player_progression where dw_insert_ts >= current_date - interval '2' day").collect()[0]['min_received_on']
    min_received_on_inc = min(prev_max_ts, min_received_on)
    
    player_prog_df = read_player_progression(environment, min_received_on_inc)
    player_sesion_df = read_player_session(environment, min_received_on_inc)
    return player_prog_df,player_sesion_df

# COMMAND ----------

def calculate_sessions(fact_player_progression, fact_player_session):
        spent = (fact_player_progression
        .filter((expr("subject_id = 'skillpointsspent'")) & 
                        (expr("action = 'SpendSkillTreePoints'")) & 
                        (expr("current_subject_value is not null")) &
                        (expr("current_subject_value > 0")))
                .select(
                expr("player_id"), 
                expr("platform"), 
                expr("service"),
                expr("game_type"), 
                expr("primary_instance_id as myplayer_id"), 
                expr("received_on as spent_timestamp"), 
                expr("cast(current_subject_value as int) as points_spent"),
                expr("ROW_NUMBER() OVER (PARTITION BY player_id, primary_instance_id ORDER BY received_on) as spent_index")))

        earn = (fact_player_progression
                .filter((expr("action = 'LevelUpMyPlayer'")) & 
                        (expr("subject_id = 'skillpointsremaining'")) & 
                        (expr("skill_points_earned is not null")) &
                        (expr("cast(skill_points_earned as float) > 0")))
                .select(
                expr("player_id"), 
                expr("platform"), 
                expr("service"), 
                expr("game_type"), 
                expr("primary_instance_id as myplayer_id"), 
                expr("received_on as earn_timestamp"), 
                expr("cast(skill_points_earned as int) as points_spent"),
                expr("ROW_NUMBER() OVER (PARTITION BY player_id, primary_instance_id ORDER BY received_on) as earn_index")))


        fifo = (earn.join(spent, 
                        (earn.player_id == spent.player_id) & 
                        (earn.myplayer_id == spent.myplayer_id) & 
                        (earn.platform == spent.platform) & 
                        (earn.service == spent.service) & 
                        (earn.game_type == spent.game_type) & 
                        (earn.earn_index == spent.spent_index))
                        .select(
                                earn.player_id.alias("player_id"), 
                                earn.myplayer_id.alias("myplayer_id"), 
                                earn.platform.alias("platform"), 
                                earn.service.alias("service"), 
                                earn.game_type.alias("game_type"), 
                                earn.earn_timestamp, 
                                spent.spent_timestamp, 
                                spent.spent_index.alias("index")))

        fifo_sess = (fifo.join(fact_player_session, 
                        (fifo.player_id == fact_player_session.player_id) & 
                        (fifo.platform == fact_player_session.platform) & 
                        (fifo.service == fact_player_session.service) & 
                        (expr("earn_timestamp <= session_start_ts")) & 
                        (expr("spent_timestamp >= session_end_ts")), 
                        how="left")
                .groupBy(fifo.player_id, fifo.myplayer_id, fifo.platform, fifo.service, fifo.game_type, expr("spent_timestamp::date as spent_date"), fifo.index)
                .agg(expr("count(CASE WHEN application_session_id IS NULL THEN 1 ELSE NULL END) as sessions_count")))

        calculated_df = (fifo_sess.groupBy("player_id", "myplayer_id", "platform", "service", "spent_date", "game_type")
                .agg(expr("sum(sessions_count) / count(index) as avg_sessions_to_spent")))
        
        return calculated_df

# COMMAND ----------

def transform(player_prog_df,calculated_df):
    transformed_df = player_prog_df.groupBy(
        "date",
        "player_id",
        "platform",
        "service",
        "primary_instance_id",
        "game_type"
    ).agg(
        expr("SUM(case when action = 'SpendAttributePoints' and subject_id = 'attributepointsspent' then current_subject_value else 0 END) as total_ap_spent"),
        expr("AVG(case when action = 'SpendAttributePoints' and subject_id = 'attributepointsspent' then round(double(current_subject_value),2) else 0 END) as avg_ap_spent"),
        expr("SUM(case when action = 'SpendSkillTreePoints' and subject_id = 'skillpointsspent' then double(current_subject_value) else 0 END) as total_sp_spent"),
        expr("AVG(case when action = 'SpendSkillTreePoints' and subject_id = 'skillpointsspent' then round(double(current_subject_value),2) else 0 END) as avg_sp_spent"),
        expr("SUM(case when action = 'LevelUpMyPlayer' and subject_id = 'attributepointsremaining' then double(attribute_points_earned) else 0 END) as ap_earned"),
        expr("SUM(case when action = 'LevelUpMyPlayer' and subject_id = 'skillpointsremaining' then double(skill_points_earned) else 0 END) as sp_earned"),
        expr("MAX(case when action = 'LevelUpMyPlayer' and subject_id = 'myplayerlevel' then int(current_subject_value) else 0 END) as max_level_reached"),
        expr("SUM(case when action = 'LevelUpMyPlayer' and subject_id = 'myplayerlevel' then 1 else 0 END) as num_level_ups"),
        expr("MAX(CASE WHEN subject_id = 'attributepointsremaining' AND rn = 1 THEN double(current_subject_value) else 0 END) AS remaning_ap_balance"),
        expr("MAX(CASE WHEN subject_id = 'skillpointsremaining' AND rn = 1 THEN double(current_subject_value) else 0 END) AS remaining_sp_balance")
    ).select(
        "date",
        "player_id",
        "platform",
        "service",
       expr("primary_instance_id as my_player_id"),
        "game_type",
        "total_ap_spent",
        expr("ROUND(avg_ap_spent,2) AS avg_ap_spent"),
        "total_sp_spent",
        expr("ROUND(avg_sp_spent,2) AS avg_sp_spent"),
        "ap_earned",
        "sp_earned",
        "max_level_reached",
        "num_level_ups",
        "remaning_ap_balance",
        "remaining_sp_balance"
        )
    
    result_with_avg_sessions = calculated_df.select("player_id", "myplayer_id", "platform", "service", "spent_date", "game_type", "avg_sessions_to_spent")
    
    final_df = transformed_df.join(result_with_avg_sessions, 
                                   (transformed_df.player_id == result_with_avg_sessions.player_id) & 
                                   (transformed_df.my_player_id == result_with_avg_sessions.myplayer_id) & 
                                   (transformed_df.platform == result_with_avg_sessions.platform) & 
                                   (transformed_df.service == result_with_avg_sessions.service) & 
                                   (transformed_df.date == result_with_avg_sessions.spent_date) & 
                                   (transformed_df.game_type == result_with_avg_sessions.game_type), 
                                   how="left").select(
        transformed_df["*"],
        result_with_avg_sessions["avg_sessions_to_spent"]
    )

    return final_df


# COMMAND ----------

def load_fact_myplayer_progression_daily(spark, df, database, environment):
    """
    Load the data into the fact_player_game_status_daily table in the specified environment.

    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        df (DataFrame): The DataFrame to load into the table.
        database (str): The database to load the table into.
        environment (str): The environment to load the table into.
    """

    # Perform the merge operation into the Delta table
    final_table = DeltaTable.forName(spark, f"{database}{environment}.managed.fact_myplayer_progression_daily")

    # get schema of the table
    table_schema = spark.read.table(f"{database}{environment}.managed.fact_myplayer_progression_daily").schema

    # create empty dataframe using the schema
    out_df = spark.createDataFrame([], table_schema)

    # union the incoming dataframe with the empty dataframe, we can set allowMissingColumns to False to be more strict
    df = df.select(
            "*",
            expr("CURRENT_TIMESTAMP() as dw_insert_ts"),
            expr("CURRENT_TIMESTAMP() as dw_update_ts"),
            expr("sha2(concat_ws('|', date, player_id, platform, service, my_player_id, game_type), 256) as merge_key")
        )
    out_df = out_df.unionByName(df, allowMissingColumns=True)

    # dynamically setup merge condition based on the aggregate columns in this database's fact_player_game_status_daily table
    agg_cols = [
    'total_ap_spent', 'avg_ap_spent', 'total_sp_spent', 'avg_sp_spent', 'ap_earned', 'sp_earned', 'max_level_reached', 'num_level_ups',
    "remaning_ap_balance", "remaining_sp_balance"
    ]
    
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

def run_batch():
    spark = create_spark_session(name=f"{database}")
    checkpoint_location = create_fact_myplayer_progression_daily(spark, database)
    fact_player_progression,fact_player_session = extract(environment, database)
    calculated_df = calculate_sessions(fact_player_progression, fact_player_session)
    df = transform(fact_player_progression,calculated_df)
    load_fact_myplayer_progression_daily(spark, df, database, environment)

# COMMAND ----------

run_batch()
