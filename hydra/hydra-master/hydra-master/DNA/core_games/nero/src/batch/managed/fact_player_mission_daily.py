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

def read_mission_status(spark,environment):
    prev_max = max_timestamp(spark, f"{database}{environment}.managed.fact_player_mission_daily", 'date')

    current_min = (
        spark.read
        .table(f"nero{environment}.intermediate.fact_player_activity")
        .where(expr("dw_insert_ts::date between current_date - 3 and current_date"))
        .select(expr(f"ifnull(min(received_on::date),'1999-01-01')::date as min_date"))
    ).collect()[0]['min_date']

    inc_min_date = min(prev_max,current_min)
    
    return (
        spark
        .read
        .table(f"nero{environment}.intermediate.fact_player_activity")
        .where(
               (expr("source_table = 'missionstatus' and extra_info_2 is not null and extra_info_1 is not null")) &
               (expr(f"received_on::date > to_date('{inc_min_date}') - INTERVAL 3 DAY"))
        )
        .select(
            expr("received_on::date as date"),
            expr("player_id"),
            expr("platform"),
            expr("service"),
            expr("country_code"),
            expr("session_id"),
            expr("extra_info_3 as mission_run_id"),
            expr("extra_info_2::int as mission_num"),
            expr("extra_info_1 as mission_name"),
            expr("extra_info_6 as difficulty"),
            expr("event_trigger"),
            expr("""
                first_value(extra_info_6) OVER (
                    PARTITION BY player_id, extra_info_3, session_id, received_on::date
                    ORDER BY extra_info_9::timestamp, extra_info_10::int
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS start_difficulty
            """),
            expr("""
                last_value(extra_info_6) OVER (
                    PARTITION BY player_id, extra_info_3, session_id, received_on::date
                    ORDER BY extra_info_9::timestamp, extra_info_10::int
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS end_difficulty
            """),
            expr("""
                LAG(extra_info_6) OVER (
                    PARTITION BY player_id, extra_info_3, session_id, received_on::date
                    ORDER BY extra_info_9::timestamp, extra_info_10::int
                ) AS prev_difficulty
            """),
            expr("""
                FIRST_VALUE(event_trigger) OVER (
                    PARTITION BY player_id, extra_info_3, session_id, received_on::date
                    ORDER BY extra_info_9::timestamp, extra_info_10::int
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS start_mission_status
            """),
            expr("""
                LAST_VALUE(event_trigger) OVER (
                    PARTITION BY player_id, extra_info_3, session_id, received_on::date
                    ORDER BY extra_info_9::timestamp, extra_info_10::int
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS end_mission_status
            """),
            expr("extra_info_9::timestamp as occurred_on"),
            expr("extra_info_10::int as sequence_number"),
            expr("extra_info_4::int as attempt_counter")
       
    )
 )
    

# COMMAND ----------

def read_application_status(spark, environment):
    prev_max = max_timestamp(spark, f"{database}{environment}.managed.fact_player_mission_daily", 'date')

    current_min = (
        spark.read
        .table(f"nero{environment}.intermediate.fact_player_activity")
        .where(expr("dw_insert_ts::date between current_date - 3 and current_date"))
        .select(expr(f"ifnull(min(received_on::date),'1999-01-01')::date as min_date"))
    ).collect()[0]['min_date']

    inc_min_date = min(prev_max,current_min)

    return (
        spark
        .read
        .table(f"nero{environment}.intermediate.fact_player_activity")
        .where(
            
            (expr("source_table = 'applicationstatus'")) &
            (expr("event_trigger IN ('application_background', 'application_foreground')")) &
            (expr("extra_info_7 IS NOT NULL")) &
            expr(f"received_on::date > to_date('{prev_max}') - INTERVAL 3 DAY")
        )
        .select(
            expr("player_id"),
            expr("session_id"),
            expr("extra_info_7 as application_background_instance_id"),
            expr("extra_info_9 as occurred_on")
        )
    )

# COMMAND ----------

def extract(environment, database):
    mission_status_df = read_mission_status(spark, environment)
    application_status_df = read_application_status(spark, environment)
    return mission_status_df, application_status_df

# COMMAND ----------

def transform(mission_status_df, application_status_df):
    
    mission_transformed_df = (
        mission_status_df.groupBy(
            expr("date"),
            expr("player_id"),
            expr("platform"),
            expr("service"),
            expr("country_code"),
            expr("session_id"),
            expr("mission_run_id"),
            expr("mission_num"),
            expr("mission_name"),
            expr("""
                CASE
                    WHEN start_difficulty = 'Easy' THEN 1
                    WHEN start_difficulty = 'Medium' THEN 2
                    WHEN start_difficulty = 'Hard' THEN 3
                END AS start_difficulty
            """),
            expr("""
                CASE
                    WHEN end_difficulty = 'Easy' THEN 1
                    WHEN end_difficulty = 'Medium' THEN 2
                    WHEN end_difficulty = 'Hard' THEN 3
                END AS end_difficulty
            """),
            expr("start_mission_status"),
            expr("end_mission_status")
        ).agg(
            expr("MIN(occurred_on) AS mission_start_ts"),
            expr("MAX(occurred_on) AS mission_end_ts"),
            expr("""
                MIN(
                    CASE
                        WHEN difficulty = 'Easy' THEN 1
                        WHEN difficulty = 'Medium' THEN 2
                        WHEN difficulty = 'Hard' THEN 3
                    END
                ) AS lowest_difficulty
            """),
            expr("""
                MAX(
                    CASE
                        WHEN difficulty = 'Easy' THEN 1
                        WHEN difficulty = 'Medium' THEN 2
                        WHEN difficulty = 'Hard' THEN 3
                    END
                ) AS highest_difficulty
            """),
            expr("MIN(sequence_number) AS mission_run_start_seqnum"),
            expr("MAX(sequence_number) AS mission_run_end_seqnum"),
            expr("MAX(attempt_counter) AS max_attempts"),
            expr("ROUND(timediff(second, MIN(occurred_on), MAX(occurred_on)) / 60, 2) AS mission_duration"),
            expr("""
                SUM(
                    CASE
                        WHEN difficulty != prev_difficulty THEN 1
                        ELSE 0
                    END
                ) AS difficulty_changes
            """)
        )
    )

    application_transformed_df = (
        application_status_df
        .groupBy(
            expr("player_id"),
            expr("session_id"),
            expr("application_background_instance_id")
        )
        .agg(
            expr("MIN(timestamp(occurred_on)) AS bg_start"),
            expr("MAX(timestamp(occurred_on)) AS bg_end"),
            expr("timediff(second, MIN(timestamp(occurred_on)), MAX(timestamp(occurred_on))) / 60.0 AS bg_duration")
        )
    )

    joined_df = (
        mission_transformed_df.alias("m")
        .join(
            application_transformed_df.alias("a"),
            on=[
                expr("m.player_id = a.player_id"),
                expr("m.session_id = a.session_id")
            ],
            how="left"
        )
        .groupBy(
            expr("m.date"),
            expr("m.player_id"),
            expr("m.platform"),
            expr("m.service"),
            expr("m.country_code"),
            expr("m.session_id"),
            expr("m.mission_run_id"),
            expr("m.mission_num"),
            expr("m.mission_name"),
            expr("m.start_difficulty"),
            expr("m.end_difficulty"),
            expr("m.start_mission_status"),
            expr("m.end_mission_status"),
            expr("m.mission_start_ts"),
            expr("m.mission_end_ts"),
            expr("m.lowest_difficulty"),
            expr("m.highest_difficulty"),
            expr("m.mission_run_start_seqnum"),
            expr("m.mission_run_end_seqnum"),
            expr("m.max_attempts"),
            expr("m.mission_duration"),
            expr("m.difficulty_changes")
        )
        .agg(
            expr("""
                SUM(
                    CASE
                        WHEN a.bg_start >= m.mission_start_ts
                        AND a.bg_end <= m.mission_end_ts
                        THEN a.bg_duration
                        ELSE 0
                    END
                ) AS application_background_duration
            """),
            expr("""
                m.mission_duration - 
                SUM(
                    CASE
                        WHEN a.bg_start >= m.mission_start_ts
                        AND a.bg_end <= m.mission_end_ts
                        THEN a.bg_duration
                        ELSE 0
                    END
                ) AS net_mission_duration
            """)
        )
        .select(
            expr("date"),
            expr("COALESCE(player_id, 'ZZ') as player_id"),
            expr("platform as platform"),
            expr("service as service"),
            expr("COALESCE(country_code, 'ZZ') as country_code"),
            expr("COALESCE(session_id, 'ZZ') as session_id"),
            expr("COALESCE(mission_run_id, 'ZZ') as mission_run_id"),
            expr("mission_num as mission_num"),
            expr("mission_name as mission_name"),
            "start_difficulty",
            "end_difficulty",
            "start_mission_status",
            "end_mission_status",
            "mission_start_ts",
            "mission_end_ts",
            "lowest_difficulty",
            "highest_difficulty",
            expr('mission_run_start_seqnum::int as mission_run_start_seqnum'),
            expr('mission_run_end_seqnum::int as mission_run_end_seqnum'),
            expr('max_attempts::int as max_attempts'),
            "mission_duration",
            "difficulty_changes",
            "application_background_duration",
            "net_mission_duration"
        )
    )


    return joined_df

# COMMAND ----------

def load_fact_player_mission_daily(spark, df, database, environment):
    """
    Load the data into the fact_player_mission_daily table in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        df (DataFrame): The DataFrame to load into the table.
        database (str): The database to load the table into.
        environment (str): The environment to load the table into.
    """

    # Perform the merge operation into the Delta table
    final_table = DeltaTable.forName(spark, f"{database}{environment}.managed.fact_player_mission_daily")

    # get schema of the table
    table_schema = spark.read.table(f"{database}{environment}.managed.fact_player_mission_daily").schema

    # create empty dataframe using the schema
    out_df = spark.createDataFrame([], table_schema)

    # union the incoming dataframe with the empty dataframe, we can set allowMissingColumns to False to be more strict
    df = df.select(
            "*",
            expr("CURRENT_TIMESTAMP() as dw_insert_ts"),
            expr("CURRENT_TIMESTAMP() as dw_update_ts"),
            expr("sha2(concat_ws('|', date, player_id,platform, service, country_code, session_id, mission_run_id, mission_num, mission_name), 256) as merge_key")
        )
    out_df = out_df.unionByName(df, allowMissingColumns=True)

    # dynamically setup merge condition based on the aggregate columns in this database's fact_igm_promos_daily table
    
    agg_cols = ['start_difficulty', 'end_difficulty', 'start_mission_status', 'end_mission_status', 'mission_start_ts', 
    'mission_end_ts', 'lowest_difficulty', 'highest_difficulty', 
    'mission_run_start_seqnum', 'mission_run_end_seqnum', 'max_attempts', 'mission_duration', 
    'difficulty_changes', 'application_background_duration', 'net_mission_duration']

    merge_condition = " OR ".join(f"old.{col_name} <> new.{col_name}" for col_name in agg_cols)

    # create the update column dict
    update_set = {
        "start_difficulty": "COALESCE(old.start_difficulty, new.start_difficulty)",
        "end_difficulty": "COALESCE(new.end_difficulty, old.end_difficulty)",
        "start_mission_status": "COALESCE(old.start_mission_status, new.start_mission_status)",
        "end_mission_status": "COALESCE(new.end_mission_status, old.end_mission_status)",
        "mission_start_ts": "COALESCE(LEAST(old.mission_start_ts, new.mission_start_ts), new.mission_start_ts)",
        "mission_end_ts": "COALESCE(GREATEST(old.mission_end_ts, new.mission_end_ts), new.mission_end_ts)",
        "lowest_difficulty": "COALESCE(LEAST(old.lowest_difficulty, new.lowest_difficulty), new.lowest_difficulty)",
        "highest_difficulty": "COALESCE(GREATEST(old.highest_difficulty, new.highest_difficulty), new.highest_difficulty)",
        "mission_run_start_seqnum": "COALESCE(LEAST(old.mission_run_start_seqnum, new.mission_run_start_seqnum), new.mission_run_start_seqnum)",
        "mission_run_end_seqnum": "COALESCE(GREATEST(old.mission_run_end_seqnum, new.mission_run_end_seqnum), new.mission_run_end_seqnum)",
        "max_attempts": "COALESCE(GREATEST(old.max_attempts, new.max_attempts), new.max_attempts)",
        "mission_duration": "COALESCE(GREATEST(old.mission_duration, new.mission_duration), new.mission_duration)",
        "difficulty_changes": "COALESCE(GREATEST(old.difficulty_changes, new.difficulty_changes), new.difficulty_changes)",
        "application_background_duration": "COALESCE(GREATEST(old.application_background_duration, new.application_background_duration), new.application_background_duration)",
        "net_mission_duration": "COALESCE(GREATEST(old.net_mission_duration, new.net_mission_duration), new.net_mission_duration)",
        "dw_update_ts": "CURRENT_TIMESTAMP()"
    }

    # update_set = {}
    # for col_name in agg_cols:
    #     update_set[f"old.{col_name}"] = f"new.{col_name}"

    # update_set[f"old.dw_update_ts"] = "CURRENT_TIMESTAMP()"

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
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.fact_player_mission_daily (
        date DATE NOT NULL,
        player_id STRING NOT NULL,
        platform STRING NOT NULL,
        service STRING NOT NULL,
        country_code STRING NOT NULL,
        session_id STRING NOT NULL,
        mission_run_id STRING NOT NULL,
        mission_num STRING ,
        mission_name STRING ,
        start_difficulty INT,
        end_difficulty INT,
        start_mission_status STRING,
        end_mission_status STRING,
        mission_start_ts TIMESTAMP,
        mission_end_ts TIMESTAMP,
        lowest_difficulty INT,
        highest_difficulty INT,
        mission_run_start_seqnum INT,
        mission_run_end_seqnum INT,
        max_attempts INT,
        mission_duration DOUBLE,
        difficulty_changes INT,
        application_background_duration DOUBLE,
        net_mission_duration DOUBLE,
        dw_insert_ts TIMESTAMP,
        dw_update_ts TIMESTAMP,
        merge_key STRING
    )
        """

    properties = {
        "delta.feature.allowColumnDefaults": "supported" 
    }

    create_table(spark, sql, properties)
    create_fact_player_mission_daily_view(spark, database)
    return f"Table fact_player_mission_daily created"


# COMMAND ----------

def create_fact_player_mission_daily_view(spark, database):
    """
    Create the fact_player_game_status_daily view in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
        mapping (dict): A dictionary of column mappings.
    """

    sql = f"""
    CREATE VIEW IF NOT EXISTS {database}{environment}.managed_view.fact_player_mission_daily AS (
        SELECT
        *
        from {database}{environment}.managed.fact_player_mission_daily
    )
    """
    spark.sql(sql)

# COMMAND ----------

def run_batch():
    spark = create_spark_session(name=f"{database}")
    create_fact_player_mission_daily(spark, database)
    df1, df2 = extract(environment, database)
    df = transform(df1, df2)
    load_fact_player_mission_daily(spark, df, database, environment)

# COMMAND ----------

run_batch()

# COMMAND ----------


