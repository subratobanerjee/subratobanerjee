# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/table_functions

# COMMAND ----------

from pyspark.sql.functions import expr
from delta.tables import DeltaTable
from datetime import date

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'nero'


# COMMAND ----------

def extract(environment, database):
    prev_max = max_timestamp(spark, f"{database}{environment}.managed.fact_player_vehicle_daily", 'dw_insert_ts')
    if prev_max is None:
        prev_max = date(1999, 1, 1)
    else:
        prev_max
    print("prev_max is ", prev_max)

    player_activity_df = (
        spark.read
            .table(f"nero{environment}.intermediate.fact_player_activity")
            .where(
                (expr(f"to_date(dw_insert_ts) >= to_date('{prev_max}') - interval 3 days"))
            )
    )

    vehicle_status_df = (
        spark.read
            .table(f"nero{environment}.raw.vehiclestatus")
            .where(
                (expr(f"to_date(insert_ts) >= to_date('{prev_max}') - interval 3 days"))
            )
    )

    return player_activity_df, vehicle_status_df


# COMMAND ----------

def transform(player_activity_df, vehicle_status_df):
    mission_ends_df = (
    	player_activity_df
    	.where(expr("source_table = 'playerstatus' OR source_table = 'missionstatus'"))
    	.groupBy("player_id", "extra_info_3")
    	.agg(expr("CAST(MAX(extra_info_9) AS TIMESTAMP) AS mission_end_ts"))
    	.select(
    	    expr("player_id"),
            expr("extra_info_3 as mission_run_id"),
    	    expr("mission_end_ts")
    	)
	)
    
    journey_embarked_df = (
        vehicle_status_df
        .where(expr("vehicle_status = 'vehicle_embark'"))
        .select(
            expr("receivedOn::timestamp::date AS date"),
            expr("playerpublicId AS player_id"),
            expr("sessionid AS session_id"),
            expr("mission_run_id"),
            expr("journey_id"),
            expr("vehicle_type"),
            expr("vehicle_model"),
            expr("timestamp(occurredOn) AS journey_started_ts"),
            expr("LEAD(timestamp(occurredOn)) OVER (PARTITION BY playerpublicId, sessionid ORDER BY timestamp(occurredOn)) AS next_journey_started_ts"),
            expr("ROW_NUMBER() OVER (PARTITION BY playerpublicId, sessionid, mission_run_id, journey_id, vehicle_type, vehicle_model, occurredOn ORDER BY receivedOn) AS row_num")
        )
        .where(expr("row_num = 1"))
        .distinct()
    )

    journey_disembarked_df = (
        vehicle_status_df
        .where(expr("vehicle_status = 'vehicle_disembark'"))
        .select(
            expr("receivedOn::timestamp::date as date"),
            expr("playerpublicId as player_id"),
            expr("sessionid as session_id"),
            expr("mission_run_id"),
            expr("journey_id"),
            expr("vehicle_type"),
            expr("vehicle_model"),
            expr("occurredOn::timestamp as journey_ended_ts"),
            expr("ROW_NUMBER() OVER (PARTITION BY playerpublicId, sessionid, mission_run_id, journey_id, vehicle_type, vehicle_model, occurredOn ORDER BY receivedOn) AS row_num")
        )
        .where(expr("row_num = 1"))
        .distinct()
    )

    cutscene_start_df = (
    	player_activity_df.alias("a")
    	.join(journey_embarked_df.alias("b"),
        	on=expr("a.player_id = b.player_id AND a.extra_info_3 = b.mission_run_id AND a.extra_info_9 > b.journey_started_ts"))
    	.where(expr("a.source_table = 'missionstatus' AND a.event_trigger = 'cutscene_start'"))
    	.groupBy("a.player_id", "a.extra_info_3", "b.journey_id")
    	.agg(expr("CAST(MIN(a.extra_info_9) AS TIMESTAMP) AS next_cutscene_start_ts"))
    	.select(
        	expr("a.player_id AS player_id"),
        	expr("a.extra_info_3 AS mission_run_id"),
        	expr("b.journey_id AS journey_id"),
        	expr("next_cutscene_start_ts")
    	)
	)


    # Final join and transformation
    final_df = (
    journey_embarked_df.alias("e")
    .join(
        journey_disembarked_df.alias("d"),
        on=expr("e.journey_id = d.journey_id AND e.player_id = d.player_id"),
        how="left"
    )
    .join(
        mission_ends_df.alias("p"),
        on=expr("e.mission_run_id = p.mission_run_id AND e.player_id = p.player_id"),
        how="left"
    )
    .join(
        cutscene_start_df.alias("c"),
        on=expr("e.player_id = c.player_id AND e.mission_run_id = c.mission_run_id AND e.journey_id = c.journey_id"),
        how="left"
    )
    
        .select(
            expr("e.date as date"),
            expr("e.player_id as player_id"),
            expr("e.session_id as session_id"),
            expr("e.mission_run_id as mission_run_id"),
            expr("e.journey_id as journey_id"),
            expr("e.vehicle_type as vehicle_type"),
            expr("e.vehicle_model as vehicle_model"),
            expr("e.journey_started_ts as journey_started_ts"),
            expr("d.journey_ended_ts IS NULL AS missing_disembark"),
            expr("""
                COALESCE(
                    CASE WHEN d.journey_ended_ts > e.journey_started_ts THEN d.journey_ended_ts ELSE NULL END,
                    CASE WHEN e.next_journey_started_ts > e.journey_started_ts and e.next_journey_started_ts < c.next_cutscene_start_ts THEN e.next_journey_started_ts ELSE NULL END,
                    CASE WHEN c.next_cutscene_start_ts > e.journey_started_ts THEN c.next_cutscene_start_ts ELSE NULL END,
                    CASE WHEN p.mission_end_ts > e.journey_started_ts THEN p.mission_end_ts ELSE NULL END
                ) AS adj_journey_ended_ts
            """),
            expr("""
                ROUND(TIMEDIFF(SECOND, e.journey_started_ts, adj_journey_ended_ts) / 60.00, 1) AS journey_duration
            """)
        )
        .distinct()
    )
    output_df=(
        final_df
        .select(
            expr("COALESCE(date, '1901-01-01')").alias("date"),
            expr("COALESCE(player_id, 'ZZ')").alias("player_id"),
            expr("COALESCE(session_id, 'ZZ')").alias("session_id"),
            expr("COALESCE(mission_run_id, 'ZZ')").alias("mission_run_id"),
            expr("COALESCE(journey_id, 'ZZ')").alias("journey_id"),
            expr("vehicle_type").alias("vehicle_type"),
            expr("vehicle_model").alias("vehicle_model"),
            expr("COALESCE(journey_started_ts, TIMESTAMP('1901-01-01 00:00:00'))").alias("journey_started_ts"),
            expr("missing_disembark"),
            expr("adj_journey_ended_ts"),
            expr("journey_duration")
        )
    )
    
    # Final deduplication on all key cols to make sure no duplicates are created in earlier join conditions
    final_df = (
        output_df
        .groupBy(
            "date",
            "player_id",
            "session_id",
            "mission_run_id",
            "journey_id",
            "vehicle_type",
            "vehicle_model",
            "journey_started_ts"
        )
        .agg(
            expr("max(missing_disembark) as missing_disembark"),
            expr("max(adj_journey_ended_ts) as adj_journey_ended_ts"),
            expr("max(journey_duration) as journey_duration")
        )
    )
    
    return final_df


# COMMAND ----------

def load_fact_player_vehicle_daily(spark, df_agg, database, environment):
    """
    Load the data into the fact_player_vehicle_daily table in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        df_agg (DataFrame): The DataFrame to load into the table.
        database (str): The database to load the table into.
        environment (str): The environment to load the table into.
    """

    # Perform the merge operation into the Delta table
    final_table = DeltaTable.forName(spark, f"{database}{environment}.managed.fact_player_vehicle_daily")

    # get schema of the table
    table_schema = spark.read.table(f"{database}{environment}.managed.fact_player_vehicle_daily").schema

    # create empty dataframe using the schema
    out_df = spark.createDataFrame([], table_schema)

    # union the incoming dataframe with the empty dataframe, we can set allowMissingColumns to False to be more strict
    df_agg = df_agg.select(
            "*",
            expr("CURRENT_TIMESTAMP() as dw_insert_ts"),
            expr("CURRENT_TIMESTAMP() as dw_update_ts"),
            expr("sha2(concat_ws('|', date, player_id, session_id, mission_run_id, journey_id,vehicle_type, vehicle_model,journey_started_ts), 256) as merge_key")
        )
    out_df = out_df.unionByName(df_agg, allowMissingColumns=True)

    # dynamically setup merge condition based on the aggregate columns in this database's fact_player_vehicle_daily table
    
    update_cols = ['missing_disembark', 'adj_journey_ended_ts', 'journey_duration']

    merge_condition = " OR ".join(f"old.{col_name} <> new.{col_name}" for col_name in update_cols)

    # create the update column dict
    update_set = {
        "missing_disembark": "COALESCE(new.missing_disembark, old.missing_disembark)",
        "adj_journey_ended_ts": "COALESCE(GREATEST(old.adj_journey_ended_ts, new.adj_journey_ended_ts), new.adj_journey_ended_ts)",
        "journey_duration": "COALESCE(GREATEST(old.journey_duration, new.journey_duration), new.journey_duration)",
        "dw_update_ts": "CURRENT_TIMESTAMP()"
    }

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

def create_fact_player_vehicle_daily(spark, database):

    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.fact_player_vehicle_daily (
        date DATE NOT NULL,
        player_id STRING NOT NULL,
        session_id STRING NOT NULL,
        mission_run_id STRING DEFAULT 'ZZ' NOT NULL,
        journey_id STRING DEFAULT 'ZZ' NOT NULL,
        vehicle_type STRING ,
        vehicle_model STRING ,
        journey_started_ts TIMESTAMP ,
        missing_disembark BOOLEAN,
        adj_journey_ended_ts TIMESTAMP,
        journey_duration DECIMAL(24,1),
        dw_insert_ts TIMESTAMP,
        dw_update_ts TIMESTAMP,
        merge_key STRING
    )
        """

    properties = {
        "delta.feature.allowColumnDefaults": "supported" 
    }

    create_table(spark, sql, properties)
    create_fact_player_vehicle_daily_view(spark, database)
    return f"Table fact_player_vehicle_daily created"


# COMMAND ----------

def create_fact_player_vehicle_daily_view(spark, database):
    """
    Create the fact_player_vehicle_daily view in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
        mapping (dict): A dictionary of column mappings.
    """

    sql = f"""
    CREATE VIEW IF NOT EXISTS {database}{environment}.managed_view.fact_player_vehicle_daily AS (
        SELECT
        *
        from {database}{environment}.managed.fact_player_vehicle_daily
    )
    """
    spark.sql(sql)

# COMMAND ----------

def run_batch():
    spark = create_spark_session(name=f"{database}")
    create_fact_player_vehicle_daily(spark, database)
    player_activity_df, vehicle_status_df = extract(environment, database)
    df_agg = transform(player_activity_df, vehicle_status_df)
    load_fact_player_vehicle_daily(spark, df_agg, database, environment)

# COMMAND ----------

run_batch()
