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
    prev_max = max_timestamp(spark, f"{database}{environment}.managed.fact_player_status_daily", 'dw_insert_ts')
    if prev_max is None:
        prev_max = date(1999, 1, 1)
    else:
        prev_max
    print("prev_max is ", prev_max)

    df_read = (
        spark.read
            .table(f"nero{environment}.raw.playerstatus")
            .where(
                (expr(f"to_date(insert_ts) >= to_date('{prev_max}') - interval 3 days")) &
                (expr("playerPublicId != 'anonymous'")) &
                (expr("mission_run_id IS NOT NULL")) &
                (expr("occurredOn IS NOT NULL")) &
                (expr("receivedOn IS NOT NULL"))
            )
    )
    return df_read

# COMMAND ----------

def transform(df_read):
    prelim_df = (
        df_read.select(
            expr("playerPublicId").alias("player_id"),
            expr("CAST(CAST(receivedOn AS TIMESTAMP) AS DATE)").alias("date"),
            expr("sessionId").alias("session_id"),
            expr("mission_run_id"),
            expr("play_state"),
            expr("TIMESTAMP(occurredOn)").alias("play_state_ts"),
            expr("sequenceNumber").alias("seqnum")
        ).distinct()
    )

    lagged_df = prelim_df.select("*",expr("LEAD(play_state_ts) OVER (PARTITION BY mission_run_id ORDER BY seqnum) AS next_play_state_ts"))

    duration_df = (
        lagged_df
        .where("play_state IS NOT NULL AND next_play_state_ts IS NOT NULL")
        .select(
            expr("date").alias("date"),
            expr("player_id").alias("player_id"),
            expr("mission_run_id").alias("mission_run_id"),
            expr("session_id").alias("session_id"),
            expr("play_state").alias("play_state"),
            expr("""
                CASE 
                    WHEN next_play_state_ts > play_state_ts THEN
                    TIMEDIFF(SECOND, play_state_ts, next_play_state_ts)/60.0
                    ELSE 0
                END
            """).alias("duration_minutes")

        )
    )

    df_agg = (
        duration_df
        .groupBy(
        expr("date"),
        expr("player_id"),
        expr("mission_run_id"),
        expr("session_id"),
        expr("play_state")
        )
        .agg(
        expr("ROUND(SUM(COALESCE(duration_minutes, 0.0)), 2) AS duration_minutes")
        )
    )

    return df_agg


# COMMAND ----------

def load_fact_player_status_daily(spark, df_agg, database, environment):
    """
    Load the data into the fact_player_status_daily table in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        df_agg (DataFrame): The DataFrame to load into the table.
        database (str): The database to load the table into.
        environment (str): The environment to load the table into.
    """

    # Perform the merge operation into the Delta table
    final_table = DeltaTable.forName(spark, f"{database}{environment}.managed.fact_player_status_daily")

    # get schema of the table
    table_schema = spark.read.table(f"{database}{environment}.managed.fact_player_status_daily").schema

    # create empty dataframe using the schema
    out_df = spark.createDataFrame([], table_schema)

    # union the incoming dataframe with the empty dataframe, we can set allowMissingColumns to False to be more strict
    df_agg = df_agg.select(
            "*",
            expr("CURRENT_TIMESTAMP() as dw_insert_ts"),
            expr("CURRENT_TIMESTAMP() as dw_update_ts"),
            expr("sha2(concat_ws('|', mission_run_id, player_id, session_id, play_state, date), 256) as merge_key")
        )
    out_df = out_df.unionByName(df_agg, allowMissingColumns=True)

    # dynamically setup merge condition based on the aggregate columns in this database's fact_player_status_daily table
    
    agg_cols = ['duration_minutes']

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

def create_fact_player_status_daily(spark, database):

    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.fact_player_status_daily (
        date DATE,
        player_id STRING,
        mission_run_id STRING,
        session_id STRING,
        play_state STRING,
        duration_minutes DECIMAL(34,2),
        dw_insert_ts TIMESTAMP,
        dw_update_ts TIMESTAMP,
        merge_key STRING
    )
        """

    properties = {
        "delta.feature.allowColumnDefaults": "supported" 
    }

    create_table(spark, sql, properties)
    create_fact_player_status_daily_view(spark, database)
    return f"Table fact_player_status_daily created"


# COMMAND ----------

def create_fact_player_status_daily_view(spark, database):
    """
    Create the fact_player_status_daily view in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
        mapping (dict): A dictionary of column mappings.
    """

    sql = f"""
    CREATE VIEW IF NOT EXISTS {database}{environment}.managed_view.fact_player_status_daily AS (
        SELECT
        *
        from {database}{environment}.managed.fact_player_status_daily
    )
    """
    spark.sql(sql)

# COMMAND ----------

def run_batch():
    spark = create_spark_session(name=f"{database}")
    create_fact_player_status_daily(spark, database)
    df_read = extract(environment, database)
    df_agg = transform(df_read)
    load_fact_player_status_daily(spark, df_agg, database, environment)

# COMMAND ----------

run_batch()