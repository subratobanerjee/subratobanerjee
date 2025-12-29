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

def read_cqc_status(spark,environment):
    prev_max = max_timestamp(spark, f"{database}{environment}.managed.fact_player_cqc_daily", 'date')

    current_min = (
        spark.read
        .table(f"nero{environment}.raw.cqcstatus")
        .where(expr("insert_ts::date between current_date - 3 and current_date"))
        .select(expr(f"ifnull(min(receivedOn::timestamp::date),'1999-01-01')::date as min_date"))
    ).collect()[0]['min_date']

    inc_min_date = min(prev_max,current_min)
    
    return (
        spark
        .read
        .table(f"nero{environment}.raw.cqcstatus")
        .where(
               (expr("mission_run_id IS NOT NULL AND playerPublicId!='anonymous' AND mission_run_id != ''")) &
               (expr(f"receivedOn::timestamp::date >= '{inc_min_date}'::date"))
        )
        .select(
            expr("receivedOn::timestamp::date as date"),
            expr("playerPublicId as player_id"),
            expr("sessionId as session_id"),
            expr("COALESCE(mission_run_id,'ZZ') AS mission_run_id"),
            expr("cqc_name AS cqc_name"),
            expr("COALESCE(enemy_health, 'ZZ') AS enemy_health"),
            expr("cqc_end_reason AS cqc_end_reason"),
            expr("receivedOn::timestamp as received_on"),
            expr("occurredOn::timestamp as occurred_on")
    )
 )
    

# COMMAND ----------

def extract(environment, database):
    cqc_status_df = read_cqc_status(spark, environment)
    return cqc_status_df

# COMMAND ----------

def transform(cqc_status_df):
    return (
        cqc_status_df
        .groupBy(
            "date",
            "player_id",
            "session_id",
            "mission_run_id",
            "cqc_name"
        )
        .agg(
            expr("MIN(received_on::timestamp) AS min_received_ts"),
            expr(
                "MIN(occurred_on::timestamp) AS cqc_mission_run_start"),
            expr(
                "MAX(occurred_on::timestamp) AS cqc_mission_run_end"),
            expr(
                '''
                COUNT(CASE WHEN cqc_end_reason = 'cqc_failed' THEN 1 END) AS cqc_failed
                '''),
            expr(
                '''
                COUNT(CASE WHEN cqc_end_reason = 'cqc_completed' THEN 1 END) AS cqc_completed
                '''),
            expr(
                '''
                 MIN(COALESCE(CASE WHEN cqc_end_reason = 'cqc_failed' THEN enemy_health END,'ZZ')) AS cqc_closest_fail
                ''')
        )

    )

# COMMAND ----------

def load_fact_player_cqc_daily(spark, df, database, environment):
    """
    Load the data into the fact_player_cqc_daily table in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        df (DataFrame): The DataFrame to load into the table.
        database (str): The database to load the table into.
        environment (str): The environment to load the table into.
    """

    # Perform the merge operation into the Delta table
    final_table = DeltaTable.forName(spark, f"{database}{environment}.managed.fact_player_cqc_daily")

    # get schema of the table
    table_schema = spark.read.table(f"{database}{environment}.managed.fact_player_cqc_daily").schema

    # create empty dataframe using the schema
    out_df = spark.createDataFrame([], table_schema)

    # union the incoming dataframe with the empty dataframe, we can set allowMissingColumns to False to be more strict
    df = df.select(
            "*",
            expr("CURRENT_TIMESTAMP() as dw_insert_ts"),
            expr("CURRENT_TIMESTAMP() as dw_update_ts"),
            expr("sha2(concat_ws('|', date, player_id, session_id, mission_run_id, cqc_name), 256) as merge_key")
        )
    out_df = out_df.unionByName(df, allowMissingColumns=True)

    # merge the table
    (
        final_table
        .alias("target")
        .merge(
            out_df.alias("src"),
            "target.merge_key = src.merge_key")
        .withSchemaEvolution()
        .whenMatchedUpdate(set={
            "target.min_received_ts": "least(src.min_received_ts,target.min_received_ts)",
            "target.cqc_mission_run_start": "least(src.cqc_mission_run_start,target.cqc_mission_run_start)",
            "target.cqc_mission_run_end": "greatest(src.cqc_mission_run_end,target.cqc_mission_run_end)",
            "target.cqc_failed": "src.cqc_failed",
            "target.cqc_completed": "src.cqc_completed",
            "target.cqc_closest_fail": "src.cqc_closest_fail",
            "target.dw_update_ts": "current_timestamp()"
        })
        .whenNotMatchedInsertAll()
        .execute()
    )





# COMMAND ----------

def create_fact_player_cqc_daily(spark, database):

    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.fact_player_cqc_daily (
        date DATE NOT NULL,
        player_id STRING NOT NULL,
        session_id STRING NOT NULL,
        mission_run_id STRING NOT NULL,
        cqc_name STRING NOT NULL,
        min_received_ts TIMESTAMP,
        cqc_mission_run_start TIMESTAMP,
        cqc_mission_run_end TIMESTAMP,
        cqc_failed INT,
        cqc_completed INT,
        cqc_closest_fail STRING,
        dw_insert_ts TIMESTAMP,
        dw_update_ts TIMESTAMP,
        merge_key STRING
    )
        """

    properties = {
        "delta.feature.allowColumnDefaults": "supported" 
    }

    create_table(spark, sql, properties)
    create_fact_player_cqc_daily_view(spark, database)
    return f"Table fact_player_cqc_daily created"


# COMMAND ----------

def create_fact_player_cqc_daily_view(spark, database):
    """
    Create the fact_player_cqc_status_daily view in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
        mapping (dict): A dictionary of column mappings.
    """

    sql = f"""
    CREATE VIEW IF NOT EXISTS {database}{environment}.managed_view.fact_player_cqc_daily AS (
        SELECT
        *
        from {database}{environment}.managed.fact_player_cqc_daily
    )
    """
    spark.sql(sql)

# COMMAND ----------

def run_batch():
    spark = create_spark_session(name=f"{database}")
    create_fact_player_cqc_daily(spark, database)
    cqc_status_df = extract(environment, database)
    transformed_df = transform(cqc_status_df)
    load_fact_player_cqc_daily(spark, transformed_df, database, environment)

# COMMAND ----------

run_batch()
