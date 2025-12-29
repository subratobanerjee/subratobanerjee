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
    prev_max = max_timestamp(spark, f"{database}{environment}.managed.fact_player_mission_status_daily", 'dw_insert_ts')
    if prev_max is None:
        prev_max = date(1999, 1, 1)
    else:
        prev_max
    print("prev_max is ", prev_max)

    df_read = (
        spark.read
            .table(f"nero{environment}.raw.missionstatus")
            .where(
                (expr(f"to_date(insert_ts) >= to_date('{prev_max}') - interval 3 days")) &
                (expr("mission_status_reason IS NOT NULL")) &
                (expr("playerPublicId != 'anonymous'")) &
                (expr("playerPublicId IS NOT NULL"))
            )
    )
    return df_read


# COMMAND ----------

def transform(df_read):
    df_agg = (
        df_read.select(
            expr("CAST(CAST(receivedOn AS TIMESTAMP) AS DATE)").alias("date"),
            expr("playerPublicId").alias("player_id"),
            expr("sessionId").alias("session_id"),
            expr("mission_run_id"),
            expr("mission_num"),
            expr("mission_name"),
            expr("mission_status_reason")
        )
        .groupBy(
            "date",
            "player_id",
            "session_id",
            "mission_run_id",
            "mission_num",
            "mission_name",
            "mission_status_reason"
        )
        .agg(
            expr("count(mission_status_reason) AS mission_status_event_quantity")
        )
    )

    result_df = df_agg.select(
        expr("COALESCE(date, to_date('1901-01-01')) as date"),
        expr("COALESCE(player_id, 'ZZ') as player_id"),
        expr("COALESCE(session_id, 'ZZ') as session_id"),
        expr("COALESCE(mission_run_id, 'ZZ') as mission_run_id"),
        expr("mission_num as mission_num"),
        expr("mission_name as mission_name"),
        expr("mission_status_reason as mission_status_reason"),
        expr("mission_status_event_quantity")
    )

    return result_df


# COMMAND ----------

def load_fact_player_mission_status_daily(spark, df_agg, database, environment):
    """
    Load the data into the fact_player_mission_status_daily table in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        df_agg (DataFrame): The DataFrame to load into the table.
        database (str): The database to load the table into.
        environment (str): The environment to load the table into.
    """

    # Perform the merge operation into the Delta table
    final_table = DeltaTable.forName(spark, f"{database}{environment}.managed.fact_player_mission_status_daily")

    # get schema of the table
    table_schema = spark.read.table(f"{database}{environment}.managed.fact_player_mission_status_daily").schema

    # create empty dataframe using the schema
    out_df = spark.createDataFrame([], table_schema)

    # union the incoming dataframe with the empty dataframe, we can set allowMissingColumns to False to be more strict
    df_agg = df_agg.select(
            "*",
            expr("CURRENT_TIMESTAMP() as dw_insert_ts"),
            expr("CURRENT_TIMESTAMP() as dw_update_ts"),
            expr("sha2(concat_ws('|', date, player_id, session_id, mission_run_id, mission_num, mission_name, mission_status_reason), 256) as merge_key")
        )
    out_df = out_df.unionByName(df_agg, allowMissingColumns=True)

    # dynamically setup merge condition based on the aggregate columns in this database's fact_player_mission_status_daily table
    
    agg_cols = ['mission_status_event_quantity']

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

def create_fact_player_mission_status_daily(spark, database):

    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.fact_player_mission_status_daily (
        date DATE NOT NULL,
        player_id STRING NOT NULL,
        session_id STRING NOT NULL,
        mission_run_id STRING NOT NULL,
        mission_num STRING ,
        mission_name STRING ,
        mission_status_reason STRING ,
        mission_status_event_quantity BIGINT,
        dw_insert_ts TIMESTAMP,
        dw_update_ts TIMESTAMP,
        merge_key STRING
    )
        """

    properties = {
        "delta.feature.allowColumnDefaults": "supported" 
    }

    create_table(spark, sql, properties)
    create_fact_player_mission_status_daily_view(spark, database)
    return f"Table fact_player_mission_status_daily created"


# COMMAND ----------

def create_fact_player_mission_status_daily_view(spark, database):
    """
    Create the fact_player_mission_status_daily view in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
        mapping (dict): A dictionary of column mappings.
    """

    sql = f"""
    CREATE VIEW IF NOT EXISTS {database}{environment}.managed_view.fact_player_mission_status_daily AS (
        SELECT
        *
        from {database}{environment}.managed.fact_player_mission_status_daily
    )
    """
    spark.sql(sql)

# COMMAND ----------

def run_batch():
    spark = create_spark_session(name=f"{database}")
    create_fact_player_mission_status_daily(spark, database)
    df_read = extract(environment, database)
    df_agg = transform(df_read)
    load_fact_player_mission_status_daily(spark, df_agg, database, environment)

# COMMAND ----------

run_batch()