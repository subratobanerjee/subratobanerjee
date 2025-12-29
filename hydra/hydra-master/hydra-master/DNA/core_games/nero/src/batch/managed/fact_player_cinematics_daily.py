# Databricks notebook source
# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/table_functions

# COMMAND ----------

from datetime import date
from pyspark.sql.functions import expr
from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,Widget Parameters
input_param = dbutils_input_params()
environment = input_param.get('environment', set_environment())
database = 'nero'

# COMMAND ----------

def extract(spark, database, environment, prev_max_date):

    mission_status_df = spark.table(f"{database}{environment}.raw.missionstatus").filter(expr(f"date >= '{prev_max_date}'"))

    player_summary_df = spark.table(f"{database}{environment}.managed.fact_player_summary_ltd")

    return mission_status_df, player_summary_df

# COMMAND ----------

def transform_cutscene_metrics(mission_status_df, player_summary_df):

    df_agg = (
        mission_status_df.alias("ms")
        .join(
            player_summary_df.alias("ltd"),
            expr("ms.playerPublicId = ltd.player_id"),
            "inner"
        )
        .filter(expr("mission_status in ('cutscene_start', 'cutscene_skip', 'cutscene_end')"))
        .groupBy(
            expr("ms.playerPublicId as player_id"),
            expr("ms.sessionId as session_id"),
            expr("ms.mission_run_id"),
            expr("to_date(cast(receivedon as timestamp)) as date"),
            expr("ltd.install_date"),
            expr("ms.mission_num"),
            expr("ms.mission_name"),
            expr("ms.current_checkpoint_name"),
            expr("ms.mission_status")
        )
        .agg(
            expr("COUNT(DISTINCT ms.receivedOn)").alias("qty_cutscene_per_mission")
        )
    )

    result_df = df_agg.select(
        expr("COALESCE(player_id, 'ZZ') as player_id"),
        expr("COALESCE(session_id, 'ZZ') as session_id"),
        expr("COALESCE(mission_run_id, 'ZZ') as mission_run_id"),
        expr("COALESCE(date, to_date('1901-01-01')) as date"),
        expr("COALESCE(install_date, to_date('1901-01-01')) as install_date"),
        expr("mission_num as mission_num"),
        expr("mission_name as mission_name"),
        expr("current_checkpoint_name as current_checkpoint_name"),
        expr("mission_status as mission_status"),
        expr("qty_cutscene_per_mission")
    )
    return result_df


# Databricks notebook source


# COMMAND ----------

def create_fact_player_cinematics_daily(spark, database, environment, properties={}):
    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.fact_player_cinematics_daily (
        date DATE,
        player_id STRING,
        session_id STRING,
        mission_run_id STRING,
        install_date DATE,
        mission_num STRING,
        mission_name STRING,
        current_checkpoint_name STRING,
        mission_status STRING,
        qty_cutscene_per_mission BIGINT,
        merge_key STRING,
        dw_insert_ts TIMESTAMP,
        dw_update_ts TIMESTAMP
    )
    """

    default_properties = {
        "delta.feature.allowColumnDefaults": "supported"
    }
    
    final_properties = {**default_properties, **properties}

    create_table(spark, sql, final_properties)

    create_fact_player_cinematics_daily_view(spark, database)
    
    return f"dbfs:/tmp/{database}/managed/batch/run{environment}/fact_player_cinematics_daily"

# COMMAND ----------

def create_fact_player_cinematics_daily_view(spark, database):
    """
    Create the fact_player_cinematics_daily view in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
        mapping (dict): A dictionary of column mappings.
    """

    sql = f"""
    CREATE VIEW IF NOT EXISTS {database}{environment}.managed_view.fact_player_cinematics_daily AS (
        SELECT
        *
        from {database}{environment}.managed.fact_player_cinematics_daily
    )
    """
    spark.sql(sql)


# COMMAND ----------

def load(df, database, environment, spark):

    target_table_name = f"{database}{environment}.managed.fact_player_cinematics_daily"
    
    target_df = DeltaTable.forName(spark, target_table_name)
    merger_condition = 'target.merge_key = source.merge_key'

    update_expr = {col: f"source.{col}" for col in df.columns if col not in ['dw_insert_ts', 'merge_key']}
    update_expr["dw_update_ts"] = "current_timestamp()"
    
    insert_expr = {col: f"source.{col}" for col in df.columns}

    (target_df.alias("target")
        .merge(df.alias("source"), merger_condition)
        .whenMatchedUpdate(set=update_expr)
        .whenNotMatchedInsert(values=insert_expr)
        .execute())

# COMMAND ----------
def run_batch(database, environment):

    database = database.lower()
    spark = create_spark_session(name=f"{database}_mission_cinematics")
    
    target_table = f"{database}{environment}.managed.fact_player_cinematics_daily"
    create_fact_player_cinematics_daily(spark, database, environment)
    
    prev_max_date = max_timestamp(spark, target_table)
    if prev_max_date is None:
        prev_max_date = date(1999, 1, 1)

    mission_status_df, player_summary_df = extract(spark, database, environment, prev_max_date)

    result_df = transform_cutscene_metrics(mission_status_df, player_summary_df)                                      

    final_df = result_df.withColumn("dw_update_ts", expr("current_timestamp()")) \
                        .withColumn("dw_insert_ts", expr("current_timestamp()")) \
                        .withColumn(
                            "merge_key", 
                            expr("sha2(concat_ws('|', date, player_id, session_id, mission_run_id, install_date, mission_num, mission_name, current_checkpoint_name, mission_status), 256)")
                        )

    load(final_df, database, environment, spark)


run_batch(database, environment)




