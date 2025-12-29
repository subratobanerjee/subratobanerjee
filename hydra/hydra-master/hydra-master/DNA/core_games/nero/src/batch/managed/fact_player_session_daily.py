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
title = 'Mafia: The Old Country'


agg_columns = {
    "session_count": expr("count(distinct st.session_id) as session_count"),
    "session_len_sec": expr("sum(total_seconds_session_played) as session_len_sec"),
    "session_avg_len_sec": expr("sum(total_seconds_session_played) / session_count as session_avg_len_sec"),
    "session_mode_len_sec": expr("mode(total_seconds_session_played) as session_mode_len_sec"),
    "agg_1": expr("median(total_seconds_session_played) as agg_1"),
    "agg_2": expr("SUM(ifnull(bg_duration, 0)) as agg_2"),
    "agg_3": expr("(sum(total_seconds_session_played) - SUM(ifnull(bg_duration, 0))) as agg_3"),
    "agg_4": expr("agg_2 / session_count as agg_4"),
    "agg_5": expr("agg_3 / session_count as agg_5")
    }

# COMMAND ----------

def read_session(environment):

    prev_max = max_timestamp(spark, f"{database}{environment}.managed.fact_player_session_daily", 'dw_insert_ts')
    
    current_min = (
        spark.read
        .table(f"nero{environment}.intermediate.fact_player_activity")
        .where(expr("dw_insert_ts::date between current_date - 3 and current_date"))
        .select(expr(f"ifnull(min(received_on::timestamp::date),'1999-01-01')::date as min_date"))
    ).collect()[0]['min_date']

    inc_min_date = min(prev_max,current_min)

    sesh_df = (
        spark
        .read
        .table(f"nero{environment}.intermediate.fact_player_activity")
        .where(expr(
            f"source_table <> 'loginevent' and player_id !='anonymous' and player_id is not null and player_id NOT LIKE '%NULL%' and received_on::date >= to_date('{inc_min_date}') - INTERVAL 3 DAY"))
        .groupby("player_id", "session_id", "platform", "service","country_code")
        .agg(
             expr("min(received_on) AS session_start_ts"),
             expr("max(received_on) AS session_end_ts"))
    )
    return sesh_df

def read_app_session(environment):
    app_session_df = (
        spark
        .read
        .table(f"nero{environment}.raw.applicationstatus")
        .where(expr(
            "application_session_status in ('application_background', 'application_foreground') and application_background_instance_id is not null and playerpublicid !='anonymous' and playerpublicid is not null"))
        .groupby("playerPublicId", "sessionId", "application_background_instance_id")
        .agg(
             expr("timediff(second, MIN(timestamp(receivedOn)), MAX(timestamp(receivedOn)) ) AS bg_duration"))
    )
    return app_session_df



# COMMAND ----------

def extract(environment):


    sesh_df = read_session(environment)
    app_session_df = read_app_session(environment)

    return sesh_df,app_session_df

# COMMAND ----------

def transform(sesh_df,app_session_df,agg_columns):
    total_session_df = (
        sesh_df
        .groupby("player_id", "session_id", "platform", "service","country_code",expr("session_start_ts::date"), expr("session_end_ts::date"))
        .agg(
             expr("timediff(second, min(session_start_ts), max(session_end_ts)) AS total_seconds_session_played"))
    )

    agg_session_time_df =(
        app_session_df
        .groupBy(expr("playerPublicId as player_id"),expr("sessionId as session_id"))
        .agg(expr("sum(bg_duration) as bg_duration")
        )
    )
    out_df = (
        total_session_df.alias("st")
        .join(agg_session_time_df.alias("ast"), on=expr("st.player_id = ast.player_id and st.session_id= ast.session_id"), how="left")
        .groupBy(
            expr("session_start_ts::date as session_start_date"),
            expr("session_end_ts::date as session_end_date"),
            "st.player_id",
            "st.platform",
            "st.service",
            "st.country_code",
            expr("'Unknown' as session_type")
        )
        .agg(*agg_columns.values())
    )

    return out_df

# COMMAND ----------

def create_nero_fact_player_session_daily(spark, database, properties={}):
    
    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.fact_player_session_daily (
        session_start_date DATE NOT NULL,
        session_end_date DATE NOT NULL,
        player_id STRING NOT NULL,
        platform STRING NOT NULL,
        service STRING NOT NULL,
        country_code STRING NOT NULL,
        session_type STRING NOT NULL,
        session_count int,
        session_len_sec int ,
        session_avg_len_sec int ,
        session_mode_len_sec int ,
        agg_1 INT ,
        agg_2 INT ,
        agg_3 INT ,
        agg_4 INT ,
        agg_5 INT ,
        dw_insert_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        dw_update_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        merge_key STRING
    )
    """

    properties = {
        "delta.feature.allowColumnDefaults": "supported" # this won't work until iceberg v3 is released
    }

    create_table(spark, sql, properties)
    create_nero_fact_player_session_daily_view(spark, database)
    return f"dbfs:/tmp/{database}/managed/batch/run{environment}/fact_player_session_daily"

# COMMAND ----------

def create_nero_fact_player_session_daily_view(spark, database):

    sql = f"""
    CREATE  VIEW IF NOT EXISTS {database}{environment}.managed_view.fact_player_session_daily AS (
        SELECT
            session_start_date,
            session_end_date,
            player_id,
            platform,
            service,
            country_code,
            session_count as total_sessions,
            round(session_len_sec/60,2) as total_session_minutes,
            round(session_avg_len_sec/60,2) as avg_session_minutes,
            round(session_mode_len_sec/60,2) as mode_session_minutes,
            round(agg_1/60,2) as median_session_minutes,
            round(agg_2/60,2) as total_background_minutes,
            round(agg_3/60,2) as total_foreground_minutes,
            round(agg_4/60,2) as average_background_minutes,
            round(agg_5/60,2) as average_foreground_minutes,
            dw_insert_ts,
            dw_update_ts
        from {database}{environment}.managed.fact_player_session_daily
    )
    """

    spark.sql(sql)

# COMMAND ----------

def load_nero_fact_player_session_daily(spark, df, agg_columns, database, environment):

    # Perform the merge operation into the Delta table
    final_table = DeltaTable.forName(spark, f"{database}{environment}.managed.fact_player_session_daily")

    # get schema of the table
    table_schema = spark.read.table(f"{database}{environment}.managed.fact_player_session_daily").schema

    # create empty dataframe using the schema
    out_df = spark.createDataFrame([], table_schema)

    # union the incoming dataframe with the empty dataframe, we can set allowMissingColumns to False to be more strict
    df = df.selectExpr(
            "*",
            "CURRENT_TIMESTAMP() as dw_insert_ts",
            "CURRENT_TIMESTAMP() as dw_update_ts",
            "SHA2(CONCAT_WS('|', session_start_date, session_end_date, PLAYER_ID, PLATFORM, SERVICE, COUNTRY_CODE, SESSION_TYPE), 256) as merge_key"
        )
    out_df = out_df.unionByName(df, allowMissingColumns=True)

    # dynamically setup merge condition based on the aggregate columns in this database's fact_player_session_daily table
    agg_cols = ['session_count','session_len_sec','session_avg_len_sec','session_mode_len_sec','agg_1','agg_2','agg_3','agg_4','agg_5']
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
    checkpoint_location = create_nero_fact_player_session_daily(spark, database)

    df1,df2 = extract(environment)
    df = transform(df1,df2,agg_columns)

    load_nero_fact_player_session_daily(spark, df, agg_columns, database, environment)

# COMMAND ----------

run_batch()
