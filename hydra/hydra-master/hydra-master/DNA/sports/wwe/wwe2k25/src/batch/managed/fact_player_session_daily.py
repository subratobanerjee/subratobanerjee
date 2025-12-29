# Databricks notebook source
# MAGIC %run ../../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../../utils/helpers


# COMMAND ----------
# MAGIC %run ../../../../../../../utils/ddl/table_functions

# COMMAND ----------
from pyspark.sql.functions import expr
from delta.tables import DeltaTable

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'wwe2k25'
data_source ='dna'
# title = 'PGA Tour 2K25'

view_mapping = {
    'session_count': 'session_count',
    'session_len_sec': 'session_len_sec',
    'session_avg_len_sec': 'session_avg_len_sec',
    'session_mode_len_sec': 'session_mode_len_sec',
    'agg_1': 'agg_1 as session_median_len_sec'
}
agg_columns = {
    "session_count": expr("count(distinct application_session_id) as session_count"),
    "session_len_sec": expr( "sum(UNIX_TIMESTAMP(SESSION_END_TS) - UNIX_TIMESTAMP(SESSION_START_TS)) as session_len_sec"),
    "session_avg_len_sec": expr( "avg(UNIX_TIMESTAMP(SESSION_END_TS) - UNIX_TIMESTAMP(SESSION_START_TS)) as session_avg_len_sec"),
    "session_mode_len_sec": expr("mode(UNIX_TIMESTAMP(SESSION_END_TS) - UNIX_TIMESTAMP(SESSION_START_TS)) as session_mode_len_sec"),
    "agg_1": expr("median(UNIX_TIMESTAMP(SESSION_END_TS) - UNIX_TIMESTAMP(SESSION_START_TS)) as agg_1")
    }

# COMMAND ----------
def read_player_session(environment,min_received_on):

    player_session_df = (spark
                         .read
                         .table(f"{database}{environment}.intermediate.fact_player_session")
                         .where((expr(f"dw_insert_ts::timestamp::date > to_date('{min_received_on}') - INTERVAL 2 DAY")))
                        )

    return player_session_df

def extract(environment, database):
    prev_max_ts = max_timestamp(spark, f"{database}{environment}.managed.fact_player_session_daily", "date")
    min_received_on = spark.sql(f"select ifnull(min(received_on), '1970-10-01')::date as min_received_on from {database}{environment}.intermediate.fact_player_activity where dw_insert_ts >= current_date - interval '2' day").collect()[0]['min_received_on']
    min_received_on_inc = min(prev_max_ts, min_received_on)

    player_sesion_df = read_player_session(environment, min_received_on_inc)
    return player_sesion_df


# COMMAND ----------

def transform(df,agg_columns):
    out_df = (
        df
        .groupBy(
            expr("session_end_ts::date as date"),
            "player_id",
            "platform",
            "service",
            "country_code",
            "session_type"
        )
        .agg(*agg_columns.values())
    )

    return out_df

# COMMAND ----------

def create_wwe_fact_player_session_daily(spark, database, view_mapping, properties={}):
    
    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.fact_player_session_daily (
        date DATE,
        player_id STRING,
        platform STRING,
        service STRING,
        country_code STRING,
        session_type STRING,
        session_count int ,
        session_len_sec int ,
        session_avg_len_sec int ,
        session_mode_len_sec int ,
        agg_1 INT DEFAULT -1,
        dw_insert_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        dw_update_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        merge_key STRING
    )
    """

    properties = {
        "delta.feature.allowColumnDefaults": "supported" # this won't work until iceberg v3 is released
    }

    create_table(spark, sql, properties)
    create_wwe_fact_player_session_daily_view(spark, database, view_mapping)
    return f"dbfs:/tmp/{database}/managed/streaming/run{environment}/fact_player_session_daily"

# COMMAND ----------

def create_wwe_fact_player_session_daily_view(spark, database, mapping):

    sql = f"""
    CREATE OR REPLACE VIEW {database}{environment}.managed_view.fact_player_session_daily AS (
        SELECT
            date,
            player_id,
            platform,
            service,
            country_code,
            session_type,
            {','.join(str(mapping[key]) for key in mapping)},
            dw_insert_ts,
            dw_update_ts
        from {database}{environment}.managed.fact_player_session_daily
    )
    """

    spark.sql(sql)

# COMMAND ----------

def load_wwe_fact_player_session_daily(spark, df, agg_columns, database, environment):

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
            "SHA2(CONCAT_WS('|', DATE, PLAYER_ID, PLATFORM, SERVICE, COUNTRY_CODE, SESSION_TYPE), 256) as merge_key"
        )
    out_df = out_df.unionByName(df, allowMissingColumns=True)

    # dynamically setup merge condition based on the aggregate columns in this database's fact_player_session_daily table
    agg_cols = [col_name for col_name in out_df.columns if 'agg_' in col_name]
    merge_condition = " OR ".join(f"old.{col_name} <> new.{col_name}" for col_name in agg_cols)

    # create the update column dict
    update_set = {}
    for col_name in agg_cols:
        col_value = agg_columns[f"{col_name}"]
        if str(col_value).lower().startswith("column<'sum(") | str(col_value).lower().startswith("column<'count("):
            update_set[f"old.{col_name}"] = f"greatest(new.{col_name}, old.{col_name})"
        else:
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


def run_batch():
    checkpoint_location = create_wwe_fact_player_session_daily(spark, database, view_mapping)

    df = extract(environment,database)
    df = transform(df,agg_columns)

    load_wwe_fact_player_session_daily(spark, df, agg_columns, database, environment)

# COMMAND ----------

run_batch()