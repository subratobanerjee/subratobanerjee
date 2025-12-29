# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/table_functions

# COMMAND ----------

from pyspark.sql.functions import expr, coalesce
from delta.tables import DeltaTable

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'nero'
data_source ='dna'
title = "'Mafia: The Old Country'"
view_mapping = {
    'agg_game_status_1': 'agg_game_status_1 as minutes_played_sub_mode',
    'agg_game_status_2': 'agg_game_status_2 as minutes_played_mode',
    'agg_gp_1': 'agg_gp_1 as total_minutes_played',
    'agg_gp_2': 'agg_gp_2 as pct_minutes_played_mode',
    'agg_gp_3': 'agg_gp_3 as pct_minutes_played_sub_mode',
    'agg_gp_4': 'agg_gp_4 as total_missions',
    'agg_gp_5': 'agg_gp_5 as total_sessions'
}



# COMMAND ----------

def read_title(environment):
    title_df = (
        spark
        .read
        .table(f"reference{environment}.title.dim_title")
        .where(expr(
            "TITLE = 'Mafia: The Old Country'"))
        .select(
            expr("display_platform as platform"),
            expr("display_service as service"),
            "app_id"
        )

    )
    return title_df

# COMMAND ----------

def read_mission_status(environment):
    title_df = read_title(environment)

    prev_max = max_timestamp(spark, f"{database}{environment}.managed.fact_player_game_status_daily", 'dw_insert_ts')

    current_min = (
        spark.read
        .table(f"nero{environment}.raw.missionstatus")
        .where(expr("insert_ts::date between current_date - 3 and current_date"))
        .select(expr(f"ifnull(min(receivedOn::timestamp::date),'1999-01-01')::date as min_date"))
    ).collect()[0]['min_date']

    inc_min_date = min(prev_max,current_min)
        
    mission_df = (
        spark
        .read
        .table(f"nero{environment}.raw.missionstatus").alias("ms")
        .join(title_df.alias("t"), on=expr("t.app_id = ms.appPublicId"), how="left")
        .where(expr(f"playerpublicid!= 'anonymous' and playerpublicid is not null and to_date(insert_ts) >= to_date('{inc_min_date}')"))
        .select(
            expr("playerpublicid as player_id"),
            "platform",
            "service",
            expr("sessionid as session_id"),
            expr("COALESCE(countrycode,'ZZ') as country_code"),
            expr("difficulty as difficulty"),
            expr("substring_index(mission_name, '.', 2) as mode"),
            expr("mission_name as sub_mode"),
            expr("receivedon::timestamp as received_on")
        )
        .distinct()
    )
    return mission_df

# COMMAND ----------

def read_mode_session_status(environment):
    title_df = read_title(environment)

    prev_max = max_timestamp(spark, f"{database}{environment}.managed.fact_player_game_status_daily", 'dw_insert_ts')

    current_min = (
        spark.read
        .table(f"nero{environment}.raw.modesessionstatus")
        .where(expr("insert_ts::date between current_date - 3 and current_date"))
        .select(expr(f"ifnull(min(receivedOn::timestamp::date),'1999-01-01')::date as min_date"))
    ).collect()[0]['min_date']

    inc_min_date = min(prev_max,current_min)

    mode_session_df = (
        spark
        .read
        .table(f"nero{environment}.raw.modesessionstatus").alias("mss")
        .join(title_df.alias("t"), on=expr("t.app_id = mss.appPublicId"), how="left")
        .where(expr(
            f"playerpublicid!= 'anonymous' and playerpublicid is not null and to_date(insert_ts) >= to_date('{inc_min_date}')"))
        .select(
            expr("playerpublicid as player_id"),
            "platform",
            "service",
            expr("sessionid as session_id"),
            expr("COALESCE(countrycode,'ZZ') as country_code"),
            expr("difficulty as difficulty"),
            expr("gameplay_type as mode"),
            expr("sub_gameplay_type as sub_mode"),
            expr("receivedon::timestamp as received_on")
        )
        .distinct()
    )
    return mode_session_df


# COMMAND ----------

def extract(environment):
    mission_df = read_mission_status(environment)
    mode_session_df = read_mode_session_status(environment)
    extract_df = mission_df.union(mode_session_df)

    return extract_df

# COMMAND ----------

def transform(df):

    mode_session_status_df =(
        df
        .groupBy("player_id","platform","service","country_code",expr("received_on::date"),"difficulty","mode","sub_mode","session_id")
        .agg(
            expr("round(timediff(second, MIN(received_on), MAX(received_on))/60,2) AS minutes_played")
        )
        .alias("mss")
    )

    total_time_df =(
        mode_session_status_df
        .groupBy("player_id","platform","service","country_code",expr("received_on::date"))
        .agg(
            expr("round(sum(DISTINCT minutes_played), 2) AS total_minutes_played")
        )
        .alias("tt")
    )
    total_time_per_mode_df=(
        mode_session_status_df
        .groupBy("player_id", "platform", "service", "country_code","mode", expr("received_on::date"))
        .agg(
            expr("round(sum(DISTINCT minutes_played), 2) AS minutes_played_mode")
        )
        .alias("ttm")
    )
    total_time_per_sub_mode_df = (
        mode_session_status_df
        .groupBy("player_id", "platform", "service", "country_code", "sub_mode", expr("received_on::date"))
        .agg(
            expr("round(sum(DISTINCT minutes_played), 2) AS minutes_played_sub_mode")
        )
        .alias("ttmss")
    )
    output_df=(
        mode_session_status_df.alias("mss")
        .join(total_time_df.alias("tt"), on=expr("mss.player_id = tt.player_id and mss.received_on::date = tt.received_on"), how="left")
        .join(total_time_per_mode_df.alias("ttm"),
              on=expr("mss.player_id = ttm.player_id and mss.received_on::date = ttm.received_on and mss.mode = ttm.mode"), how="left")
        .join(total_time_per_sub_mode_df.alias("ttmss"),
              on=expr(
                  "mss.player_id = ttmss.player_id and mss.received_on::date = ttmss.received_on and mss.sub_mode = ttmss.sub_mode"),
              how="left")
        .groupBy("mss.player_id", "mss.platform", "mss.service", "mss.country_code",expr("mss.mode as game_mode"), "mss.sub_mode", expr("mss.received_on::date as date"),expr("'Unused' as extra_grain_1"),expr("'Unused' as extra_grain_2"))
        .agg(
            expr("ifnull(sum(distinct minutes_played_sub_mode), 0) as agg_game_status_1"),
            expr("ifnull(sum(distinct ttm.minutes_played_mode), 0) AS agg_game_status_2"),
            expr("ifnull(sum(distinct tt.total_minutes_played), 0) as agg_gp_1"),
            expr('''ifnull(
             ROUND((sum(DISTINCT ttm.minutes_played_mode) / sum(DISTINCT tt.total_minutes_played)), 2), 0
            )
            * 100 as agg_gp_2
            '''
             ),
            expr('''ifnull(
            ROUND((sum(Distinct ttmss.minutes_played_sub_mode) / sum(DISTINCT ttm.minutes_played_mode)), 2),0)
              * 100 as agg_gp_3
            '''),
            expr("count(DISTINCT mss.sub_mode) as agg_gp_4"),
            expr("count(DISTINCT mss.session_id) AS agg_gp_5")

        )
        .where(expr("agg_game_status_2 != 0 and agg_game_status_1 != 0"))

    )


    return output_df

# COMMAND ----------

def create_nero_fact_player_game_status_daily(spark, database, view_mapping, properties={}):
 
    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.fact_player_game_status_daily (
        date DATE NOT NULL,
        player_id STRING NOT NULL,
        platform STRING NOT NULL,
        service STRING NOT NULL,
        game_mode STRING DEFAULT 'Unused' NOT NULL,
        sub_mode STRING DEFAULT 'Unused' NOT NULL,
        country_code STRING DEFAULT 'ZZ' NOT NULL,
        extra_grain_1 STRING DEFAULT 'Unused' NOT NULL,
        extra_grain_2 STRING DEFAULT 'ZZ' NOT NULL,
        agg_game_status_1 decimal(38,2) ,
        agg_game_status_2 decimal(38,2) ,
        agg_gp_1 decimal(38,2) ,
        agg_gp_2 decimal(38,2) ,
        agg_gp_3 decimal(38,2) ,
        agg_gp_4 decimal(38,2) ,
        agg_gp_5 decimal(38,2) ,
        dw_insert_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        dw_update_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        merge_key STRING
    )
    """

    properties = {
        "delta.feature.allowColumnDefaults": "supported" # this won't work until iceberg v3 is released
    }

    create_table(spark, sql, properties)
    create_nero_fact_player_game_status_daily_view(spark, database, view_mapping)
    return f"dbfs:/tmp/{database}/managed/streaming/run{environment}/fact_player_game_status_daily"



# COMMAND ----------

def create_nero_fact_player_game_status_daily_view(spark, database, mapping):

    sql = f"""
    CREATE view if not exists {database}{environment}.managed_view.fact_player_game_status_daily AS (
        SELECT
            date,
            player_id,
            platform,
            service,
            game_mode,
            sub_mode,
            country_code,
            {','.join(str(mapping[key]) for key in mapping)}
        from {database}{environment}.managed.fact_player_game_status_daily
    )
    """

    spark.sql(sql)

# COMMAND ----------

def load_nero_fact_player_game_status_daily(spark, df, database, environment):

    # Perform the merge operation into the Delta table
    final_table = DeltaTable.forName(spark, f"{database}{environment}.managed.fact_player_game_status_daily")

    # get schema of the table
    table_schema = spark.read.table(f"{database}{environment}.managed.fact_player_game_status_daily").schema

    # create empty dataframe using the schema
    out_df = spark.createDataFrame([], table_schema)

    # union the incoming dataframe with the empty dataframe, we can set allowMissingColumns to False to be more strict
    df = df.selectExpr(
            "*",
            "CURRENT_TIMESTAMP() as dw_insert_ts",
            "CURRENT_TIMESTAMP() as dw_update_ts",
            "SHA2(CONCAT_WS('|', DATE, PLAYER_ID, PLATFORM, SERVICE, GAME_MODE, SUB_MODE, COUNTRY_CODE, EXTRA_GRAIN_1, EXTRA_GRAIN_2), 256) as merge_key"
        )
    out_df = out_df.unionByName(df, allowMissingColumns=True)

    # dynamically setup merge condition based on the aggregate columns in this database's fact_player_session_daily table
    agg_cols = [col_name for col_name in out_df.columns if 'agg_' in col_name]
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
    checkpoint_location = create_nero_fact_player_game_status_daily(spark, database, view_mapping)

    df = extract(environment)
    df = transform(df)

    load_nero_fact_player_game_status_daily(spark, df, database, environment)

# COMMAND ----------

run_batch()
