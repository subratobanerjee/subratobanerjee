# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/managed/agg_game_settings_daily

# COMMAND ----------

from pyspark.sql.functions import expr, when

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment', set_environment())
database = 'bluenose'
data_source = 'dna'
view_mapping = {}

def read_fact_player_game_settings_daily_df(environment, spark):
    transaction_filter = "player_id NOT ILIKE '%Null%' AND player_id is not null AND player_id != 'anonymous'"
    inc_filter = f"{transaction_filter} and (dw_insert_ts::date between current_date - 2 and current_date)"
    prev_max = max_timestamp(spark, f"{database}{environment}.managed.fact_player_game_settings_daily", 'dw_insert_ts')
    prev_max = str(prev_max)
    current_min = (
        spark.read
        .table(f"{database}{environment}.managed.fact_player_game_settings_daily")
        .where(expr(inc_filter))
        .select(expr(f"ifnull(min(date), '1999-01-01') as min_date"))
    ).collect()[0]['min_date']

    if current_min is None:
        current_min = prev_max

    inc_min_date = min(prev_max, current_min)

    batch_filter = f"({transaction_filter}) and date >= '{inc_min_date}'::date"

    title_df = (
        spark.read
        .table(f"{database}{environment}.managed.fact_player_game_settings_daily")
        .where(expr(batch_filter))
    )

    return title_df




def transform(spark, database, environment):
    title_df = read_fact_player_game_settings_daily_df(environment, spark)
    
    result_df = (
        title_df
        .groupBy("date", "platform", "service")
        .agg(
            expr("count(distinct setting_instance_id) as agg_settings_change_count"),
            expr("count(distinct CASE WHEN is_first_seen_player_setting THEN setting_instance_id ELSE NULL END) as agg_new_settings_count"),
            expr("""
                count(distinct CASE WHEN setting_name = 'swing_input' 
                    AND player_current_setting_value = '0' 
                    AND is_first_seen_player_setting THEN setting_instance_id 
                    ELSE NULL END) 
                as agg_new_swing_stick_settings_count
            """),
            expr("""
                count(distinct CASE WHEN setting_name = 'swing_input' 
                    AND player_current_setting_value = '1' 
                    AND is_first_seen_player_setting THEN setting_instance_id 
                    ELSE NULL END) 
                as agg_new_three_clicks_settings_count
            """),
            expr("""
                count(distinct CASE WHEN setting_name = 'swing_input' 
                    AND player_current_setting_value = '0' 
                    AND is_first_seen_player_setting = False 
                    AND player_current_setting_value != player_previous_setting_value 
                    THEN setting_instance_id 
                    ELSE NULL END) 
                as agg_swing_stick_settings_change_count
            """),
            expr("""
                count(distinct CASE WHEN setting_name = 'swing_input' 
                    AND player_current_setting_value = '1' 
                    AND is_first_seen_player_setting = False 
                    AND player_current_setting_value != player_previous_setting_value 
                    THEN setting_instance_id 
                    ELSE NULL END) 
                as agg_three_clicks_settings_change_count
            """),
            expr("""
                count(distinct CASE WHEN setting_name = 'swing_input' 
                    AND player_current_setting_value = '0' 
                    AND is_first_seen_player_setting = False 
                    AND player_current_setting_value != player_previous_setting_value 
                    THEN player_id 
                    ELSE NULL END) 
                as agg_swing_stick_settings_change_player_count
            """),
            expr("""
                count(distinct CASE WHEN setting_name = 'swing_input' 
                    AND player_current_setting_value = '1' 
                    AND is_first_seen_player_setting = False 
                    AND player_current_setting_value != player_previous_setting_value 
                    THEN player_id 
                    ELSE NULL END) 
                as agg_three_clicks_settings_change_player_count
            """)
        )
    ).select(
        "date", "platform", "service",
        "agg_settings_change_count", "agg_new_settings_count", "agg_new_swing_stick_settings_count", "agg_new_three_clicks_settings_count", 
        "agg_swing_stick_settings_change_count", "agg_three_clicks_settings_change_count", 
        "agg_swing_stick_settings_change_player_count", "agg_three_clicks_settings_change_player_count",
        expr("current_timestamp() as dw_insert_ts"),
        expr("current_timestamp() as dw_update_ts"), 
        expr("sha2(concat_ws('|', date, platform, service), 256) as merge_key")
    )

    return result_df


def load_agg_game_settings_daily(spark,df, database, environment):
    """
    Load the data into the agg_game_settings_daily table in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        df (DataFrame): The DataFrame to load into the table.
        database (str): The database to load the table into.
        environment (str): The environment to load the table into.
    """
    final_table = DeltaTable.forName(spark, f"{database}{environment}.managed.agg_game_settings_daily")

    # get schema of the table
    table_schema = spark.read.table(f"{database}{environment}.managed.agg_game_settings_daily").schema

    # create empty dataframe using the schema
    out_df = spark.createDataFrame([], table_schema)
    out_df = out_df.unionByName(df, allowMissingColumns=True)
    agg_cols = [col_name for col_name in out_df.columns if 'agg_' in col_name]
    merge_condition = " OR ".join(f"old.{col_name} <> new.{col_name}" for col_name in agg_cols)

    update_set = {}
    for col_name in agg_cols:
        update_set[f"old.{col_name}"] = f"greatest(new.{col_name}, old.{col_name})"
    update_set[f"old.dw_update_ts"] = "CURRENT_TIMESTAMP()"

    # merge the table
    (
    final_table.alias('old')
    .merge(
        out_df.alias('new'),
        "new.merge_key = old.merge_key"
    )
    .whenMatchedUpdate(condition=merge_condition,set=update_set)
    .whenNotMatchedInsertAll()
    .execute()
    )

def run_batch():
    spark = create_spark_session(name=f"{database}")
    create_agg_game_settings_daily(spark, database, view_mapping)
    df = transform(spark, database, environment)
    load_agg_game_settings_daily(spark, df, database, environment)

# COMMAND ----------
run_batch()