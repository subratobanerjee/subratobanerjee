# Databricks notebook source
# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/managed/fact_player_discovery_daily

# COMMAND ----------

# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.types import LongType, DoubleType, IntegerType
from delta.tables import DeltaTable
from datetime import date

input_param = dbutils_input_params()
environment = input_param.get('environment', set_environment())
database = 'oak2'
# environment='_stg' # Note : stg has latest data with latest new columns added which might not present on dev
data_source = 'gearbox'
title = 'Borderlands 4'

# For dev environment, we want to read from stg but write to dev
source_environment = '_stg' if environment == '_dev' else environment


# COMMAND ----------

def extract(spark, database, environment, prev_max):
    # Raw table data load and filter - using source_environment for reading
    df_read = (spark.table(f"{database}{source_environment}.raw.oaktelemetry_gbxdiscovery")
               .filter(expr(f"to_date(maw_time_received) >= to_date('{prev_max}') - interval 2 days"))) \
               .filter("buildconfig_string = 'Shipping Game'") \
               .filter(expr("player_character_id is not null and player_platform_id is not null"))

    return df_read


# COMMAND ----------

def transform(spark, database, environment, df_read):
    df_agg = (
        df_read.groupBy(
            expr("coalesce(player_id,player_platform_id) as player_id"),
            expr("player_platform_id"),
            expr("player_character_id"),
            expr("date"),
            expr("player_level"),
            expr("prev_map_string"),
            expr("prev_biome_string"),
            expr("prev_region_string")
        )
        .agg(
            expr("SUM(time_since_prev_transition_long) / 60.0 AS time_in_region_mins"),
            expr("SUM(distance_traveled_on_foot_long) / 100 AS meters_on_foot"),
            expr("SUM(distance_traveled_in_vehicle_long) / 100 AS meters_in_vehicle"),
            expr("SUM((distance_traveled_on_foot_long + distance_traveled_in_vehicle_long) / 100) AS total_meters")
        )
        .select(
            expr("coalesce(player_id,player_platform_id) as player_id"),
            expr("player_platform_id"),
            expr("player_character_id"),
            expr("date"),
            expr("coalesce(player_level, -1) AS character_level"),
            expr("prev_map_string AS world"),
            expr("""
                CASE 
                    WHEN prev_biome_string = 'Dominion City' THEN 'Dominion'
                    ELSE prev_biome_string
                END AS biome
            """),
            expr("prev_region_string AS region"),
            expr("ROUND(time_in_region_mins, 2) AS time_in_region_mins"),
            expr("meters_on_foot"),
            expr("meters_in_vehicle"),
            expr("total_meters")
        )
    )

    return df_agg


# COMMAND ----------

def create_fact_player_discovery_daily_view(spark, database):
    """
    Create the fact_player_discovery_daily view in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
        mapping (dict): A dictionary of column mappings.
    """

    sql = f"""
    CREATE VIEW IF NOT EXISTS {database}{environment}.managed_view.fact_player_discovery_daily AS (
        SELECT
        *
        from {database}{environment}.managed.fact_player_discovery_daily
    )
    """
    spark.sql(sql)


# COMMAND ----------

def load(df, database, environment, spark):
    # Reference to the target Delta table
    target_df = DeltaTable.forName(spark, f"{database}{environment}.managed.fact_player_discovery_daily")

    # Merge condition
    merger_condition = 'target.merge_key = source.merge_key'

    # Generate update conditions for numeric fields
    numeric_types = (LongType, DoubleType, IntegerType)
    numeric_fields = [
        field.name for field in df.schema.fields if isinstance(field.dataType, numeric_types)
    ]

    # Dynamically build set_fields dict
    set_fields = {
        field: f"greatest(target.{field}, source.{field})" for field in numeric_fields
    }
    set_fields["dw_update_ts"] = "source.dw_update_ts"

    # Optional: construct a condition string (can be improved or removed)
    update_condition = " OR ".join([f"target.{field} != source.{field}" for field in numeric_fields])

    merge_update_conditions = [
        {
            'condition': update_condition,
            'set_fields': set_fields
        }
    ]

    # Begin merge operation
    merge_df = target_df.alias("target").merge(df.alias("source"), merger_condition)
    merge_df = set_merge_update_condition(merge_df, merge_update_conditions)
    merge_df = set_merge_insert_condition(merge_df, df)

    merge_df.execute()


# COMMAND ----------

def run_batch(database, environment):
    database = database.lower()

    # Setting the spark session
    spark = SparkSession.builder.appName(f"{database}").getOrCreate()

    # Creating the table structure
    create_fact_player_discovery_daily(spark, database, environment)
    create_fact_player_discovery_daily_view(spark, database)

    print('Extracting the data')

    prev_max = max_timestamp(spark, f"{database}{environment}.managed.fact_player_discovery_daily", 'dw_insert_ts')
    if prev_max is None:
        prev_max = date(1999, 1, 1)
    print(prev_max)

    # extract
    df_read = extract(spark, database, environment, prev_max)
    # transform
    df_agg = transform(spark, database, environment, df_read)

    df = df_agg.withColumn("dw_update_ts", expr("current_timestamp()")) \
        .withColumn("dw_insert_ts", expr("current_timestamp()")) \
        .withColumn("merge_key", expr(
        "sha2(concat_ws('|', player_id, player_platform_id, player_character_id, date, character_level, world, biome, region, time_in_region_mins), 256)"))

    print('Merge Data')
    load(df, database, environment, spark)


# COMMAND ----------

run_batch(database, environment)