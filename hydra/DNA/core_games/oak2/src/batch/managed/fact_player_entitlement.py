# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/table_functions

# COMMAND ----------

from pyspark.sql.functions import expr, explode, from_json, regexp_replace, col
from pyspark.sql.types import ArrayType, StructType, StructField, StringType
from delta.tables import DeltaTable
from datetime import date

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment', set_environment())
database = 'oak2'

# COMMAND ----------

def extract(environment, database):
    prev_max = max_timestamp(spark, f"{database}{environment}.managed.fact_player_entitlement", 'dw_insert_ts')
    if prev_max is None:
        prev_max = date(1999, 1, 1)
    else:
        prev_max
    print("prev_max is ", prev_max)

    durable_entitlement_df = (
        spark.read
            .table("gbx_prod.raw.durable_entitlement_events")
            .where(
                expr(f"date >= to_date('{prev_max}') - interval 3 days and date >= '2025-08-01'")
            )
    )

    dim_title_df = (
        spark.read
            .table(f"reference{environment}.title.dim_title")
    )

    return durable_entitlement_df, dim_title_df

# COMMAND ----------

def transform(durable_entitlement_df, dim_title_df):
    # First explode the durable_entitlement_events array
    exploded_events_df = (
        durable_entitlement_df
        .select(
            expr("maw_time_received"),
            expr("explode(durable_entitlement_events) as exploded")
        )
        .select(
            expr("maw_time_received"),
            expr("exploded.game_title_string as game_title_string"),
            expr("exploded.event_string as event_string"),
            expr("exploded.player_id_platformid as player_id_platformid"),
            expr("exploded.service_string as service_string"),
            expr("exploded.first_party_entitlements_string as first_party_entitlements_string"),
            expr("exploded.time_timestamp as time_timestamp")
        )
    )

    # Clean and parse the first_party_entitlements JSON string
    cleaned_json_df = (
        exploded_events_df
        .withColumn(
            "cleaned_json",
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(col("first_party_entitlements_string"), '"\\[', '['),
                        '=>', ':'
                    ),
                    '\\\\', ''
                ),
                '\\]"', ']'
            )
        )
    )

    # Define the schema for the entitlements array
    entitlements_schema = ArrayType(
        StructType([
            StructField("entitlement_id", StringType(), True),
            StructField("created_at", StringType(), True)
        ])
    )

    # Parse JSON and explode entitlements
    oak2_entitlement_data = (
        cleaned_json_df
        .withColumn("entitlements_array", from_json(col("cleaned_json"), entitlements_schema))
        .select(
            expr("maw_time_received"),
            expr("game_title_string"),
            expr("event_string"),
            expr("player_id_platformid"),
            expr("service_string"),
            expr("time_timestamp"),
            expr("explode(entitlements_array) as first_party_entitlement")
        )
        .select(
            expr("maw_time_received"),
            expr("game_title_string"),
            expr("event_string"),
            expr("player_id_platformid"),
            expr("service_string"),
            expr("first_party_entitlement.entitlement_id as entitlement_id"),
            expr("first_party_entitlement.created_at as created_at"),
            expr("CAST(CAST(time_timestamp AS INT) AS TIMESTAMP) as time_timestamp")
        )
        .where(expr("game_title_string = 'oak2'"))
    )

    # Join with dim_title and apply final transformations
    joined_df = (
        oak2_entitlement_data.alias("oed")
        .join(
            dim_title_df.alias("dt"),
            expr("LOWER(oed.service_string) = dt.service AND dt.title = 'Borderlands 4'"),
            "left"
        )
        .where(
            expr("oed.player_id_platformid IS NOT NULL AND oed.entitlement_id IS NOT NULL")
        )
    )

    # Apply window functions and final selection
    final_df = (
        joined_df
        .select(
            expr("oed.player_id_platformid").alias("player_id"),
            expr("oed.entitlement_id").alias("entitlement"),
            expr("CAST(NULL AS STRING)").alias("entitlement_name"),
            expr("CAST(CAST(oed.created_at AS TIMESTAMP) AS DATE)").alias("entitlement_create_date"),
            expr("dt.display_platform").alias("platform"),
            expr("dt.display_service").alias("service"),
            expr("""
                FIRST_VALUE(CAST(oed.maw_time_received AS TIMESTAMP)) OVER (
                    PARTITION BY oed.player_id_platformid, oed.entitlement_id 
                    ORDER BY CAST(oed.maw_time_received AS TIMESTAMP) ASC
                )
            """).alias("first_seen"),
            expr("""
                FIRST_VALUE(CAST(oed.maw_time_received AS TIMESTAMP)) OVER (
                    PARTITION BY oed.player_id_platformid, oed.entitlement_id 
                    ORDER BY CAST(oed.maw_time_received AS TIMESTAMP) DESC
                )
            """).alias("last_seen")
        )
        .distinct()
    )

    return final_df

# COMMAND ----------

def load_fact_player_entitlement(spark, df_agg, database, environment):
    """
    Load the data into the fact_player_entitlement table in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        df_agg (DataFrame): The DataFrame to load into the table.
        database (str): The database to load the table into.
        environment (str): The environment to load the table into.
    """

    # Perform the merge operation into the Delta table
    final_table = DeltaTable.forName(spark, f"{database}{environment}.managed.fact_player_entitlement")

    # get schema of the table
    table_schema = spark.read.table(f"{database}{environment}.managed.fact_player_entitlement").schema

    # create empty dataframe using the schema
    out_df = spark.createDataFrame([], table_schema)

    # union the incoming dataframe with the empty dataframe, we can set allowMissingColumns to False to be more strict
    df_agg = df_agg.select(
            "*",
            expr("CURRENT_TIMESTAMP() as dw_insert_ts"),
            expr("CURRENT_TIMESTAMP() as dw_update_ts"),
            expr("sha2(concat_ws('|', player_id, entitlement,entitlement_create_date,platform,service), 256) as merge_key")
        )
    out_df = out_df.unionByName(df_agg, allowMissingColumns=True)

    # dynamically setup merge condition based on the aggregate columns in this database's fact_player_entitlement table
    
    update_cols = ['first_seen', 'last_seen']

    merge_condition = " OR ".join(f"old.{col_name} <> new.{col_name}" for col_name in update_cols)

    # create the update column dict
    update_set = {}
    for col_name in update_cols:
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

def create_fact_player_entitlement(spark, database):

    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.fact_player_entitlement (
        player_id STRING,
        entitlement STRING,
        entitlement_name STRING,
        entitlement_create_date DATE,
        platform STRING,
        service STRING,
        first_seen TIMESTAMP,
        last_seen TIMESTAMP,
        dw_insert_ts TIMESTAMP,
        dw_update_ts TIMESTAMP,
        merge_key STRING
    )
        """

    properties = {
        "delta.feature.allowColumnDefaults": "supported" 
    }

    create_table(spark, sql, properties)
    create_fact_player_entitlement_view(spark, database)
    return f"Table fact_player_entitlement created"

# COMMAND ----------

def create_fact_player_entitlement_view(spark, database):
    """
    Create the fact_player_entitlement view in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
    """

    sql = f"""
    CREATE VIEW IF NOT EXISTS {database}{environment}.managed_view.fact_player_entitlement AS (
        SELECT
        *
        from {database}{environment}.managed.fact_player_entitlement
    )
    """
    spark.sql(sql)

# COMMAND ----------

def run_batch():
    spark = create_spark_session(name=f"{database}")
    create_fact_player_entitlement(spark, database)
    durable_entitlement_df, dim_title_df = extract(environment, database)
    df_agg = transform(durable_entitlement_df, dim_title_df)
    load_fact_player_entitlement(spark, df_agg, database, environment)

# COMMAND ----------

run_batch()
