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
    prev_max = max_timestamp(spark, f"{database}{environment}.managed.fact_igm_promo_daily", 'dw_insert_ts')
    if prev_max is None:
        prev_max = date(1999, 1, 1)
    else:
        prev_max
    print("prev_max is ", prev_max)

    igmstatus_df = (
        spark.read
            .table(f"nero{environment}.raw.igmstatus")
            .where(
                (expr(f"to_date(insert_ts) >= to_date('{prev_max}') - interval 3 days"))
            )
    )

    dim_title_df = (
        spark.read
            .table(f"reference{environment}.title.dim_title")
    )

    promotions_df = (
        spark.read
            .table(f"nero{environment}.reference.promotions")
    )

    return igmstatus_df, dim_title_df, promotions_df


# COMMAND ----------

def transform(igmstatus_df, dim_title_df, promotions_df):
    joined_df = (
        igmstatus_df.alias("iss")
        .join(dim_title_df.alias("t"), expr("iss.appGroupId = t.APP_GROUP_ID"), "left")
        .join(promotions_df.alias("p"), expr("iss.igmid = p.id"), "left")
        .where(expr("t.title = 'Mafia: The Old Country'"))
    )

    aggregated_df = (
        joined_df.groupBy(
            expr("CAST(iss.receivedon AS TIMESTAMP)").cast("date").alias("date"),
            expr("iss.playerPublicId").alias("player_id"),
            expr("iss.appGroupId").alias("app_group_id"),
            expr("t.display_platform").alias("platform"),
            expr("t.display_service").alias("service"),
            expr("iss.countryCode").alias("country_code"),
            expr("iss.igmid").alias("promo_id"),
            expr("IFNULL(p.name, 'None')").alias("promo_name"),
            expr("iss.igmtype").alias("promo_type"),
            expr("iss.igmlocationid").alias("location_id"),
            expr("iss.igmcarouselslot").alias("carousel_slot")
        )
        .agg(
            expr("SUM(CASE WHEN iss.igmstatus = 'IGM Impression' THEN 1 ELSE 0 END)").alias("impressions"),
            expr("SUM(CASE WHEN iss.igmstatus = 'IGM Click' THEN 1 ELSE 0 END)").alias("clicks")
        )
    )

    final_df = aggregated_df.alias("e").select(
        expr("e.date"),
        expr("e.player_id"),
        expr("e.app_group_id"),
        expr("e.platform"),
        expr("e.service"),
        expr("e.country_code"),
        expr("e.promo_id"),
        expr("e.promo_name"),
        expr("e.promo_type"),
        expr("e.location_id"),
        expr("e.carousel_slot"),
        expr("e.impressions"),
        expr("e.clicks")
    )

    return final_df


# COMMAND ----------

def load_fact_igm_promo_daily(spark, df_agg, database, environment):
    """
    Load the data into the fact_igm_promo_daily table in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        df_agg (DataFrame): The DataFrame to load into the table.
        database (str): The database to load the table into.
        environment (str): The environment to load the table into.
    """

    # Perform the merge operation into the Delta table
    final_table = DeltaTable.forName(spark, f"{database}{environment}.managed.fact_igm_promo_daily")

    # get schema of the table
    table_schema = spark.read.table(f"{database}{environment}.managed.fact_igm_promo_daily").schema

    # create empty dataframe using the schema
    out_df = spark.createDataFrame([], table_schema)

    # union the incoming dataframe with the empty dataframe, we can set allowMissingColumns to False to be more strict
    df_agg = df_agg.select(
            "*",
            expr("CURRENT_TIMESTAMP() as dw_insert_ts"),
            expr("CURRENT_TIMESTAMP() as dw_update_ts"),
            expr("sha2(concat_ws('|', date, player_id, app_group_id, platform, service, country_code, promo_id, promo_name, promo_type, location_id, carousel_slot), 256) as merge_key")
        )
    out_df = out_df.unionByName(df_agg, allowMissingColumns=True)

    # dynamically setup merge condition based on the aggregate columns in this database's fact_igm_promo_daily table
    
    update_cols = ['impressions', 'clicks']

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

def create_fact_igm_promo_daily(spark, database):

    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.fact_igm_promo_daily (
        date DATE,
        player_id STRING,
        app_group_id STRING,
        platform STRING,
        service STRING,
        country_code STRING,
        promo_id STRING,
        promo_name STRING,
        promo_type STRING,
        location_id STRING,
        carousel_slot STRING,
        impressions BIGINT,
        clicks BIGINT,
        dw_insert_ts TIMESTAMP,
        dw_update_ts TIMESTAMP,
        merge_key STRING
    )
        """

    properties = {
        "delta.feature.allowColumnDefaults": "supported" 
    }

    create_table(spark, sql, properties)
    create_fact_igm_promo_daily_view(spark, database)
    return f"Table fact_igm_promo_daily created"


# COMMAND ----------

def create_fact_igm_promo_daily_view(spark, database):
    """
    Create the fact_igm_promo_daily view in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
        mapping (dict): A dictionary of column mappings.
    """

    sql = f"""
    CREATE VIEW IF NOT EXISTS {database}{environment}.managed_view.fact_igm_promo_daily AS (
        SELECT
        *
        from {database}{environment}.managed.fact_igm_promo_daily
    )
    """
    spark.sql(sql)

# COMMAND ----------

def run_batch():
    spark = create_spark_session(name=f"{database}")
    create_fact_igm_promo_daily(spark, database)
    igmstatus_df, dim_title_df, promotions_df = extract(environment, database)
    df_agg = transform(igmstatus_df, dim_title_df, promotions_df)
    load_fact_igm_promo_daily(spark, df_agg, database, environment)

# COMMAND ----------

run_batch()