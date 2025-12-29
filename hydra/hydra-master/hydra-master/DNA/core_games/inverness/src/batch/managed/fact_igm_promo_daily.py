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
database  = 'inverness'
data_source ='dna'
title = 'Civilization VII'

# COMMAND ----------

def read_igm_status(spark,environment):
    return (
        spark
        .read
        .table(f"inverness{environment}.raw.igmstatus")
         .where((expr("insert_ts::date > CURRENT_DATE() - INTERVAL 3 DAY")) & 
                (expr("playerpublicid is not null"))
                & 
                (expr("playerpublicid!='anonymous'")))  
        .select(
        expr("to_date(cast(receivedon as timestamp)) as date"),
        expr("igmid as promo_id"),
        expr("playerpublicid as player_id"),
        expr("appPublicId as app_id"),
        expr("countryCode as country_code"),
        "igmevent",
        expr("igmcarouselslot as carousel_slot"),
        expr("igmlocationid as location_id")
        )
    )
    

# COMMAND ----------

def read_promotions(spark,environment):
    return (
        spark
        .read
        .table(f"inverness{environment}.reference.promotions")
        .select("id", expr("name as promo_name"), "promo_type")
    )

# COMMAND ----------

def extract(environment, database):
    igm_status_df = read_igm_status(spark, environment).alias("igm")
    promo_df = read_promotions(spark, environment).alias("promo")

    joined_df = (
        igm_status_df
        .join(
            promo_df,
            on= expr("""igm.promo_id = promo.id 
                    """),
            how="left"
        )
    )

    return joined_df

# COMMAND ----------

def transform(joined_df):
    transformed_df = (
        joined_df.groupBy(
        "date",
        "player_id",
        "app_id",
        "country_code",
        "promo_id",
        "promo_name",
        "promo_type",
        "location_id",
        "carousel_slot",
    ).agg(
        expr("sum(case when igmevent = 'IGM Seen' then 1 else 0 end) as impressions"),
        expr("sum(case when igmevent = 'IGM Interacted' then 1 else 0 end) as clicks"),
    )
    )
    return transformed_df

# COMMAND ----------

def load_fact_igm_promo_daily(spark, df, database, environment):
    """
    Load the data into the fact_igm_promo_daily table in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        df (DataFrame): The DataFrame to load into the table.
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
    df = df.select(
            "*",
            expr("CURRENT_TIMESTAMP() as dw_insert_ts"),
            expr("CURRENT_TIMESTAMP() as dw_update_ts"),
            expr("sha2(concat_ws('|', date, player_id,app_id, country_code, promo_id, promo_name, promo_type, location_id, carousel_slot), 256) as merge_key")
        )
    out_df = out_df.unionByName(df, allowMissingColumns=True)

    # dynamically setup merge condition based on the aggregate columns in this database's fact_igm_promos_daily table
    agg_cols = ['impressions', 'clicks']

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

def create_fact_igm_promo_daily(spark, database):

    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.fact_igm_promo_daily (
        date DATE,
        player_id STRING,
        app_id STRING,
        country_code STRING,
        promo_id STRING,
        promo_name STRING,
        promo_type STRING,
        location_id STRING,
        carousel_slot STRING,
        impressions INT,
        clicks INT,
        dw_insert_ts TIMESTAMP,
        dw_update_ts TIMESTAMP,
        merge_key STRING
    )
        """

    properties = {
        "delta.feature.allowColumnDefaults": "supported" 
    }

    create_table(spark, sql, properties)
    return f"Table fact_igm_promo_daily created"


# COMMAND ----------

def run_batch():
    spark = create_spark_session(name=f"{database}")
    create_fact_igm_promo_daily(spark, database)
    df = extract(environment, database)
    df = transform(df)
    load_fact_igm_promo_daily(spark, df, database, environment)

# COMMAND ----------

run_batch()
