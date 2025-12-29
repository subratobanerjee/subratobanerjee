# Databricks notebook source
# MAGIC %run ../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../utils/ddl/managed/agg_logins_nrt

# COMMAND ----------

from pyspark.sql.functions import expr, coalesce, window
from delta.tables import DeltaTable

# COMMAND ----------

def read_title_df(environment):
    return (
        spark
        .read
        .table(f"reference{environment}.title.dim_title")
        .select(
            "title",
            "app_id",
            "app_group_id",
            "display_platform",
            "display_service"
        )
        .where(expr(f"title = '{title}'"))
    )

# COMMAND ----------

def read_logins_df(environment, app_group_ids=None):
    df = (
        spark
        .readStream
        .option('maxFilesPerTrigger', 10000)
        .table(f"coretech_prod.sso.loginevent")
    )
    
    if app_group_ids:
        app_group_ids_str = ', '.join([f"'{id}'" for id in app_group_ids])
        df = df.where(expr(f"appGroupId in ({app_group_ids_str})"))
    
    return df.select(
        expr("accountId as player_id"),
        "appId",
        "appgroupid",
        expr("ifnull(ifnull(get_json_object(geoip, '$.countryCode'),geoip_countrycode),'ZZ') as countrycode"),
        expr("occurredOn::timestamp as received_on"),
        expr("window(occurredOn::timestamp,'10 minutes').end as received_on_10min_slice")
    )

# COMMAND ----------

def read_country_df(environment):
    return (
        spark
        .read
        .table(f"reference{environment}.location.dim_country")
    )

# COMMAND ----------

def read_platform_territory_df(environment):
    return (
        spark
        .read
        .table(f"dataanalytics{environment}.standard_metrics.platform_territory_10min_ts")
        .select(
            "timestamp_10min_slice",
            "platform",
            "service",
            "territory_name"
        )
        .where(
            expr("timestamp_10min_slice between current_timestamp() - interval '12 hour' and current_timestamp() + interval '10 minute'") &
            expr("platform in ('XBSX', 'PS5', 'Windows', 'XSX')")
        )
        .distinct()
    )

# COMMAND ----------

def extract(environment, app_group_ids):
    title_df = read_title_df(environment).alias("title")
    logins_df = read_logins_df(environment, app_group_ids).alias("logins")
    country_df = read_country_df(environment).alias("country")

    joined_df = (
        logins_df
        .join(country_df, on=expr("logins.countrycode = country.country_code"),
            how="left")
        .join(title_df, on=expr("title.app_id = logins.appId"),
            how="left")
        .select(
            expr("title.title as title"),
            expr("logins.received_on_10min_slice"),
            expr("title.display_platform"),
            expr("title.display_service"),
            expr("country.territory_name as logins_territory_name"),
            expr("logins.player_id as player_id")
        )
    )

    return joined_df

# COMMAND ----------

def transform(df):
    transform_df = (
        df
        .groupBy(
            expr("received_on_10min_slice as timestamp_10min_slice"),
            expr("display_platform as platform"),
            expr("display_service as service"),
            expr("logins_territory_name as territory_name"),
            expr("title")
        )
        .agg(
            expr("count(player_id)").alias("agg_gp_1")
        )
    )

    return transform_df
 

# COMMAND ----------

def proc_batch(df, environment):
    # this enables us to do "stateless" microbatch processing off of a "stateful" streaming dataframe
    df = transform(df)
    load_agg_logins_nrt(spark, df, database, environment)

# COMMAND ----------

def stream_agg_logins(data_source, database, title, view_mapping, app_group_ids):
    checkpoint_location = create_agg_logins_nrt(spark, database, view_mapping)
    df = extract(environment, app_group_ids)
    (
        df
        .writeStream
        .outputMode("update")
        .queryName(f"{title} Logins NRT")
        .trigger(processingTime = "10 minutes")
        .option("checkpointLocation", checkpoint_location)
        .foreachBatch(lambda df, batch_id: proc_batch(df, environment))
        .start()
    )
