# Databricks notebook source
# MAGIC %run ../../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../../utils/ddl/managed/agg_logins_nrt

# COMMAND ----------

from pyspark.sql.functions import expr, coalesce, window
from delta.tables import DeltaTable

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'wwe2k25'
data_source ='dna'
view_mapping = {
    'agg_gp_1': 'agg_gp_1 as login_count'
}

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
    )

# COMMAND ----------

def read_logins_df(environment):
    return (
        spark
        .readStream
        .option('maxFilesPerTrigger', 10000)
        .table(f"coretech_prod.sso.loginevent")
        .where(expr("""appGroupId = 'c5770d7b11bb48279c8dc7883a9fcbf2'"""))
        .select(
            expr("accountId as player_id"),
            "appId",
            "appgroupid",
            expr("ifnull(ifnull(get_json_object(geoip, '$.countryCode'),geoip_countrycode),'ZZ') as countrycode"),
            expr("occurredOn::timestamp as received_on"),
            expr("window(occurredOn::timestamp,'10 minutes').end as received_on_10min_slice")
        )
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
            expr("timestamp_10min_slice between current_timestamp() - interval '12 hour' and current_timestamp() + interval '10 minute' and platform in ('PS4', 'PS5', 'XBSX', 'XB1', 'Windows')")
            # expr("timestamp_10min_slice >= '2024-10-01T00:00:00.000'")
            # & expr("timestamp_10min_slice < current_timestamp()")
            )
        .distinct()
    )

# COMMAND ----------

def extract(environment):
    title_df = read_title_df(environment).alias("title")
    logins_df = read_logins_df(environment).alias("logins")
    country_df = read_country_df(environment).alias("country")

    joined_df = (
        logins_df
        .join(country_df, on=expr("logins.countrycode = country.country_code"),
            how="left")
        .join(title_df, on=expr("title.app_id = logins.appId"),
            how="left")
        .select(
            expr("logins.received_on_10min_slice"),
            expr("title.display_platform"),
            expr("title.display_service"),
            expr("country.territory_name as logins_territory_name"),
            expr("logins.player_id as player_id")
        )
        # .where(
        #     expr("title.display_platform is not null and title.display_service is not null")
        # )
    )

    return joined_df

# COMMAND ----------

def transform(df):
    plat_df = read_platform_territory_df(environment).alias("plat")

    transform_df = (
        df
        .groupBy(
            expr("received_on_10min_slice as timestamp_10min_slice"),
            expr("display_platform as platform"),
            expr("display_service as service"),
            expr("logins_territory_name as territory_name")
        )
        .agg(
            expr("count(player_id)").alias("agg_gp_1")
        )
    )

    joined_df = (
        plat_df.alias("plat")
        .join(transform_df.alias("source"), on=expr("plat.timestamp_10min_slice = source.timestamp_10min_slice and plat.platform = source.platform and plat.service = source.service and plat.territory_name = source.territory_name"),
            how="left")
        .select(
            'plat.timestamp_10min_slice',
            'plat.platform',
            'plat.service',
            'plat.territory_name',
            expr('coalesce(agg_gp_1, 0) as agg_gp_1')
        )
    )

    return joined_df
 

# COMMAND ----------

def proc_batch(df, environment):
    # this enables us to do "stateless" microbatch processing off of a "stateful" streaming dataframe
    df = transform(df)
    load_agg_logins_nrt(spark, df, database, environment)

# COMMAND ----------

def run_stream():
    checkpoint_location = create_agg_logins_nrt(spark, database, view_mapping)
    # environment, checkpoint_location = "_dev", "dbfs:/tmp/wwe2k25/managed/streaming/run_dev/agg_logins_nrt"

    df = extract(environment)

    (
        df
        .writeStream
        .outputMode("update")
        .queryName("WWE 2K25 Logins NRT")
        .trigger(processingTime = "10 minutes")
        .option("checkpointLocation", checkpoint_location)
        .foreachBatch(lambda df, batch_id: proc_batch(df, environment))
        .start()
    )

# COMMAND ----------

run_stream()

# COMMAND ----------

# dbutils.fs.rm("dbfs:/tmp/wwe2k25/managed/streaming/run_dev/agg_logins_nrt", True)
# spark.sql("DROP TABLE IF EXISTS wwe2k25_dev.managed.agg_logins_nrt")
