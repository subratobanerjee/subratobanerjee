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

def read_title_df(title):
    return (
        spark
        .read
        .table(f"reference{environment}.title.dim_title")
        .select(
            "title",
            "display_platform",
            "display_service",
            "platform",
            "service"
        )
        .where(expr(f"title = '{title}'"))
    )

# COMMAND ----------

def read_logins_df(database):
    return (
        spark
        .readStream
        .option('maxFilesPerTrigger', 10000)
        .table(f"gbx_prod.raw.logins")
        .where(expr(f"game_title_string = '{database}'"))
        .select(
            expr("player_id_platformid as player_id"),
            expr("(case when player_address_country_string is null then 'ZZ' when player_address_country_string = '--' then 'ZZ' when player_address_country_string = 'CA-QC' then 'CA' else player_address_country_string end) as countrycode"),
            expr("(case when lower(hardware_string) = 'ps5pro' then 'ps5' when lower(hardware_string) = 'xsx' then 'xbsx' when lower(hardware_string) = 'switch2' then 'nsw2' else lower(hardware_string) end) as platform"),
            expr("(case when lower(service_string) = 'nintendo' then 'nso' else lower(service_string) end) as service"),
            expr("maw_time_received::timestamp as received_on"),
            expr("window(maw_time_received::timestamp,'10 minutes').end as received_on_10min_slice")
        )
    )

# COMMAND ----------

def read_country_df():
    return (
        spark
        .read
        .table(f"reference{environment}.location.dim_country")
    )

# COMMAND ----------

def read_platform_territory_df():
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
            expr("timestamp_10min_slice between current_timestamp() - interval '12 hour' and current_timestamp() + interval '10 minute' and platform in ('PS5', 'XBSX', 'Windows', 'NSW2')") # check if this includes all required platforms
            # expr("timestamp_10min_slice >= '2025-01-01T00:00:00.000'")
            # & expr("timestamp_10min_slice < current_timestamp() + interval '10 minute'")
            )
        .distinct()
    )

# COMMAND ----------

def extract(database, title):
    title_df = read_title_df(title).alias("title")
    logins_df = read_logins_df(database).alias("logins")
    country_df = read_country_df().alias("country")

    joined_df = (
        logins_df
        .join(country_df, on=expr("logins.countrycode = country.country_code"),
            how="left")
        .join(title_df, on=expr("title.platform = logins.platform and title.service = logins.service"),
            how="left")
        .select(
            expr("logins.received_on_10min_slice"),
            expr("title.title"),
            expr("iff(title.display_platform = 'WindowsEpic', 'Windows', title.display_platform) as display_platform"),
            expr("title.display_service"),
            expr("ifnull(country.territory_name, 'Unknown') as logins_territory_name"),
            expr("logins.player_id as player_id")
        )
        # .where(
        #     expr("title.display_platform is not null and title.display_service is not null")
        # )
    )

    return joined_df

# COMMAND ----------

def transform(df, title):
    plat_df = read_platform_territory_df().alias("plat")

    transform_df = (
        df
        .groupBy(
            expr("received_on_10min_slice as timestamp_10min_slice"),
            expr("title"),
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
            expr(f'coalesce(title, "{title}") as title'),
            'plat.platform',
            'plat.service',
            'plat.territory_name',
            expr('coalesce(agg_gp_1, 0) as agg_gp_1')
        )
    )

    return joined_df
 

# COMMAND ----------

def proc_batch(df, data_source, database, title, view_mapping):
    # this enables us to do "stateless" microbatch processing off of a "stateful" streaming dataframe
    df = transform(df, title)
    load_agg_logins_nrt(spark, df, database, environment)

# COMMAND ----------

def stream_agg_logins(data_source, database, title, view_mapping):

    checkpoint_location = create_agg_logins_nrt(spark, database, view_mapping)
    # environment, checkpoint_location = "_dev", "dbfs:/tmp/oak2/managed/streaming/run_dev/agg_logins_nrt"
    print(f"checkpoint_location: {checkpoint_location}")

    df = extract(database, title)

    (
        df
        .writeStream
        .outputMode("update")
        .queryName(f"{title} Logins NRT")
        .trigger(processingTime = "10 minutes")
        .option("checkpointLocation", checkpoint_location)
        .foreachBatch(lambda df, batch_id: proc_batch(df, data_source, database, title, view_mapping))
        .start()
    )
