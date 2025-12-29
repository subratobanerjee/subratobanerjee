# Databricks notebook source
# MAGIC %run ../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../utils/ddl/managed/agg_installs_nrt

# COMMAND ----------

from pyspark.sql.functions import expr,lit,current_timestamp
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
            expr("""
                (case 
                    when lower(hardware_string) = 'ps5pro' then 'ps5'
                    when lower(hardware_string) = 'xsx' then 'xbsx'
                    when lower(hardware_string) = 'switch2' then 'nsw2'
                    else lower(hardware_string) 
                end) as platform
            """),
            expr("""
                (case 
                    when lower(service_string) = 'nintendo' then 'nso'
                    else lower(service_string)
                end) as service
            """),
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
            "country_code",
            "territory_name"
        )
        .where(
            expr("timestamp_10min_slice between current_timestamp() - interval '12 hour' and current_timestamp() + interval '10 minute' and platform in ('PS5', 'XBSX', 'Windows', 'NSW2')") # check if this includes all required platforms
            # expr("timestamp_10min_slice >= '2025-01-01T00:00:00.000'")
            # & expr("timestamp_10min_slice < current_timestamp() + interval '10 minute' and platform in ('PS5', 'XBSX', 'Windows')")
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
        .join(title_df, on=expr("title.platform = logins.platform and title.service = logins.service"),
            how="left")
        .join(country_df, on=expr("logins.countrycode = country.country_code"),
            how="left")
        .select(
            "logins.player_id",
            "title.title",
            expr("iff(title.display_platform = 'WindowsEpic', 'Windows', title.display_platform) as platform"),
            expr("title.display_service as service"),
            "country.territory_name",
            "logins.received_on"
        )
    )

    return joined_df

# COMMAND ----------

def transform(joined_df):
    platform_territory_df = read_platform_territory_df().alias("dt")

    # read lookup
    inst_df = (
        spark
        .read
        .table(f"{database}{environment}.intermediate.player_install_ledger")
        .alias("lkp")
    )

    # get first country & territory
    player_first_country_df= (
        joined_df
        .select("player_id",
          "title",
          "received_on",
          expr("window(received_on, '10 minutes').end as timestamp_10min_slice"),
          "platform", 
          "service",
          expr("coalesce(first_value(territory_name) ignore nulls over (partition by player_id, title, platform order by received_on), 'Unknown') as first_territory_name"),
          )
        .distinct()
        .select(
            "*",
            expr("sha2(concat_ws('|', player_id, title, platform, service), 256) as login_join_key")
        )
    )

    # filter out players that have already been counted as installed
    new_installs = (
        player_first_country_df.alias("logins")
        .join(inst_df, on=expr("logins.login_join_key = lkp.merge_key"), how="left")
        .where(expr("lkp.player_id is null or lkp.game_install = false"))
    )

    # get min timestamp for install ts
    first_install_df= (
        new_installs
        .groupBy(
            "logins.player_id",
            "logins.title",
            "logins.platform",
            "logins.service",
            expr("first_territory_name as territory_name"),
            "login_join_key"
        )
        .agg(
            expr("min(received_on) as install_timestamp"),
            expr("min(logins.timestamp_10min_slice) as timestamp_10min_slice"),
        )
    )

    # create dataframe for merging into lookup
    mrg_df = (
        first_install_df
        .select(
            "player_id",
            "title",
            "platform",
            "service",
            "territory_name",
            expr("True as game_install"),
            expr("install_timestamp as game_install_ts"),
            "timestamp_10min_slice",
            expr("current_timestamp() as dw_insert_ts"),
            expr("current_timestamp() as dw_update_ts"),
            expr("login_join_key as merge_key")
        )
        .distinct()
    )

    # write to checkpoint
    mrg_df.write.mode("overwrite").saveAsTable(f"{database}{environment}.intermediate.install_tmp")

    # merge into the lookup table
    install_lkp_table = DeltaTable.forName(spark, f"{database}{environment}.intermediate.player_install_ledger")

    (
        install_lkp_table
        .alias("target")
        .merge(
            mrg_df.alias("source"),
            "target.merge_key = source.merge_key")
        .whenMatchedUpdate(condition="(target.game_install_ts > source.game_install_ts)",
                           set =
                           {
                               "target.territory_name": "source.territory_name",
                               "target.game_install_ts": "source.game_install_ts",
                               "target.game_install": "source.game_install",
                               "target.timestamp_10min_slice": "source.timestamp_10min_slice",
                               "target.dw_update_ts": "current_timestamp()"
                           }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    # aggregate the install metric
    transform_df = (
        spark
        .read
        .table(f"{database}{environment}.intermediate.install_tmp")
        .groupBy(
            "timestamp_10min_slice",
            "title",
            "platform",
            "service",
            "territory_name",
        )
        .agg(
            expr("ifnull(count(distinct player_id), 0) as agg_gp_1")
        )
        .select(
            "*"
        ).distinct()
    )
    
    return transform_df

# COMMAND ----------

def proc_batch(df, data_source, database, title, view_mapping):
    # this enables us to do "stateless" microbatch processing off of a "stateful" streaming dataframe
    df = transform(df)
    load_agg_installs_nrt(spark, df, database, environment)

# COMMAND ----------

def stream_agg_installs(data_source, database, title, view_mapping):
    checkpoint_location_ledger = create_player_installs_ledger(spark, database)
    print(checkpoint_location_ledger)
    checkpoint_location = create_agg_installs_nrt(spark, database, view_mapping)
    print(checkpoint_location)
    # environment, checkpoint_location = "_dev", "dbfs:/tmp/oak2/managed/streaming/run_dev/agg_installs_nrt"

    df = extract(database, title)

    (
        df
        .writeStream
        .trigger(processingTime = "10 minutes")
        # .trigger(availableNow = True)
        .option("checkpointLocation", checkpoint_location)
        .foreachBatch(lambda df, batch_id: proc_batch(df, data_source, database, title, view_mapping))
        .start()
    )

