# Databricks notebook source
# MAGIC %run ../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../utils/ddl/managed/agg_installs_nrt

# COMMAND ----------

from pyspark.sql.functions import expr, lit, current_timestamp
from delta.tables import DeltaTable


# COMMAND ----------

def read_title_df(title):
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

def read_logins_df(environment):
    return (
        spark
        .readStream
        .option('maxFilesPerTrigger', 10000)
        .table(f"coretech_prod.sso.loginevent")
        .select(
            expr("accountId as player_id"),
            "obfuscatedAccountId",
            "appId",
            "appgroupid",
            expr("ifnull(ifnull(get_json_object(geoip, '$.countryCode'),geoip_countrycode),'ZZ') as countrycode"),
            expr("occurredOn::timestamp as received_on"),
            expr("window(occurredOn::timestamp,'10 minutes').end as received_on_10min_slice")
        )
    )


# COMMAND ----------

def read_sso_mapping_df(environment):
    return (
        spark
        .read
        .table(f"reference_prod.sso_mapping.dna_obfuscated_id_mapping")
        .where(expr(f"title in ('{title}', '2K Portal') AND obfuscated_platform_id <> unobfuscated_platform_id"))
        .select(
            "obfuscated_platform_id",
            "unobfuscated_platform_id",
            "title",
            expr(f"rank() over (order by case when title = '{title}' then 1 else 2 end) as rnk")
        )
        .where(expr("rnk=1"))
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
            "country_code",
            "territory_name"
        )
        .where(expr("timestamp_10min_slice between current_timestamp() - interval '12 hour' and current_timestamp() + interval '10 minute' and platform in ('XBSX', 'PS5', 'Windows')"))
        .distinct()
    )


# COMMAND ----------

def extract(database, title):
    title_df = read_title_df(title).alias("title")
    logins_df = read_logins_df(environment).alias("logins")
    country_df = read_country_df(environment).alias("country")

    joined_df = (
        logins_df.alias("le")
        .join(title_df.alias("dt"), on=expr("dt.app_id = le.appid"),
              how="inner")
        .join(country_df.alias("dc"), on=expr("le.countrycode = dc.country_code"),
              how="inner")
        .select(
            "le.player_id",
            "le.obfuscatedAccountId",
            "dt.title",
            expr("dt.display_platform as platform"),
            expr("dt.display_service as service"),
            "le.countrycode",
            "dc.territory_name",
            "le.received_on"
        )
    )

    return joined_df


# COMMAND ----------

def transform(joined_df):
    # read lookup
    inst_df = (
        spark
        .read
        .table(f"{database}{environment}.intermediate.player_install_ledger")
        .alias("lkp")
    )

    # read dna mapping
    mapping_df = read_sso_mapping_df(environment)

    # get first country & territory
    player_first_country_df = (
        joined_df.alias("logins")
        .join(mapping_df.alias("mapping"),
              on=expr("logins.player_id = mapping.unobfuscated_platform_id and logins.title = mapping.title"),
              how="left")
        .select(
            "player_id",
            "obfuscated_platform_id",
            "logins.title",
            "platform",
            "service",
            "received_on",
            expr("window(received_on, '10 minutes').end as timestamp_10min_slice"),
            expr(
                "coalesce(first_value(countrycode) ignore nulls over (partition by player_id, logins.title, platform order by received_on), 'ZZ') as first_country_code"),
            expr(
                "coalesce(first_value(territory_name) ignore nulls over (partition by player_id, logins.title, platform order by received_on), 'Unknown') as first_territory_name")
        )
        .distinct()
        .select(
            "*",
            expr("sha2(concat_ws('|', player_id, logins.title, platform, service), 256) as login_join_key")
        )
    )

    # filter out players that have already been counted as installed
    new_installs = (
        player_first_country_df.alias("logins")
        .join(inst_df, on=expr("logins.login_join_key = lkp.merge_key"), how="left")
        .where(expr("lkp.player_id is null or lkp.game_install = false"))
    )

    # get min timestamp for install ts
    first_install_df = (
        new_installs
        .groupBy(
            "logins.player_id",
            "logins.obfuscated_platform_id",
            "logins.title",
            "logins.platform",
            "logins.service",
            expr("first_country_code as country_code"),
            expr("first_territory_name as territory_name"),
            "login_join_key"
        )
        .agg(
            expr("min(received_on) as install_timestamp"),
            expr("min(logins.timestamp_10min_slice) as timestamp_10min_slice"),
        )
        .select(
            "*",
            expr("install_timestamp::date as install_date")
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
                           set=
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
            "territory_name"
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

    df = extract(database, title)

    (
        df
        .writeStream
        .trigger(processingTime="10 minutes")
        # .trigger(availableNow = True)
        .option("checkpointLocation", checkpoint_location)
        .foreachBatch(lambda df, batch_id: proc_batch(df, data_source, database, title, view_mapping))
        .start()
    )




