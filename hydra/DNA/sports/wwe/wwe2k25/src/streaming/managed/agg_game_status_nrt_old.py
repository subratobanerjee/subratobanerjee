# Databricks notebook source
# MAGIC %run ../../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../../utils/ddl/managed/agg_game_status_nrt

# COMMAND ----------

from pyspark.sql.functions import expr

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment', set_environment())
database = 'wwe2k25'
data_source = 'dna'
view_mapping = {
    'agg_gp_1': 'agg_gp_1 as num_games_played',
    'agg_gp_2': 'agg_gp_2 as num_unique_players'
}

# COMMAND ----------

def read_player_match_status_df(environment):
    # Load source and reference dataframes
    return (
        spark
        .read
        .option('maxFilesPerTrigger', 10000)
        .table(f"wwe2k25{environment}.raw.matchstatus")
        .filter(
            expr("UPPER(buildtype) = 'FINAL'")
        )
        .groupBy(
            expr("playerPublicId as player_id"),
            "appPublicId",
            expr("gamemode as game_mode"),
            expr("ifnull(countryCode::string, 'ZZ') AS country_code")
        )
        .agg(
            expr("min(case when matchstatus = 'Start' then receivedOn::timestamp else null end) as start_received_on")
        )
        .select(
            "player_id",
            "appPublicId",
            "game_mode",
            expr("country_code"),
            expr("window(start_received_on::timestamp,'10 minutes').end as received_on_10min_slice")
        ).where(expr("received_on_10min_slice is not null"))
    )

# COMMAND ----------

def read_game_match_status_df(environment):
    # Load source and reference dataframes
    return (
        spark
        .readStream
        .option('maxFilesPerTrigger', 10000)
        .table(f"wwe2k25{environment}.raw.matchstatus")
        .select(
            expr("gamemode as game_mode"),
            expr("case when matchstatus = 'Start' then matchguid else null end as matchguid"),
            "appPublicId",
            expr("ifnull(countryCode::string, 'ZZ') AS country_code"),
            expr("window(receivedOn::timestamp,'10 minutes').end as received_on_10min_slice")
        ).where(expr(f"UPPER(buildtype) = 'FINAL' and matchguid is not null"))
    )


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
        .where(
            expr("timestamp_10min_slice between current_timestamp() - interval '12 hour' and current_timestamp()")
            # expr("timestamp_10min_slice >= '2024-10-01T00:00:00.000'")
            # & expr("timestamp_10min_slice < current_timestamp()")
        )
        .distinct()
    )

# COMMAND ----------

def extract(environment):
    # Join the source and reference tables
    games_match_df = read_game_match_status_df(environment).alias("games")
    players_match_df = read_player_match_status_df(environment).alias("players")
    source_df = (
        games_match_df.alias("match")
        .join(players_match_df.alias("player"), on=expr("match.appPublicId = player.appPublicId and match.received_on_10min_slice = player.received_on_10min_slice and match.country_code = player.country_code and match.game_mode = player.game_mode"), how='left')
        .select(
            "match.received_on_10min_slice",
            "match.country_code",
            "match.game_mode",
            "matchguid",
            "player_id",
            "match.appPublicId"
        )
    ).alias("source")

    title_df = read_title_df(environment).alias("title")
    country_df = read_country_df(environment).alias("country")
    joined_df = (
        source_df
        .join(title_df, on=expr("title.app_id = source.appPublicId"),
            how="inner")
        .join(country_df, on=expr("source.country_code = country.country_code"),
            how="inner")
        .select(
            expr("source.received_on_10min_slice as timestamp_10min_slice"),
            expr("title.display_platform as platform"),
            expr("title.display_service as service"),
            expr("source.country_code"),
            expr("country.territory_name"),
            expr("source.game_mode"),
            expr("source.matchguid"),
            expr("source.player_id")
        ).distinct()
    )

    return joined_df


# COMMAND ----------

def transform(df):
    plat_df = read_platform_territory_df(environment)
    
    transform_df = (
        df
        .groupBy(
            "timestamp_10min_slice",
            "platform",
            "service",
            "country_code",
            "territory_name",
            "game_mode"
        )
        .agg(
            expr("COUNT(DISTINCT matchguid)").alias("agg_gp_1"),
            expr("COUNT(DISTINCT player_id)").alias("agg_gp_2")
        ).distinct()
    )

    joined_df = (
        plat_df.alias("plat")
        .join(transform_df.alias("source"), on=expr("plat.timestamp_10min_slice = source.timestamp_10min_slice and plat.platform = source.platform and plat.service = source.service and plat.country_code = source.country_code and plat.territory_name = source.territory_name"),
            how="left")
        .select(
            "plat.timestamp_10min_slice",
            "plat.platform",
            "plat.service",
            "plat.country_code",
            "plat.territory_name",
            "game_mode",
            "agg_gp_1",
            "agg_gp_2"
        ).distinct()
    )

    return joined_df

# COMMAND ----------

def proc_batch(df, environment):
    # this enables us to do "stateless" microbatch processing off of a "stateful" streaming dataframe
    df = transform(df)
    load_agg_game_status_nrt(spark, df, database, environment)

# COMMAND ----------

def run_stream():
    checkpoint_location = create_agg_game_status_nrt(spark, database, view_mapping)
    #environment, checkpoint_location = "_dev", "dbfs:/tmp/wwe2k25/managed/streaming/run_dev/agg_game_status_nrt"

    df = extract(environment)

    (
        df
        .writeStream
        .outputMode("update")
        .queryName("WWE 2K25 Agg Game Status NRT")
        .trigger(processingTime = "10 minutes")
        .option("checkpointLocation", checkpoint_location)
        .foreachBatch(lambda df, batch_id: proc_batch(df, environment))
        .start()
    )

# COMMAND ----------

run_stream()
