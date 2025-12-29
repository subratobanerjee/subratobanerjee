# Databricks notebook source
# MAGIC %run ../../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../../utils/helpers

# COMMAND ----------

from pyspark.sql.functions import expr,lit,current_timestamp
from delta.tables import DeltaTable

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'wwe2k25'
data_source ='dna'
spark = create_spark_session()

# COMMAND ----------

def create_agg_game_status(spark, environment):
    (
        DeltaTable.createIfNotExists(spark)
        .tableName(f"wwe2k25{environment}.intermediate.player_game_status_ledger")
        .addColumn("player_id", "string")
        .addColumn("title", "string")
        .addColumn("platform", "string")
        .addColumn("service", "string")
        .addColumn("country_code", "string")
        .addColumn("territory_name", "string")
        .addColumn("game_mode", "string")
        .addColumn("games_played", "boolean")
        .addColumn("first_game_played_ts", "timestamp")
        .addColumn("timestamp_10min_slice", "timestamp")
        .addColumn("dw_insert_ts", "timestamp")
        .addColumn("dw_update_ts", "timestamp")
        .addColumn("merge_key", "string")
        .property('delta.enableIcebergCompatV2', 'true')
        .property('delta.universalFormat.enabledFormats', 'iceberg')
        .execute()
    )

    (
        DeltaTable.createIfNotExists(spark)
        .tableName(f"wwe2k25{environment}.managed.agg_game_status_nrt")
        .addColumn("timestamp_10min_slice", "timestamp")
        .addColumn("title", "string")
        .addColumn("platform", "string")
        .addColumn("service", "string")
        .addColumn("country_code", "string")
        .addColumn("territory_name", "string")
        .addColumn("game_mode", "string")
        .addColumn("num_games_played", "int")
        .addColumn("num_unique_players", "int")
        .addColumn("dw_insert_ts", "timestamp")
        .addColumn("dw_update_ts", "timestamp")
        .addColumn("merge_key", "string")
        .property('delta.enableIcebergCompatV2', 'true')
        .property('delta.universalFormat.enabledFormats', 'iceberg')
        .execute()
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

def read_game_match_status_df(environment):
    # Load source and reference dataframes
    return (
        spark
        .readStream
        .option('maxFilesPerTrigger', 10000)
        .table(f"wwe2k25{environment}.raw.matchstatus")
        .select(
            expr("playerPublicId as player_id"),
            expr("gamemode as game_mode"),
            expr("receivedOn::timestamp as received_on"),
            expr("case when matchstatus = 'Start' then matchguid else null end as matchguid"),
            "appPublicId",
            expr("ifnull(countryCode::string, 'ZZ') AS country_code")
        ).where(expr(f"UPPER(buildtype) = 'FINAL' and matchguid is not null"))
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
        (expr("timestamp_10min_slice between current_timestamp() - interval '12 hour' and current_timestamp() + interval '10 minute' and platform in ('PS4', 'PS5', 'XBSX', 'XB1', 'Windows')")))
        #  expr("timestamp_10min_slice >= '2024-10-01T00:00:00.000'")
        #     & expr("timestamp_10min_slice < current_timestamp()"))
        .distinct()
    )

# COMMAND ----------

def extract(environment):
    # Join the source and reference tables
    source_df = read_game_match_status_df(environment).alias("source")
    title_df = read_title_df(environment).alias("title")
    country_df = read_country_df(environment).alias("country")
    joined_df = (
        source_df
        .join(title_df, on=expr("title.app_id = source.appPublicId"),
            how="inner")
        .join(country_df, on=expr("source.country_code = country.country_code"),
            how="inner")
        .select(
            expr("received_on"),
            expr("'WWE 2K25' as title"),
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

def transform(joined_df, enviroment):
    platform_territory_df = read_platform_territory_df(environment).alias("dt")

    # read lookup
    inst_df = (
        spark
        .read
        .table(f"wwe2k25{environment}.intermediate.player_game_status_ledger")
        .alias("lkp")
    )

    # get first country & territory
    player_first_country_df= (
        joined_df
        .select("player_id",
          "matchguid",
          "title",
          "received_on",
          expr("window(received_on, '10 minutes').end as timestamp_10min_slice"),
          "platform", 
          "service",
          "game_mode",
          expr("coalesce(first_value(country_code) ignore nulls over (partition by title, player_id, platform order by received_on), 'ZZ') as first_country_code"),
          expr("coalesce(first_value(territory_name) ignore nulls over (partition by title, player_id, platform order by received_on), 'zz') as first_territory_name"),
          )
        .distinct()
        .select(
            "*",
            expr("sha2(concat_ws('|', player_id, title, platform, service, game_mode), 256) as game_status_join_key")
        )
    )

    # filter out players that have already been counted as players
    new_players = (
        player_first_country_df.alias("gs")
        .join(inst_df, on=expr("gs.game_status_join_key = lkp.merge_key"), how="left")
        .where(expr("lkp.player_id is null or lkp.games_played = false"))
    )

    # get min timestamp for games played ts
    first_games_played_df= (
        new_players
        .groupBy(
            "gs.player_id",
            "gs.title",
            "gs.platform",
            "gs.service",
            expr("first_country_code as country_code"),
            expr("first_territory_name as territory_name"),
            "gs.game_mode",
            "game_status_join_key"
        )
        .agg(
            expr("min(received_on) as first_game_played_ts"),
            expr("min(gs.timestamp_10min_slice) as timestamp_10min_slice"),
        )
    )

    # create dataframe for merging into lookup
    mrg_df = (
        first_games_played_df
        .select(
            "player_id",
            "title",
            "platform",
            "service",
            "country_code",
            "territory_name",
            "game_mode",
            expr("True as games_played"),
            expr("first_game_played_ts"),
            "timestamp_10min_slice",
            expr("current_timestamp() as dw_insert_ts"),
            expr("current_timestamp() as dw_update_ts"),
            expr("game_status_join_key as merge_key")
        )
        .distinct()
    )

    games_transform_df = (
        player_first_country_df
        .groupBy(
            "timestamp_10min_slice",
            "title",
            "platform",
            "service",
            expr("first_country_code as country_code"),
            expr("first_territory_name as territory_name"),
            "game_mode"
        )
        .agg(
            expr("COUNT(DISTINCT matchguid)").alias("num_games_played"),
            expr("current_timestamp() as dw_insert_ts"),
            expr("current_timestamp() as dw_update_ts")
        ).select(
            "*",
            expr("sha2(concat_ws('|', timestamp_10min_slice, title, platform, service,country_code,territory_name, game_mode), 256) as merge_key")
        ).distinct()
    )

    # write to checkpoint
    mrg_df.write.mode("overwrite").saveAsTable(f"wwe2k25{environment}.intermediate.game_status_tmp")

    # write to checkpoint
    games_transform_df.write.mode("overwrite").saveAsTable(f"wwe2k25{environment}.intermediate.games_transform_tmp")

    # merge into the lookup table
    gs_lkp_table = DeltaTable.forName(spark, f"wwe2k25{environment}.intermediate.player_game_status_ledger")

    (
        gs_lkp_table
        .alias("target")
        .merge(
            mrg_df.alias("source"),
            "target.merge_key = source.merge_key")
        .whenMatchedUpdate(condition="(target.first_game_played_ts > source.first_game_played_ts)",
                           set =
                           {
                               "target.country_code": "source.country_code",
                               "target.territory_name": "source.territory_name",
                               "target.first_game_played_ts": "source.first_game_played_ts",
                               "target.games_played": "source.games_played",
                               "target.timestamp_10min_slice": "source.timestamp_10min_slice",
                               "target.dw_update_ts": "current_timestamp()"
                           }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    # aggregate the game status metric
    player_transform_df = (
        spark
        .read
        .table(f"wwe2k25{environment}.intermediate.game_status_tmp")
        .groupBy(
            "timestamp_10min_slice",
            "title",
            "platform",
            "service",
            "country_code",
            "territory_name",
            "game_mode"
        )
        .agg(
            expr("COUNT(DISTINCT player_id)").alias("num_unique_players"),
            expr("current_timestamp() as dw_insert_ts"),
            expr("current_timestamp() as dw_update_ts")
        )
        .select(
            "*",
            expr("sha2(concat_ws('|', timestamp_10min_slice, title, platform, service,country_code,territory_name, game_mode), 256) as merge_key")
        ).distinct()
    )

    plat_df = read_platform_territory_df(environment)

    games_transform_table = (
        spark
        .read
        .table(f"wwe2k25{environment}.intermediate.games_transform_tmp")
    )

    transform_df = (
        plat_df.alias("plat")
        .join(games_transform_table.alias("source"), on=expr("plat.timestamp_10min_slice = source.timestamp_10min_slice and plat.platform = source.platform and plat.service = source.service and plat.country_code = source.country_code and plat.territory_name = source.territory_name"),
            how="left")
        .join(player_transform_df.alias("games"), on=expr("plat.timestamp_10min_slice = games.timestamp_10min_slice and plat.platform = games.platform and plat.service = games.service and plat.country_code = games.country_code and plat.territory_name = games.territory_name and source.game_mode = games.game_mode"),
            how="left")
        .select(
            "plat.timestamp_10min_slice",
            "plat.platform",
            "plat.service",
            "plat.country_code",
            "plat.territory_name",
            expr("ifnull(coalesce(source.title, games.title), 'WWE 2K25') as title"),
            expr("ifnull(coalesce(source.game_mode, games.game_mode), 'Unknown') as game_mode"),
            expr("ifnull(num_games_played, 0) as num_games_played"),
            expr("ifnull(num_unique_players, 0) as num_unique_players")
        ).distinct()
    )

    final_df = (
        transform_df
        .select(
            "*",
            expr("current_timestamp() as dw_insert_ts"),
            expr("current_timestamp() as dw_update_ts"),
            expr("sha2(concat_ws('|', timestamp_10min_slice, title, platform, service,country_code,territory_name, game_mode), 256) as merge_key")
        ).distinct()        
    )
    
    return final_df

# COMMAND ----------

def load(df, environment):

    target_table = DeltaTable.forName(spark, f"wwe2k25{environment}.managed.agg_game_status_nrt")

    (
        target_table.alias("old")
        .merge(
            df.alias("new"),
            "old.merge_key = new.merge_key"
        )
        .whenMatchedUpdate(condition = "new.num_games_played > 0 or new.num_unique_players > 0",
                           set = {
                               "old.num_games_played": "new.num_games_played + old.num_games_played",
                               "old.num_unique_players": "new.num_unique_players + old.num_unique_players",
                               "dw_update_ts": "current_timestamp()"
                           })
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

def proc_batch(df, environment):
    # this enables us to do "stateless" microbatch processing off of a "stateful" streaming dataframe
    df = transform(df, environment)
    load(df, environment)

# COMMAND ----------

def run_stream():
    checkpoint_location = dbutils.widgets.get("checkpoint")
    # environment, checkpoint_location = "_dev", "dbfs:/tmp/wwe2k25/managed/streaming/run_dev/agg_game_status_nrt"
    create_agg_game_status(spark, environment)

    df = extract(environment)
    (
        df
        .writeStream
        .trigger(processingTime = "10 minutes")
        # .trigger(availableNow = True)
        .option("checkpointLocation", checkpoint_location)
        .foreachBatch(lambda df, batch_id: proc_batch(df, environment))
        .start()
    )

# COMMAND ----------

run_stream()
