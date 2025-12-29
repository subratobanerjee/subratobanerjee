# Databricks notebook source
# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/oak2/agg_character_creation_nrt

# COMMAND ----------

from pyspark.sql.functions import expr,lit,current_timestamp,row_number,col
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'oak2'
data_source ='gearbox'
title = 'Borderlands 4'
view_mapping = {
    'agg_gp_1': 'characters_created',
    'agg_gp_2': 'player_count'
}
spark = create_spark_session()

# COMMAND ----------

def read_title_df(title):
    return (
        spark
        .read
        .table(f"reference{environment}.title.dim_title")
        .select(
            "title",
            "display_platform"
        )
        .where(expr(f"title = 'Borderlands 4'"))
    )

# COMMAND ----------

def read_mission_state_df(database):
    return (
        spark
        .readStream
        .option('maxFilesPerTrigger', 10000)
        .table(f"{database}{environment}.raw.oaktelemetry_mission_state")
        .filter("BuildConfig_string = 'Shipping Game'")
        .select(
            expr("player_platform_id as player_id"),
            expr("player_character_id"),
            expr("player_character"),
            expr("(case when platform_string = 'XSX' then 'XBSX' when platform_string = 'PS5Pro' then 'PS5' when lower(platform_string) = 'sage' then 'NSW2' else platform_string end) as platform"),
            expr("maw_time_received::timestamp as character_creation_ts"),
            expr("window(maw_time_received::timestamp,'10 minutes').end as timestamp_10min_slice")
        )
        .where(expr("player_platform_id is not null and player_character_id is not null and player_character not in ('OakCharacter', 'OakPlayerState', 'HexPlayerState', 'Player_Hex', 'ozoneplayer')"))
    )

# COMMAND ----------

def extract(database, title):
    title_df = read_title_df(title).alias("title")
    mission_df = read_mission_state_df(database).alias("mission")

    joined_df = (
        mission_df
        .join(title_df, on=expr("title.display_platform = mission.platform"), how="left")
        .select(
            "mission.player_id",
            "mission.player_character_id",
            "mission.player_character",
            expr("coalesce(title.title, 'Borderlands 4') as title"),
            expr("mission.timestamp_10min_slice"),
            "mission.character_creation_ts",
            expr("case when title.display_platform = 'WindowsEpic' then 'Windows' else coalesce(title.display_platform, 'Unknown') end as platform")
        )
        .distinct()
    )

    return joined_df

# COMMAND ----------

def transform(joined_df):
    # read lookup
    char_df = (
        spark
        .read
        .table(f"{database}{environment}.intermediate.player_character_creation_ledger")
        .alias("lkp")
    )

    # get first character creation per player/character combination
    # Take first player_id for each player_character_id to avoid double counting
    player_character_df = (
        joined_df
        .select(
            "player_id",
            "player_character_id",
            "player_character",
            "title",
            "character_creation_ts",
            "timestamp_10min_slice",
            expr("first_value(platform) over (partition by player_id, player_character_id order by character_creation_ts) as platform"),
            expr("first_value(player_id) over (partition by player_character_id order by character_creation_ts) as first_player_id")
        )
        .distinct()
        .filter(expr("player_id = first_player_id"))  # Only keep the first player_id for each character
        .select(
            "player_id",
            "player_character_id",
            "player_character",
            "title",
            "character_creation_ts",
            "timestamp_10min_slice",
            "platform",
            expr("sha2(concat_ws('|', player_id, first_player_id, player_character_id, player_character, title), 256) as char_join_key")
        )
    )

    # filter out characters that have already been counted as created
    new_characters = (
        player_character_df.alias("chars")
        .join(char_df.alias("lkp"), on=expr("chars.char_join_key = lkp.merge_key"), how="left")
        .where(expr("lkp.player_id is null or lkp.character_created = false"))
    )

    # get min timestamp for character creation ts
    first_character_df = (
        new_characters
        .groupBy(
            "chars.player_id",
            "chars.player_character_id",
            "chars.player_character",
            "chars.title",
            "chars.platform",
            "char_join_key"
        )
        .agg(
            expr("min(chars.timestamp_10min_slice) as timestamp_10min_slice"),
            expr("min(chars.character_creation_ts) as character_creation_ts")
        )
    )

    # create dataframe for merging into lookup
    mrg_df = (
        first_character_df
        .select(
            "player_id",
            "player_character_id",
            "player_character",
            "title",
            "platform",
            expr("True as character_created"),
            "character_creation_ts",
            "timestamp_10min_slice",
            expr("player_id as first_player_id"),  # Set first_player_id for new records
            expr("current_timestamp() as dw_insert_ts"),
            expr("current_timestamp() as dw_update_ts"),
            expr("char_join_key as merge_key")
        )
        .distinct()
    )

    # write to checkpoint
    mrg_df.write.mode("overwrite").saveAsTable(f"{database}{environment}.intermediate.character_creation_tmp")

    # merge into the lookup table
    character_lkp_table = DeltaTable.forName(spark, f"{database}{environment}.intermediate.player_character_creation_ledger")

    (
        character_lkp_table
        .alias("target")
        .merge(
            mrg_df.alias("source"),
            "target.player_character_id = source.player_character_id"
            )
        .whenMatchedUpdate(condition="(target.character_creation_ts > source.character_creation_ts AND target.first_player_id = source.first_player_id)",
                           set =
                           {
                               "target.character_creation_ts": "source.character_creation_ts",
                               "target.character_created": "source.character_created",
                               "target.timestamp_10min_slice": "source.timestamp_10min_slice",
                               "target.dw_update_ts": "current_timestamp()"
                           }
        )
        .whenMatchedUpdate(condition="(target.first_player_id IS NULL)",
                           set =
                           {
                               "target.first_player_id": "source.first_player_id",
                               "target.character_creation_ts": "source.character_creation_ts",
                               "target.character_created": "source.character_created",
                               "target.timestamp_10min_slice": "source.timestamp_10min_slice",
                               "target.dw_update_ts": "current_timestamp()"
                           }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    first_char_df = (
        spark
        .read
        .table(f"{database}{environment}.intermediate.player_character_creation_ledger")
        .groupBy(
            "player_id"
        )
        .agg(
            expr("min(character_creation_ts) as first_char_ts")
        )
    ).alias("first_char")

    # aggregate the character creation metrics
    # Join with ledger to get first_player_id and ensure only first player per character is counted
    character_creation_agg_df = (
        spark
        .read
        .table(f"{database}{environment}.intermediate.character_creation_tmp").alias("tmp")
        .join(
            char_df.select("player_character_id", "first_player_id", "character_creation_ts").alias("ledger"),
            expr("tmp.player_character_id = ledger.player_character_id"),
            "left"
        )
        .join(first_char_df, on=expr("tmp.player_id = first_char.player_id and tmp.character_creation_ts = first_char.first_char_ts"), how="left")
        .filter(expr("""
            ledger.first_player_id is null OR 
            tmp.player_id = ledger.first_player_id OR
            tmp.character_creation_ts < ledger.character_creation_ts
        """))
        # Additional deduplication: if multiple records for same character_id in this batch, take earliest
        .withColumn("row_num", row_number().over(Window.partitionBy(col("tmp.player_character_id")).orderBy(col("tmp.character_creation_ts"), col("tmp.player_id"))))
        .filter(col("row_num") == 1)
        .groupBy(
            "tmp.timestamp_10min_slice",
            "title",
            "platform",
            "tmp.player_character"
        )
        .agg(
            expr("count(distinct tmp.player_character_id) as agg_gp_1"),  # characters_created
            expr("count(distinct case when first_char_ts is not null then tmp.player_id end) as agg_gp_2") # player_count
        )
        .select(
            "tmp.timestamp_10min_slice",
            "title",
            "platform",
            expr("tmp.player_character as vault_hunter"),
            "agg_gp_1",
            "agg_gp_2"
        ).distinct()
    )
    
    return character_creation_agg_df

# COMMAND ----------

def proc_batch(df, data_source, database, title, view_mapping):
    # this enables us to do "stateless" microbatch processing off of a "stateful" streaming dataframe
    df = transform(df)
    load_agg_character_creation_nrt(spark, df, database, environment)

# COMMAND ----------

def stream_agg_character_creation(data_source, database, title, view_mapping):
    create_player_character_creation_ledger(spark, database)
    checkpoint_location = create_agg_character_creation_nrt(spark, database, view_mapping)
    print(checkpoint_location)

    df = extract(database, title)

    (
        df
        .writeStream
        .queryName("Oak 2 Agg Character Creation NRT")
        .trigger(processingTime = "1 minutes")
        .option("checkpointLocation", checkpoint_location)
        .foreachBatch(lambda df, batch_id: proc_batch(df, data_source, database, title, view_mapping))
        .start()
    )

# COMMAND ----------

stream_agg_character_creation(data_source, database, title, view_mapping)

# COMMAND ----------

# dbutils.fs.rm("dbfs:/tmp/oak2/managed/streaming/run_stg/agg_character_creation_nrt", True)
# spark.sql("DROP TABLE IF EXISTS oak2_stg.managed.agg_character_creation_nrt")
# spark.sql("DROP TABLE IF EXISTS oak2_stg.intermediate.player_character_creation_ledger")
# spark.sql("DROP TABLE IF EXISTS oak2_stg.intermediate.character_creation_tmp")
