from pyspark.sql.functions import (
    lit, col, current_timestamp, window, from_unixtime, round, floor, unix_timestamp,
    sum, when, concat_ws, sha2, ifnull, get_json_object
)
from utils.helpers import arg_parser, setup_logger, setup_spark

logger = setup_logger()

# Step 1: Read from the horizon table
def read_party_event_df(environment, spark):
    return (
        spark
        .readStream
        .table(f"t2{environment}.t2gp_raw.horizon")
        .withColumn("received_on", col("date_posted").cast("timestamp"))
    )


# Step 2: Read the dim_player table
def read_dim_player_df(environment, spark):
    return (
        spark
        .table(f"horizon{environment}.managed.dim_player")
        .select("player_id", "first_seen_country_code")
        .withColumnRenamed("first_seen_country_code", "country_code") \
        .withColumn("country_code", ifnull(col("country_code"), lit("ZZ")))
        .dropDuplicates(["player_id"])
    )

# Step 3: Transform the data with windowing, aggregations, and join
def transform(environment, spark):
    party_event_df = read_party_event_df(environment, spark)
    dim_player_df = read_dim_player_df(environment, spark)

    # perform the join to get country_code
    joined_df = party_event_df.join(
        dim_player_df,
        party_event_df["subject"] == dim_player_df["player_id"],
        how="left"
    ).withColumn("country_code", ifnull(col("country_code"), lit("ZZ")))


    df = (
        joined_df
        .withColumn("timestamp_1hr_slice", from_unixtime(round(floor(unix_timestamp(col("received_on")) / 3600) * 3600)).cast("timestamp"))
        .withWatermark("received_on", "10 minutes")
        .groupBy(
            window("received_on", "1 hour").alias("window"),
            col("timestamp_1hr_slice"),
            col("country_code")
        )
        .agg(
            sum(when(col("event") == 't2gp.social.group.invite', 1).otherwise(0)).alias("total_invites"),
            sum(when(col("event") == 't2gp.social.group.accept', 1).otherwise(0)).alias("accepted_invites")
        )
        .withColumn("party_invite_accept_rate", when(col("total_invites") > 0, col("accepted_invites") / col("total_invites")).otherwise(lit(0)))  # Handle division by zero
        .withColumn("dw_insert_ts", current_timestamp())
        .withColumn("dw_update_ts", current_timestamp())
        .withColumn("merge_key", sha2(concat_ws("|", col("timestamp_1hr_slice"), col("country_code")), 256))
    )

    return df

# Step 4: Start the streaming process
def stream_party_invite_agg():
    environment, checkpoint_location = arg_parser()
    spark = setup_spark()

    # Set up Spark configurations
    spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "false")
    spark.conf.set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")
    spark.conf.set("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")

    df_transformed = transform(environment, spark)

    logger.info("Starting the streaming process")

    # Stream the transformed data to Delta table
    (
        df_transformed.writeStream
        .trigger(processingTime="5 minutes")
        .option("mergeSchema", "true")
        .option("checkpointLocation", checkpoint_location)
        .toTable(f"horizon{environment}.managed.agg_party_activity_1hr")
    )

    logger.info("Streaming process started successfully")

if __name__ == "__main__":
    stream_party_invite_agg()