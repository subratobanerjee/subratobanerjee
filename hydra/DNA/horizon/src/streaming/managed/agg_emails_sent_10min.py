from pyspark.sql.functions import (
    lit, col, current_timestamp, window, from_unixtime, round, floor, unix_timestamp,
    sum, when, concat_ws, sha2, ifnull, get_json_object,expr
)
from utils.helpers import arg_parser, setup_logger, setup_spark

logger = setup_logger()

# Step 1: Read from the emailEvent table

def read_title_df(environment, spark):
    title_df= (
                spark.
                read.
                table(f"reference{environment}.title.dim_title")
                .alias("title")
                )
    return title_df


def read_emailevent_df(environment, spark):
    title_df = read_title_df(environment, spark)

    email_event_df = (
        spark
        .readStream
        .table(f"coretech{environment}.sso.emailEvent")
       .alias("emails")
        .withColumns({
                        "player_id": col("accountid"),
                        "appid": col("appId"),
                        "appgroupid": col("appGroupId"),
                        "received_on": col("occurredOn").cast("timestamp"),
                        "received_on_10min_slice": from_unixtime(round(floor(unix_timestamp(col('received_on')) / 600) * 600)).cast("timestamp"),
                        "countrycode": ifnull(get_json_object(col("geoip"), "$.countryCode"),lit("ZZ")),
                    })
        .join(title_df, expr("appId = title.app_id"), "left")
        .where(col("app_id").isin("1f9a1706bd8444838b5eb44087589d64", "3b90f3160348447db69d2176b39be97a", "6f148ac38a854ccba6d423ce372d23ea"))
        .where(col("received_on") >= '2024-10-01T00:00:00.000')
        .select(
                "player_id",
                col("title.display_platform"),
                col("title.display_service"),
                "received_on",
                "appid",
                "appgroupid",
                "received_on_10min_slice",
                "countrycode"
        )
    )
    
    return email_event_df


# Step 2: Read the platform_territory_df table
def read_platform_territory_df(environment, spark):
    return (
        spark.table(f"dataanalytics{environment}.standard_metrics.platform_territory_10min_ts")
        .where(col("timestamp_10min_slice")>='2024-10-01T00:00:00.000')
        .where(col("timestamp_10min_slice")<'2024-11-01T00:00:00.000')
        .persist()
    )

# Step 3: Transform the data with windowing, aggregations, and join
def transform(environment, spark):
    emailevent_df = read_emailevent_df(environment, spark)
    platform_territory_df = read_platform_territory_df(environment, spark)

    df = (
        emailevent_df
        .withColumn("timestamp_10min_slice", from_unixtime(round(floor(unix_timestamp(col("received_on")) / 600) * 600)).cast("timestamp"))
        .withWatermark("received_on", "1 minutes")
        .groupBy(
            window("received_on", "10 minutes").alias("window"),
            col("received_on_10min_slice"),
            col("display_platform"),
            col("display_service"),
            col("countrycode")
        )
        .agg(
            sum(when(col("appId") == '3b90f3160348447db69d2176b39be97a', 1).otherwise(0)).alias("num_emails_sent"),
        )
        .withColumn("dw_insert_ts", current_timestamp())
        .withColumn("dw_update_ts", current_timestamp())
        .withColumn("merge_key", sha2(concat_ws("|", col("received_on_10min_slice"), col("countrycode"), col("display_platform"), col("display_service")), 256))
    )

    # Perform the join with platform_territory_df
    df = df.join(
        platform_territory_df,
        (col("timestamp_10min_slice") == col("received_on_10min_slice")) &
         (col("country_code") == col("countrycode")) &
         (col("platform") == col("display_platform")) &
         (col("service") == col("display_service")),
        how="left"
    )

    # Select relevant columns
    df = df.select(
        "window",
        "timestamp_10min_slice",
        "platform",
        "service",
        "country_code",
        "num_emails_sent",
        "dw_insert_ts",
        "dw_update_ts",
        "merge_key"
    ).fillna(0, subset=["num_emails_sent"])

    return df

# Step 4: Start the streaming process
def stream_agg_emails_sent():
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
        .trigger(processingTime="1 minutes")
        .option("mergeSchema", "true")
        .option("checkpointLocation", checkpoint_location)
        .toTable(f"horizon{environment}.managed.agg_email_sent_10min")
    )

    logger.info("Streaming process started successfully")

if __name__ == "__main__":
    stream_agg_emails_sent()