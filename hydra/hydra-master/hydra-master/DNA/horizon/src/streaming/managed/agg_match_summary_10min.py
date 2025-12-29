from pyspark.sql import functions as F
from pyspark.sql.functions import col, count, current_timestamp, expr, sha2, concat_ws, min, from_unixtime, round, floor, unix_timestamp,ifnull,lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from utils.helpers import arg_parser, setup_logger, setup_spark
logger = setup_logger()


def create_old_player_table(spark):
    schema = StructType([
        StructField("player_id", StringType(), True),
        StructField("first_match_time", TimestampType(), True),
        StructField("timestamp", TimestampType(), True)])
    empty_schema_df = spark.createDataFrame([], schema)
    return empty_schema_df 

def load_processed_players(environment, spark, empty_schema_df):
    table_name = f"horizon{environment}.intermediate.old_player_id"  
    if spark.catalog.tableExists(table_name):
        logger.info(f"Table '{table_name}' exists.")
        return spark.table(table_name)
    else:
        logger.info(f"Table '{table_name}' does not exist. Creating it...")
        empty_schema_df.write \
            .mode("overwrite") \
            .format("delta") \
            .saveAsTable(f"horizon{environment}.intermediate.old_player_id")
        logger.info(f"Table '{table_name}' created successfully.")
        return spark.table(table_name)    

def extract_fact_player(environment, spark):
    fact_df = spark.readStream.table(f"horizon{environment}.managed.fact_player_match_summary")\
        .filter(F.col("medal").isNotNull())\
        .select("player_id", "end_ts")\
        .withColumn('timestamp', F.current_timestamp())\
        .withWatermark("timestamp", "1 minutes")\
        .groupBy("player_id","timestamp") \
        .agg(F.min("end_ts").alias("first_match_time"))
    return fact_df

def extract_dim_player(environment, spark): 
    dim_df = spark.read.table(f"horizon{environment}.managed.dim_player")\
        .select("player_id", "platform", "service", "first_seen_country_code")\
        .dropDuplicates(["player_id"])
    return dim_df

def transform(fact_df, dim_df, processed_players_df):    
    # Filter out players who have already been processed
    new_players_df = fact_df.join(processed_players_df, "player_id", "left_anti")
    transform_df = (
        new_players_df
        .join(dim_df, "player_id", "left")
        .withColumnRenamed("first_seen_country_code", "country_code")
        .withColumn("country_code", ifnull(col("country_code"), lit("ZZ")))
        .withColumn(
            "timestamp_10min_slice", 
            from_unixtime(
                round(floor(unix_timestamp(col("first_match_time")) / 600) * 600)
            ).cast("timestamp")
        )
        .groupBy(
            "timestamp",
            "timestamp_10min_slice",
            "platform", 
            "service",
            "country_code"
        )
        .agg(
            count(col("player_id")).alias("first_time_match_count")
        )
        .withColumn("title", expr("'Horizon Beta'"))
        .withColumnRenamed("timestamp", "dw_insert_ts")
        .withColumn("dw_update_ts", current_timestamp())
        .withColumn("merge_key", sha2(concat_ws("|", col("timestamp_10min_slice"), col("country_code"), col("platform"), col("service")), 256))
    )
    transform_df = transform_df.select(
        "timestamp_10min_slice",
        "platform",
        "service",
        "country_code",
        "first_time_match_count",
        "title",
        "dw_insert_ts",
        "dw_update_ts",
        "merge_key"
    )

    return transform_df, new_players_df

def agg_match_summary_10min():
    spark = setup_spark()
    environment, checkpoint_location = arg_parser()
    agg_match_summary_checkpoint = f"{checkpoint_location}"
    old_player_id_checkpoint = f"{checkpoint_location}/old_player_id"

    # Set up Spark configurations
    spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
    spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "false")
    spark.conf.set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")
    spark.conf.set("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")

    empty_schema_df = create_old_player_table(spark)
    fact_df = extract_fact_player(environment, spark)
    dim_df = extract_dim_player(environment, spark)
    processed_players_df = load_processed_players(environment, spark, empty_schema_df)
    processed_players_df = processed_players_df.dropDuplicates(["player_id"])
    transform_df, new_players_df = transform(fact_df, dim_df, processed_players_df)

    logger.info("Starting the streaming process")
    # Stream the new players data to Delta table
    (  
    new_players_df.writeStream \
        .trigger(processingTime="1 minutes") \
        .option("mergeSchema", "true") \
        .option("checkpointLocation", old_player_id_checkpoint) \
        .toTable(f"horizon{environment}.intermediate.old_player_id") 
    )  

    # Stream the transformed data with new players to Delta table
    (
        transform_df.writeStream \
        .trigger(processingTime="1 minutes") \
        .option("mergeSchema", "true") \
        .option("checkpointLocation", agg_match_summary_checkpoint) \
        .toTable(f"horizon{environment}.managed.agg_match_summary_10min") 
    )  
    
    logger.info("Streaming process started successfully")

if __name__ == "__main__":
    agg_match_summary_10min()