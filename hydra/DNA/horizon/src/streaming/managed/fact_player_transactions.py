import json
from pyspark.sql.functions import (lit, col, when, get_json_object, current_timestamp, split, size)
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
import argparse
from pyspark.sql import SparkSession


# COMMAND ----------

def arg_parser():
    parser = argparse.ArgumentParser(description="Example script to consume parameters from Databricks job")
    parser.add_argument("--environment", type=str, required=True, help="Description for param1")
    parser.add_argument("--checkpoint_location", type=str, required=True, help="Description for param2")

    args = parser.parse_args()
    environment = args.environment
    checkpoint_location = args.checkpoint_location

    return environment, checkpoint_location

# COMMAND ----------

# read from the reference{env}.title.dim_title table to extract the platform and service data 
def read_title_df(environment, spark):

    # read the data from the dim_title table this is a non-stream read will need a waterMark against a readStream
    title_df = (
        spark
        .read
        .table(f"reference{environment}.title.dim_title")
        .alias("title")
        .select("title", "app_id", "platform", "service", "display_platform", "display_service")
    )

    return title_df

# COMMAND ----------

# read from the horizon(env}.raw.transactions table to extract the player transactions data
# this function reads the transactions stream table and joins onto the dim_tile non-stream table
def read_player_transaction_df(environment, spark):

    # load the title_df to join dim_title and transactions dataframes together
    title_df = read_title_df(environment, spark)

    # read the horizon{env}.raw.transactions table data
    transactions_df = (
        spark
        .readStream
        .table(f"horizon{environment}.raw.transactions")
        .withColumns({
                "transaction_id": col("extra_details.TransactionInstanceId").cast("string"),
                "player_id": col("player.dna_account_id").cast("string"),
                "received_on": col("receivedOn").cast("timestamp"),
                "game_id": col("gmi.gameplay_instance_id").cast("string"),
                "game_mode": col("event_defaults.match_type").cast("string"),
                "group_member": from_json(col("group.members"), "array<struct<name: string, id: string>>"), # struct used to pull out the group members definition
                "group_member_name": col("group_member.name"), # derived from the group_member to extract the name of each group member in the json object array item
                "group_member_size": size(col("group_member")), # use the size() function to count the number of items with the name definition in the json object array item
                "source_desc": col("extra_details.TransactionDescription").cast("string"),
                "country_code": lit("N/A").cast("string"),
                "currency_type": get_json_object(col("extra_details.TransactionType"), "$[0].earn.ItemId").cast("string"),
                "currency_amount": get_json_object(col("extra_details.TransactionType"), "$[0].earn.ItemAmount").cast("decimal(38,0)"),
                "action_type": col("extra_details.TransactionDescription").cast("string"),
                "item": get_json_object(col("extra_details.TransactionType"), "$[0].earn.ItemId").cast("string"),
                "item_type": get_json_object(col("extra_details.TransactionType"), "$[0].earn.ItemType").cast("string"),
                "item_price": get_json_object(col("extra_details.TransactionType"), "$[0].earn.ItemAmount").cast("float"),
                "build_changelist": col("event_defaults.cl").cast("string"),
                "extra_info_1": lit(None).cast("string"),
                "extra_info_2": lit(None).cast("string"),
                "extra_info_3": lit(None).cast("string"),
                "extra_info_4": lit(None).cast("string"),
                "extra_info_5": lit(None).cast("string"),
                "extra_info_6": lit(None).cast("string"),
                "extra_info_7": lit(None).cast("string"),
                "extra_info_8": lit(None).cast("string"),
                "dw_insert_ts": current_timestamp(),
                "dw_update_ts": current_timestamp()
            })
        .withColumn("sub_mode", when(col("group_member_size") == 1, lit("solos")) # case where group member contains 1 name
            .when(col("group_member_size") == 2, lit("duos")) # case where group member contains 2 names
            .when(col("group_member_size") == 3, lit("trios")) # case where group member contains 3 names
            .when(col("group_member_size") == 4, lit("quads"))) # case where group member contains 4 names
        .select("transaction_id", 
                "player_id", 
                "received_on", 
                "game_id", 
                "game_mode",
                "sub_mode", 
                "group_member_name", 
                "group_member_size", 
                "group_member", 
                "source_desc", 
                "country_code", 
                "currency_type", 
                "currency_amount", 
                "action_type", 
                "item", 
                "item_type", 
                "item_price", 
                "build_changelist", 
                "extra_info_1",
                "extra_info_2",
                "extra_info_3",
                "extra_info_4",
                "extra_info_5",
                "extra_info_6",
                "extra_info_7",
                "extra_info_8",
                "dw_insert_ts",
                "dw_update_ts")
        .join(title_df, expr("player_id = title.app_id"), "left")
        .withWatermark("received_on", "10 minutes")
        .select( # final schema columns
                "transaction_id", # transactions table
                "player_id", # transactions table
                "platform", # title table
                "service", # title table
                "received_on", # transactions table
                "game_id", # transactions table
                "game_mode", # transactions table
                "sub_mode", # transactions table
                "source_desc", # transactions table
                "country_code", # transactions table
                "currency_type", # transactions table
                "currency_amount", # transactions table
                "action_type", # transactions table
                "item", # transactions table
                "item_type", # transactions table
                "item_price", # transactions table
                "build_changelist", # transactions table
                "extra_info_1", # transactions table
                "extra_info_2", # transactions table
                "extra_info_3", # transactions table
                "extra_info_4", # transactions table
                "extra_info_5", # transactions table
                "extra_info_6", # transactions table
                "extra_info_7", # transactions table
                "extra_info_8", # transactions table
                "dw_insert_ts",
                "dw_update_ts") 
    )
    
    return transactions_df

# COMMAND ----------

def stream_player_transactions():
    environment, checkpoint_location = arg_parser()
    spark = SparkSession.builder.appName("Hydra").getOrCreate()

    spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "false")
    spark.conf.set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")
    spark.conf.set("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")
    
    # define the iceberg table horizon{env}.managed.fact_player_transaction
    (
    DeltaTable.createIfNotExists(spark)
    .tableName(f"horizon{environment}.managed.fact_player_transaction")
    .addColumn("transaction_id", "string")
    .addColumn("player_id", "string")
    .addColumn("platform", "string")
    .addColumn("service", "string")
    .addColumn("received_on", "timestamp")
    .addColumn("game_id", "string")
    .addColumn("game_mode", "string")
    .addColumn("sub_mode", "string")
    .addColumn("source_desc", "string")
    .addColumn("country_code", "string")
    .addColumn("currency_type", "string")
    .addColumn("currency_amount", "decimal(38, 0)")
    .addColumn("action_type", "string")
    .addColumn("item", "string")
    .addColumn("item_type", "string")
    .addColumn("item_price", "float")
    .addColumn("build_changelist", "string")
    .addColumn("extra_info_1", "string", nullable = True)
    .addColumn("extra_info_2", "string", nullable = True)
    .addColumn("extra_info_3", "string", nullable = True)
    .addColumn("extra_info_4", "string", nullable = True)
    .addColumn("extra_info_5", "string", nullable = True)
    .addColumn("extra_info_6", "string", nullable = True)
    .addColumn("extra_info_7", "string", nullable = True)
    .addColumn("extra_info_8", "string", nullable = True)
    .addColumn("dw_insert_ts", "timestamp")
    .addColumn("dw_update_ts", "timestamp")
    .property('delta.enableIcebergCompatV2', 'true')
    .property('delta.universalFormat.enabledFormats', 'iceberg')
    .execute()
    )

    transformed_df = read_player_transaction_df(environment, spark)

    (
        transformed_df
        .writeStream
        .option("checkpointLocation", checkpoint_location)
        .option("mergeSchema", "true")
        .toTable(f"horizon{environment}.managed.fact_player_transaction")
    )

# COMMAND ----------

if __name__ == "__main__":
    stream_player_transactions()

