# Databricks notebook source
#from utils.helpers import arg_parser,setup_spark
import json
from pyspark.sql.functions import (
    col, 
    sha2, 
    concat_ws, 
    current_timestamp, 
    get_json_object,
    first_value,
    last_value,
    coalesce,
    when,
    max
    )
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import Window
import argparse
import logging

# COMMAND ----------


def arg_parser():
    parser = argparse.ArgumentParser(description="consume parameters from Databricks job")
    parser.add_argument("--environment", type=str, required=True, help="Description for param1")
    parser.add_argument("--checkpoint_location", type=str, required=True, help="Description for param2")
    
    args = parser.parse_args()
    environment = args.environment
    checkpoint_location = args.checkpoint_location

    return environment,checkpoint_location

# COMMAND ----------

def read_entitlement_granted(spark,environment):
    entitlement_granted_df = (
        spark
        .readStream
        .table(f"coretech{environment}.ecommercev2.entitlementgrantedevent")
        .where(
            (get_json_object(col("eventData"), "$.productId") == '2a0041fa85174ca19d01803957f975d1') |
            ((get_json_object(col("eventData"), "$.appPublicId")).isin('a6713e10fe55485db1a19c6c4c474a33', '3f2709f3cc1e4f34f2af221cfbb79918')) &
            ((get_json_object(col("eventData"), "$.referenceId")).isNull() | (get_json_object(col("eventData"), "$.referenceId") == 'N/A'))
        )
)

    return entitlement_granted_df

# COMMAND ----------

def read_wallet_balance(spark,environment):
    wallet_balance_df = (
        spark
        .readStream
        .table(f"coretech{environment}.ecommercev2.walletbalancechangeevent")
        .where(
            (get_json_object(col("eventData"), "$.productId") == '2a0041fa85174ca19d01803957f975d1') |
            ((get_json_object(col("eventData"), "$.appPublicId")).isin('a6713e10fe55485db1a19c6c4c474a33', '3f2709f3cc1e4f34f2af221cfbb79918')) &
            ((get_json_object(col("eventData"), "$.referenceId")).isNull() | (get_json_object(col("eventData"), "$.referenceId") == 'N/A'))
        )
)

    return wallet_balance_df

# COMMAND ----------

def extract(spark,environment):
    
    entitlement_granted_df = read_entitlement_granted(spark,environment)
    wallet_balance_df = read_wallet_balance(spark,environment)
    extract_df = extract_df=entitlement_granted_df.union(wallet_balance_df)

    return extract_df

# COMMAND ----------

def transform(df, environment, spark):
    transformed_df = (
        df
        .alias("fact_player_entitlement")
        .withColumns({
                    "player_id": when(col("name") == "WalletBalanceChangeEvent",get_json_object(col("eventData"), "$.affectedAccountId"))
                                .when(col("name") == "EntitlementGrantedEvent",get_json_object( col("eventData"), "$.accountId")),

                    "country_code": col("countryCode"),
                    "app_public_id": when(col("name") == "WalletBalanceChangeEvent",col("appPublicId"))
                                    .when(col("name") == "EntitlementGrantedEvent",get_json_object( col("eventData"), "$.appPublicId")),
                    "occurred_on": col("occurredOn").cast("int").cast("timestamp"),
                    "received_on": col("receivedOn").cast("int").cast("timestamp"),
                    "currency_code": get_json_object(col("eventData"), "$.currencyCode"),
                    "item_amount": get_json_object(col("eventData"), "$.amounts[0].amount") .cast("int"),
                    "transaction_description": get_json_object(col("eventData"), "$.reason"),
                    "request_id": get_json_object(col("eventData"), "$.requestId"),
                    "resulting_balance": get_json_object(col("eventData"), "$.resultingBalance"),
                    "reference_id": get_json_object(col("eventData"), "$.referenceId"),
                    "item_id": get_json_object(col("eventData"), "$.itemId"),
                    "entitlement_id": get_json_object(col("eventData"), "$.entitlementId"),
                    "item_name": get_json_object(col("eventData"), "$.itemName"),
                    "item_type": get_json_object(col("eventData"), "$.itemType"),
                    "transaction_id": get_json_object(col("eventData"), "$.requestId"),
                    "session_id": col("sessionId"),
                    "trace_id": get_json_object(col("eventData"), "$.traceId"),
                    "merge_key": sha2(concat_ws("|", col("player_id"),col("app_public_id")), 256),
                    "dw_insert_ts": current_timestamp(),
                    "dw_update_ts": current_timestamp()
                })
        .where(col("player_id").isNotNull())  
        .select
        ("name",
         "player_id",
         "session_id",
         "occurred_on",
         "received_on",
         "currency_code",
         "item_amount",
         "transaction_description",
         "request_id",
         "resulting_balance",
         "trace_id",
         "reference_id",
         "item_id",
         "entitlement_id",
         "item_name",
         "item_type",
         "transaction_id",
         "merge_key",
         "dw_insert_ts",
         "dw_update_ts"
         )
    )

    return transformed_df


# COMMAND ----------

def load(df, environment, spark,checkpoint_location):
    (
        DeltaTable.createIfNotExists(spark)
        .tableName(f"horizon{environment}.intermediate.fact_player_entitlement")
        .addColumn("name" , "string")
        .addColumn("player_id" , "string")
        .addColumn("session_id" , "string")
        .addColumn("occurred_on" , "timestamp")
        .addColumn("received_on" , "timestamp")
        .addColumn("currency_code" , "string")
        .addColumn("item_amount" , "int")
        .addColumn("transaction_description" , "string")
        .addColumn("request_id" , "string")
        .addColumn("resulting_balance" , "string")
        .addColumn("trace_id" , "string")
        .addColumn("reference_id" , "string")
        .addColumn("item_id" , "string")
        .addColumn("entitlement_id" , "string")
        .addColumn("item_name" , "string")
        .addColumn("item_type" , "string")
        .addColumn("transaction_id" , "string")
        .property('delta.enableIcebergCompatV2', 'true')
        .property('delta.universalFormat.enabledFormats', 'iceberg')
        .execute()
    )

    (
    df
    .writeStream
    .option("mergeSchema", "true")
    .option("checkpointLocation", checkpoint_location)
    .trigger(availableNow=True)
    .format("delta")
    .outputMode("append")
    .toTable(f"horizon{environment}.intermediate.fact_player_entitlement")
    )

# COMMAND ----------

def process_fact_player_entitlement():
    environment, checkpoint_location = arg_parser()
    #environment, checkpoint_location = "_dev", "dbfs:/tmp/horizon/intermediate/run_dev/fact_player_entitlement"
    spark = SparkSession.builder.appName("Hydra").getOrCreate()

    spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

    df = extract(spark,environment)
    
    df = transform(df, environment, spark)

    load(df, environment, spark,checkpoint_location)


# COMMAND ----------

if __name__ == "__main__":
    process_fact_player_entitlement()
