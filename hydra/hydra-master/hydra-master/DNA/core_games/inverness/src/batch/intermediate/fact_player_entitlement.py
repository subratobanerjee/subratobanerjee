# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------
# From utils.helpers import arg_parser, setup_spark
from pyspark.sql.functions import (window, current_timestamp, first_value, last_value, col, from_unixtime, 
                                   lit, expr, date_format, when, split, explode, get_json_object, from_json,
                                   sha2, concat_ws, coalesce)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from delta.tables import DeltaTable
from pyspark.sql.window import Window
import argparse
from pyspark.sql import SparkSession


# COMMAND ----------


# helper function from reusable source code
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
def read_dim_title_df(environment, spark):

    # read the data from the dim_title table this is a non-stream read will need a waterMark against a readStream
    title_df = (
        spark
        .read
        .table(f"reference{environment}.title.dim_title")
        .alias("title")
        .withColumns({
            "app_id": col("app_id").cast("string"), # join key against the raw.applicationsessionstatus source table
            "platform": col("display_platform").cast("string"),
            "service": col("display_service").cast("string")
          })
        .select("app_id", "platform", "service")
    )

    return title_df


# COMMAND ----------


# read the inverness{environment}.raw.applicationsessionstatus to extract the entitlements data
def read_application_session_status_df(environment, spark):
    title_df = read_dim_title_df(environment, spark)

    ass_active_dlc_df = (
        spark
        .read
        .table(f"inverness{environment}.raw.applicationsessionstatus")
        .where((expr("insert_ts::date > CURRENT_DATE() - INTERVAL 3 DAY")) & 
                (expr("playerpublicid is not null"))
                & 
                (expr("playerpublicid!='anonymous'")))  
        .withColumns({
            "app_public_id": col("appPublicId").cast("string"),
            "player_id": col("playerPublicId").cast("string"),
            "entitlement_id": explode(split(col("activedlc"), ",").cast("array<string>")),
            "entitlement_type": lit("DLC").cast("string"),
            "country_code": col("countryCode").cast("string"),
            "purchase_price": lit(0).cast("double"),
            "extra_info_1": lit("TBD").cast("string"),
            "extra_info_2": lit(None).cast("string"),
            "extra_info_3_raw": explode(split(col("purchaseddlc"), ", ").cast("array<string>")),
            "extra_info_4": lit(None).cast("string"),
            "extra_info_5": lit(None).cast("string"),
            "received_on": from_unixtime(col("receivedOn")).cast("timestamp"),
            "dw_insert_ts": current_timestamp(),
            "dw_update_ts": current_timestamp()
        })
        .withColumn("entitlement_name", col("entitlement_id"))
        .withColumn("extra_info_3", when(col("extra_info_3_raw").contains(col("entitlement_id")), lit("1")).otherwise(lit("0")))
            .select(
                "app_public_id",
                "player_id",
                "country_code",
                "entitlement_id",
                "entitlement_name",
                "entitlement_type",
                "purchase_price",
                "extra_info_1",
                "extra_info_2",
                "extra_info_3",
                "extra_info_4",
                "extra_info_5",
                "received_on",
                "dw_insert_ts",
                "dw_update_ts"
        )
    )
    ass_purchased_package_df = (
        spark
        .read
        .table(f"inverness{environment}.raw.applicationsessionstatus")
        .where((expr("insert_ts::date > CURRENT_DATE() - INTERVAL 3 DAY")) & 
                (expr("playerpublicid is not null"))
                & 
                (expr("playerpublicid!='anonymous'")))  
        .withColumns({
            "app_public_id": col("appPublicId").cast("string"),
            "player_id": col("playerPublicId").cast("string"),
            "entitlement_id": explode(split(col("purchasedpackages"), ",").cast("array<string>")),
            "entitlement_type": lit("DLC").cast("string"),
            "country_code": col("countryCode").cast("string"),
            "purchase_price": lit(0).cast("double"),
            "extra_info_1": lit("TBD").cast("string"),
            "extra_info_2": lit(None).cast("string"),
            "extra_info_3_raw": explode(split(col("purchaseddlc"), ", ").cast("array<string>")),
            "extra_info_4": lit(None).cast("string"),
            "extra_info_5": lit(None).cast("string"),
            "received_on": from_unixtime(col("receivedOn")).cast("timestamp"),
            "dw_insert_ts": current_timestamp(),
            "dw_update_ts": current_timestamp()
        })
        .withColumn("entitlement_name", col("entitlement_id"))
        .withColumn("extra_info_3", when(col("extra_info_3_raw").contains(col("entitlement_id")), lit("1")).otherwise(lit("0")))
            .select(
                "app_public_id",
                "player_id",
                "country_code",
                "entitlement_id",
                "entitlement_name",
                "entitlement_type",
                "purchase_price",
                "extra_info_1",
                "extra_info_2",
                "extra_info_3",
                "extra_info_4",
                "extra_info_5",
                "received_on",
                "dw_insert_ts",
                "dw_update_ts"
        )
    )
    application_session_status_df=(
        ass_active_dlc_df.unionByName(ass_purchased_package_df, True)
    )

    application_session_status_df_join = (
        application_session_status_df.join(title_df, expr("app_public_id = app_id "), "left")
        .withColumn("first_seen", first_value(col("received_on"))
                    .over(Window.partitionBy(col("player_id"), col("entitlement_id"), col("platform"), col("service"))
                        .orderBy(col("received_on").asc(), col("received_on"))))
        .withColumn("last_seen", first_value(col("received_on"))
                    .over(Window.partitionBy(col("player_id"), col("entitlement_id"), col("platform"), col("service"))
                        .orderBy(col("received_on").desc(), col("received_on"))))
        .withColumn("first_seen_country_code", when(col("country_code").isNull(), lit("ZZ")).otherwise(first_value(col("country_code"))
                    .over(Window.partitionBy(col("player_id"), col("entitlement_id"), col("platform"), col("service"))
                        .orderBy(col("received_on").asc(), col("received_on")))))
        .select(
            "player_id",
            "first_seen_country_code",
            "platform",
            "service",
            "entitlement_id",
            "entitlement_name",
            "entitlement_type",
            "purchase_price",
            "extra_info_1",
            "extra_info_2",
            "extra_info_3",
            "extra_info_4",
            "extra_info_5",
            "first_seen",
            "last_seen",
            "dw_insert_ts",
            "dw_update_ts"
        )
    )

    application_session_status_df_join = application_session_status_df_join.dropDuplicates(["player_id", "entitlement_id", "entitlement_name", "platform", "service"])

    return application_session_status_df_join       


# COMMAND ----------


# read the coretech{environment}.ecommercev2.entitlementgrantedevent to extract the entitlements data
def read_entititlement_granted_event_df(environment, spark):
    title_df = read_dim_title_df(environment, spark)

    # Define schema for eventData
    eventDataSchema = StructType([
        StructField("accountId", StringType(), True),
        StructField("entitlementId", StringType(), True),
        StructField("itemName", StringType(), True),
        StructField("itemType", StringType(), True),
        StructField("itemId", StringType(), True),
        StructField("appPublicId", StringType(), True)
    ])

    entitlement_granted_event_df = (
        spark
        .read
        .table(f"coretech{environment}.ecommercev2.entitlementgrantedevent")
        .where(expr("get_json_object(eventData, '$.productId') = 'd885935758854604911bbeb027f15dca'") &
               (expr("insert_ts::date > CURRENT_DATE() - INTERVAL 3 DAY")))
        .withColumn("eventData", from_json(col("eventData"), eventDataSchema))
        .withColumns({
            "app_public_id": col("eventData.appPublicId").cast("string"),
            "player_id": col("eventData.accountId"),
            "entitlement_id": col("eventData.entitlementId"),
            "entitlement_name": col("eventData.itemName"),
            "entitlement_type": col("eventData.itemType"),
            "country_code": col("countryCode").cast("string"),
            "purchase_price": lit(0).cast("double"),
            "extra_info_1": lit("TBD").cast("string"),
            "extra_info_2": col("eventData.itemId"),
            "extra_info_3": lit(None).cast("string"),
            "extra_info_4": lit(None).cast("string"),
            "extra_info_5": lit(None).cast("string"),
            "occurred_on": from_unixtime(col("occurredOn")).cast("timestamp"),
            "dw_insert_ts": current_timestamp(),
            "dw_update_ts": current_timestamp()
        })
        .select("app_public_id",
                "player_id",
                "country_code", 
                "entitlement_id",
                "entitlement_name",
                "entitlement_type",
                "purchase_price",
                "extra_info_1",
                "extra_info_2",
                "extra_info_3",
                "extra_info_4",
                "extra_info_5",
                "occurred_on",
                "dw_insert_ts",
                "dw_update_ts"
            )
    )

    entitlement_granted_event_df_join = (
        entitlement_granted_event_df.join(title_df, expr("app_public_id = app_id "), "left")
        .withColumn("first_seen", first_value(col("occurred_on"))
                    .over(Window.partitionBy(col("player_id"), col("entitlement_id"), col("platform"), col("service"))
                        .orderBy(col("occurred_on").asc(), col("occurred_on"))))
        .withColumn("last_seen", first_value(col("occurred_on"))
                    .over(Window.partitionBy(col("player_id"), col("entitlement_id"), col("platform"), col("service"))
                        .orderBy(col("occurred_on").desc(), col("occurred_on"))))
        .withColumn("first_seen_country_code", when(col("country_code").isNull(), lit("ZZ")).otherwise(first_value(col("country_code"))
                    .over(Window.partitionBy(col("player_id"), col("entitlement_id"), col("platform"), col("service"))
                        .orderBy(col("occurred_on").asc(), col("occurred_on")))))
        .select(
            "player_id",
            "first_seen_country_code",
            "platform",
            "service",
            "entitlement_id",
            "entitlement_name",
            "entitlement_type",
            "purchase_price",
            "extra_info_1",
            "extra_info_2",
            "extra_info_3",
            "extra_info_4",
            "extra_info_5",
            "first_seen",
            "last_seen",
            "dw_insert_ts",
            "dw_update_ts"
        )
    )

    return entitlement_granted_event_df_join



# COMMAND ----------


# refresh the schema and write data to the intermediate table
def batch_player_entitlement():

    # environment, checkpoint_location = "_dev", "dbfs:/tmp/inverness/intermediate/batch/run_dev/fact_player_entitlement"
    environment = dbutils.widgets.get("environment")
    checkpoint_location = dbutils.widgets.get("checkpoint")
    spark = create_spark_session()

    application_session_status_df_join = read_application_session_status_df(environment, spark)
    entitlement_granted_event_df_join = read_entititlement_granted_event_df(environment, spark)

    union_entitlement_df = (
        application_session_status_df_join.union(entitlement_granted_event_df_join)
        .withColumn("merge_key", sha2(concat_ws("|", col("player_id"), col("platform"), col("service"), col("entitlement_id")), 256))
        .select(
            "player_id",
            "first_seen_country_code",
            "platform",
            "service",
            "entitlement_id",
            "entitlement_name",
            "entitlement_type",
            "purchase_price",
            "extra_info_1",
            "extra_info_2",
            "extra_info_3",
            "extra_info_4",
            "extra_info_5",
            "first_seen",
            "last_seen",
            "dw_insert_ts",
            "dw_update_ts",
            "merge_key"
        )
        .dropDuplicates(["merge_key"])
    )

    (
    DeltaTable.createIfNotExists(spark)
        .tableName(f"inverness{environment}.intermediate.fact_player_entitlement")
        .addColumn("player_id", "string")
        .addColumn("platform", "string")
        .addColumn("service", "string")
        .addColumn("first_seen_country_code", "string")
        .addColumn("entitlement_id", "string")
        .addColumn("entitlement_name", "string")
        .addColumn("entitlement_type", "string")
        .addColumn("purchase_price", "double")
        .addColumn("extra_info_1", "string")
        .addColumn("extra_info_2", "string")
        .addColumn("extra_info_3", "string")
        .addColumn("extra_info_4", "string")
        .addColumn("extra_info_5", "string")
        .addColumn("first_seen", "timestamp")
        .addColumn("last_seen", "timestamp")
        .addColumn("dw_insert_ts", "timestamp")
        .addColumn("dw_update_ts", "timestamp")
        .addColumn("merge_key", "string")
        .property('delta.enableIcebergCompatV2', 'true')
        .property('delta.universalFormat.enabledFormats', 'iceberg')
        .execute()
    )

    target_table = DeltaTable.forName(spark, f"inverness{environment}.intermediate.fact_player_entitlement")
    (
        target_table
        .alias("target")
        .merge(
            union_entitlement_df.alias("source"),
            "target.merge_key = source.merge_key")
        .withSchemaEvolution()
        .whenMatchedUpdate(condition="source.first_seen < target.first_seen", set = 
                           {
                                "target.first_seen": "source.first_seen",
                                "target.first_seen_country_code": "source.first_seen_country_code",
                                "target.dw_update_ts": "current_timestamp()"
                           })
        .whenMatchedUpdate(condition="source.last_seen > target.last_seen", set =
                           {                                
                                "target.last_seen": "source.last_seen",
                                "target.dw_update_ts": "current_timestamp()"
                           })
        .whenNotMatchedInsertAll()
        .execute()
    )


# COMMAND ----------

if __name__ == "__main__":
    batch_player_entitlement()