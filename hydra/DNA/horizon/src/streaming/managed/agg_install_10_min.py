# Databricks notebook source
import json
from pyspark.sql.functions import (lit, col, when, get_json_object, current_timestamp, split, window, from_unixtime, floor, unix_timestamp, sha2, concat_ws, expr, ifnull, first_value, countDistinct, round, max, min)
from delta.tables import DeltaTable
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import Window

# COMMAND ----------

def arg_parser():
    parser = argparse.ArgumentParser(description="Example script to consume parameters from Databricks job")
    parser.add_argument("--environment", type=str, required=True, help="Description for param1")
    parser.add_argument("--checkpoint_location", type=str, required=True, help="Description for param2")
    
    args = parser.parse_args()
    environment = args.environment
    checkpoint_location = args.checkpoint_location

    return environment,checkpoint_location

# COMMAND ----------

def create_agg_installs(spark,environment):
    (
        DeltaTable.createIfNotExists(spark)
        .tableName(f"horizon{environment}.intermediate.player_install_lkp")
        .addColumn("player_id", "string")
        .addColumn("app_id", "string")
        .addColumn("platform", "string")
        .addColumn("service", "string")
        .addColumn("country_code", "string")
        .addColumn("game_install", "boolean")
        .addColumn("game_install_ts", "timestamp")
        .addColumn("web_install", "boolean")
        .addColumn("web_install_ts", "timestamp")
        .addColumn("dw_insert_ts", "timestamp")
        .addColumn("dw_update_ts", "timestamp")
        .addColumn("merge_key", "string")
        .property('delta.enableIcebergCompatV2', 'true')
        .property('delta.universalFormat.enabledFormats', 'iceberg')
        .execute()
    )

    (
        DeltaTable.createIfNotExists(spark)
        .tableName(f"horizon{environment}.managed.agg_install_10min")
        .addColumn("timestamp_10min_slice", "timestamp")
        # .addColumn("platform", "string")
        # .addColumn("service", "string")
        .addColumn("country_code", "string")
        .addColumn("game_installs", "Long")
        .addColumn("website_installs", "Long")
        .addColumn("dw_update_ts", "timestamp")
        .addColumn("merge_key", "string")
        .property('delta.enableIcebergCompatV2', 'true')
        .property('delta.universalFormat.enabledFormats', 'iceberg')
        .execute()
    )

# COMMAND ----------

def read_title_df(environment, spark):
    title_df= (
                spark.
                read.
                table(f"reference{environment}.title.dim_title")
                .alias("title")
                )
    return title_df

# COMMAND ----------

def read_country_df(environment, spark):
    country_df = (
                spark.
                read.
                table(f"reference{environment}.location.dim_country")
                .alias("country")
                )
    return country_df

# COMMAND ----------

def read_players_df(environment, spark):
    title_df = read_title_df(environment, spark)
    country_df = read_country_df(environment, spark)

    logins_df = (
        spark
        .readStream
        .table(f"coretech{environment}.sso.loginevent")
        .alias("logins")
        .withColumns({
                        "player_id": col("accountid"),
                        "parent_id": col("parentAccountId"),
                        "appid": col("appId"),
                        "appgroupid": col("appGroupId"),
                        "received_on": col("occurredOn").cast("timestamp"),
                        "received_on_10min_slice": from_unixtime(round(floor(col("occurredOn") / 600) * 600)).cast("timestamp"),
                        "countrycode": get_json_object(col("geoip"), "$.countryCode"),
                        "DOB_Verified": col("isParentAccountDOBVerified"),                                                          
                        "login_join_key": sha2(concat_ws("|", col("player_id"), col("appid"), col("appgroupid")), 256)
                    })
        .join(title_df, expr("appId = title.app_id"), "left")
        .join(country_df.withColumnRenamed("name", "country_name"), expr("countrycode=country.country_code"), "left")
        .where(col("app_id").isin("1f9a1706bd8444838b5eb44087589d64", "3b90f3160348447db69d2176b39be97a", "6f148ac38a854ccba6d423ce372d23ea"))
        .select(
                "player_id",
                "parent_id",
                col("display_platform").alias("platform"),
                col("display_service").alias("service"),
                "received_on",
                "appid",
                "appgroupid",
                "received_on_10min_slice",
                "DOB_Verified",
                "login_join_key",
                "countrycode",
                "territory_name"
        )
    )

    return logins_df

# COMMAND ----------

def proc_batch(df, batch_id, checkpoint_path,environment,spark):
    target_table = DeltaTable.forName(spark, f"horizon{environment}.intermediate.player_install_lkp")

    inst_df = (
        spark
        .read
        .table(f"horizon{environment}.intermediate.player_install_lkp")
        .alias("lkp")
    )
    
    df = (
        df.alias("logins")
        .join(inst_df, expr("lkp.merge_key = login_join_key"), "left")
        .where((col("lkp.player_id").isNull()) | 
               (((col("lkp.game_install") == False) & (col("logins.appId") == '6f148ac38a854ccba6d423ce372d23ea') & (col("DOB_Verified") == lit(True))) | 
                ((col("lkp.web_install") == False) & col("logins.appId").isin("1f9a1706bd8444838b5eb44087589d64","3b90f3160348447db69d2176b39be97a"))))
        .withColumn("country_code",
                    when(col("lkp.country_code").isNotNull(), col("lkp.country_code"))
                    .otherwise(ifnull(first_value(col("logins.countrycode"))
                                      .over(Window.partitionBy(col("logins.player_id"), col("logins.platform"))
                                            .orderBy(col("logins.received_on"))),
                                      lit("ZZ"))))
        .selectExpr(
            "logins.*",
            "country_code"
        )
    )

    ndf = (
        df
        .groupBy(
                "player_id",
                "appid",
                "DOB_Verified",
                "platform",
                "service",
                "country_code"
                # "first_country_code"
            )
        .agg(
                min(col("received_on")).alias("install_timestamp"),
                min(col("received_on_10min_slice")).alias("install_timestamp_10min_slice")
            )
        .where(col("player_id").isNotNull())
        .withColumn("player_join_key", sha2(concat_ws("|", col("install_timestamp_10min_slice"), col("platform"), col("service"), col("country_code")), 256))
        .select("player_id",
                "appid",
                "DOB_Verified",
                "platform",
                "service",
                "country_code",
                "install_timestamp",
                col("install_timestamp_10min_slice").alias("timestamp_10min_slice"),
                "player_join_key")
    )

    df1 = (
        ndf
        .groupBy(
            "timestamp_10min_slice",
            # "platform",
            # "service",
            "country_code"
            )
        .agg(
            countDistinct(when((col("appId") == '6f148ac38a854ccba6d423ce372d23ea') & (col("DOB_Verified") == lit(True)), col("player_id"))).alias("game_installs"),
            countDistinct(when((col("appId").isin("1f9a1706bd8444838b5eb44087589d64","3b90f3160348447db69d2176b39be97a")), col("player_id"))).alias("website_installs"),
            current_timestamp().alias("dw_update_ts")
            )
        .withColumn(
            "merge_key", 
            sha2(concat_ws("|", col("timestamp_10min_slice"), col("country_code")), 256)
            # sha2(concat_ws("|", col("timestamp_10min_slice"), col("platform"), col("service"), col("country_code")), 256)
        )
        .select("timestamp_10min_slice", 
                # "platform", 
                # "service", 
                "country_code", 
                "game_installs",
                "website_installs",
                "dw_update_ts",
                "merge_key")
    )

    ttable = DeltaTable.forName(spark, f"horizon{environment}.managed.agg_install_10min")

    (
        ttable
        .alias("target")
        .merge(
            df1.alias("source"),
            "target.merge_key = source.merge_key")
        .withSchemaEvolution()
        .whenMatchedUpdate(condition="(source.game_installs <> target.game_installs) or (source.website_installs <> target.website_installs)", set = 
                           {
                                "target.game_installs": "source.game_installs",
                                "target.website_installs": "source.website_installs",
                                "target.dw_update_ts": "current_timestamp()"
                           })
        .whenNotMatchedInsertAll()
        .execute()
    )

    mrg_df = (
        df
        .groupBy(
            col("player_id"),
            col("appid").alias("app_id"),
            col("platform"),
            col("service"),
            col("country_code"),
            col("login_join_key").alias("merge_key")
        )
        .agg(
            max(when((col("logins.appId") == '6f148ac38a854ccba6d423ce372d23ea') & (col("DOB_Verified") == lit(True)), True).otherwise(False)).alias("game_install"),
            min(when((col("logins.appId") == '6f148ac38a854ccba6d423ce372d23ea') & (col("DOB_Verified") == lit(True)), col("received_on"))).alias("game_install_ts"),
            max(when((col("logins.appId").isin("1f9a1706bd8444838b5eb44087589d64","3b90f3160348447db69d2176b39be97a")), True).otherwise(False)).alias("web_install"),
            min(when((col("logins.appId").isin("1f9a1706bd8444838b5eb44087589d64","3b90f3160348447db69d2176b39be97a")), col("received_on"))).alias("web_install_ts")
        )
        .withColumns({
            "dw_insert_ts": current_timestamp(),
            "dw_update_ts": current_timestamp()
        })
    )

    (
        target_table
        .alias("target")
        .merge(
            mrg_df.alias("source"),
            "target.merge_key = source.merge_key")
        .withSchemaEvolution()
        .whenMatchedUpdate(condition="(target.web_install_ts > source.web_install_ts and target.web_install is True)",
                           set =
                           {
                               "target.country_code": "source.country_code",
                               "target.web_install_ts": "source.web_install_ts",
                               "target.dw_update_ts": "current_timestamp()"
                           }
                           )
        .whenMatchedUpdate(condition="(target.game_install_ts > source.game_install_ts and target.game_install is True)",
                           set =
                           {
                               "target.country_code": "source.country_code",
                               "target.game_install_ts": "source.game_install_ts",
                               "target.dw_update_ts": "current_timestamp()"
                           }
                           )
        .whenMatchedUpdate(condition="(target.game_install is False and source.game_install is True)",
                           set = 
                           {
                                "target.game_install": "source.game_install",
                                "target.game_install_ts": "source.game_install_ts",
                                "target.dw_update_ts": "current_timestamp()"
                           })
        .whenMatchedUpdate(condition="(target.web_install is False and source.web_install is True)",
                           set = 
                           {
                                "target.web_install": "source.web_install",
                                "target.web_install_ts": "source.web_install_ts",
                                "target.dw_update_ts": "current_timestamp()"
                           })
        .whenNotMatchedInsertAll()
        .execute()
    )


# COMMAND ----------

def stream_installs():
    environment, checkpoint_location = arg_parser()
    spark = SparkSession.builder.appName("Hydra").getOrCreate()

    spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "false")
    spark.conf.set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")
    spark.conf.set("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")

    create_agg_installs(spark,environment)

    transformed_df = read_players_df(environment, spark)

    (
        transformed_df
        .writeStream
        .trigger(processingTime = "10 minutes")
        .outputMode("update")
        .foreachBatch(lambda df, batch_id: proc_batch(df, batch_id, checkpoint_location,environment,spark))
        .option("checkpointLocation", checkpoint_location)
        .start()
    )

# COMMAND ----------

if __name__ == "__main__":
    stream_installs()
