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

def create_agg_installs(spark, environment):
    (
        DeltaTable.createIfNotExists(spark)
        .tableName(f"wwe2k25{environment}.intermediate.player_install_ledger")
        .addColumn("player_id", "string")
        .addColumn("title", "string")
        .addColumn("platform", "string")
        .addColumn("service", "string")
        .addColumn("country_code", "string")
        .addColumn("territory_name", "string")
        .addColumn("segment", "string")
        .addColumn("game_install", "boolean")
        .addColumn("game_install_ts", "timestamp")
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
        .tableName(f"wwe2k25{environment}.managed.agg_installs_nrt")
        .addColumn("timestamp_10min_slice", "timestamp")
        .addColumn("title", "string")
        .addColumn("platform", "string")
        .addColumn("service", "string")
        .addColumn("country_code", "string")
        .addColumn("territory_name", "string")
        .addColumn("segment", "string")
        .addColumn("install_count", "int")
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

def read_logins_df(environment):
    return (
        spark
        .readStream
        .option('maxFilesPerTrigger', 10000)
        .table(f"coretech_prod.sso.loginevent")
        .where(expr("""appGroupId = 'c5770d7b11bb48279c8dc7883a9fcbf2'"""))
        .select(
            expr("accountId as player_id"),
            "appId",
            "appgroupid",
            expr("ifnull(ifnull(get_json_object(geoip, '$.countryCode'),geoip_countrycode),'ZZ') as countrycode"),
            expr("occurredOn::timestamp as received_on"),
            expr("window(occurredOn::timestamp,'10 minutes').end as received_on_10min_slice")
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

def read_w24_clusters(environment):
    return (
        spark
        .read
        .table(f"wwe2k25{environment}.reference.wwe2k24_clusters")
        .select(
            expr("PLAYER_ID as clus_player_id"),
            "INITIAL_CLUSTER_LABEL"
        )
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
        .where(expr("timestamp_10min_slice between current_timestamp() - interval '12 hour' and current_timestamp() + interval '10 minute' and platform in ('PS4', 'PS5', 'XBSX', 'XB1', 'Windows')"))
        .distinct()
    )

# COMMAND ----------

def extract(environment):
    title_df = read_title_df(environment).alias("title")
    logins_df = read_logins_df(environment).alias("logins")
    country_df = read_country_df(environment).alias("country")
    cluster_df = read_w24_clusters(environment).alias("cluster")

    joined_df = (
        logins_df
        .join(title_df, on=expr("title.app_id = logins.appid"),
            how="left")
        .join(country_df, on=expr("logins.countrycode = country.country_code"),
            how="left")
        .join(cluster_df, on=expr("logins.player_id = cluster.clus_player_id"),
            how="left")
        .select(
            "logins.player_id",
            "title.title",
            expr("title.display_platform as platform"),
            expr("title.display_service as service"),
            expr("logins.countrycode as country_code"),
            "country.territory_name",
            expr("ifnull(INITIAL_CLUSTER_LABEL, 'No Segment') as segment"),
            "logins.received_on"
        )
    )

    return joined_df

# COMMAND ----------

def transform(joined_df, enviroment):
    platform_territory_df = read_platform_territory_df(environment).alias("dt")

    # read lookup
    inst_df = (
        spark
        .read
        .table(f"wwe2k25{environment}.intermediate.player_install_ledger")
        .alias("lkp")
    )

    # get first country & territory
    player_first_country_df= (
        joined_df
        .select("player_id",
          "title",
          "received_on",
          expr("window(received_on, '10 minutes').end as timestamp_10min_slice"),
          "platform", 
          "service",
          expr("coalesce(first_value(country_code) ignore nulls over (partition by player_id, title, platform order by received_on), 'ZZ') as first_country_code"),
          expr("coalesce(first_value(territory_name) ignore nulls over (partition by player_id, title, platform order by received_on), 'zz') as first_territory_name"),
          expr("ifnull(segment, 'No Segment') as segment")
          )
        .distinct()
        .select(
            "*",
            expr("sha2(concat_ws('|', player_id, title, platform, service), 256) as login_join_key")
        )
    )

    # filter out players that have already been counted as installed
    new_installs = (
        player_first_country_df.alias("logins")
        .join(inst_df, on=expr("logins.login_join_key = lkp.merge_key"), how="left")
        .where(expr("lkp.player_id is null or lkp.game_install = false"))
    )

    # get min timestamp for install ts
    first_install_df= (
        new_installs
        .groupBy(
            "logins.player_id",
            "logins.title",
            "logins.platform",
            "logins.service",
            expr("first_country_code as country_code"),
            expr("first_territory_name as territory_name"),
            "logins.segment",
            "login_join_key"
        )
        .agg(
            expr("min(received_on) as install_timestamp"),
            expr("min(logins.timestamp_10min_slice) as timestamp_10min_slice"),
        )
    )

    # create dataframe for merging into lookup
    mrg_df = (
        first_install_df
        .select(
            "player_id",
            "title",
            "platform",
            "service",
            "country_code",
            "territory_name",
            "segment",
            expr("True as game_install"),
            expr("install_timestamp as game_install_ts"),
            "timestamp_10min_slice",
            expr("current_timestamp() as dw_insert_ts"),
            expr("current_timestamp() as dw_update_ts"),
            expr("login_join_key as merge_key")
        )
        .distinct()
    )

    # write to checkpoint
    mrg_df.write.mode("overwrite").saveAsTable(f"wwe2k25{environment}.intermediate.install_tmp")

    # merge into the lookup table
    install_lkp_table = DeltaTable.forName(spark, f"wwe2k25{environment}.intermediate.player_install_ledger")

    (
        install_lkp_table
        .alias("target")
        .merge(
            mrg_df.alias("source"),
            "target.merge_key = source.merge_key")
        .whenMatchedUpdate(condition="(target.game_install_ts > source.game_install_ts)",
                           set =
                           {
                               "target.country_code": "source.country_code",
                               "target.territory_name": "source.territory_name",
                               "target.segment": "source.segment",
                               "target.game_install_ts": "source.game_install_ts",
                               "target.game_install": "source.game_install",
                               "target.timestamp_10min_slice": "source.timestamp_10min_slice",
                               "target.dw_update_ts": "current_timestamp()"
                           }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    # aggregate the install metric
    transform_df = (
        spark
        .read
        .table(f"wwe2k25{environment}.intermediate.install_tmp")
        .groupBy(
            "timestamp_10min_slice",
            "title",
            "platform",
            "service",
            "country_code",
            "territory_name",
            "segment"
        )
        .agg(
            expr("ifnull(count(distinct player_id), 0) as install_count"),
            expr("current_timestamp() as dw_insert_ts"),
            expr("current_timestamp() as dw_update_ts")
        )
        .select(
            "*",
            expr("sha2(concat_ws('|', timestamp_10min_slice, title, platform, service,country_code,territory_name, segment), 256) as merge_key")
        ).distinct()
    )
    
    return transform_df

# COMMAND ----------

def load(df, environment):

    target_table = DeltaTable.forName(spark, f"wwe2k25{environment}.managed.agg_installs_nrt")

    (
        target_table.alias("old")
        .merge(
            df.alias("new"),
            "old.merge_key = new.merge_key"
        )
        .whenMatchedUpdate(condition = "new.install_count>0",
                           set = {
                               "old.install_count": "new.install_count + old.install_count",
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
    # environment, checkpoint_location = "_dev", "dbfs:/tmp/wwe2k25/managed/streaming/run_dev/agg_installs_nrt"
    create_agg_installs(spark, environment)

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
