# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

from pyspark.sql.functions import expr,lit,current_timestamp
from delta.tables import DeltaTable

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database = 'inverness'
data_source = 'dna'
spark = create_spark_session()

# COMMAND ----------

def create_agg_installs(spark, environment):
    (
        DeltaTable.createIfNotExists(spark)
        .tableName(f"inverness{environment}.intermediate.player_install_lkp")
        .addColumn("player_id", "string")
        .addColumn("platform", "string")
        .addColumn("service", "string")
        .addColumn("country_code", "string")
        .addColumn("territory_name", "string")
        .addColumn("game_install", "boolean")
        .addColumn("game_install_ts", "timestamp")
        .addColumn("dw_insert_ts", "timestamp")
        .addColumn("dw_update_ts", "timestamp")
        .addColumn("merge_key", "string")
        .property('delta.enableIcebergCompatV2', 'true')
        .property('delta.universalFormat.enabledFormats', 'iceberg')
        .execute()
    )

    (
        DeltaTable.createIfNotExists(spark)
        .tableName(f"inverness{environment}.managed.agg_install_10min")
        .addColumn("timestamp_10min_slice", "timestamp")
        .addColumn("platform", "string")
        .addColumn("service", "string")
        .addColumn("country_code", "string")
        .addColumn("territory_name", "string")
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
        .table(f"coretech{environment}.sso.loginevent")
        .where(expr("""appGroupId = '9bd01e5b8d924235aeb50423c88a70f5'"""))
        .select(
            expr("accountId as player_id"),
            "obfuscatedAccountId",
            "appId",
            "appgroupid",
            expr("ifnull(ifnull(get_json_object(geoip, '$.countryCode'),geoip_countrycode),'ZZ') as countrycode"),
            expr("occurredOn::timestamp as received_on"),
            expr("window(occurredOn::timestamp,'10 minutes').end as received_on_10min_slice")
        )
    )

# COMMAND ----------

def read_sso_mapping_df(environment):
    return (
        spark
        .read
        .table(f"reference{environment}.sso_mapping.dna_obfuscated_id_mapping")
        .where(expr("title in ('Civilization VII','2K Portal')"))
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
        .where(expr("timestamp_10min_slice between current_timestamp() - interval '2 hour' and current_timestamp()"))
        .distinct()
    )

# COMMAND ----------

def extract(environment):
    title_df = read_title_df(environment).alias("title")
    logins_df = read_logins_df(environment).alias("logins")
    country_df = read_country_df(environment).alias("country")
    sso_mapping_df = read_sso_mapping_df(environment).alias("sso_mapping")

    joined_df = (
        logins_df
        .join(sso_mapping_df, on=expr("logins.player_id = sso_mapping.unobfuscated_platform_id"))
        .join(title_df, on=expr("title.app_id = logins.appid"),
            how="left")
        .join(country_df, on=expr("logins.countrycode = country.country_code"),
            how="left")
    )

    return joined_df

# COMMAND ----------

def transform(joined_df):
    platform_territory_df = read_platform_territory_df(environment).alias("dt")

    # read lookup
    inst_df = (
        spark
        .read
        .table(f"inverness{environment}.intermediate.player_install_lkp")
        .alias("lkp")
    )

    # get first country & territory
    player_first_country_df= (
        joined_df
        .select("player_id", 
        "obfuscated_platform_id",
         "received_on",
         "received_on_10min_slice", 
          expr("display_platform as platform"), 
          expr("display_service as service"),
          expr("coalesce(first_value(country_code) ignore nulls over (partition by player_id, display_platform order by received_on), 'ZZ') as first_country_code"),
          expr("coalesce(first_value(territory_name) ignore nulls over (partition by player_id, display_platform order by received_on), 'zz') as first_territory_name")
          )
        .distinct()
        .select(
            "*",
            expr("sha2(concat_ws('|', player_id, platform, service), 256) as login_join_key")
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
            "obfuscated_platform_id",
            "logins.platform",
            "logins.service",
            expr("first_country_code as country_code"),
            expr("first_territory_name as territory_name"),
            "login_join_key"
        )
        .agg(
            expr("min(received_on) as install_timestamp"),
            expr("min(received_on_10min_slice) as install_timestamp_10min_slice"),
        )
    )

    # dedupe players that have already installed with their "obfuscated id"
    dedupe_df = (
        first_install_df.alias('fi')
        .join(
            first_install_df.alias('fii'),
            expr("fi.obfuscated_platform_id = fii.player_id"),
            'left')
        .where(expr("fi.obfuscated_platform_id is null or fii.player_id is null"))
        .select(
            'fi.player_id',
            'fi.platform',
            'fi.service',
            'fi.country_code',
            'fi.territory_name',
            'fi.install_timestamp',
            expr('fi.install_timestamp_10min_slice as timestamp_10min_slice'),
            'fi.login_join_key'
        )
    )

    # create dataframe for merging into lookup
    mrg_df = (
        dedupe_df
        .select(
            "player_id",
            "platform",
            "service",
            "country_code",
            "territory_name",
            expr("True as game_install"),
            expr("install_timestamp as game_install_ts"),
            expr("current_timestamp() as dw_insert_ts"),
            expr("current_timestamp() as dw_update_ts"),
            expr("login_join_key as merge_key")
        )
        .distinct()
    )

    # aggregate the install metric
    transform_df = (
        platform_territory_df.alias("pt")
        .join(dedupe_df.alias("dd"), 
              on=expr("""pt.timestamp_10min_slice = dd.timestamp_10min_slice
                      and pt.country_code = dd.country_code
                      and pt.territory_name = dd.territory_name
                      and pt.platform = dd.platform
                      and pt.service = dd.service"""), 
              how="left")
        .groupBy(
            "pt.timestamp_10min_slice",
            "pt.platform",
            "pt.service",
            "pt.country_code",
            "pt.territory_name"
        )
        .agg(
            expr("count(distinct player_id) as install_count"),
            expr("current_timestamp() as dw_insert_ts"),
            expr("current_timestamp() as dw_update_ts")
        )
        .select(
            "*",
            expr("sha2(concat_ws('|', timestamp_10min_slice, platform, service,country_code,territory_name), 256) as merge_key")
        )
    )

    # the agg table needs to be loaded before the lookup, because for some reason
    # loading the lookup table first causes the dataframe to be emptied
    load(transform_df, environment)

    # merge into the lookup table
    install_lkp_table = DeltaTable.forName(spark, f"inverness{environment}.intermediate.player_install_lkp")

    (
        install_lkp_table
        .alias("target")
        .merge(
            mrg_df.alias("source"),
            "target.merge_key = source.merge_key")
        .withSchemaEvolution()
        .whenMatchedUpdate(condition="(target.game_install_ts > source.game_install_ts and target.game_install is True)",
                           set =
                           {
                               "target.country_code": "source.country_code",
                               "target.territory_name": "source.territory_name",
                               "target.game_install_ts": "source.game_install_ts",
                               "target.dw_update_ts": "current_timestamp()"
                           }
                           )
        .whenNotMatchedInsertAll()
        .execute()
    )
    
    return transform_df
               

# COMMAND ----------

def load(df, environment):

    target_table = DeltaTable.forName(spark, f"inverness{environment}.managed.agg_install_10min")

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
    transform(df)
    # load(df, environment)

# COMMAND ----------

def run_stream():
    checkpoint_location = dbutils.widgets.get("checkpoint")
    # environment, checkpoint_location = "_dev", "dbfs:/tmp/inverness/managed/streaming/run_dev/agg_install_10min"
    create_agg_installs(spark, environment)

    df = extract(environment)

    (
        df
        .writeStream
        .trigger(availableNow=True)
        # .trigger(availableNow = True)
        .option("checkpointLocation", checkpoint_location)
        .foreachBatch(lambda df, batch_id: proc_batch(df, environment))
        .start()
    )

# COMMAND ----------

run_stream()
