# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

from pyspark.sql.functions import expr,lit
from delta.tables import DeltaTable

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database = 'inverness'
data_source = 'dna'
spark = create_spark_session()

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

    joined_df = (
        logins_df
        .join(title_df, on=expr("title.app_id = logins.appid"),
            how="left")
        .join(country_df, on=expr("logins.countrycode = country.country_code"),
            how="left")
        .select(
            expr("logins.received_on_10min_slice as timestamp_10min_slice"),
            expr("title.display_platform as platform"),
            expr("title.display_service as service"),
            expr("logins.countrycode as country_code"),
            expr("country.territory_name as territory_name"),
            expr("logins.player_id as player_id")
        )
    )

    return joined_df

# COMMAND ----------

def transform(joined_df):

    transform_df = (
        joined_df
        .groupBy(
            "timestamp_10min_slice",
            "platform",
            "service",
            "country_code",
            "territory_name"
        )
        .agg(
            expr("count(player_id) as login_count"),
            expr("current_timestamp() as dw_insert_ts"),
            expr("current_timestamp()  as dw_update_ts")
        )
        .select(
            "*",
            expr("sha2(concat_ws('|', timestamp_10min_slice, platform, service, country_code, territory_name), 256) as merge_key")
        )
        .alias("transform")
    )
    

    return transform_df
               

# COMMAND ----------

def load(df, environment):
    (
        DeltaTable.createIfNotExists(spark)
        .tableName(f"inverness{environment}.managed.agg_login_10min")
        .addColumn("timestamp_10min_slice", "timestamp")
        .addColumn("platform", "string")
        .addColumn("service", "string")
        .addColumn("country_code", "string")
        .addColumn("territory_name", "string")
        .addColumn("login_count", "int")
        .addColumn("dw_insert_ts", "timestamp")
        .addColumn("dw_update_ts", "timestamp")
        .addColumn("merge_key", "string")
        .property('delta.enableIcebergCompatV2', 'true')
        .property('delta.universalFormat.enabledFormats', 'iceberg')
        .execute()
    )

    target_table = DeltaTable.forName(spark, f"inverness{environment}.managed.agg_login_10min")

    (
        target_table.alias("old")
        .merge(
            df.alias("new"),
            "old.merge_key = new.merge_key"
        )
        .whenMatchedUpdate(condition = "new.login_count>0",
                           set = {
                               "old.login_count": "new.login_count+old.login_count",
                               "dw_update_ts": "current_timestamp()"
                           })
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

def proc_batch(df, environment):
    # this enables us to do "stateless" microbatch processing off of a "stateful" streaming dataframe
    df = transform(df)
    load(df, environment)

# COMMAND ----------

def run_stream():
    checkpoint_location = dbutils.widgets.get("checkpoint")
    #environment, checkpoint_location = "_dev", "dbfs:/tmp/inverness/managed/streaming/run_dev/agg_login_10min"

    df = extract(environment)

    (
        df
        .writeStream
        .trigger(availableNow=True)
        .option("checkpointLocation", checkpoint_location)
        .foreachBatch(lambda df, batch_id: proc_batch(df, environment))
        .start()
    )

# COMMAND ----------

run_stream()
