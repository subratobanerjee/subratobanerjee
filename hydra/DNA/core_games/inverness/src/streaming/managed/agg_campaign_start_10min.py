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
data_source = 'inverness game telemetry'
spark = create_spark_session()

# COMMAND ----------

def create_tables(environment):
    (
        DeltaTable.createIfNotExists(spark)
        .tableName(f"inverness{environment}.managed.agg_campaign_start_10min")
        .addColumn("timestamp_10min_slice", "timestamp")
        .addColumn("platform", "string")
        .addColumn("service", "string")
        .addColumn("country_code", "string")
        .addColumn("territory_name", "string")
        .addColumn("age", "string")
        .addColumn("leader", "string")
        .addColumn("civilization", "string")
        .addColumn("campaign_start_count", "int")
        .addColumn("dw_insert_ts", "timestamp")
        .addColumn("dw_update_ts", "timestamp")
        .addColumn("merge_key", "string")
        .property('delta.enableIcebergCompatV2', 'true')
        .property('delta.universalFormat.enabledFormats', 'iceberg')
        .execute()
    )

    (
        DeltaTable.createIfNotExists(spark)
        .tableName(f"inverness{environment}.intermediate.campaign_start_ledger")
        .addColumn("campaign_id", "string")
        .addColumn("platform", "string")
        .addColumn("service", "string")
        .addColumn("country_code", "string")
        .addColumn("age", "string")
        .addColumn("leader", "string")
        .addColumn("civilization", "string")
        .addColumn("campaign_start_ts", "timestamp")
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

def read_campaign_start_df(environment):
    return (
        spark
        .readStream
        .option("skipChangeCommits", "true")
        .option('maxFilesPerTrigger', 10000)
        .option("withEventTimeOrder", "true")
        .table(f"inverness{environment}.raw.campaignstatus")
        .where(expr("""appGroupId = '9bd01e5b8d924235aeb50423c88a70f5' and campaignStatusEvent = 'CampaignEnter' and playerType = 'HUMAN'"""))
        .select(
            expr("playerpublicid as player_id"),
            "campaignInstanceId",
            expr("appPublicId as appId"),
            "appgroupid",
            "countryCode",
            "age",
            "leader",
            expr("civilizationselected as civilization"),
            expr("CAST(receivedOn AS TIMESTAMP) as received_on"),
            expr("window(CAST(receivedOn AS TIMESTAMP),'10 minutes').end as received_on_10min_slice")
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
    campaign_df = read_campaign_start_df(environment).alias("campaign")
    country_df = read_country_df(environment).alias("country")

    joined_df = (
        campaign_df
        .join(title_df, on=expr("title.app_id = campaign.appid"),
            how="left")
        .join(country_df, on=expr("campaign.countrycode = country.country_code"),
            how="left")
        .select(
            expr("campaign.received_on_10min_slice as timestamp_10min_slice"),
            expr("title.display_platform as platform"),
            expr("title.display_service as service"),
            expr("campaign.countrycode as country_code"),
            expr("country.territory_name as territory_name"),
            expr("campaign.campaignInstanceId as campaignInstanceId"),
            "campaign.age",
            "campaign.leader",
            "campaign.civilization",
            "campaign.received_on"
        )
    )

    return joined_df

# COMMAND ----------

def transform(joined_df):
    platform_territory_df = read_platform_territory_df(environment).alias("dt")

    # load the lookup (ledger) table
    lkp_table = DeltaTable.forName(spark, f"inverness{environment}.intermediate.campaign_start_ledger")
    lkp_df = (
        spark
        .read
        .table(f"inverness{environment}.intermediate.campaign_start_ledger")
    )

    # filter out rows that are already in the ledger
    joined_df = (
        joined_df
        .select(
            "*",
            expr("sha2(concat_ws('|', campaignInstanceId, platform, service, country_code, age, leader, civilization), 256) as merge_key")
        ).alias("joined")
        .join(lkp_df.alias("lkp"),
              on = expr("joined.merge_key = lkp.merge_key"),
              how = "left")
        .where(expr("lkp.merge_key is null"))
    )

    # calculate start timestamp for the campaign
    mrg_df = (
        joined_df
        .groupBy(
            expr("campaignInstanceId as campaign_id"),
            "joined.platform",
            "joined.service",
            "joined.country_code",
            "joined.territory_name",
            "joined.age",
            "joined.leader",
            "joined.civilization",
            "joined.merge_key"
        )
        .agg(
            expr("min(received_on) as campaign_start_ts")
        )
        .select(
            "*",
            expr("current_timestamp() as dw_insert_ts"),
            expr("current_timestamp() as dw_update_ts")
        )
    )

    # checkpoint the dataframe by writing into a temp table
    # this is required because merging the dataframe into our intermediate (ledger) table will force
    # the dataframe to be re-computed, causing data loss between the intermediate and managed calculation
    mrg_df.write.mode("overwrite").saveAsTable(f"inverness{environment}.intermediate.campaign_starts_tmp")

    # merge the dataframe
    (
        lkp_table.alias("tgt")
        .merge(mrg_df.select(expr("* except (territory_name)")).alias("src"),
            "tgt.merge_key = src.merge_key"
        )
        .whenMatchedUpdate(condition = "src.campaign_start_ts < tgt.campaign_start_ts",
                            set = {
                                "tgt.campaign_start_ts": "src.campaign_start_ts",
                                "tgt.dw_update_ts": "src.dw_update_ts"
                            })
        .whenNotMatchedInsertAll()
        .execute()
    )

    # read from the temp table (checkpoint)
    transform_df = (
        spark
        .read
        .table(f"inverness{environment}.intermediate.campaign_starts_tmp")
        .groupBy(
            expr("window(campaign_start_ts, '10 minutes').end as timestamp_10min_slice"),
            "platform",
            "service",
            "country_code",
            "territory_name",
            "age",
            "leader",
            "civilization"
        )
        .agg(
            expr("count(distinct campaign_id) as campaign_start_count")
        )
        .select(
            "*",
            expr("current_timestamp() as dw_insert_ts"),
            expr("current_timestamp() as dw_update_ts"),
            expr("sha2(concat_ws('|', timestamp_10min_slice, platform, service, country_code, territory_name, age, leader, civilization), 256) as merge_key")
        )
    )

    return transform_df
               

# COMMAND ----------

def load(df, environment):
    target_table = DeltaTable.forName(spark, f"inverness{environment}.managed.agg_campaign_start_10min")

    (
        target_table.alias("old")
        .merge(
            df.alias("new"),
            "old.merge_key = new.merge_key"
        )
        .whenMatchedUpdate(condition = "new.campaign_start_count > 0",
                           set = {
                               "old.campaign_start_count": "new.campaign_start_count+old.campaign_start_count",
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
    # environment, checkpoint_location = "_dev", "dbfs:/tmp/inverness/managed/streaming/run_dev/agg_campaign_start_10min"
    create_tables(environment)

    df = extract(environment)

    (
        df
        .writeStream
        .queryName("Inverness Campaign_start NRT")
        .trigger(availableNow=True)
        .option("checkpointLocation", checkpoint_location)
        .foreachBatch(lambda df, batch_id: proc_batch(df, environment))
        .start()
    )

# COMMAND ----------

run_stream()
