# Databricks notebook source
# MAGIC %run ../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../utils/helpers

# COMMAND ----------

from pyspark.sql.functions import expr
from delta.tables import DeltaTable

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database = 'reference'
data_source = 'dna'
spark = create_spark_session()

# COMMAND ----------

def read_logins(environment, title_df):
    return (
        spark
        .readStream
        .option('maxFilesPerTrigger', 10000)
        .table(f"coretech{environment}.sso.loginevent")
        .where(expr("""isParentAccountDOBVerified = 'True'"""))
        .select(
            "accountId",
            "obfuscatedAccountId",
            "appId",
            expr("occurredOn::timestamp as received_on")
        )
    )

# COMMAND ----------

def read_links(environment, title_df):
    return (
        spark
        .readStream
        .option('maxFilesPerTrigger', 10000)
        .table(f"coretech{environment}.sso.linkevent")
        .select(
            expr("""
                 case when accounttype = '3' and targetaccounttype = '2' then targetAccountId
                      when accounttype = '2' and targetaccounttype = '3' then accountId end as accountId
                 """),
            "obfuscatedAccountId",
            "appId",
            expr("occurredOn::timestamp as received_on")
        )
    )

# COMMAND ----------

def read_title(environment):
    return (
        spark
        .read
        .table(f"reference{environment}.title.dim_title")
    )

def read_unlinkevent(environment):
    return (
        spark
        .read
        .table(f"coretech{environment}.sso.unlinkevent")
        .groupBy("accountId")
        .agg(expr("max(to_timestamp(occurredOn)) as ul_max_received_on"))
    )

# COMMAND ----------

def extract(environment):
    title_df = read_title(environment).alias("dt")
    logins_df = read_logins(environment, title_df)
    links_df = read_links(environment, title_df)

    unioned_df = (
        logins_df
        .unionByName(links_df)
        .alias("le")
        .join(title_df, on=expr("dt.app_id = le.appid"))
        .select(
            "dt.title",
            expr("accountId as unobfuscated_platform_id"),
            expr("obfuscatedAccountId as obfuscated_platform_id"),
            "received_on"
        )
    )

    return unioned_df

# COMMAND ----------

def transform(df):
    out_df = (
        df
        .groupBy(
            "title",
            "unobfuscated_platform_id",
            "obfuscated_platform_id"
        )
        .agg(
            expr("min(received_on) as min_received_on"),
            expr("max(received_on) as max_received_on")
        )
        .where(expr("obfuscated_platform_id is not null and unobfuscated_platform_id is not null"))
        .select(
            "*",
            expr("sha2(concat_ws('|', title, unobfuscated_platform_id, obfuscated_platform_id), 256) as merge_key")
        )
    )

    return out_df

# COMMAND ----------

def load(df, unlink_df, environment):
    (
        DeltaTable.createIfNotExists(spark)
        .tableName(f"reference{environment}.sso_mapping.dna_obfuscated_id_mapping")
        .addColumn("title", "string")
        .addColumn("unobfuscated_platform_id", "string")
        .addColumn("obfuscated_platform_id", "string")
        .addColumn("min_received_on", "timestamp")
        .addColumn("max_received_on", "timestamp")
        .addColumn("merge_key", "string")
        .property('delta.enableIcebergCompatV2', 'true')
        .property('delta.universalFormat.enabledFormats', 'iceberg')
        .execute()
    )

    target_table = DeltaTable.forName(spark, f"reference{environment}.sso_mapping.dna_obfuscated_id_mapping")

    target_table.alias("t").merge(
        unlink_df.alias("u"),
        "t.unobfuscated_platform_id = u.accountId AND t.max_received_on < u.ul_max_received_on"
    ).whenMatchedDelete().execute()
    

    (
        target_table.alias("old")
        .merge(
            df.alias("new"),
            "old.merge_key = new.merge_key"
        )
        .whenMatchedUpdate(condition = "new.max_received_on > old.max_received_on",
                           set = {
                               "old.max_received_on": "new.max_received_on"
                           })
        .whenMatchedUpdate(condition = "new.min_received_on < old.min_received_on",
                           set = {
                               "old.min_received_on": "new.min_received_on"
                           })
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

def proc_batch(df,environment):
    # this enables us to do "stateless" microbatch processing off of a "stateful" streaming dataframe
    unlink_df = read_unlinkevent(environment).alias("unl")
    df = transform(df)
    load(df, unlink_df, environment)

# COMMAND ----------

def run_stream():
    checkpoint_location = dbutils.widgets.get("checkpoint")
    # environment, checkpoint_location = "_dev", "dbfs:/tmp/map_test/dna_mapping"

    df = extract(environment)

    (
        df
        .writeStream
        .queryName("DNA SSO Mapping")
        .trigger(processingTime = "5 minutes")
        .option("checkpointLocation", checkpoint_location)
        .foreachBatch(lambda df, batch_id: proc_batch(df, environment))
        .start()
    )

# COMMAND ----------

run_stream()
