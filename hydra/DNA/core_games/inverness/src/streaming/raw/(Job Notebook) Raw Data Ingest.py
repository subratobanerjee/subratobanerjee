# Databricks notebook source
import json
import os
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
import concurrent.futures

# COMMAND ----------

bootstrap_servers = "pkc-1jv6v.us-east-1.aws.confluent.cloud:9092"
user_id = dbutils.secrets.get(scope="data-engineering", key="confluent_svc_acc_userid")
password = dbutils.secrets.get(scope="data-engineering", key="confluent_svc_acc_password")

dbutils.widgets.text(name="checkpointLocation", defaultValue="dbfs:/tmp/core_games/checkpoint")
checkpoint_location = dbutils.widgets.get("checkpointLocation")

dbutils.widgets.text(name="title", defaultValue="inverness")
dbutils.widgets.text(name="topics", defaultValue="telemetry.events.read.good.9bd01e5b8d924235aeb50423c88a70f5.1")
dbutils.widgets.text(name="environment", defaultValue="_dev")

spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "false")

topic_db_map = {'telemetry.events.read.good.9bd01e5b8d924235aeb50423c88a70f5.1': 'inverness',
                'telemetry.events.read.good.317c0552032c4804bb10d81b89f4c37e.1': 'bluenose'}

# COMMAND ----------

title = dbutils.widgets.get("title")
topics = dbutils.widgets.get("topics")
environment = dbutils.widgets.get("environment")
checkpoint_location = dbutils.widgets.get("checkpoint_location")

# COMMAND ----------

df = (
    spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrap_servers)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(user_id, password))
    .option("kafka.ssl.endpoint.identification.algorithm", "https")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.group.id", f"data_eng_dbx_cg_consumer")
    .option("subscribe", topics)
    .option("startingOffsets", "latest")
    .option("minPartitions", spark.sparkContext.defaultParallelism)
    .load()
    .withColumn('value', col('value').cast(StringType())) # cast the binary json to a string
)


# COMMAND ----------

def event_to_dict(event_data):
    """
    parses the input string json object, then "flattens" the `eventData` node
    """

    flat_data = json.loads(event_data[0])
    for key in flat_data['eventData'].keys():
        if type(flat_data['eventData'][key]) is dict:
            flat_data[key] = flat_data['eventData'][key]
            for k in flat_data[key].keys():
                flat_data[key][k] = str(flat_data[key][k])
        else:
            flat_data[key] = str(flat_data['eventData'][key])

    del flat_data['eventData']
    return flat_data

def parallel_write(df, topic, event_name, batch_id, checkpoint_path):
    """
    writes the flattened data to a uniform table
    """

    title = topic_db_map[topic]

    ndf = (
        spark
        .read
        .json(df
            .filter(df.topic == topic)
            .where(f"get_json_object(value, '$.name')::string = '{event_name}'")
            .select("value")
            .rdd
            .map(event_to_dict)
        )
        .withColumn("insert_ts", current_timestamp())
        .withColumn("date", to_date(current_timestamp()))
    )

    (
        DeltaTable.createIfNotExists(spark)
        .tableName(f"{title}{environment}.raw.{event_name}")
        .addColumns(ndf.schema)
        .property('delta.enableIcebergCompatV2', 'true')
        .property('delta.universalFormat.enabledFormats', 'iceberg')
        .execute()
    )
    
    (
        ndf
        .write
        .option("mergeSchema", "true")
        .option("txnVersion", batch_id)
        .option("txnAppId", checkpoint_path)
        .format("delta")
        .mode("append")
        .saveAsTable(f"{title}{environment}.raw.{event_name}")
    )

def parallel_write_topic(df, topic, batch_id, checkpoint_path):
    """
    writes the flattened data to a uniform table
    """
    ndf = (
        spark
        .read
        .json(df
            .filter(df.topic == topic)
            .select("value")
            .rdd
            .map(event_to_dict)
        )
        .withColumn("insert_ts", current_timestamp())
        .withColumn("date", to_date(current_timestamp()))
    )

    (
        DeltaTable.createIfNotExists(spark)
        .tableName(f"{topic_table_map[topic]}")
        .addColumns(ndf.schema)
        .property('delta.enableIcebergCompatV2', 'true')
        .property('delta.universalFormat.enabledFormats', 'iceberg')
        .execute()
    )
    
    (
        ndf
        .write
        .option("mergeSchema", "true")
        .option("txnVersion", batch_id)
        .option("txnAppId", checkpoint_path)
        .format("delta")
        .mode("append")
        .saveAsTable(f"{topic_table_map[topic]}")
    )

def proc_batch(df: DataFrame, topic, id, checkpoint_path):
    """
    1. pulls distinct list of events out of this microbatch
    2. starts up a pool of workers based on number of driver CPU cores
    3. assigns 1 event to each worker to parallely write to uniform tables
    """

    event_list = df.filter(df.topic == topic).selectExpr("get_json_object(value, '$.name')::string").distinct().collect()

    with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        events = {executor.submit(parallel_write, df, topic, event[0], id, checkpoint_path): event for event in event_list}

        for future in concurrent.futures.as_completed(events):
            try:
                future.result()
            except Exception as exc:
                print(f"ERROR: {exc}")

    df.unpersist()

def proc_batch_pre_split(df: DataFrame, id, checkpoint_path):
    """
    1. pulls distinct list of topics out of this microbatch
    2. starts up a pool of workers based on number of driver CPU cores
    3. assigns 1 topic to each worker to parallely write to uniform tables
    """

    df.persist()
    topic_list = df.select("topic").distinct().collect()

    with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        topics = {executor.submit(proc_batch, df, topic[0], id, checkpoint_path): topic for topic in topic_list}

        for future in concurrent.futures.as_completed(topics):
            try:
                future.result()
            except Exception as exc:
                print(f"ERROR: {exc}")

    df.unpersist()
    

# COMMAND ----------

df\
    .writeStream\
    .foreachBatch(lambda df, batch_id: proc_batch_pre_split(df, batch_id, checkpoint_location))\
    .option("checkpointLocation", checkpoint_location)\
    .start()

