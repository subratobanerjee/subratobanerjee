# Databricks notebook source
import json
import os
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
import concurrent.futures

# COMMAND ----------

cluster = "data_engineering"

bootstrap_servers = "pkc-6kr6m3.us-east-1.aws.confluent.cloud:9092"
user_id = dbutils.secrets.get(scope="data-engineering", key="de-conflu-load-test-user")
password = dbutils.secrets.get(scope="data-engineering", key="de-conflu-load-test-password")
topic = "telemetry.events.read.good.7ab8dd18fc874574b3d62afd2156e5fc.1"

dbutils.widgets.text(name="checkpointLocation", defaultValue="dbfs:/tmp/kewert/checkpoint")
checkpoint_location = dbutils.widgets.get("checkpointLocation")

spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "false")
# spark.conf.set("spark.databricks.variant.enabled","true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "false")

topic_table_map = {'6aeb63f4494c4dca89e6f71b79789a84.player_action_event': 'horizon.raw_ingest_test.test_player_action_event'}

# COMMAND ----------

df = (
    spark
    .readStream
    .format("kafka")
    .option("spark.sql.streaming.schemaInference", "true")
    .option("kafka.bootstrap.servers", bootstrap_servers)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(user_id, password))
    .option("kafka.ssl.endpoint.identification.algorithm", "https")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.group.id", f"data_eng_dbx_consumer")
    .option("subscribe", topic)
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

def parallel_write(df, event_name, batch_id, checkpoint_path):
    """
    writes the flattened data to a uniform table
    """

    ndf = (
        spark
        .read
        .json(df
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
        .tableName(f"horizon_load_test.raw.{event_name}")
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
        .saveAsTable(f"horizon_load_test.raw.{event_name}")
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

def proc_batch(df: DataFrame, id, checkpoint_path):
    """
    1. pulls distinct list of events out of this microbatch
    2. starts up a pool of workers based on number of driver CPU cores
    3. assigns 1 event to each worker to parallely write to uniform tables
    """

    df.persist()
    event_list = df.selectExpr("get_json_object(value, '$.name')::string").distinct().collect()

    with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        events = {executor.submit(parallel_write, df, event[0], id, checkpoint_path): event for event in event_list}

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
        topics = {executor.submit(parallel_write_topic, df, topic[0], id, checkpoint_path): topic for topic in topic_list}

        for future in concurrent.futures.as_completed(topics):
            try:
                future.result()
            except Exception as exc:
                print(f"ERROR: {exc}")

    df.unpersist()
    

# COMMAND ----------

def string_merge_to_destinations(df: DataFrame, id):
    """
    This function writes the output data to a `dna_all`-like table, without any transformations
    """
    (
        DeltaTable.createIfNotExists(spark)
        .tableName(f"horizon.raw_ingest_test.dna_all")
        .addColumns(df.schema)
        .property('delta.enableIcebergCompatV2', 'true')
        .property('delta.universalFormat.enabledFormats', 'iceberg')
        .execute()
    )

    (
        df
        .withColumn("insert_ts", current_timestamp())
        .write
        .option("mergeSchema", "true")
        .format("delta")
        .mode("append")
        .saveAsTable(f"horizon.raw_ingest_test.dna_all")
    )

# COMMAND ----------

checkpoint_location = "dbfs:/tmp/kewert/checkpoint/test_run_24"

df\
    .writeStream\
    .foreachBatch(lambda df, batch_id: proc_batch(df, batch_id, checkpoint_location))\
    .option("checkpointLocation", checkpoint_location)\
    .start()

# df\
#     .writeStream\
#     .foreachBatch(string_merge_to_destinations)\
#     .option("checkpointLocation", checkpoint_location)\
#     .start()
