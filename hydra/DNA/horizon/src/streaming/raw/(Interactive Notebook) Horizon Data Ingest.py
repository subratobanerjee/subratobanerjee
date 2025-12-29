# Databricks notebook source
import json
import os
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
import concurrent.futures

# COMMAND ----------

cluster = "data_engineering"
# bootstrap_servers = "pkc-1jv6v.us-east-1.aws.confluent.cloud:9092"
# user_id = dbutils.secrets.get(scope="data-engineering", key="confluent_svc_acc_userid")
# password = dbutils.secrets.get(scope="data-engineering", key="confluent_svc_acc_password")
# topic = "telemetry.events.read.good.6aeb63f4494c4dca89e6f71b79789a84.1"

bootstrap_servers = "pkc-p11xm.us-east-1.aws.confluent.cloud:9092"
user_id = dbutils.secrets.get(scope="data-engineering", key="de-conflu-cluster-user")
password = dbutils.secrets.get(scope="data-engineering", key="de-conflu-cluster-password")
topic = "kevin.datagen.topic"

dbutils.widgets.text(name="checkpointLocation", defaultValue="dbfs:/tmp/kewert/checkpoint")
checkpoint_location = dbutils.widgets.get("checkpointLocation")

spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", True)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", False)
spark.conf.set("spark.databricks.variant.enabled","true")

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
    .option("minPartitions", os.cpu_count())
    .option("minOffsetsPerTrigger", 100)
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
    )

    (
        DeltaTable.createIfNotExists(spark)
        .tableName(f"horizon.raw_ingest_test.test_{event_name}")
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
        .saveAsTable(f"horizon.raw_ingest_test.test_{event_name}")
    )

def proc_batch(df: DataFrame, id, checkpoint_path):
    """
    1. pulls distinct list of events out of this microbatch
    2. starts up a pool of workers based on number of CPU cores
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

# COMMAND ----------

def string_merge_to_destinations(df: DataFrame, id):
    """
    This function writes the output data to tables, dependent on the topic:table map created in an above step.
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

df\
    .writeStream\
    .trigger(processingTime=".1 second")\
    .foreachBatch(string_merge_to_destinations)\
    .option("checkpointLocation", checkpoint_location)\
    .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC     insert_ts,
# MAGIC     round(avg((unix_timestamp(insert_ts) - parse_json(value):receivedOn::int)), 2) as avg_diff_ts,
# MAGIC     min((unix_timestamp(insert_ts) - parse_json(value):receivedOn::int)) as min_diff_ts,
# MAGIC     max((unix_timestamp(insert_ts) - parse_json(value):receivedOn::int)) as max_diff_ts,
# MAGIC     count(*) as rec_count
# MAGIC from horizon.raw_ingest_test.dna_all
# MAGIC where insert_ts >= '2024-05-20T16:41:00.581+00:00'
# MAGIC group by 1
# MAGIC order by insert_ts desc;

# COMMAND ----------

dbutils.fs.rm(checkpoint_location, True)
dbutils.fs.rm(f"dbfs:/tmp/kewert/", True)
dbutils.fs.mkdirs(f"dbfs:/tmp/kewert/")


df\
    .writeStream\
    .foreachBatch(lambda df, batch_id: proc_batch(df, batch_id, checkpoint_location))\
    .option("checkpointLocation", checkpoint_location)\
    .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC with raw_diff as (
# MAGIC     select 
# MAGIC         receivedOn::int::timestamp as received_on,
# MAGIC         insert_ts,
# MAGIC         (unix_timestamp(insert_ts) - receivedOn) as diff
# MAGIC     from horizon.raw_ingest_test.test_player_action_event
# MAGIC     where insert_ts > '2024-05-15T20:03:19.211+00:00'
# MAGIC )
# MAGIC select 
# MAGIC     "2 levels" as option,
# MAGIC     max(diff),
# MAGIC     min(diff),
# MAGIC     avg(diff),
# MAGIC     approx_percentile(diff, 0.99) as p99,
# MAGIC     approx_percentile(diff, 0.99) as p95,
# MAGIC     approx_percentile(diff, 0.75) as p75,
# MAGIC     approx_percentile(diff, 0.50) as p50,
# MAGIC     count(*) as rec_count
# MAGIC from raw_diff;

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC     insert_ts,
# MAGIC     avg((unix_timestamp(insert_ts) - receivedOn::int)) as avg_diff_ts,
# MAGIC     min((unix_timestamp(insert_ts) - receivedOn::int)) as min_diff_ts,
# MAGIC     max((unix_timestamp(insert_ts) - receivedOn::int)) as max_diff_ts,
# MAGIC     -- approx_percentile((unix_timestamp(insert_ts) - receivedOn::int), 0.99) as p99,
# MAGIC     -- approx_percentile((unix_timestamp(insert_ts) - receivedOn::int), 0.99) as p95,
# MAGIC     -- approx_percentile((unix_timestamp(insert_ts) - receivedOn::int), 0.75) as p75,
# MAGIC     -- approx_percentile((unix_timestamp(insert_ts) - receivedOn::int), 0.50) as p50,
# MAGIC     count(*) as rec_count
# MAGIC from horizon.raw_ingest_test.test_player_action_event
# MAGIC where insert_ts >= '2024-05-20T16:41:00.581+00:00'
# MAGIC group by 1
# MAGIC order by insert_ts desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC     insert_ts,
# MAGIC     avg((unix_timestamp(insert_ts) - receivedOn)) as avg_diff,
# MAGIC     min((unix_timestamp(insert_ts) - receivedOn)) as min_diff,
# MAGIC     max((unix_timestamp(insert_ts) - receivedOn)) as max_diff,
# MAGIC     count(*) as rec_count
# MAGIC from horizon.raw_ingest_test.test_player_action_event
# MAGIC where insert_ts > '2024-05-16T17:54:35.682+00:00'
# MAGIC group by 1
# MAGIC order by 1 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC     insert_ts,
# MAGIC     avg((unix_timestamp(insert_ts) - receivedOn)) as avg_diff_ts,
# MAGIC     min((unix_timestamp(insert_ts) - receivedOn)) as min_diff_ts,
# MAGIC     max((unix_timestamp(insert_ts) - receivedOn)) as max_diff_ts
# MAGIC     -- (insert_ts - receivedOn::timestamp) as min_diff,
# MAGIC     -- (insert_ts - receivedOn::timestamp) as max_diff
# MAGIC from horizon.raw_ingest_test.test_player_action_event
# MAGIC where insert_ts = '2024-05-16T20:25:12.144+00:00'
# MAGIC group by 1
# MAGIC order by 1 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table horizon.raw_ingest_test.test_player_action_event;

# COMMAND ----------

# MAGIC %sql
# MAGIC with raw_diff as (
# MAGIC     select 
# MAGIC         parse_json(value):receivedOn::int as received_on,
# MAGIC         insert_ts,
# MAGIC         (unix_timestamp(insert_ts) - received_on) as diff
# MAGIC     from horizon.raw_ingest_test.dna_all
# MAGIC     where insert_ts between '2024-05-17T15:03:28.674+00:00' and '2024-05-17T15:22:21.276+00:00'
# MAGIC )
# MAGIC select 
# MAGIC     "2 levels" as option,
# MAGIC     max(diff),
# MAGIC     min(diff),
# MAGIC     avg(diff),
# MAGIC     approx_percentile(diff, 0.99) as p99,
# MAGIC     approx_percentile(diff, 0.99) as p95,
# MAGIC     approx_percentile(diff, 0.75) as p75,
# MAGIC     approx_percentile(diff, 0.50) as p50,
# MAGIC     count(*) as rec_count
# MAGIC from raw_diff;

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC     insert_ts,
# MAGIC     avg((unix_timestamp(insert_ts) - parse_json(value):receivedOn::int)) as avg_diff_ts,
# MAGIC     min((unix_timestamp(insert_ts) - parse_json(value):receivedOn::int)) as min_diff_ts,
# MAGIC     max((unix_timestamp(insert_ts) - parse_json(value):receivedOn::int)) as max_diff_ts,
# MAGIC     count(*) as rec_count
# MAGIC from horizon.raw_ingest_test.dna_all
# MAGIC where insert_ts >= '2024-05-17T16:41:00.581+00:00'
# MAGIC group by 1
# MAGIC order by insert_ts desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC     count(*) as rec_count,
# MAGIC     unix_timestamp('2024-05-17T03:01:55.484+00:00'::timestamp) - unix_timestamp('2024-05-17T02:59:55.233+00:00'::timestamp) as sec_diff,
# MAGIC     rec_count/sec_diff as rec_per_sec
# MAGIC from horizon.raw_ingest_test.dna_all
# MAGIC where insert_ts between '2024-05-17T02:59:55.233+00:00' and '2024-05-17T03:01:55.484+00:00';

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   *
# MAGIC from horizon.raw_ingest_test.dna_all
# MAGIC where parse_json(value):name::string = 'player_action_event'
# MAGIC limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table horizon.raw_ingest_test.dna_all set tblproperties (delta.autoOptimize.autoCompact = false);

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table horizon.raw_ingest_test.dna_all;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists horizon.raw_ingest_test.ability_pickup;
# MAGIC drop table if exists horizon.raw_ingest_test.ability_usage_event;
# MAGIC drop table if exists horizon.raw_ingest_test.accomplishment_status;
# MAGIC drop table if exists horizon.raw_ingest_test.application_session_status;
# MAGIC drop table if exists horizon.raw_ingest_test.battlepass_status;
# MAGIC drop table if exists horizon.raw_ingest_test.bot_score_events;
# MAGIC drop table if exists horizon.raw_ingest_test.bot_stuck_spots;
# MAGIC drop table if exists horizon.raw_ingest_test.bounty_status;
# MAGIC drop table if exists horizon.raw_ingest_test.dev_destroyed_when_stuck;
# MAGIC drop table if exists horizon.raw_ingest_test.hero_customization;
# MAGIC drop table if exists horizon.raw_ingest_test.match_hero_progression;
# MAGIC drop table if exists horizon.raw_ingest_test.match_status_event;
# MAGIC drop table if exists horizon.raw_ingest_test.meta_progression_status;
# MAGIC drop table if exists horizon.raw_ingest_test.player_action_event;
# MAGIC drop table if exists horizon.raw_ingest_test.transactions;
# MAGIC drop table if exists horizon.raw_ingest_test.trials_status_event;
# MAGIC drop table if exists horizon.raw_ingest_test.weapon_usage_event;
