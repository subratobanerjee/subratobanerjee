# Databricks notebook source
import json
import os
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
import concurrent.futures

# COMMAND ----------

cluster = "data_engineering"
bootstrap_servers = "pkc-1jv6v.us-east-1.aws.confluent.cloud:9092"
user_id = dbutils.secrets.get(scope="data-engineering", key="confluent_svc_acc_userid")
password = dbutils.secrets.get(scope="data-engineering", key="confluent_svc_acc_password")
topic = "telemetry.events.read.good.6aeb63f4494c4dca89e6f71b79789a84.1"

# bootstrap_servers = "pkc-p11xm.us-east-1.aws.confluent.cloud:9092"
# user_id = dbutils.secrets.get(scope="data-engineering", key="de-conflu-cluster-user")
# password = dbutils.secrets.get(scope="data-engineering", key="de-conflu-cluster-password")
# topic = "kevin.datagen.topic"

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
    .option("minPartitions", spark.sparkContext.defaultParallelism)
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
        .tableName(f"horizon.raw.{event_name}")
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
        .saveAsTable(f"horizon.raw.{event_name}")
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

dbutils.fs.rm(checkpoint_location, True)
dbutils.fs.rm(f"dbfs:/tmp/kewert/", True)
dbutils.fs.mkdirs(f"dbfs:/tmp/kewert/")

# COMMAND ----------

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
# MAGIC     from horizon_load_test.raw.collectedammo1
# MAGIC     where insert_ts > '2024-06-19T20:03:19.211+00:00'
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
# MAGIC from horizon_load_test.raw.collectedammo1
# MAGIC where insert_ts >= '2024-06-19T16:41:00.581+00:00'
# MAGIC group by 1
# MAGIC order by insert_ts desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC with rec_cnt as (
# MAGIC   select
# MAGIC     insert_ts,
# MAGIC     count(*) as rec_count
# MAGIC   from horizon_load_test.raw.collectedammo1
# MAGIC   where insert_ts between '2024-06-20T16:00:54.931+00:00' and '2024-06-20T16:55:28.428+00:00'
# MAGIC   group by 1
# MAGIC )
# MAGIC select
# MAGIC   avg(rec_count) * 15 as avg_rec_cnt
# MAGIC from rec_cnt;

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC     insert_ts,
# MAGIC     max(receivedOn::int::timestamp) as max_received_on
# MAGIC from horizon_load_test.raw.collectedammo1
# MAGIC where insert_ts >= '2024-06-19T16:41:00.581+00:00'
# MAGIC group by 1
# MAGIC order by insert_ts desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC     count(*)
# MAGIC from horizon_load_test.raw.collectedammo1
# MAGIC where insert_ts >= '2024-06-19T16:41:00.581+00:00';

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists horizon_load_test.raw.ability_pickup;
# MAGIC drop table if exists horizon_load_test.raw.ability_usage_event;
# MAGIC drop table if exists horizon_load_test.raw.accomplishment_status;
# MAGIC drop table if exists horizon_load_test.raw.application_session_status;
# MAGIC drop table if exists horizon_load_test.raw.battlepass_status;
# MAGIC drop table if exists horizon_load_test.raw.bot_score_events;
# MAGIC drop table if exists horizon_load_test.raw.bot_stuck_spots;
# MAGIC drop table if exists horizon_load_test.raw.bounty_status;
# MAGIC drop table if exists horizon_load_test.raw.dev_destroyed_when_stuck;
# MAGIC drop table if exists horizon_load_test.raw.hero_customization;
# MAGIC drop table if exists horizon_load_test.raw.match_hero_progression;
# MAGIC drop table if exists horizon_load_test.raw.match_status_event;
# MAGIC drop table if exists horizon_load_test.raw.meta_progression_status;
# MAGIC drop table if exists horizon_load_test.raw.player_action_event;
# MAGIC drop table if exists horizon_load_test.raw.transactions;
# MAGIC drop table if exists horizon_load_test.raw.trials_status_event;
# MAGIC drop table if exists horizon_load_test.raw.weapon_usage_event;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table horizon_load_test.raw.test_table (test_field string);

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into horizon_load_test.raw.test_table values ("test123"), ("asdf456");

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table horizon.raw_ingest_test.dna_all;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE horizon.raw.player_action_event SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
