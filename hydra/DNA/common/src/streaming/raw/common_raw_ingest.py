# Databricks notebook source
import os
import json
from pyspark.sql.functions import (
    col,
    current_timestamp,
    to_date
)
from pyspark.sql.types import StringType
from delta.tables import DeltaTable
import argparse
from pyspark.sql import SparkSession
import concurrent.futures
import logging
from pyspark.sql.streaming import StreamingQueryListener
from pathlib import Path
from uuid import uuid4

# Configure logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)


# COMMAND ----------

# this class holds a query listener in order to output our streaming metrics to a volume
# TODO: move this into utils
class ProgressToVolumeListener(StreamingQueryListener):
    def __init__(self, output_path):
        self.output_path = Path(output_path)

    def onQueryProgress(self, event):
        progress = event.progress.json
        file_path = self.output_path / f"{uuid4()}.json"
        with open(file_path, 'w') as f:
            f.write(progress)

    def onQueryStarted(self, event):
        pass

    def onQueryTerminated(self, event):
        pass


def get_dbutils(spark):
    try:
        from pyspark.dbutils import DBUtils
        return DBUtils(spark)
    except ImportError:
        import IPython
        return IPython.get_ipython().user_ns["dbutils"]


def arg_parser():

    parser = argparse.ArgumentParser(description="Consume parameters from Databricks job")
    parser.add_argument("--environment", type=str, required=True, help="Description for environment")
    parser.add_argument("--checkpoint_location", type=str, required=True, help="Description for checkpoint_location")
    parser.add_argument("--title", type=str, required=True, help="Description for title")
    parser.add_argument("--topics", type=str, required=True, help="Description for topics")
    parser.add_argument("--bootstrap_servers", type=str, required=True, help="Description for bootstrap_servers")
    parser.add_argument("--splitter_field", type=str, required=False, help="Description for splitter_field")
    parser.add_argument("--starting_offsets", type=str, required=False, help="Description for starting_offsets")
    

    args = parser.parse_args()

    environment = args.environment
    checkpoint_location = args.checkpoint_location
    title = args.title
    topics = args.topics
    bootstrap_servers = args.bootstrap_servers
    splitter_field = args.splitter_field if args.splitter_field else 'name'
    starting_offsets = args.starting_offsets if args.starting_offsets else 'latest'

    return environment,checkpoint_location,title,topics,bootstrap_servers,starting_offsets,splitter_field

# COMMAND ----------

def create_df(environment,bootstrap_servers,topics,starting_offsets,user_id,password,spark):

    topics_list = spark.sql(f"select topics from reference{environment}.utils.topic_configs where isEnabled = 1").collect()
    topics_dict_list = [row.asDict() for row in topics_list]
    topic_db_map = {k: v for d in topics_dict_list for k, v in d.get('topics', {}).items()}
    topics_subscribed= ','.join([str(i) for i in topics.split(',') if i in topic_db_map.keys()])
    if len(topics_subscribed)==0:
            raise Exception (f"Topic doesn't exist in config table")
    try:
        df = (
            spark
            .readStream
            .format("kafka")
            .option("failOnDataLoss", "false")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(user_id, password))
            .option("kafka.ssl.endpoint.identification.algorithm", "https")
            .option("kafka.sasl.mechanism", "PLAIN")
            .option("groupIdPrefix", f"data_eng{environment}")
            .option("subscribe", topics_subscribed)
            .option("startingOffsets", f"{starting_offsets}")
            .option("minPartitions", spark.sparkContext.defaultParallelism)
            .load()
            .withColumn('value', col('value').cast(StringType())) # cast the binary json to a string
        )
    except Exception as exc:
        raise Exception (f"Error reading from kafka stream: {exc}")

                

    return df,topic_db_map


# COMMAND ----------

def event_to_dict(event_data, title):
    """
    parses the input string json object, then "flattens" the `eventData` node

    Parameters:
    event_data: a string containing a json object

    Returns:
    a dictionary containing the flattened json object
    """

    flat_data = json.loads(event_data[0])
    keys = [key for key in flat_data.keys()]
    for key in keys:
        flat_data[key.lower()] = flat_data.pop(key)
    
    columns = list(key.lower() for key in flat_data.keys())

    for key in flat_data['eventdata'].keys():
        newKey = key.lower()
        oldKey = key
        i = 1
        if newKey in columns:
            newKey = 'eventdata_'+key.lower()
        while newKey in columns:
            newKey = 'eventdata_'+key.lower()+'_'+str(i)
            i += 1
        if type(flat_data['eventdata'][oldKey]) in (dict ,list):
            flat_data[newKey] = str(json.dumps(flat_data['eventdata'][oldKey])) 
        else:
            flat_data[newKey] = str(flat_data['eventdata'][oldKey])
        columns.append(newKey)

    del flat_data['eventdata']

    # json needs to be dumped for certain sources, otherwise we get _corrupt_records
    if title in ['ecommerce']:
        return json.dumps(flat_data)
    else:
        return flat_data

def parallel_write(df, topic, event_name, batch_id, checkpoint_path,title,environment,splitter_field,topic_db_map,spark):
    """
    writes the flattened data to a uniform table
    """

    catalog = topic_db_map[topic][0]
    schema = topic_db_map[topic][1]
    #logger.info(f"Starting parallel_write {title}")

    # Hammer has 2 different "splitter fields" depending on the event types
    # TODO: maybe make this configurable if more titles send their data this way
    if title in ['hammer'] and event_name in ['applicationStartInfo']:
        final_splitter_field = 'name'
    else:
        final_splitter_field = splitter_field

    ndf = (
        spark
        .read
        .json(df
            .filter(df.topic == topic)
            .where(f"get_json_object(value, '$.{final_splitter_field}')::string = '{event_name}'")
            .select("value")
            .rdd
            .map(lambda row: event_to_dict(row, title))
        )
        .withColumn("insert_ts", current_timestamp())
        .withColumn("date", to_date(current_timestamp()))
    )

    (
        DeltaTable.createIfNotExists(spark)
        .tableName(f"{catalog}{environment}.{schema}.{event_name}")
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
        .option("txnAppId", f"{checkpoint_path}/private/{topic}")
        .format("delta")
        .mode("append")
        .saveAsTable(f"{catalog}{environment}.{schema}.{event_name}")
    )

    #logger.info(f"Finished parallel_write {title}")

def parallel_write_topic(df, topic, batch_id, checkpoint_path, title, topic_table_map,spark):
    """
    writes the flattened data to a uniform table
    """
    #logger.info("Starting parallel_write_topic")
    ndf = (
        spark
        .read
        .json(df
            .filter(df.topic == topic)
            .select("value")
            .rdd
            .map(lambda row: event_to_dict(row, title))
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
        .option("txnAppId", f"{checkpoint_path}/private/{topic}")
        .format("delta")
        .mode("append")
        .saveAsTable(f"{topic_table_map[topic]}")
    )
    #logger.info("Finished parallel_write_topic")

def proc_batch(df, topic, id, checkpoint_path,title,environment,splitter_field,topic_db_map,spark):
    """
    1. pulls distinct list of events out of this microbatch
    2. starts up a pool of workers based on number of driver CPU cores
    3. assigns 1 event to each worker to parallely write to uniform tables
    """
    event_list = df.filter(df.topic == topic).selectExpr(f"get_json_object(value, '$.{splitter_field}')::string").distinct().collect()

    # explicitly add applicationStartInfo to the event list for hammer
    if title in ['hammer']:
        event_list.append(['applicationStartInfo'])

    with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        events = {executor.submit(parallel_write, df, topic, event[0], id, checkpoint_path,title,environment,splitter_field,topic_db_map,spark): event for event in event_list}

        for future in concurrent.futures.as_completed(events):
            try:
                future.result()
            except Exception as exc:
                print(f"ERROR: {exc}")


def proc_batch_pre_split(df, id, checkpoint_path,title,environment,splitter_field,topic_db_map,spark):
    """
    1. pulls distinct list of topics out of this microbatch
    2. starts up a pool of workers based on number of driver CPU cores
    3. assigns 1 topic to each worker to parallely write to uniform tables
    """

    df.persist()
    topic_list = df.select("topic").distinct().collect()

    with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        topics = {executor.submit(proc_batch, df, topic[0], id, checkpoint_path,title,environment,splitter_field,topic_db_map,spark): topic for topic in topic_list}

        for future in concurrent.futures.as_completed(topics):
            try:
                future.result()
            except Exception as exc:
                print(f"ERROR: {exc}")

    df.unpersist()
    

# COMMAND ----------

def stream_raw_ingest():
    spark = SparkSession.builder.appName("Hydra").getOrCreate()

    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "false")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "false")

    environment,checkpoint_location,title,topics,bootstrap_servers,starting_offsets,splitter_field = arg_parser()
    # environment = "_dev"
    # checkpoint_location = "dbfs:/tmp/kevin_test/test123456789111"
    # title = "wwe2k25"
    # topics = "telemetry.events.read.good.c5770d7b11bb48279c8dc7883a9fcbf2.1"
    # bootstrap_servers = "pkc-3nrx70.us-east-1.aws.confluent.cloud:9092"
    # starting_offsets = "earliest"
    # splitter_field = "name"
    dbutils = get_dbutils(spark)

    # create listener to track progress
    listener = ProgressToVolumeListener(f"/Volumes/reference{environment}/utils/stream_query_progress")
    spark.streams.addListener(listener)

    user_id = dbutils.secrets.get(scope="data-engineering", key="confluent_svc_acc_userid_pvt")
    password = dbutils.secrets.get(scope="data-engineering", key="confluent_svc_acc_password_pvt")


    df,topic_db_map = create_df(environment,bootstrap_servers,topics,starting_offsets,user_id,password,spark)

    df\
        .writeStream\
        .queryName(title)\
        .trigger(processingTime = "10 seconds")\
        .foreachBatch(lambda df, batch_id: proc_batch_pre_split(df, batch_id, checkpoint_location,title,environment,splitter_field,topic_db_map,spark))\
        .option("checkpointLocation", checkpoint_location)\
        .start()


# COMMAND ----------

if __name__ == "__main__":
    stream_raw_ingest()
