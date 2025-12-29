# Databricks notebook source


# COMMAND ----------

# MAGIC %pip install snowflake

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ../../../../utils/helpers

# COMMAND ----------

from pyspark.sql import SparkSession
from snowflake.connector import ProgrammingError
import snowflake.connector
import argparse
import asyncio
import time
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
from delta.tables import DeltaTable
import argparse
from pyspark.sql import SparkSession

# COMMAND ----------

def generate_sql(table, latest_path, environment, table_map):
    print(f'generate_sql:- {table}: {table_map[table]["status"]}')
    if table_map[table]['status'] == 'New':
        sql = f"""
            CREATE OR REPLACE ICEBERG TABLE {table_map[table]['target_table']}
            EXTERNAL_VOLUME='de{environment}_volume'
            CATALOG='uniform_demo_catalog_int'
            METADATA_FILE_PATH='{latest_path}';"""
    elif table_map[table]['status'] == 'Table Created' or table_map[table]['status'] == 'Table Refreshed' or table_map[table]['status'] == 'Table Refreshing':
        sql = f"ALTER ICEBERG TABLE {table_map[table]['target_table']} REFRESH '{latest_path}'"
    else:
        sql = f"""
            CREATE OR REPLACE ICEBERG TABLE {table_map[table]['target_table']}
            EXTERNAL_VOLUME='de{environment}_volume'
            CATALOG='uniform_demo_catalog_int'
            METADATA_FILE_PATH='{latest_path}';
        """
    return sql

# COMMAND ----------

def main():
    input_param = dbutils_input_params()
    environment = input_param.get('environment', set_environment())
    run_start_time = f'{time.gmtime().tm_year}-{time.gmtime().tm_mon:02d}-{time.gmtime().tm_mday:02d} {time.gmtime().tm_hour:02d}:{time.gmtime().tm_min - (time.gmtime().tm_min % 5):02d}:00'

    df = spark.sql(
        f"""
            SELECT 
                    CONCAT(dbx_db, '.', dbx_schema, '.', dbx_table) AS source_table,
                    CONCAT(snowflake_db, '.', snowflake_schema, '.', snowflake_table) AS target_table,
                    max(run_instance_id) as run_instance_id,
                    CASE 
                        WHEN arrays_overlap(array_agg(status), array('Table Refreshed', 'Table Refreshing', 'Table Created')) 
                        THEN 'Table Refreshed' 
                        WHEN array_contains(array_agg(status), 'New') Then 'New'
                        ELSE NULL
                    END AS status
            FROM reference{environment}.utils.uniform_snowflake_refresh
            WHERE is_active = True 
            AND (  (status = 'New' and ifnull(start_timestamp,current_timestamp()) <= current_timestamp()) 
                OR (updated_ts + refresh_interval::INTERVAL <= current_timestamp())) 
            GROUP BY 1,2
        """).toPandas()

    table_map = {item['source_table']: {'target_table' : item['target_table'],
                                        'status': item['status'] }
                for item in df.to_dict(orient="records") }

    user = dbutils.secrets.get("data-engineering", "snowflake_svc_acc_userid")
    password = dbutils.secrets.get("data-engineering", "snowflake_svc_acc_password")

    conn = snowflake.connector.connect(
            user=user,
            password=password,
            account="twokgames",
            warehouse="wh_uniform_refresh",
            database="databricks_migration")

    cur = conn.cursor()

    sql_map = {}
    for table in table_map.keys():
        try:
            metadata_location = spark.sql(f"DESCRIBE EXTENDED {table}").filter('col_name = "Metadata location" ').select('data_type').collect()[0][0]
            latest_path = "/".join(metadata_location.split("/")[3:])
            table_map[table]['latest_path'] =  latest_path
        except Exception as e:
            print(f'Error: {table} {e}')
            table_map[table]['status'] = f'Error: {table} {e}. Table format should be Delta Uniform Iceberg'
            continue
        table_map[table]['sql'] = generate_sql(table, latest_path, environment, table_map)
        cur.execute_async(table_map[table]['sql'])
        table_map[table]['sql_id'] = cur.sfqid
        sql_map[cur.sfqid] = table


    for query_id in sql_map.keys():
        while conn.is_still_running(conn.get_query_status(query_id)):
            table_map[sql_map[query_id]]['status'] = 'Table Refreshing'
            time.sleep(60)
        table = sql_map[query_id]
        try:
            cur.get_results_from_sfqid(query_id)
            result = cur.fetchall()
            print(result[0][0])
            if 'created' in result[0][0]:
                table_map[sql_map[query_id]]['status'] = 'Table Created'
            if 'already exists' in result[0][0]:
                cur.execute(generate_sql(table,table_map[table]['latest_path'], environment, table_map)).fetchall()
                table_map[sql_map[query_id]]['status'] = 'Table Refreshed'
            if 'successfully' in result[0][0]:
                table_map[sql_map[query_id]]['status'] = 'Table Refreshed'
        except snowflake.connector.errors.ProgrammingError as e:
            table_map[table]['status'] = f'Error: {e}'
            try:
                result = cur.execute(generate_sql(table,table_map[table]['latest_path'], environment, table_map)).fetchall()
                table_map[table]['status'] = 'Table Replaced'
            except Exception as inner_e:
                table_map[table]['status'] = f'Error: ({cur.sfqid}) {inner_e}'

    if df is not None and not df.empty:
        for table in table_map.keys():
            df.loc[df['source_table'] == table, 'status'] = table_map[table]['status']
        df = spark.createDataFrame(df)
        df.createOrReplaceTempView(f'snowflake_refresh_status{environment}')
        status_merge_sql = f"""MERGE INTO reference{environment}.utils.uniform_snowflake_refresh target
                            USING snowflake_refresh_status{environment} result
                            ON CONCAT(dbx_db, '.', dbx_schema, '.', dbx_table) = result.source_table and target.run_instance_id = result.run_instance_id
                            WHEN MATCHED THEN UPDATE SET status = result.status, updated_ts = '{run_start_time}';"""
        spark.sql(status_merge_sql)
    conn.close()


# COMMAND ----------

main()
