# Databricks notebook source
from pyspark.sql.functions import col, current_date, lit
from pyspark.sql.functions import (expr, when)
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/reference/bundle_table

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------


base_s3_path = "s3://2k-ana-prd-wwe/reference/wwe2k25/"

# List the directories in the base S3 path
directories = dbutils.fs.ls(base_s3_path)

# Extract the latest date from the folder name
date_folders = [d.name for d in directories if d.name.startswith("date=")]
date_str = date_folders[-1].split('=')[1].strip('/')

# Generate the S3 path for the date folder
date_folder_s3_path = f"{base_s3_path}date={date_str}/"

# Filter for CSV files
database = 'wwe2k25'
input_param = dbutils_input_params()
environment = input_param.get('environment', set_environment())
csv_file = 'Analytics-Bundles.csv'



from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, current_date, expr

def extract(database, environment, spark, date_folder_s3_path, csv_file):
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(f"{date_folder_s3_path}{csv_file}")
    return df

def transform(df, database, spark):
    selected_df = df.select(
        expr("id AS id"),
        lit(None).alias("release_date"),
        expr("`Info\\Size` AS size"),
        expr("`Info\\Quantity` AS qty"),
        expr("PackId AS pack_id"),
        lit(None).alias("name_loc_key"),
        current_date().alias("dw_update_ts")
    ).select(
        "id", 
        "release_date", 
        "size", 
        "qty", 
        "pack_id", 
        "name_loc_key", 
        "dw_update_ts", 
        expr("SHA2(CONCAT_WS('|', id, release_date, size, qty, pack_id, name_loc_key), 256) AS merge_key")
    )
    return selected_df


# selected_df.printSchema()



def load(selected_df, database, environment, spark):
    target_df = DeltaTable.forName(spark, f"{database}{environment}.reference.bundle_table")

    merger_condition = 'target.merge_key = source.merge_key'
    
    merge_update_conditions = [
        { 
            'condition' : """target.id != source.id OR
                             target.size != source.size OR
                             target.qty != source.qty OR
                             target.pack_id != source.pack_id OR
                             target.release_date != source.release_date OR
                             target.name_loc_key != source.name_loc_key""",
            'set_fields' : {
                'id': 'greatest(target.id, source.id)',
                'size': 'greatest(target.size, source.size)',
                'qty': 'greatest(target.qty, source.qty)',
                'pack_id': 'greatest(target.pack_id, source.pack_id)',
                'release_date': 'greatest(target.release_date, source.release_date)',
                'name_loc_key': 'greatest(target.name_loc_key, source.name_loc_key)',
                'dw_update_ts': 'source.dw_update_ts'
            }
        }
    ]

    merge_df = target_df.alias("target") \
                        .merge(selected_df.alias("source"), merger_condition)

    merge_df = set_merge_update_condition(merge_df, merge_update_conditions)

    merge_df = set_merge_insert_condition(merge_df, selected_df)

    merge_df.execute()

def run_batch(database,environment):
    #setting the spark session
    spark = create_spark_session(name=f"{database}")
    
    #Creating the table and view, checkpoint
    create_bundle_table(spark, database, environment, properties={})

    print('Extracting the data')
    
    # Reading the data using bath data
    df = extract(database, environment, spark, date_folder_s3_path, csv_file)

    #Applying Transformation
    selected_df = transform(df, database, spark)

    #Merge data
    load(selected_df, database, environment, spark)

    return 'Merge data completed'

# COMMAND ----------

run_batch(database,environment)