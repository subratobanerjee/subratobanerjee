# Databricks notebook source
from pyspark.sql.functions import col, current_date, lit
from pyspark.sql.functions import (expr, when)
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/reference/card_table

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------


database = 'wwe2k25'
input_param = dbutils_input_params()
environment = input_param.get('environment', set_environment())


# Read the source table
mf_cards_df = spark.table(f"wwe2k25{environment}.reference.mf_cards")

# Select the required columns
selected_columns_df = mf_cards_df.select(
    "card_id",
    "name",
    # "rarity",
    # "series_id",
    # "gender",
    # "roster_id",
    # "type"
)

def extract(database, environment, spark):
    # Read the CSV file into a DataFrame
    df = spark.table(f"wwe2k25{environment}.reference.mf_cards")
    return df


def transform(df, database, spark):
    # Select and rename columns using only expr()
    selected_df = df.select(
    expr("card_id AS card_id"),
    expr("name AS name"),
    lit(None).alias("rarity"),
    lit(None).alias("series_id"),
    lit(None).alias("gender"),
    lit(None).alias("roster_id"),
    lit(None).alias("type"),
    current_date().alias("dw_update_ts")    
).select(
        "card_id", 
        "name", 
        "rarity", 
        "series_id", 
        "gender", 
        "roster_id", 
        "type",
        "dw_update_ts", 
        expr("SHA2(CONCAT_WS('|', card_id, name, rarity, series_id, gender, roster_id, type), 256) AS merge_key")
    )
    return selected_df


def load(selected_df, database, environment, spark):
    target_df = DeltaTable.forName(spark, f"{database}{environment}.reference.card_table")

    merger_condition = 'target.merge_key = source.merge_key'
    
    merge_update_conditions = [
        { 
            'condition' : """target.card_id != source.card_id OR
                             target.name != source.name OR
                             target.rarity != source.rarity OR
                             target.series_id != source.series_id OR
                             target.gender != source.gender OR
                             target.roster_id != source.roster_id OR
                             target.type != source.type""",
            'set_fields' : {
                'card_id': 'greatest(target.card_id, source.card_id)',
                'name': 'greatest(target.name, source.name)',
                'rarity': 'greatest(target.rarity, source.rarity)',
                'series_id': 'greatest(target.series_id, source.series_id)',
                'gender': 'greatest(target.gender, source.gender)',
                'roster_id': 'greatest(target.roster_id, source.roster_id)',
                'type': 'greatest(target.type, source.type)',
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
    create_card_table(spark, database, environment, properties={})

    print('Extracting the data')
    
    # Reading the data using bath data
    df = extract(database, environment, spark)

    #Applying Transformation
    selected_df = transform(df, database, spark)

    #Merge data
    load(selected_df, database, environment, spark)

    return 'Merge data completed'

# COMMAND ----------

run_batch(database,environment)