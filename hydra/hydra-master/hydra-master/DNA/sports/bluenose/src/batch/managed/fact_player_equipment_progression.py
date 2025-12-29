# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/table_functions

# COMMAND ----------

from pyspark.sql.functions import (expr,when,countDistinct, current_timestamp, sha2, concat_ws,to_date,col)
from delta import DeltaTable

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment', set_environment())
database = 'bluenose'
data_source = 'dna'

def read_equipment_progression():
    return (
        spark
        .readStream
        .table(f"bluenose{environment}.raw.equipmentprogressionstatus")
        .where(expr("equipmenttype = 'Club' AND buildenvironment = 'RELEASE'"))
        .select(
            expr("*"),
            expr("from_json(equipmentslots, 'ARRAY<STRUCT<fittingId: STRING, fittingType: STRING, slotNumber: BIGINT, status: BIGINT>>') as slots"),
            expr("case when equipmentevotier = 'Baseline' then 1 else split(equipmentevotier, 'Evo')[1]::int + 1 end as mult"),
            expr("mult * 2 as attribute_increase")
        )
    )

def extract():
    equip_prog = read_equipment_progression()
    return equip_prog

def transform(equip_prog_df):    
    transformed_df = (
        equip_prog_df
        .select(
            expr("playerPublicId as player_id"),
            expr("activemyplayersaveid as my_player_id"),
            expr("receivedOn::timestamp as received_on"),
            expr("equipmentid as equipment_id"),
            expr("equipmentinstanceid as equipment_instance_id"),
            expr("equipmentlevel::int as equipment_level"),
            expr("equipmentevotier as evo_tier"),
            expr("case when equipmentevotier = 'Baseline' then 1 else split(equipmentevotier, 'Evo')[1]::int + 1 end as evo_rank"),
            expr("equipmenttype as equipment_type"),
            expr("equipmentprogressionstatus as event_trigger"),
            expr("""transform(slots, 
                        slot -> struct(
                            slot.fittingId, 
                            slot.fittingType, 
                            slot.slotNumber::int as slotNumber, 
                            slot.status::int as status, 
                            split(slot.fittingType, 'Fitting')[0] as attribute_type, 
                            case when slot.fittingId is not null then attribute_increase::int else 0::int end as attribute_increase)) as equipment_slots"""),
            expr("current_timestamp() as dw_insert_ts"),
            expr("current_timestamp() as dw_update_ts")
        )
    )

    return transformed_df

def create_fact_player_equipment_progression(spark):
    sql = f"""
        CREATE TABLE IF NOT EXISTS bluenose{environment}.managed.fact_player_equipment_progression (
            player_id STRING,
            my_player_id STRING,
            received_on TIMESTAMP,
            equipment_id STRING,
            equipment_instance_id STRING,
            equipment_level INT,
            evo_tier STRING,
            evo_rank INT,
            equipment_type STRING,
            event_trigger STRING,
            equipment_slots ARRAY<STRUCT<fittingId: STRING, fittingType: STRING, slotNumber: INT, status: INT, attribute_type: STRING, attribute_increase: INT>>,
            dw_insert_ts TIMESTAMP,
            dw_update_ts TIMESTAMP
        )
        """

    properties = {
        "delta.feature.allowColumnDefaults": "supported" 
    }

    create_table(spark, sql, properties)
    return f"dbfs:/tmp/bluenose/managed/streaming/run{environment}/fact_player_equipment_progression"

def load_fact_player_equipment_progression(spark, df, database, environment, checkpoint_location):
    """
    Load the data into the fact_player_equipment_progression table in the specified environment.

    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        df (DataFrame): The DataFrame to load into the table.
        database (str): The database to load the table into.
        environment (str): The environment to load the table into.
    """

    (
        df
        .writeStream
        .queryName("Bluenose fact_player_equipment_progression")
        .trigger(availableNow = True)
        .format("delta")
        .option("checkpointLocation", checkpoint_location)
        .toTable(f"bluenose{environment}.managed.fact_player_equipment_progression")
    )


def run_batch():
    spark = create_spark_session(name=f"{database}")
    checkpoint_location = create_fact_player_equipment_progression(spark)
    equip_prog_df = extract()
    df = transform(equip_prog_df)
    load_fact_player_equipment_progression(spark, df, database, environment, checkpoint_location)



# COMMAND ----------

run_batch()
