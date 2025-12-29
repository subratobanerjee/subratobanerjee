import requests
import json
import argparse
from pyspark.sql.functions import current_timestamp
from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from src.utils.helpers import get_dbutils,arg_parser_api,get_api_response


def transform_playtest_stats(playtest_df):
    
    if not isinstance(playtest_df, DataFrame):
        raise ValueError("Returned value is not a DataFrame, possible API response code error")

    # Add current datetime
    flat_df = playtest_df.withColumn("insert_ts", current_timestamp())

    return flat_df

def load_playtest_stats(flat_df,environment,spark):
    # Write the DataFrame to a Delta table with Iceberg format
    # Define the table name
    table_name = f"coretech{environment}.gameshapers.playtests"

    # Create the Delta table if it does not exist
    DeltaTable.createIfNotExists(spark
        ).tableName(table_name
        ).addColumn("adminNotes", "STRING"
        ).addColumn("allowList", "ARRAY<STRING>"
        ).addColumn("allowSignupsAfterEnrollmentPeriod", "BOOLEAN"
        ).addColumn("attributeId", "STRING"
        ).addColumn("canSendKeys", "BOOLEAN"
        ).addColumn("capacity", "LONG"
        ).addColumn("countries", "ARRAY<MAP<STRING, STRING>>"
        ).addColumn("enrollmentEndDate", "TIMESTAMP"
        ).addColumn("enrollmentStartDate", "TIMESTAMP"
        ).addColumn("gameTitle", "STRING"
        ).addColumn("genre", "STRING"
        ).addColumn("id", "STRING"
        ).addColumn("instructions", "STRING"
        ).addColumn("isButtonDisabled", "BOOLEAN"
        ).addColumn("isCrossplay", "BOOLEAN"
        ).addColumn("isPlaytestAtCapacity", "BOOLEAN"
        ).addColumn("isPrivate", "BOOLEAN"
        ).addColumn("isUserRegistered", "BOOLEAN"
        ).addColumn("keyDistributionButtonText", "STRING"
        ).addColumn("keyDistributionInProgress", "STRING"
        ).addColumn("keyDistributionStatus", "STRING"
        ).addColumn("keyEntitlements", "ARRAY<STRING>"
        ).addColumn("keysRequested", "BOOLEAN"
        ).addColumn("lastValidationAt", "TIMESTAMP"
        ).addColumn("platform", "STRING"
        ).addColumn("platformAppId", "STRING"
        ).addColumn("platformHardware", "STRING"
        ).addColumn("playtestTitle", "STRING"
        ).addColumn("requiredNda", "BOOLEAN"
        ).addColumn("requiresTCs", "BOOLEAN"
        ).addColumn("testStartDate", "TIMESTAMP"
        ).addColumn("testEndDate", "TIMESTAMP"
        ).addColumn("twitchDropEntitlementId", "STRING"
        ).addColumn("userHasPlatformLinked", "BOOLEAN"
        ).addColumn("userId", "STRING"                 
        ).addColumn("insert_ts", "TIMESTAMP"
        ).execute()

    # Load the Delta table
    delta_table = DeltaTable.forName(spark, table_name)

    # schema  validation step
    target_schema = delta_table.toDF().columns
    source_schema = flat_df.columns
    # if target_schema != source_schema:
    #     raise ValueError("Schema mismatch between source DataFrame and target Delta table")

    # Merge the data into the Delta table
    delta_table.alias("target").merge(
        flat_df.alias("source"),
        "target.insert_ts = source.insert_ts"
    ).whenNotMatchedInsertAll().execute()


def process_playtests():

    spark = SparkSession.builder.appName("Hydra").getOrCreate()
    dbutils = get_dbutils(spark)

    api_key = dbutils.secrets.get(scope="data-engineering", key="gameshaper_playstat_apikey")

    environment,endpoint = arg_parser_api()

    # Make the API request
    response = requests.get(endpoint, headers={"sharedkey": api_key})
    print(response)
    # Check if the request was successful
    if response.status_code == 200:
        # Convert the response JSON to a DataFrame
        playtest_df = get_api_response(spark,endpoint,api_key)
        flat_df = transform_playtest_stats(playtest_df)
        load_playtest_stats(flat_df,environment,spark)
    else:
        print(f"Failed to fetch data: {response.status_code}")

if __name__ == "__main__":
    process_playtests()