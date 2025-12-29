import requests
import json
import argparse
from pyspark.sql.functions import current_timestamp
from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from src.utils.helpers import get_dbutils,arg_parser_api,flatten_df,get_api_response


def transform_playtest_stats(playtest_df):
    
    if not isinstance(playtest_df, DataFrame):
        raise ValueError("Returned value is not a DataFrame, possible API response code error")

    # Flatten the DataFrame until there are no more nested columns
    flat_playtest_df = flatten_df(playtest_df)
    flat_playtest_country_df = flatten_df(flat_playtest_df)

    # Add current datetime
    flat_df = flat_playtest_country_df.withColumn("insert_ts", current_timestamp())

    return flat_df

def load_playtest_stats(flat_df,environment,spark):
    # Write the DataFrame to a Delta table with Iceberg format
    # Define the table name
    table_name = f"coretech{environment}.gameshapers.playtest_stats"

    # Create the Delta table if it does not exist
    DeltaTable.createIfNotExists(spark
        ).tableName(table_name
        ).addColumn("perPlaytest_id", "STRING"
        ).addColumn("perPlaytest_playtestTitle", "STRING"
        ).addColumn("perPlaytest_totalKeysSent", "INTEGER"
        ).addColumn("perPlaytest_totalUsersAccepted", "INTEGER"
        ).addColumn("perPlaytest_totalUsersRegistered", "INTEGER"
        ).addColumn("perPlaytest_totalUsersWaitlisted", "INTEGER"
        ).addColumn("perPlaytest_perCountry_country", "STRING"
        ).addColumn("perPlaytest_perCountry_totalKeysSent", "INTEGER"
        ).addColumn("perPlaytest_perCountry_totalUsersAccepted", "INTEGER"
        ).addColumn("perPlaytest_perCountry_totalUsersRegistered", "INTEGER"
        ).addColumn("perPlaytest_perCountry_totalUsersWaitlisted", "INTEGER"
        ).addColumn("insert_ts", "TIMESTAMP"
        ).execute()

    # Load the Delta table
    delta_table = DeltaTable.forName(spark, table_name)

    # schema  validation step
    target_schema = delta_table.toDF().columns
    source_schema = flat_df.columns
    if target_schema != source_schema:
        raise ValueError("Schema mismatch between source DataFrame and target Delta table")

    # Merge the data into the Delta table
    delta_table.alias("target").merge(
        flat_df.alias("source"),
        "target.insert_ts = source.insert_ts AND target.perPlaytest_id = source.perPlaytest_id"
    ).whenNotMatchedInsertAll().execute()


def process_playtest_stats():

    spark = SparkSession.builder.appName("Hydra").getOrCreate()
    dbutils = get_dbutils(spark)

    api_key = dbutils.secrets.get(scope="data-engineering", key="gameshaper_playstat_apikey")

    environment,endpoint = arg_parser_api()

    # Loop through pages
    page = 0
    while True:
        # Construct the API URL for the current page
        api_url = f"{endpoint}?page={page}"
        # Make the API request
        response = requests.get(api_url, headers={"sharedkey": api_key})
         # Check if the request was successful
        if response.status_code == 200:
            # Convert the response JSON to a DataFrame
            data = response.json()
            if not data.get("perPlaytest"):  # Break the loop if perPlaytest array is empty
                break
            playtest_df = get_api_response(spark,api_url,api_key)
            playtest_df = playtest_df.select("perPlaytest")
            flat_df = transform_playtest_stats(playtest_df)
            load_playtest_stats(flat_df,environment,spark)
            page += 1
        else:
            print(f"Failed to fetch data: {response.status_code}")
            break


if __name__ == "__main__":
    process_playtest_stats()