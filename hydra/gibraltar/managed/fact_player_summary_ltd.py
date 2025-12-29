# Databricks notebook source
from pyspark.sql.functions import (expr, when)
from pyspark.sql.window import Window
from functools import reduce


# COMMAND ----------

# MAGIC %run ../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../utils/ddl/managed/fact_player_summary_ltd

# COMMAND ----------

# MAGIC %run ../../utils/helpers

# COMMAND ----------

# Global defaults for columns / filter values can be updated with local defaults
ltd_bool_fields_count = 5
ltd_ts_fields_count = 5
ltd_float_field_count = 7
ltd_string_fields_count = 11

global_defaults = {
    'player_id': 'player_id',
    'platform': 'platform',
    'service': 'service',
    'config_1': '-1',
    'first_seen': 'min(RECEIVED_ON)',
    'last_seen': 'max(RECEIVED_ON)',
    'first_seen_lang': 'first_value(language_setting,True) OVER (PARTITION BY player_id, platform, service ORDER BY received_on)',
    'last_seen_lang': 'last_value(language_setting,True) OVER (PARTITION BY player_id, platform, service ORDER BY received_on)',
    **{f"ltd_bool_{i}": 'Null' for i in range(1, ltd_bool_fields_count)},
    **{f"ltd_ts_{i}": 'Null' for i in range(1, ltd_ts_fields_count)},
    **{f"ltd_float_{i}": 'Null' for i in range(1, ltd_float_field_count)},
    **{f"ltd_string_{i}": 'Null' for i in range(1, ltd_string_fields_count)},
    'common_filter': "player_id not ilike '%NULL%' and platform !='Unknow'",
    'additional_filter': '1=1',
    'finial_agg': {},
    'custom_attribute_functions': [],
    'merge_condition': 'target.merge_key = source.merge_key',
    'merge_update_conditions': [  
                                 { 'condition' : 'target.first_seen < source.first_seen', 
                                    'set_fields' : {
                                                     'first_seen' : 'source.first_seen',
                                                     'first_seen_lang': 'ifnull(source.first_seen_lang,target.first_seen_lang)',
                                                     'dw_updated_ts': 'source.dw_updated_ts'
                                                   }},
                                { 'condition' : 'target.last_seen < source.last_seen', 
                                    'set_fields' : {
                                                     'last_seen' : 'source.last_seen',
                                                     'last_seen_lang': 'ifnull(source.last_seen_lang,target.last_seen_lang)',
                                                     'dw_updated_ts': 'source.dw_updated_ts'
                                                   }}]

}

# COMMAND ----------

def read_player_activity(database, local_defaults, spark):
    """
    Creates a streaming DataFrame from the given player activity data in the game telemetry and logins 
    from the coretech login events, applying a filter and ensuring the results are distinct.

    This function reads data from the intermediate player activity table in a specified database, applies 
    global and local filters, selects the relevant columns (excluding those with default 'Null' values), 
    and ensures that the resulting DataFrame contains only distinct rows.

    Parameters:
    input
        database: str - The project database without the environment (e.g., 'game_database')
        local_defaults: dict - A dictionary of properties (local overrides) that will be merged with global_defaults
        spark: SparkSession - The Spark session for executing the SQL command.

    output:
        fact_player_ltd_df: DataFrame - A streaming DataFrame containing fact_player_ltd values data with distinct rows.
        
    Details:
        - The function dynamically builds the `select_columns` dictionary, filtering out fields with 'Null' values from 
          the `global_defaults`.
        - Applies an `additional_filter` combined with the global `common_filter`, if specified.
        - Ensures the final DataFrame contains only unique rows by applying `.distinct()` on the selected columns.
    Example:
        df = read_player_activity('bluenose', "'PGA Tour 2K25','PGA Tour 2K25: Demo'", local_defaults, spark)
    """

    #Updating the global_defaults values with local defaults
    global_defaults.update(local_defaults)

    print('fact_player_ltd : Started')
    
    # Get the common and additional optional filter
    common_filter = global_defaults['common_filter']
    additional_filter = global_defaults['additional_filter']

    combined_filter = None
    # Determine the combined filter
    if global_defaults.get('exclude_common_filter', 'n') == 'n':
        combined_filter = expr(f"({common_filter}) AND ({additional_filter})")
    else:
        combined_filter = expr(f"{additional_filter}")
        
    # Read the DataFrame and chain transformations using select
    fact_player_ltd_df = (
                            spark
                            .readStream
                            .table(f"{database}{environment}.intermediate.fact_player_activity")
                            .where(combined_filter)
                         )
    
    print('fact_player_ltd : Done')
    return fact_player_ltd_df

# COMMAND ----------

def merge_fact_player_summary_ltd(micro_batch_df, batch_id, database, global_defaults, spark):
    """
    Merges the incoming player activity data (source DataFrame) with the existing data (target Iceberg table)
    using PySpark's merge functionality with dynamic update logic that allows for user-provided expressions.

    Parameters:
    - micro_batch_df (DataFrame): The incoming micro-batch DataFrame to be processed.
    - batch_id (int): The batch ID for the micro-batch.
    - database (str): The project database name.
    - spark (SparkSession): The Spark session to perform the operations.
    - dynamic_update_conditions (list of dict): A list of dictionaries, where each dictionary contains the dynamic conditions for updating columns in a `whenMatchedUpdate`.

    Returns:
    - None: The function performs the merge and updates the target table.
    """
    # Prepare fields using expression and excluding those with default value 'Null' from the selection
    min_time = spark.sql(f"select ifnull(max(last_seen) , '2025-02-03 00:00:00') as min_time from {database}{environment}.managed.fact_player_summary_ltd").collect()[0]['min_time']
    common_filter = global_defaults['common_filter']
    additional_filter = global_defaults['additional_filter']
    date_filter = f"received_on between '{min_time}'::timestamp-interval '12 hours' and current_timestamp"

    combined_filter = None
    # Determine the combined filter
    if global_defaults.get('exclude_common_filter', 'n') == 'n':
        combined_filter = expr(f"({common_filter}) AND ({additional_filter}) AND ({date_filter})")
    else:
        combined_filter = expr(f"({additional_filter}) AND ({date_filter})")
        
    # Read the DataFrame and chain transformations using select
    fact_player_ltd_df = (
                            spark
                            .read
                            .table(f"{database}{environment}.intermediate.fact_player_activity")
                            .where(combined_filter)
                         )
    

    ltd_columns = {
        "RECEIVED_ON": "RECEIVED_ON",
        "first_seen_lang": expr(f"{global_defaults['first_seen_lang']}::STRING as first_seen_lang"),
        "last_seen_lang": expr(f"{global_defaults['last_seen_lang']}::STRING as last_seen_lang"),
        **{f"ltd_bool_{i}": expr(f"{global_defaults[f'ltd_bool_{i}']}::BOOLEAN as ltd_bool_{i}") for i in range(1, ltd_bool_fields_count) if global_defaults[f"ltd_bool_{i}"] != 'Null'},
        **{f"ltd_ts_{i}": expr(f"{global_defaults[f'ltd_ts_{i}']}::TIMESTAMP as ltd_ts_{i}") for i in range(1, ltd_ts_fields_count) if global_defaults[f"ltd_ts_{i}"] != 'Null'},
        **{f"ltd_float_{i}": expr(f"{global_defaults[f'ltd_float_{i}']}::FLOAT as ltd_float_{i}") for i in range(1, ltd_float_field_count) if global_defaults[f"ltd_float_{i}"] != 'Null'},
        **{f"ltd_string_{i}": expr(f"{global_defaults[f'ltd_string_{i}']}::STRING as ltd_string_{i}") for i in range(1, ltd_string_fields_count) if global_defaults[f"ltd_string_{i}"] != 'Null'}}

    groupby_columns = {
                    "player_id": expr(f"{global_defaults['player_id']}::STRING as player_id"),
                    "platform": expr(f"{global_defaults['platform']}::STRING as platform"),
                    "service": expr(f"{global_defaults['service']}::STRING as service"),
                    "config_1": expr(f"{global_defaults['config_1']}::STRING as config_1")}

    player_activity_ltd = fact_player_ltd_df.select(
        *groupby_columns.values(),
        *ltd_columns.values())

    ltd_columns_final_agg = {
        "first_seen": expr(f"{global_defaults['first_seen']}::TIMESTAMP as first_seen"),
        "last_seen": expr(f"{global_defaults['last_seen']}::TIMESTAMP as last_seen"),
        "first_seen_lang": expr(f"any_value(first_seen_lang) as first_seen_lang"),
        "last_seen_lang": expr(f"any_value(last_seen_lang) as last_seen_lang"),
        
        **{
            f"ltd_bool_{i}": expr(f"{global_defaults['finial_agg'].get(f'ltd_bool_{i}', 'any_value')}(ltd_bool_{i}) as ltd_bool_{i}")
            for i in range(1, ltd_bool_fields_count) 
            if global_defaults[f"ltd_bool_{i}"] != 'Null'
        },
        
        **{
            f"ltd_ts_{i}": expr(f"{global_defaults['finial_agg'].get(f'ltd_ts_{i}', 'any_value')}(ltd_ts_{i}) as ltd_ts_{i}")
            for i in range(1, ltd_ts_fields_count) 
            if global_defaults[f"ltd_ts_{i}"] != 'Null'
        },
        
        **{
            f"ltd_float_{i}": expr(
                f"count(distinct ltd_float_{i}) as ltd_float_{i}"
                if global_defaults['finial_agg'].get(f'ltd_float_{i}', 'any_value') == 'count_distinct'
                else f"{global_defaults['finial_agg'].get(f'ltd_float_{i}', 'any_value')}(ltd_float_{i}) as ltd_float_{i}"
            )
            for i in range(1, ltd_float_field_count)
            if global_defaults[f"ltd_float_{i}"] != 'Null'
        },
        
        **{
            f"ltd_string_{i}": expr(f"{global_defaults['finial_agg'].get(f'ltd_string_{i}', 'any_value')}(ltd_string_{i}) as ltd_string_{i}")
            for i in range(1, ltd_string_fields_count) 
            if global_defaults[f"ltd_string_{i}"] != 'Null'}}

    date_fields = {
        "dw_insert_date": expr("current_date() as dw_insert_date"),
        "dw_insert_ts": expr("current_timestamp() as dw_insert_ts"),
        "dw_updat_ts": expr("current_timestamp() as dw_update_ts")}
        
    player_ltd = player_activity_ltd.groupBy(
        *groupby_columns.keys()) \
    .agg(
        *ltd_columns_final_agg.values())  \
    .select(
        *groupby_columns.keys(),
        *ltd_columns_final_agg.keys()
        )

    # This loop iterates over the custom-defined functions in the `custom_attribute_fucntions` list in the local defaults,
    # which will update the global defaults executing each function to generate a DataFrame. The resulting DataFrames are then
    # joined to the base DataFrame on common columns, adding their specific LTD attributes without duplicating the existing ones.
    custom_columns = set()
    for custom_attribute_function in global_defaults['custom_attribute_functions']:
        df = custom_attribute_function(fact_player_ltd_df,batch_id,spark)
        custom_columns.update(set(df.columns) - set(groupby_columns.keys()))
        player_ltd = (
                       player_ltd.alias('ltd')
                       .join(df.alias('df'),
                        expr("""ltd.player_id = df.player_id AND ltd.platform = df.platform AND ltd.service = df.service AND ltd.config_1 = df.config_1"""),
                        how='left')
                        .select(
                                'ltd.*',
                                *custom_columns
                            )

                       )
    player_ltd = player_ltd.select('*', *date_fields.values(),expr("SHA2(CONCAT_WS('|',PLAYER_ID, PLATFORM, SERVICE, config_1),256) AS merge_key"))

    #Merge variables and logic
    target_df = f"{database}{environment}.managed.fact_player_summary_ltd"
    source_df = 'player_ltd'
    merge_condition = global_defaults['merge_condition']
    merge_update_conditions = global_defaults['merge_update_conditions']

    merge_data(target_df, source_df, merge_condition, player_ltd , merge_update_conditions)

    print(f"Batch {batch_id} merged successfully with dynamic update conditions.")
# COMMAND ----------

def stream_fact_player_summary_ltd(database,defaults,view_mapping,trigger = "streaming"):
    database = database.lower()

    #setting the spark session
    spark = create_spark_session(name=f"{database}")
    
    #Setting the checkpoint_location from the fucntion output
    checkpoint_location = create_fact_player_summary_ltd(spark, database,view_mapping)
    print(f'checkpoint_location : {checkpoint_location}')
    
    # Reading the data using readStream
    fact_player_ltd_df = read_player_activity(database,defaults,spark)

    print('Started writeStream')

    # Writing the stream output to the title level fact_player_ltd table using custom function
    if trigger == "streaming":
        (
            fact_player_ltd_df
            .writeStream
            .trigger(processingTime="5 minutes")
            .foreachBatch(lambda batch_df, batch_id: merge_fact_player_summary_ltd(batch_df, batch_id, database, global_defaults, spark))
            .option("checkpointLocation", checkpoint_location)
            .queryName(f'{database}_fact_player_summary_ltd')
            .outputMode("append")
            .start()
        )
    else:
        (
            fact_player_ltd_df
            .writeStream
            .trigger(availableNow=True)
            .foreachBatch(lambda batch_df, batch_id: merge_fact_player_summary_ltd(batch_df, batch_id, database, global_defaults, spark))
            .option("checkpointLocation", checkpoint_location)
            .queryName(f'{database}_fact_player_summary_ltd')
            .outputMode("append")
            .start()
        )