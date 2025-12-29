# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/table_functions

# COMMAND ----------

from pyspark.sql.functions import expr, coalesce, window
from delta.tables import DeltaTable

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'bluenose'
data_source ='dna'
title = "'PGA Tour 2K25','PGA Tour 2K25: Demo'"

# COMMAND ----------

def create_agg_installs_demo_nrt(spark, database, environment):
    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.agg_installs_demo_nrt (
        timestamp_10min_slice TIMESTAMP,
        platform STRING,
        service STRING,
        country_code STRING,
        install_count INT,
        dw_insert_ts TIMESTAMP,
        dw_update_ts TIMESTAMP,
        merge_key STRING
    )
    """

    properties = {
        "delta.feature.allowColumnDefaults": "supported" # this won't work until iceberg v3 is released
    }

    create_table(spark,sql, properties)
    create_agg_installs_demo_nrt_view(spark, database, environment)
    return f"dbfs:/tmp/{database}/managed/streaming/run{environment}/agg_installs_demo_nrt"

# COMMAND ----------

def create_agg_installs_demo_nrt_view(spark, database, environment):
    """
    Create the fact_player_game_status_daily view in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
        mapping (dict): A dictionary of column mappings.
    """


    sql = f"""
    CREATE OR REPLACE VIEW {database}{environment}.managed_view.agg_installs_demo_nrt AS (
        SELECT
        *
        from {database}{environment}.managed.agg_player_funnel_nrt
    )
    """

    spark.sql(sql)

# COMMAND ----------

def read_player_funnel(spark, environment):
    fact_player_funnel = (
        spark
        .readStream
        .table(f"bluenose{environment}.intermediate.fact_player_funnel_nrt")
        .where(expr("game_type = 'demo' and installed = True"))
        .select(
            'player_id',
            'platform',
            'service',
            'first_seen_country_code',
            'installed',
            'install_ts',
            expr("from_unixtime(round(floor(unix_timestamp(install_ts) / 600) * 600))").cast("timestamp").alias("install_ts_10min_slice"))
    )

    return fact_player_funnel

# COMMAND ----------

def read_player_funnel_lkp(spark, environment):
    fact_player_funnel_lkp = (
        spark
        .read
        .table(f"bluenose{environment}.intermediate.fact_player_funnel_nrt")
        .where((expr("game_type = 'demo' and installed = True")))
        .alias("lkp")
        .select(
            'player_id',
            'platform',
            'service',
            'first_seen_country_code',
            'installed',
            'install_ts')
    )
    
    return fact_player_funnel_lkp

# COMMAND ----------

def read_platform_territory_df(spark, environment):
    print("reading platform territory")

    return (
        spark
        .read
        .table(f"dataanalytics{environment}.standard_metrics.platform_territory_10min_ts")
        .select(
            "timestamp_10min_slice",
            "platform",
            "service",
            "country_code"
        )
        .where(expr("timestamp_10min_slice between current_timestamp() - interval '2 hour' and current_timestamp()"))
        .distinct()
    )

# COMMAND ----------

def read_dna_sso_mapping(spark, environment):
    dna_sso_mapping = (
        spark
        .read
        .table(f"reference{environment}.sso_mapping.dna_obfuscated_id_mapping")
        .select(
            "unobfuscated_platform_id",
            "obfuscated_platform_id"
        )
    )
    return dna_sso_mapping

# COMMAND ----------

def extract(environment, database):
    fact_player_funnel = read_player_funnel(spark, environment)
    fact_player_funnel_lkp = read_player_funnel_lkp(spark, environment)

    dna_sso_mapping = read_dna_sso_mapping(spark, environment)

    print("Transformation started...")
    # get obfuscated ids for players
    fact_player_funnel_joined = (
        fact_player_funnel.alias('pf')
        .join(
            dna_sso_mapping.alias('dna_sso'),
            expr("pf.player_id = dna_sso.unobfuscated_platform_id"),
            'left')
        .where(expr("pf.installed = True"))
        .select(
            'pf.player_id',
            'platform',
            'service',
            'first_seen_country_code',
            'installed',
            'install_ts_10min_slice',
            'dna_sso.obfuscated_platform_id'
        )
    )

    final_players = (
        fact_player_funnel_joined.alias('mm')
        .join(
            fact_player_funnel_lkp.alias('mp'),
            expr("mm.obfuscated_platform_id = mp.player_id"),
            'left')
        .where(expr("mm.obfuscated_platform_id is null or mp.player_id is null"))
        .select(
            'mm.player_id',
            'mm.platform',
            'mm.service',
            expr('mm.first_seen_country_code as country_code'),
            expr('mm.install_ts_10min_slice as timestamp_10min_slice')
        )
    )
    
    return final_players
    

# COMMAND ----------

def transform(df):
    plat_df = read_platform_territory_df(spark, environment)
    
    fact_player_funnel_final_df = (
        plat_df
        .unionByName(df, True)
        .select(
            'timestamp_10min_slice',
            'platform',
            'service',
            'country_code',
            'player_id'
        )
    )

    agg_installs_df = (
        fact_player_funnel_final_df
        .groupBy(
            "timestamp_10min_slice",
            "platform", 
            "service",
            "country_code")
        .agg(
            expr("COUNT(distinct player_id)::int AS install_count"))
        .select(
            "timestamp_10min_slice",
            "platform",
            "service",
            "country_code",
            "install_count"
        )
    )

    return agg_installs_df

# COMMAND ----------

def load_agg_installs_demo_nrt(spark, df, database, environment):
    """
    Load the data into the agg_player_funnel_nrt table in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        df (DataFrame): The DataFrame to load into the table.
        database (str): The database to load the table into.
        environment (str): The environment to load the table into.
    """
    print("Load started...")

    # Perform the merge operation into the Delta table
    final_table = DeltaTable.forName(spark, f"{database}{environment}.managed.agg_installs_demo_nrt")

    # Get schema of the table
    table_schema = spark.read.table(f"{database}{environment}.managed.agg_installs_demo_nrt").schema

    # Create empty dataframe using the schema
    out_df = spark.createDataFrame([], table_schema)

    # Union the incoming dataframe with the empty dataframe, we can set allowMissingColumns to False to be more strict
    df = df.select(
        "*",
        expr("CURRENT_TIMESTAMP() as dw_insert_ts"),
        expr("CURRENT_TIMESTAMP() as dw_update_ts"),
        expr("sha2(concat_ws('|', timestamp_10min_slice, platform, service, country_code), 256) as merge_key")
    )
    out_df = out_df.unionByName(df, allowMissingColumns=True)


    merge_condition = """
    old.timestamp_10min_slice = new.timestamp_10min_slice AND
    old.platform = new.platform AND
    old.service = new.service
    old.country_code = new.country_code
    """

    update_set = {
        "install_count": "GREATEST(new.install_count, old.install_count)",
    }

    # Merge the table
    (
        final_table.alias('old')
        .merge(
            out_df.alias('new'),
            "new.merge_key = old.merge_key"
        )
        .whenMatchedUpdate(set=update_set)
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

def proc_batch(df, environment):
    transform_df = transform(df)
    load_agg_installs_demo_nrt(spark, transform_df, database, environment)

# COMMAND ----------

def run_stream():
    spark = create_spark_session(name=f"{database}")
    checkpoint_location = create_agg_installs_demo_nrt(spark, database, environment)
    df = extract(environment, database)
    (
            df
            .writeStream
            .trigger(availableNow=True)
            .outputMode("update")
            .foreachBatch(lambda df, batch_id: proc_batch(df, environment ))
            .option("checkpointLocation", checkpoint_location)
            .start()
    )

# COMMAND ----------

run_stream()

# COMMAND ----------


# dbutils.fs.rm("dbfs:/tmp/bluenose/managed/streaming/run_dev/agg_installs_demo_nrt", True)
# spark.sql("drop table bluenose_dev.managed.agg_installs_demo_nrt")

