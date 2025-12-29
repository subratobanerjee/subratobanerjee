# Databricks notebook source
# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/table_functions

# COMMAND ----------

from pyspark.sql.functions import expr, lit, current_timestamp
from delta.tables import DeltaTable

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database = 'nero'

# COMMAND ----------

def create_mission_start_ledger(spark, database, environment):
    """
    Creates the ledger table to track mission runs that have already been processed.
    This is the key to state management in the stream.
    """
    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.intermediate.mission_start_ledger (
        mission_run_id STRING,
        mission_start_ts TIMESTAMP
    )
    """
    create_table(spark, sql)

def create_agg_mission_starts_nrt(spark, database, environment):

    sql = f"""
    CREATE TABLE IF NOT EXISTS {database}{environment}.managed.agg_mission_starts_nrt (
        timestamp_10min_slice TIMESTAMP,
        territory_name STRING,
        platform STRING,
        service STRING,
        country_code STRING,
        mission_start_count BIGINT,
        title STRING,
        merge_key STRING,
        dw_insert_ts TIMESTAMP,
        dw_update_ts TIMESTAMP
    )
    """
    create_table(spark, sql)
    create_agg_mission_starts_nrt_view(spark, database)
    # Returning checkpoint location is a good practice from the template
    return f"dbfs:/tmp/{database}/managed/streaming/run{environment}/agg_mission_starts_nrt"



# COMMAND ----------

def create_agg_mission_starts_nrt_view(spark, database):
    """
    Create the agg_mission_starts_nrt_view view in the specified environment.
    Parameters:
        spark (SparkSession): The Spark session for executing the SQL command.
        database (str): The database to create the view in.
        mapping (dict): A dictionary of column mappings.
    """

    sql = f"""
    CREATE VIEW IF NOT EXISTS {database}{environment}.managed_view.agg_mission_starts_nrt AS (
        SELECT
        *
        from {database}{environment}.managed.agg_mission_starts_nrt
    )
    """
    spark.sql(sql)

# COMMAND ----------

def read_stream_df(database, environment):
    """Reads the raw mission status table as a stream."""
    return (
        spark
        .readStream
        .option('maxFilesPerTrigger', 10000)
        .table(f"{database}{environment}.raw.missionstatus")
    )

def extract_dimensions(spark, environment):

    dim_title_df = spark.read.table(f"reference{environment}.title.dim_title")
    dim_country_df = spark.read.table(f"reference{environment}.location.dim_country")
    return dim_title_df, dim_country_df

def proc_batch(micro_batch_df, batch_id):

    # 2. Load the stateful ledger and deduplicate to find only brand-new mission runs
    ledger_df = spark.read.table(f"{database}{environment}.intermediate.mission_start_ledger").alias("ledger")
    
    new_mission_starts_unfiltered = (
        micro_batch_df
        .filter(expr("""mission_status = 'new_mission' 
                     and to_date(from_unixtime(receivedOn)) >= '2025-04-01' 
                     and appGroupId in ('c818ad1d80dd4638961b95d154151402') 
                     and mission_name = 'Plotline.Main.ch_000_prologue' """))
    )

    newly_identified_missions = (
        new_mission_starts_unfiltered.alias("new")
        .filter(expr("new.mission_run_id IS NOT NULL"))
        .join(
            ledger_df.alias("ledger"),
            expr("new.mission_run_id = ledger.mission_run_id"),
            "left"
        )
        .where(expr("ledger.mission_run_id IS NULL"))
        .select("new.*")
    )
    
    mission_start_ts_df = (
        newly_identified_missions
        .groupBy("mission_run_id", "apppublicid", "countryCode")
        .agg(expr("min(CAST(receivedOn AS TIMESTAMP)) as mission_start_ts"))
    )

    mission_start_ts_df.write.mode("overwrite").saveAsTable(f"{database}{environment}.intermediate.mission_starts_tmp")

    dim_title_df, dim_country_df = extract_dimensions(spark, environment)

    mission_agg_df = (
        spark.read.table(f"{database}{environment}.intermediate.mission_starts_tmp")
        .join(dim_title_df.alias("title"), expr("apppublicid = title.APP_ID"))
        .join(dim_country_df.alias("country"), expr("countryCode = country.country_code"), "left")
        .groupBy(
            expr("window(mission_start_ts::timestamp, '10 minutes').end as timestamp_10min_slice"),
            expr("title.display_platform as platform"),
            expr("title.display_service as service"),
            expr("countryCode"),
            expr("country.territory_name"),
            expr("title")
        )
        .agg(expr("count(distinct mission_run_id) as mission_start_count"))
    )

    platform_territory_scaffold = (
        spark.read.table(f"dataanalytics_prod.standard_metrics.platform_territory_10min_ts")
        .filter("""
            timestamp_10min_slice >= '2025-05-01T00:00:00.000' AND 
            platform in ('XBSX', 'PS5', 'Windows', 'XSX')
        """)
    )

    final_df = (
        platform_territory_scaffold.alias("t")
        .join(
            mission_agg_df.alias("l"),
            expr("""
                t.timestamp_10min_slice = l.timestamp_10min_slice AND
                t.territory_name = l.territory_name AND
                t.platform = l.platform AND
                t.service = l.service AND
                t.country_code = l.countryCode
            """),
            "inner"
        )
        .select(
            expr("t.timestamp_10min_slice"),
            expr("t.territory_name"),
            expr("t.platform"),
            expr("t.service"),
            expr("COALESCE(l.title, 'Mafia: The Old Country') as title"),
            expr("coalesce(l.countryCode, t.country_code) as country_code"),
            expr("COALESCE(l.mission_start_count, 0) as mission_start_count")
        )
        .distinct()
    )


    df_to_load = final_df.withColumn("dw_update_ts", current_timestamp()) \
                         .withColumn("dw_insert_ts", current_timestamp()) \
                         .withColumn("merge_key", expr("sha2(concat_ws('|', timestamp_10min_slice, territory_name, platform, service, country_code, title), 256)"))
    
    target_agg_table = DeltaTable.forName(spark, f"{database}{environment}.managed.agg_mission_starts_nrt")
    update_expr = {
            "timestamp_10min_slice": "source.timestamp_10min_slice",
            "territory_name": "source.territory_name",
            "platform": "source.platform",
            "service": "source.service",
            "country_code": "source.country_code",
            "title": "source.title",
            "mission_start_count": "target.mission_start_count + source.mission_start_count",
            "dw_update_ts": "current_timestamp()"
            }
    
    (
        target_agg_table.alias("target")
        .merge(df_to_load.alias("source"), "target.merge_key = source.merge_key")
        .whenMatchedUpdate(set=update_expr)
        .whenNotMatchedInsertAll()
        .execute()
    )

    df_for_ledger_merge = ( 
                            spark
                            .read
                            .table(f"{database}{environment}.intermediate.mission_starts_tmp")
                            .groupBy("mission_run_id")
                            .agg(expr("min(mission_start_ts) as mission_start_ts"))
                    )
    
    ledger_table = DeltaTable.forName(spark, f"{database}{environment}.intermediate.mission_start_ledger")
    (
        ledger_table.alias("target")
        .merge(df_for_ledger_merge.alias("source"), "target.mission_run_id = source.mission_run_id")
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

# DBTITLE 1,Stream Orchestration
def stream_mission_starts_nrt(database, environment):
    """
    Sets up and starts the streaming query.
    """
    create_mission_start_ledger(spark, database, environment)
    checkpoint_location = create_agg_mission_starts_nrt(spark, database, environment)
    
    streaming_df = read_stream_df(database, environment)

    (
        streaming_df
        .writeStream
        .queryName("Nero Mission Starts NRT - Stateless")
        .trigger(processingTime = "10 minutes")
        .option("checkpointLocation", checkpoint_location)
        .foreachBatch(proc_batch)
        .start()
    )

# COMMAND ----------

stream_mission_starts_nrt(database, environment)