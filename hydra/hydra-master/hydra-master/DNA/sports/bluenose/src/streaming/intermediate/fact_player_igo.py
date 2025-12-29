# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/intermediate/fact_player_igo

# COMMAND ----------

from pyspark.sql.functions import col, when, explode, split, regexp_replace, array, expr


# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get("environment", set_environment())
database = "bluenose"
data_source = "dna"

# COMMAND ----------


def read_quest_status():
    return (
        spark.readStream.table(f"{database}{environment}.raw.queststatus")
        .withColumn("received_on", col("receivedon").cast("timestamp"))
        .where((col("playerPublicId").isNotNull()) & (col("playerPublicId") != 'anonymous') & (col("buildenvironment") == 'RELEASE'))
    )


def read_title():
    return spark.read.table(f"reference{environment}.title.dim_title")


def extract():

    quest_df = read_quest_status()
    title_df = read_title()

    return quest_df, title_df


# COMMAND ----------


def transform(df, title_df):
    # Perform the join
    joined_df = df.join(title_df, df.appPublicId == title_df.APP_ID)
   
    # Select and transform the columns
    transformed_df = (
    joined_df
    .withColumn(
        "questid",
        when(col("questid").isNull(), col("questlistid")).otherwise(col("questid"))
    )
    # Normalize questid to always be an array
    .withColumn(
        "questid",
        when(
            col("questid").rlike(r"^\[.*\]"),  # Detect array-like format
            split(regexp_replace(col("questid"), "[\\[\\]']", ""), ",\\s*")  # Convert string to array
        ).otherwise(array(col("questid")))  # Wrap single string in an array
    )
    # Normalize objectiveid to always be an array
    .withColumn(
        "objectiveid",
        when(
            col("objectiveid").rlike(r"^\[.*\]"),
            split(regexp_replace(col("objectiveid"), "[\\[\\]']", ""), ",\\s*")
        ).otherwise(array(col("objectiveid")))
    )
    # Normalize chapterid to always be an array
    .withColumn(
        "chapterid",
        when(
            col("chapterid").rlike(r"^\[.*\]"),
            split(regexp_replace(col("chapterid"), "[\\[\\]']", ""), ",\\s*")
        ).otherwise(array(col("chapterid")))
    )
    # Explode arrays into individual rows
    .withColumn("questid", explode(col("questid")))
    .withColumn("objectiveid", explode(col("objectiveid")))
    .withColumn("chapterid", explode(col("chapterid")))
    # Select and transform columns
    .select(
        expr("received_on"),
        expr("playerPublicId AS player_id"),
        expr("display_platform AS platform"),
        expr("display_service AS service"),
        expr("coalesce(countryCode, 'ZZ') as country_code"),
        expr("currentmyplayerid AS primary_instance_id"),
        expr(
            "CASE WHEN queststatus IN ('Complete', 'Activated', 'Deactivated', 'InitializedAndActivated', 'Deactivated', 'Pinned' ) THEN 'quest' "
            + "WHEN queststatus = 'ObjectiveComplete' THEN 'objective' "
            + "WHEN queststatus = 'ChapterComplete' THEN 'chapter' END as igo_type"
        ),
        expr(
            "CASE WHEN queststatus IN ('Complete', 'Activated', 'Deactivated', 'InitializedAndActivated', 'Deactivated', 'Pinned' ) THEN questid "
            + "WHEN queststatus = 'ObjectiveComplete' THEN objectiveid "
            + "WHEN queststatus = 'ChapterComplete' THEN chapterid END as igo_id"
        ),
        expr("queststatus AS status"),
        expr("questid AS extra_info_1"),
        expr("objectiveid AS extra_info_2"),
        expr("chapterid AS extra_info_3"),
        expr("CASE WHEN appgroupid IN ('d3963fb6e4f54a658e1a14846f0c9a8c', '317c0552032c4804bb10d81b89f4c37e') THEN 'full game'"
             + "WHEN APPGROUPID = '886bcab8848046d0bd89f5f1ce3b057b' THEN 'demo' END AS extra_info_4"),
        expr("questtype AS extra_info_5"),
    )
    )  
    transformed_df = transformed_df.fillna({"extra_info_5": "N/A"})

    return transformed_df


# COMMAND ----------


def run_streaming():
    spark = create_spark_session(name=f"{database}")
    checkpoint_location = create_fact_player_igo(spark, database, environment)

    quest_df, title_df = extract()
    load_df = transform(quest_df, title_df)
    load_fact_player_igo(spark, load_df, database, environment, checkpoint_location,"batch")


# COMMAND ----------

run_streaming()

# COMMAND ----------

# dbutils.fs.rm("dbfs:/tmp/bluenose/intermediate/streaming/run_dev/fact_player_igo", True)
# spark.sql("drop table bluenose_dev.intermediate.fact_player_igo")

