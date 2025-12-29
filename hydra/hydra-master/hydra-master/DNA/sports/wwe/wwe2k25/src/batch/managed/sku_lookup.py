# Databricks notebook source
# MAGIC %run ../../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../../utils/ddl/wwe_game_specific/sku_lookup

# COMMAND ----------

from pyspark.sql.functions import expr

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment', set_environment())
database = 'wwe2k25'
data_source = 'dna'
view_mapping = {}


# COMMAND ----------

def read_dlc_df(environment):
    # Load source and reference dataframes
    return (
        spark
        .read
        .option('maxFilesPerTrigger', 10000)
        .table(f"wwe2k25{environment}.raw.dlclist")
        .where(expr(f"UPPER(buildtype) = 'FINAL' and receivedOn::timestamp::date >= current_date() - INTERVAL 3 DAY"))
    )

# COMMAND ----------

def read_title_df(environment):
    return (
        spark
        .read
        .table(f"reference{environment}.title.dim_title")
        .select(
            "title",
            "app_id",
            "app_group_id",
            expr("display_platform as platform"),
            expr("display_service as service")
        )
    )

# COMMAND ----------

def extract(environment):
    # Join the source and reference tables
    dlc_df = read_dlc_df(environment).alias("dlc")
    title_df = read_title_df(environment).alias("title")
    joined_df = (
        dlc_df
        .join(title_df, on=expr("title.app_id = dlc.appPublicId"),
            how="left")
        .select(
            expr(f"playerPublicId as player_id"),
            expr(f"title.platform"),
            expr(f"title.service"),
            expr(f"auxcool as bloodline"),
            expr(f"auxawesome as deadman"),
            expr(f"basegame as basegame"),
            expr(f"auxexcellent as wm41"),
            expr(f"auxgreat as domrock"),
            expr(f"PreOrderBonus as wyattsicks"),
            expr(f"seasonPass as seasonpass"),
            expr(f"sequenceNumber as seq_num"),
            expr(f"receivedOn::timestamp as received_on")
        )
    )
    print(joined_df.count())
    
    return joined_df

# COMMAND ----------

def transform(df, environment):
    new_df = (
        df
        .groupBy(
            "player_id",
            "platform",
            "service"
        )
        .agg(
            expr("case when max(bloodline) = 1 and max(deadman) = 1 and max(domrock) = 1 and max(wyattsicks) = 1 then 'SuperDeluxe' \
                 when max(deadman) = 1 and max(wyattsicks) = 1 then 'Deluxe' \
                 else 'BaseGame' end as sku"),
            expr("ifnull(case when max(wyattsicks) = 1 and min(to_date(received_on)) < to_date('2025-04-14') then True else False end, False)::boolean as preorder_flag"),
            expr("ifnull(case when max(seasonpass) = 1 then True else False end, False)::boolean as seasonpass_flag"),
            expr("max(seq_num) as seq_num")
        )
        .select(
            "player_id",
            "platform",
            "service",
            expr("ifnull(sku, 'BaseGame') as sku"),
            expr("ifnull(preorder_flag, False)::boolean as preorder_flag"),
            expr("ifnull(case when seasonpass_flag = True or sku = 'Deluxe' then True else False end, False)::boolean as seasonpass_flag"),
            expr("ifnull(case when sku in ('SuperDeluxe', 'Deluxe') then True else False end, False)::boolean as premium_flag"),
            expr("seq_num")
        )
    )

    return new_df


# COMMAND ----------

def run_batch():
    checkpoint_location = create_sku_lookup(spark, database)

    df = extract(environment)
    df = transform(df, environment)

    load_sku_lookup(spark, df, database, environment)

# COMMAND ----------

run_batch()