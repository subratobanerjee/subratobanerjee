# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/table_functions
# MAGIC

# COMMAND ----------

def create_civ7_daily_events_view(environment, spark):
  sql_create_view = f"""
   create view if not exists dataanalytics{environment}.cdp_ng.vw_civ7_daily_events
    as
  with summary_view as (
  select
    civ_civ7_install_date,
    civ_civ7_last_seen_date,
    brand_firstpartyid,
    salted_puid,
    puid,
    platform_account_id,
    civ_civ7_vendor
  from
    dataanalytics{environment}.cdp_ng.civ7_summary
),
all_events as (
  select distinct civ_civ7_install_date::date event_date,
    platform_account_id player_id,
    civ_civ7_vendor vendor,
    'civ_civ7' AS GAME_TITLE,
    'total' AS SUB_GAME,
    'install' AS EVENT_NAME,
    NULL AS EVENT_ACTION,
    1 AS EVENT_RESULT,
    NULL AS EVENT_DESCRIPTION
  from
    summary_view
  union all
  select
    received_on::date AS EVENT_DATE,
    F.PLAYER_ID AS player_id,
    civ_civ7_vendor vendor,
    'civ_civ7' AS GAME_TITLE,
    'total' AS SUB_GAME,
    'reactivation' AS EVENT_NAME,
      NULL AS EVENT_ACTION,
      1 AS EVENT_RESULT,
      NULL AS EVENT_DESCRIPTION
  FROM
    inverness{environment}.intermediate.fact_player_activity F
      join summary_view sum on F.player_id = sum.platform_account_id
  QUALIFY
    DATEDIFF(
      CAST(received_on AS DATE),
      LAG(CAST(received_on AS DATE), 1) OVER (PARTITION BY F.PLAYER_ID ORDER BY received_on::date)
    ) >= 14
  union all
  SELECT distinct
    min(received_on)::date as event_date,
    ent.player_id as player_id,
    civ_civ7_vendor vendor,
    'civ_civ7' AS GAME_TITLE,
    'total' AS SUB_GAME,
    'dlc_install' AS EVENT_NAME,
    NULL AS EVENT_ACTION,
    1 AS EVENT_RESULT,
    NULL AS EVENT_DESCRIPTION
  from
    ( select distinct playerpublicid as player_id,cast(receivedon as timestamp) as received_on,
            explode(split(activedlc, ",")) as entitlement_id
            from inverness{environment}.raw.applicationsessionstatus ass
            where playerpublicid != 'anonymous' and playerpublicid is not null
            )as ent
      join summary_view sum on ent.player_id = sum.platform_account_id
  WHERE
    player_id is not null
    and player_id != 'anonymous'
    and entitlement_id ilike '%shawnee-tecumseh%'
    or entitlement_id ilike '%collection-1%'
    or entitlement_id ilike '%collection-2%'
    or entitlement_id ilike '%collection-3%'
    or entitlement_id ilike '%collection-4%'
  group by
    player_id,
    vendor
)
select distinct
  event_date::date,
  brand_firstpartyid,
  salted_puid,
  puid,
  vendor,
  game_title,
  sub_game,
  event_name,
  cast(null as varchar(100)) event_action,
  cast(cast(event_result as decimal(20, 2)) as varchar(100)) event_result,
  cast(null as varchar(100)) event_description,
  dataanalytics{environment}.cdp_ng.mk_cdp_field2(
    game_title, sub_game, event_name, event_action
  ) cdp_field_name
from
  all_events al join summary_view sum on al.player_id = sum.platform_account_id;

  """
  
  df = spark.sql(sql_create_view)
  return df


# COMMAND ----------

def create_civ7_daily_events_table(environment, spark):
    sql = f"""
    CREATE TABLE IF NOT EXISTS dataanalytics{environment}.cdp_ng.civ7_daily_events (
        EVENT_DATE date,
        BRAND_FIRSTPARTYID STRING,
        SALTED_PUID STRING,
        PUID STRING,
        VENDOR STRING,
        GAME_TITLE STRING,
        SUB_GAME STRING,
        EVENT_NAME STRING,
        EVENT_ACTION STRING,
        EVENT_RESULT STRING,
        EVENT_DESCRIPTION STRING,
        CDP_FIELD_NAME STRING
    )
    """

    properties = {
        "delta.feature.allowColumnDefaults": "supported"  # this won't work until iceberg v3 is released
    }

    create_table(spark, sql, properties)
    return (
        f"dbfs:/tmp/dataanalytics/cdp_ng/streaming/run{environment}/civ7_daily_events"
    )


# COMMAND ----------

def run_batch():
    # Parse the environment argument
    input_param = dbutils_input_params()
    environment = input_param.get('environment', set_environment())

    # Create a Spark session
    spark = SparkSession.builder.appName("Hydra").getOrCreate()
    
    # Create the view
    df = create_civ7_daily_events_view(environment, spark)
    checkpoint_location = create_civ7_daily_events_table(environment ,spark)

    spark.sql(f"""select * from dataanalytics{environment}.cdp_ng.vw_civ7_daily_events""").show()

# COMMAND ----------

if __name__ == "__main__":
    run_batch()


