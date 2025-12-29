# Databricks notebook source
# MAGIC %run ../../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../../utils/helpers

# COMMAND ----------


def create_bdl4_daily_events_view(environment, spark):
  sql_create_view = f"""
    create view if not exists dataanalytics{environment}.cdp_ng.vw_bdl4_daily_events AS
with services as (
        SELECT
            CASE
            SERVICE
            WHEN 'xbl' THEN 'Xbox Live' -- WHEN 'steam' THEN 'PC'
            ELSE SERVICE
            END AS JOIN_KEY,
            SERVICE,
            VENDOR
        FROM
            REFERENCE_CUSTOMER.PLATFORM.SERVICE
        GROUP BY
            1,
            2,
            3
        ),
        FACT_PLAYERS_LTD AS (
        SELECT
            DISTINCT lower(f.player_id) player_id,
            first_value(v.vendor) over (
            partition by f.player_id
            order by
                Install_date :: timestamp 
            ) vendor,
            first_value(s.service) over (
            partition by f.player_id
            order by
                Install_date :: timestamp 
            ) service,
            Install_date :: date as Install_date,
            last_seen :: date as last_seen
        from
            (select * from dataanalytics{environment}.standard_metrics.fact_player_ltd where title = 'Borderlands 4') f           
            join services s on lower(s.join_key) = lower(f.service)
            join reference_customer.platform.platform v on f.platform = v.src_platform
        where
            f.platform IS NOT NULL
        )    
        ,

        ALL_EVENTS AS (
        SELECT
        CAST(MIN(Install_date) AS DATE) AS EVENT_DATE,
        PLAYER_ID,
        VENDOR,
        CAST('bdl_bl4' AS VARCHAR(50)) AS GAME_TITLE,
        CAST('total' AS VARCHAR(50)) AS SUB_GAME,
        CAST('install' AS VARCHAR(50)) AS EVENT_NAME,
        CAST(NULL AS VARCHAR(50)) AS EVENT_ACTION,
        CAST('1' AS VARCHAR(50)) AS EVENT_RESULT,
        CAST('First date that a player appears in the game' AS VARCHAR(255)) AS EVENT_DESCRIPTION,
        CAST('bdl_bl4_total_install_daily' AS VARCHAR(50)) AS CDP_FIELD_NAME
        FROM
            FACT_PLAYERS_LTD
        GROUP BY
            PLAYER_ID,
            VENDOR
            
       UNION ALL
        SELECT
    CAST(LAST(last_seen) AS DATE) AS EVENT_DATE,
    S.PLAYER_ID,
    CASE
        WHEN V.VENDOR = CAST('Valve' AS VARCHAR(50)) THEN CAST('PC' AS VARCHAR(50))
        ELSE CAST(V.VENDOR AS VARCHAR(50))
    END AS VENDOR,
    CAST('bdl_bl4' AS VARCHAR(50)) AS GAME_TITLE,
    CAST('total' AS VARCHAR(50)) AS SUB_GAME,
    CAST('reactivation' AS VARCHAR(50)) AS EVENT_NAME,
    CAST('' AS VARCHAR(50)) AS EVENT_ACTION,
    CAST('' AS VARCHAR(50)) AS EVENT_RESULT,
    CAST('This event is triggered when a user leaves the game for 14 days (has churned) and then returns (reactivates)' AS VARCHAR(255)) AS EVENT_DESCRIPTION,
    CAST('bdl_bl4_total_reactivation_daily' AS VARCHAR(50)) AS CDP_FIELD_NAME
        FROM
            FACT_PLAYERS_LTD S
            JOIN REFERENCE_CUSTOMER.PLATFORM.SERVICE V ON S.SERVICE = V.SERVICE
        GROUP BY
            S.PLAYER_ID,
            V.VENDOR 
            QUALIFY DATEDIFF(CAST(EVENT_DATE AS DATE), LAG(CAST(EVENT_DATE AS DATE), 1) OVER (PARTITION BY S.PLAYER_ID ORDER BY EVENT_DATE)) >= 14
        ),
        fpids AS (
  SELECT
    PLAYER_ID as platform_id,
    FPID AS puid
  FROM
    reference_customer.customer.vw_fpid_all
  WHERE
    FPID IS NOT NULL
)
                   
        SELECT
        DISTINCT EVENT_DATE,
        VENDOR || ':' || dataanalytics{environment}.cdp_ng.salted_puid(fpd.puid) AS brand_firstpartyid,
        dataanalytics{environment}.cdp_ng.salted_puid(fpd.puid) AS salted_puid,
        fpd.puid,
        VENDOR,
        GAME_TITLE,
        SUB_GAME,
        EVENT_NAME,
        EVENT_ACTION,
        CDP_FIELD_NAME,
        EVENT_DESCRIPTION,
        CAST(
            CAST(EVENT_RESULT AS DECIMAL(20, 2)) AS VARCHAR(50)
        ) AS EVENT_RESULT
        FROM
        ALL_EVENTS A
        JOIN fpids fpd ON LOWER(fpd.platform_id) = LOWER(a.PLAYER_ID)
        JOIN dataanalytics{environment}.cdp_ng.vw_bdl4_summary sum on fpd.puid = sum.puid 
            

  """
  
  df = spark.sql(sql_create_view)
  return df



# COMMAND ----------

def run_batch():
    # Parse the environment argument
    input_param = dbutils_input_params()
    environment = input_param.get('environment', set_environment())

    # Create a Spark session
    spark = SparkSession.builder.appName("Hydra").getOrCreate()
    
    # Create the view
    df = create_bdl4_daily_events_view(environment, spark)

    spark.sql(f"""select * from dataanalytics{environment}.cdp_ng.vw_bdl4_daily_events""").display()

# COMMAND ----------

if __name__ == "__main__":
    run_batch()
