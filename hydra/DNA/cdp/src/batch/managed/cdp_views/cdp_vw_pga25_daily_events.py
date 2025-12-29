# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------
# MAGIC %run ../../../../../../utils/ddl/table_functions

# COMMAND ----------
def create_daily_events_view(environment, spark):
    # Define the SQL query based on the environment
    sql_query_events = f"""
    create view if not exists dataanalytics{environment}.cdp_ng.vw_pga2k25_daily_events AS
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
                received_on :: timestamp desc
            ) vendor,
            first_value(s.service) over (
            partition by f.player_id
            order by
                received_on :: timestamp desc
            ) service,
            received_on :: date as player_dates,
            extra_info_5 install_type
        from
            bluenose{environment}.intermediate.fact_player_activity f
            join services s on lower(s.join_key) = lower(f.service)
            join reference_customer.platform.platform v on f.platform = v.src_platform
        where
            f.platform IS NOT NULL
        ),
        ALL_EVENTS AS (
        SELECT
            cast(min(player_dates) as date) AS EVENT_DATE,
            PLAYER_ID,
            VENDOR,
            'pga2k25' AS GAME_TITLE,
            'total' AS SUB_GAME,
            'fl_install' AS EVENT_NAME,
            NULL AS EVENT_ACTION,
            1 AS EVENT_RESULT,
            NULL AS EVENT_DESCRIPTION
        FROM
            FACT_PLAYERS_LTD
        where
            install_type = 'demo'
        GROUP BY
            PLAYER_ID,
            VENDOR
        UNION ALL
        SELECT
            cast(min(player_dates) as date) AS EVENT_DATE,
            PLAYER_ID,
            VENDOR,
            'pga2k25' AS GAME_TITLE,
            'total' AS SUB_GAME,
            'fg_install' AS EVENT_NAME,
            NULL AS EVENT_ACTION,
            1 AS EVENT_RESULT,
            NULL AS EVENT_DESCRIPTION
        FROM
            FACT_PLAYERS_LTD
        where
            install_type = 'full game'
        GROUP BY
            PLAYER_ID,
            VENDOR
        UNION ALL
        SELECT
            cast(min(player_dates) as date) AS EVENT_DATE,
            S.PLAYER_ID,
            CASE
            WHEN V.VENDOR = 'Valve' THEN 'PC'
            ELSE V.VENDOR
            END AS VENDOR,
            'pga2k25' AS GAME_TITLE,
            'total' AS SUB_GAME,
            'reactivation' AS EVENT_NAME,
            NULL AS EVENT_ACTION,
            1 AS EVENT_RESULT,
            NULL AS EVENT_DESCRIPTION
        FROM
            FACT_PLAYERS_LTD S
            JOIN REFERENCE_CUSTOMER.PLATFORM.SERVICE V ON S.SERVICE = V.SERVICE
        GROUP BY
            S.PLAYER_ID,
            V.VENDOR QUALIFY DATEDIFF(
            CAST(EVENT_DATE AS DATE),
            LAG(CAST(EVENT_DATE AS DATE), 1) OVER (
                PARTITION BY S.PLAYER_ID
                ORDER BY
                EVENT_DATE
            )
            ) >= 14
        UNION ALL
        SELECT
            CAST(S.received_on AS DATE) AS EVENT_DATE,
            PLAYER_ID,
            V.VENDOR,
            'pga2k25' AS GAME_TITLE,
            'total' AS SUB_GAME,
            'bought_vc' AS EVENT_NAME,
            'purchase' AS EVENT_ACTION,
            SUM(currency_amount) AS EVENT_RESULT,
            NULL AS EVENT_DESCRIPTION
        FROM
            bluenose{environment}.intermediate.fact_player_transaction S
            JOIN REFERENCE_CUSTOMER.PLATFORM.PLATFORM V ON S.PLATFORM = V.SRC_PLATFORM
        WHERE
            lower(action_type) = 'purchase'
        GROUP BY
            CAST(S.received_on AS DATE),
            PLAYER_ID,
            V.VENDOR
        HAVING
            SUM(currency_amount) > 0
        UNION ALL
        SELECT
            CAST(TRANS.received_on AS DATE) AS EVENT_DATE,
            TRANS.PLAYER_ID AS PLAYER_ID,
            FP.VENDOR,
            'pga2k25' AS GAME_TITLE,
            'total' AS SUB_GAME,
            'recurrent_consumer_spend' AS EVENT_NAME,
            'rcs' AS EVENT_ACTION,
            1 AS EVENT_RESULT,
            SUM(currency_amount) AS EVENT_DESCRIPTION
        FROM
            bluenose{environment}.intermediate.fact_player_transaction TRANS
            INNER JOIN FACT_PLAYERS_LTD FP ON TRANS.PLAYER_ID = FP.PLAYER_ID
        WHERE
            (
            sub_mode ilike '%Clubhouse%'
            and action_type ilike '%Spend%'
            )
            or action_type ilike '%Purchase%'
        GROUP BY
            CAST(TRANS.received_on AS DATE),
            TRANS.PLAYER_ID,
            FP.VENDOR
        ),
        fpids as (
        select
            distinct le.accountid platform_id,
            dataanalytics{environment}.cdp_ng.fpid_decryption(le.firstpartyId) puid
        from
            coretech{environment}.sso.linkevent le
            join reference{environment}.title.dim_title t on le.appId = t.app_id
            and title ilike '%2K Portal%'
            and le.firstpartyId is not null
            and le.firstpartyId not ilike '%@%'
        where
            accounttype = 2
            and targetaccounttype = 3
            and le.firstPartyId != 'None' -- fix AES_CRYPTO_ERROR
        union
        select
            distinct le.targetaccountId platform_id,
            dataanalytics{environment}.cdp_ng.fpid_decryption(le.firstpartyId) puid
        from
            coretech{environment}.sso.linkevent le
            join reference{environment}.title.dim_title t on le.appId = t.app_id
            and title ilike '%2K Portal%'
            and le.firstpartyId is not null
            and le.firstpartyId not ilike '%@%'
        where
            accounttype = 3
            and targetaccounttype = 2
            and le.firstPartyId != 'None' -- fix AES_CRYPTO_ERROR
        union
        select
            distinct f.player_id platform_id,
            first_party_id puid
        from
            bluenose{environment}.intermediate.fact_player_activity f
            join reference_customer.customer.vw_ctp_profile le on lower(f.player_id) = lower(le.public_id)
        where
            le.first_party_id is not null
            and f.received_on :: timestamp >= '2025-02-04' :: timestamp
        )
        SELECT
        DISTINCT EVENT_DATE,
        VENDOR || ':' || DATAANALYTICS{environment}.CDP_NG.SALTED_PUID(CTP.PUID) AS BRAND_FIRSTPARTYID,
        DATAANALYTICS{environment}.CDP_NG.SALTED_PUID(CTP.PUID) AS SALTED_PUID,
        CTP.PUID AS PUID,
        VENDOR,
        GAME_TITLE,
        SUB_GAME,
        EVENT_NAME,
        EVENT_ACTION,
        CAST(
            CAST(EVENT_RESULT AS DECIMAL(20, 2)) AS VARCHAR(50)
        ) AS EVENT_RESULT,
        cast(null as varchar(100)) event_description,
        DATAANALYTICS{environment}.CDP_NG.MK_CDP_FIELD2(GAME_TITLE, SUB_GAME, EVENT_NAME, EVENT_ACTION) AS CDP_FIELD_NAME
        FROM
        ALL_EVENTS A
        JOIN fpids CTP ON A.PLAYER_ID = CTP.platform_id
        JOIN dataanalytics{environment}.cdp_ng.vw_pga2k25_summary sum on ctp.puid = sum.puid
    """

    # Execute the SQL query and return the result as a DataFrame
    df = spark.sql(sql_query_events)
    return df

# COMMAND ----------

def create_pga2k25_daily_events_table(environment, spark):
    sql = f"""
    CREATE TABLE IF NOT EXISTS dataanalytics{environment}.cdp_ng.pga2k25_daily_events (
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
        CDP_FIELD_NAME STRING
    )
    """

    properties = {
        "delta.feature.allowColumnDefaults": "supported"  # this won't work until iceberg v3 is released
    }

    create_table(spark, sql, properties)
    return (
        f"dbfs:/tmp/dataanalytics/cdp_ng/streaming/run{environment}/pga2k25_daily_events"
    )

# COMMAND ----------
def run_batch():
    # Parse the environment argument
    input_param = dbutils_input_params()
    environment = input_param.get('environment', set_environment())

    # Create a Spark session
    spark = SparkSession.builder.appName("Hydra").getOrCreate()
    
    # Create the view
    df = create_daily_events_view(environment, spark)
    #create delta table
    checkpoint_location = create_pga2k25_daily_events_table(environment ,spark)

    spark.sql(f"""select * from dataanalytics{environment}.cdp_ng.vw_pga2k25_daily_events""").show()

# COMMAND ----------

if __name__ == "__main__":
    run_batch()
