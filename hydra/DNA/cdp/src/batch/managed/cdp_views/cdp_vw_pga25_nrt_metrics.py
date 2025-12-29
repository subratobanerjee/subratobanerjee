# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------
# MAGIC %run ../../../../../../utils/ddl/table_functions

# COMMAND ----------

def create_nrt_metrics_view(environment, spark):
    # Define the SQL query based on the environment
    sql_query_events = f"""
    create view if not exists dataanalytics{environment}.cdp_ng.vw_pga2k25_nrt_metrics AS
    WITH 
        
    fpids as (
        select
            distinct le.accountid platform_id,
            dataanalytics_prod.cdp_ng.fpid_decryption(le.firstpartyId) puid
        from
            coretech_prod.sso.linkevent le
            join reference{environment}.title.dim_title t on le.appId = t.app_id
            and title ilike '%PGA Tour 2K25%'
            and le.firstpartyId is not null
        where
            accounttype = 2
            and targetaccounttype = 3
            and le.firstPartyId != 'None'  -- fix AES_CRYPTO_ERROR
        union
        select
            distinct le.targetaccountId platform_id,
            dataanalytics_prod.cdp_ng.fpid_decryption(le.firstpartyId) puid
        from
            coretech_prod.sso.linkevent le
            join reference{environment}.title.dim_title t on le.appId = t.app_id
            and title ilike '%PGA Tour 2K25%'
            and le.firstpartyId is not null
        where
            accounttype = 3
            and targetaccounttype = 2
            and le.firstPartyId != 'None'  -- fix AES_CRYPTO_ERROR
        union
        select
            distinct f.player_id platform_id,
            first_party_id puid
        from
            bluenose{environment}.intermediate.fact_player_activity f
            join reference_customer.customer.vw_ctp_profile le on lower(f.player_id) = lower(le.public_id)
        where
            le.first_party_id is not null
            and f.source_table = 'loginevent'
        ),

    BRAND_PUID AS (
        SELECT DISTINCT
            brand_firstpartyid,
            puid,
            COUNT(DISTINCT pga2k25_install_date) AS fl_login,
            COUNT(pga2k25_days_active) AS days_active
        FROM dataanalytics{environment}.cdp_ng.pga2k25_summary
        GROUP BY brand_firstpartyid, puid
    ),


    FG_LOGIN AS (
        SELECT
            fpids.puid,
            COUNT(*) AS fg_login
    FROM bluenose{environment}.intermediate.fact_player_activity f
        INNER JOIN fpids ON LOWER(f.player_id) = LOWER(fpids.platform_id)
        WHERE f.extra_info_5 = 'full game'
        GROUP BY fpids.puid
    ),


    OVR_LEVEL AS (
        SELECT
            fpids.puid,
            COUNT(DISTINCT cp.ovr) AS ovr_level
        FROM bluenose{environment}.raw.characterprogression cp
        INNER JOIN fpids ON LOWER(cp.playerPublicId) = LOWER(fpids.platform_id)
        GROUP BY fpids.puid
    ),
    SESSION_START AS (
        SELECT
            fpids.puid,
            COUNT(DISTINCT fps.application_session_id) AS session_start
        FROM bluenose{environment}.intermediate.fact_player_session fps
        INNER JOIN fpids ON LOWER(fps.player_id) = LOWER(fpids.platform_id)
        WHERE fps.session_start_ts IS NOT NULL
        GROUP BY fpids.puid
    )

    SELECT
        BP.brand_firstpartyid,
        BP.puid,
        COALESCE(BP.fl_login, 0) AS fl_login,
        COALESCE(FG.fg_login, 0) AS fg_login,
        COALESCE(OV.ovr_level, 0) AS ovr_level,
        COALESCE(BP.days_active, 0) AS days_active,
        COALESCE(SS.session_start, 0) AS session_start
    FROM BRAND_PUID BP
    LEFT JOIN SESSION_START SS ON BP.puid = SS.puid
    LEFT JOIN FG_LOGIN FG ON BP.puid = FG.puid
    LEFT JOIN OVR_LEVEL OV ON BP.puid = OV.puid;
    """

    # Execute the SQL query and return the result as a DataFrame
    df = spark.sql(sql_query_events)
    return df

# COMMAND ----------

def create_pga2k25_nrt_metric_table(environment, spark):
    sql = f"""
    CREATE TABLE IF NOT EXISTS dataanalytics{environment}.cdp_ng.pga2k25_nrt_metrics (
        brand_firstpartyid STRING,
        puid STRING,
        fl_login BIGINT,
        fg_login BIGINT,
        ovr_level BIGINT,
        days_active BIGINT,
        session_start BIGINT
    )
    """

    properties = {
        "delta.feature.allowColumnDefaults": "supported"  # this won't work until iceberg v3 is released
    }

    create_table(spark, sql, properties)
    return (
        f"dbfs:/tmp/dataanalytics/cdp_ng/streaming/run{environment}/pga2k25_nrt_metrics"
    )

# COMMAND ----------
def run_batch():
    # Parse the environment argument
    input_param = dbutils_input_params()
    environment = input_param.get('environment', set_environment())

    # Create a Spark session
    spark = SparkSession.builder.appName("Hydra").getOrCreate()
    
    # Create the view
    df = create_nrt_metrics_view(environment, spark)

    #create the table
    checkpoint_location = create_pga2k25_nrt_metric_table(environment ,spark)
    spark.sql(f"""select * from dataanalytics{environment}.cdp_ng.vw_pga2k25_nrt_metrics""").show()

# COMMAND ----------

if __name__ == "__main__":
    run_batch()