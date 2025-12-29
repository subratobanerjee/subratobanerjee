# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------
# MAGIC %run ../../../../../../utils/ddl/table_functions

# COMMAND ----------
def create_pga2k25_franchise_view(environment, spark):
    # Define the SQL query based on the environment
    sql_query_events = f"""
    create view if not exists dataanalytics{environment}.cdp_ng.vw_pga_franchise 
        AS WITH 

            UNION_TITLES AS (

        SELECT 'PGA25' TAG, BRAND_FIRSTPARTYID
                    , SALTED_PUID   
                    , PUID
                            
                    , COALESCE(PGA2K25_CURRENT_GEN_PLATFORM,  PGA2K25_NEXT_GEN_PLATFORM) as            PLATFORM
                    , PGA2K25_CURRENT_GEN_PLATFORM              CURRENT_GEN_PLATFORM
                    , PGA2K25_NEXT_GEN_PLATFORM   NEXT_GEN_PLATFORM

                    , PGA2K25_COUNTRY_LAST          LAST_COUNTRY

                    , PGA2K25_LAST_SEEN_DATE        LAST_SEEN
                    , PGA2K25_BOUGHT_VC_LAST_DATE   BOUGHT_VC_LAST_DATE

                    FROM  dataanalytics{environment}.cdp_ng.pga2k25_summary

                    union all 

                SELECT 'PGA21' TAG, BRAND_FIRSTPARTYID
                    , SALTED_PUID   
                    , PUID
                            
                    , PGA2K21_PLATFORM              PLATFORM
                    , PGA2K21_PLATFORM              CURRENT_GEN_PLATFORM
                    , CAST(NULL AS VARCHAR(255))    NEXT_GEN_PLATFORM

                    , PGA2K21_COUNTRY_LAST          LAST_COUNTRY

                    , PGA2K21_LAST_SEEN_DATE        LAST_SEEN
                    , PGA2K21_BOUGHT_VC_LAST_DATE   BOUGHT_VC_LAST_DATE

                    FROM  sf_databricks_migration.sf_dbx.PGA2K21_GAME_SUMMARY

                UNION ALL 

                -- from CDP-904
                SELECT  'TGC19' , BRAND_FIRSTPARTYID
                    , SALTED_PUID   
                    , PUID

                    , TGC_2019_PLATFORM
                    , TGC_2019_PLATFORM                             CURRENT_GEN_PLATFORM
                    , CAST(NULL AS VARCHAR(255))                    NEXT_GEN_PLATFORM

                    , TGC_2019_COUNTRY_LAST
                    , CAST(TGC_2019_LAST_SEEN AS DATE)              TGC_2019_LAST_SEEN
                    , CAST(TGC_2019_BOUGHT_VC_LAST_DATE AS DATE)    TGC_2019_BOUGHT_VC_LAST_DATE

                    FROM sf_databricks_migration.sf_dbx.TGC2019_PARTIAL_SUMMARY

            UNION ALL 
            
                SELECT  'PGA23' , BRAND_FIRSTPARTYID
                    , SALTED_PUID   
                    , PUID

                    , COALESCE(PGA2K23_NEXT_GEN_PLATFORM, PGA2K23_CURRENT_GEN_PLATFORM)     PLATFORM
                    , PGA2K23_CURRENT_GEN_PLATFORM
                    , PGA2K23_NEXT_GEN_PLATFORM

                    , PGA2K23_COUNTRY_LAST
                    , PGA2K23_LAST_SEEN_DATE                        
                    , PGA2K23_BOUGHT_VC_LAST_DATE                   

                    FROM sf_databricks_migration.sf_dbx.PGA2K23_SUMMARY

            )

            , UNION_PIVOTED AS (
            
                SELECT DISTINCT  BRAND_FIRSTPARTYID, SALTED_PUID,  PUID

                    , FIRST_VALUE(PLATFORM) OVER(PARTITION BY PUID ORDER BY LAST_SEEN DESC)                               LAST_PLATFORM     
                    , FIRST_VALUE(CURRENT_GEN_PLATFORM) OVER(PARTITION BY PUID 
                        ORDER BY  NVL2(CURRENT_GEN_PLATFORM, 1, 0) ASC, LAST_SEEN DESC)                                  GEN8_PLATFORM
                    , FIRST_VALUE(NEXT_GEN_PLATFORM) OVER(PARTITION BY PUID 
                        ORDER BY NVL2(NEXT_GEN_PLATFORM, 1, 0) ASC, LAST_SEEN DESC)                                      GEN9_PLATFORM  

                    , FIRST_VALUE(LAST_COUNTRY) OVER(PARTITION BY PUID ORDER BY LAST_SEEN DESC)                           LAST_COUNTRY   

                    , MAX(LAST_SEEN) OVER(PARTITION BY PUID)                                                        LAST_SEEN
                    , MAX(BOUGHT_VC_LAST_DATE) OVER(PARTITION BY PUID)                                              BOUGHT_VC_LAST_DATE

                    , MAX(CASE WHEN TAG = 'PGA21' THEN LAST_SEEN ELSE NULL END) OVER(PARTITION BY PUID)             PGA2K21_LAST_SEEN_DATE
                    , MAX(CASE WHEN TAG = 'PGA21' THEN BOUGHT_VC_LAST_DATE ELSE NULL END) OVER(PARTITION BY PUID)   PGA2K21_BOUGHT_VC_LAST_DATE

                    , MAX(CASE WHEN TAG = 'TGC19' THEN LAST_SEEN ELSE NULL END) OVER(PARTITION BY PUID)             TCG2019_LAST_SEEN_DATE
                    , MAX(CASE WHEN TAG = 'TGC19' THEN BOUGHT_VC_LAST_DATE ELSE NULL END) OVER(PARTITION BY PUID)   TCG2019_BOUGHT_VC_LAST_DATE

                    , MAX(CASE WHEN TAG = 'PGA23' THEN LAST_SEEN ELSE NULL END) OVER(PARTITION BY PUID)             PGA2K23_LAST_SEEN_DATE
                    , MAX(CASE WHEN TAG = 'PGA23' THEN BOUGHT_VC_LAST_DATE ELSE NULL END) OVER(PARTITION BY PUID)   PGA2K23_BOUGHT_VC_LAST_DATE

                    , MAX(CASE WHEN TAG = 'PGA25' THEN LAST_SEEN ELSE NULL END) OVER(PARTITION BY PUID)             PGA2K25_LAST_SEEN_DATE
                    , MAX(CASE WHEN TAG = 'PGA25' THEN BOUGHT_VC_LAST_DATE ELSE NULL END) OVER(PARTITION BY PUID)   PGA2K25_BOUGHT_VC_LAST_DATE

                FROM  UNION_TITLES

            )

            , test_priority_prelim as (
                select puid, title, min(priority) as priority_filter 
                    from sf_databricks_migration.sf_dbx.cdp_testing_lookup 
                    where live = 1 and title = 'PGA'
                    group by 1,2
                        )

            , prioritized_test_group as (
                select ctl.puid,  test_name
                    from sf_databricks_migration.sf_dbx.cdp_testing_lookup ctl
                    inner join test_priority_prelim priors
                    on ctl.puid = priors.puid
                    and ctl.title = priors.title
                    and ctl.priority = priors.priority_filter
                    where ctl.live = 1 and ctl.title = 'PGA'
            )

            , fpids as (
                select
                    distinct le.accountid platform_id,
                    dataanalytics_prod.cdp_ng.fpid_decryption(le.firstpartyId) puid
                from
                    coretech_prod.sso.linkevent le
                    join reference{environment}.title.dim_title t on le.appId = t.app_id
                    and title ilike '%2K Portal%'
                    and le.firstpartyId is not null and le.firstpartyId not ilike '%@%'
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
                    and title ilike '%2K Portal%'
                    and le.firstpartyId is not null and le.firstpartyId not ilike '%@%'
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
            )

            , pga_cohorts as (
                select distinct puid,
                mode_cohort,
                monetization_cohort,
                combined_cohort,
                cluster
                from sf_databricks_migration.sf_dbx.pga_cohorts pga
                join fpids on pga.player_id = fpids.platform_id

            )


        SELECT 
        BRAND_FIRSTPARTYID                                                           brand_firstpartyid
            , SALTED_PUID                                                               salted_puid

            , U.PUID                                                                      puid

            , LAST_PLATFORM                                                             pga_platform_last
            , GEN8_PLATFORM                                                             pga_platform_gen8
            , GEN9_PLATFORM                                                             pga_platform_gen9

            , LAST_COUNTRY                                                              pga_country_last

            , LAST_SEEN                                                                 pga_last_seen_date
            , BOUGHT_VC_LAST_DATE                                                       pga_bought_vc_last_date

            , CAST(NULL AS  VARCHAR(255))                                               pga_player_type    

            , TCG2019_LAST_SEEN_DATE        IS NOT NULL                                 pga_has_played_TGC_2019
            , PGA2K21_LAST_SEEN_DATE        IS NOT NULL                                 pga_has_played_PGA2K21
            , PGA2K23_LAST_SEEN_DATE        IS NOT NULL                                 pga_has_played_PGA2K23
            , PGA2K25_LAST_SEEN_DATE        IS NOT NULL                                 pga_has_played_PGA2K25

            , TCG2019_BOUGHT_VC_LAST_DATE   IS NOT NULL                                 pga_has_converted_TGC_2019
            , PGA2K21_BOUGHT_VC_LAST_DATE   IS NOT NULL                                 pga_has_converted_PGA2K21
            , PGA2K23_BOUGHT_VC_LAST_DATE   IS NOT NULL                                 pga_has_converted_PGA2K23
            , PGA2K25_BOUGHT_VC_LAST_DATE   IS NOT NULL                                 pga_has_converted_PGA2K25

            , CAST(NULL AS  VARCHAR(255))                                               pga_player_type_TGC_2019 
            , CAST(NULL AS  VARCHAR(255))                                               pga_player_type_PGA2K21
            , CAST(NULL AS  VARCHAR(255))                                               pga_player_type_pga2k23
            , CAST(NULL AS  VARCHAR(255))                                               pga_player_type_pga2k25

            , pga.mode_cohort                                                           pga_custom_grouping_01		 
            , pga.monetization_cohort                                                   pga_custom_grouping_02		 
            , pga.combined_cohort                                                       pga_custom_grouping_03		 
            , testing.test_name                                                         pga_custom_grouping_04		 
            , cluster                                                                   pga_custom_grouping_05		 
            , (PGA2K23_LAST_SEEN_DATE IS NOT NULL and PGA2K25_LAST_SEEN_DATE IS NOT NULL) pga_custom_grouping_06		 
            , CAST(NULL AS  VARCHAR(255))                                               pga_custom_grouping_07		 
            , CAST(NULL AS  VARCHAR(255))                                               pga_custom_grouping_08		 
            , CAST(NULL AS  VARCHAR(255))                                               pga_custom_grouping_09		 
            , CAST(NULL AS  VARCHAR(255))                                               pga_custom_grouping_10	
            , CAST(NULL AS  VARCHAR(255))                                               pga_custom_grouping_11	
            , CAST(NULL AS  VARCHAR(255))                                               pga_custom_grouping_12	
            , CAST(NULL AS  VARCHAR(255))                                               pga_custom_grouping_13	
            , CAST(NULL AS  VARCHAR(255))                                               pga_custom_grouping_14	
            , CAST(NULL AS  VARCHAR(255))                                               pga_custom_grouping_15	
            , CAST(NULL AS  VARCHAR(255))                                               pga_custom_grouping_16	
            , CAST(NULL AS  VARCHAR(255))                                               pga_custom_grouping_17	
            , CAST(NULL AS  VARCHAR(255))                                               pga_custom_grouping_18	
            , CAST(NULL AS  VARCHAR(255))                                               pga_custom_grouping_19	
            , CAST(NULL AS  VARCHAR(255))                                               pga_custom_grouping_20	

            , CAST(NULL AS  DECIMAL(18,2))                                              pga_custom_value_01		 
            , CAST(NULL AS  DECIMAL(18,2))                                              pga_custom_value_02		 
            , CAST(NULL AS  DECIMAL(18,2))                                              pga_custom_value_03		 
            , CAST(NULL AS  DECIMAL(18,2))                                              pga_custom_value_04		 
            , CAST(NULL AS  DECIMAL(18,2))                                              pga_custom_value_05		 
            , CAST(NULL AS  DECIMAL(18,2))                                              pga_custom_value_06		 
            , CAST(NULL AS  DECIMAL(18,2))                                              pga_custom_value_07		 
            , CAST(NULL AS  DECIMAL(18,2))                                              pga_custom_value_08		 
            , CAST(NULL AS  DECIMAL(18,2))                                              pga_custom_value_09		 
            , CAST(NULL AS  DECIMAL(18,2))                                              pga_custom_value_10

        FROM UNION_PIVOTED U

        LEFT JOIN PRIORITIZED_TEST_GROUP TESTING ON U.PUID = TESTING.PUID
        LEFT JOIN PGA_COHORTS PGA ON U.PUID = PGA.PUID

    """
    # Execute the SQL query and return the result as a DataFrame
    df = spark.sql(sql_query_events)
    return df

# COMMAND ----------

def create_pga2k25_franchise_table(environment, spark):
    sql = f"""
    CREATE TABLE IF NOT EXISTS dataanalytics{environment}.cdp_ng.franchise_pga (
        brand_firstpartyid STRING,
        salted_puid STRING,
        puid STRING,
        pga_platform_last STRING,
        pga_platform_gen8 STRING,
        pga_platform_gen9 STRING,
        pga_country_last STRING,
        pga_last_seen_date DATE,
        pga_bought_vc_last_date DATE,
        pga_has_played_TGC_2019 BOOLEAN,
        pga_has_played_PGA2K21 BOOLEAN,
        pga_has_played_PGA2K23 BOOLEAN,
        pga_has_played_PGA2K25 BOOLEAN,
        pga_has_converted_TGC_2019 BOOLEAN,
        pga_has_converted_PGA2K21 BOOLEAN,
        pga_has_converted_PGA2K23 BOOLEAN,
        pga_has_converted_PGA2K25 BOOLEAN,
        pga_player_type_TGC_2019 STRING,
        pga_player_type_PGA2K21 STRING,
        pga_player_type_pga2k23 STRING,
        pga_player_type_pga2k25 STRING,
        pga_custom_grouping_01 STRING,
        pga_custom_grouping_02 STRING,
        pga_custom_grouping_03 STRING,
        pga_custom_grouping_04 STRING,
        pga_custom_grouping_05 STRING,
        pga_custom_grouping_06 STRING,
        pga_custom_grouping_07 STRING,
        pga_custom_grouping_08 STRING,
        pga_custom_grouping_09 STRING,
        pga_custom_grouping_10 STRING,
        pga_custom_grouping_11 STRING,
        pga_custom_grouping_12 STRING,
        pga_custom_grouping_13 STRING,
        pga_custom_grouping_14 STRING,
        pga_custom_grouping_15 STRING,
        pga_custom_grouping_16 STRING,
        pga_custom_grouping_17 STRING,
        pga_custom_grouping_18 STRING,
        pga_custom_grouping_19 STRING,
        pga_custom_grouping_20 STRING,
        pga_custom_value_01 DECIMAL(18,2),
        pga_custom_value_02 DECIMAL(18,2),
        pga_custom_value_03 DECIMAL(18,2),
        pga_custom_value_04 DECIMAL(18,2),
        pga_custom_value_05 DECIMAL(18,2),
        pga_custom_value_06 DECIMAL(18,2),
        pga_custom_value_07 DECIMAL(18,2),
        pga_custom_value_08 DECIMAL(18,2),
        pga_custom_value_09 DECIMAL(18,2),
        pga_custom_value_10 DECIMAL(18,2)
    )
    """

    properties = {
        "delta.feature.allowColumnDefaults": "supported"  # this won't work until iceberg v3 is released
    }

    create_table(spark, sql, properties)
    return (
        f"dbfs:/tmp/dataanalytics/cdp_ng/streaming/run{environment}/franchise_pga"
    )

# COMMAND ----------

def run_batch():
    # Parse the environment argument
    input_param = dbutils_input_params()
    environment = input_param.get('environment', set_environment())

    # Create a Spark session
    spark = SparkSession.builder.appName("Hydra").getOrCreate()
    
    # Create the view
    df = create_pga2k25_franchise_view(environment, spark)
    checkpoint_location = create_pga2k25_franchise_table(environment ,spark)

    spark.sql(f"""select * from dataanalytics{environment}.cdp_ng.vw_pga_franchise""").show()

# COMMAND ----------

if __name__ == "__main__":
    run_batch()