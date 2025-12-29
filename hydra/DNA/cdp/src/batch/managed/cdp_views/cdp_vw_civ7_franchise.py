# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------
# MAGIC %run ../../../../../../utils/ddl/table_functions

# COMMAND ----------

def create_civ7_franchise_view(environment, spark):
    # Define the SQL query based on the environment
    sql_query_events = f"""
    CREATE view if not exists dataanalytics{environment}.cdp_ng.vw_civ7_franchise AS
    WITH 
        CC_LOOKUP AS (
            SELECT COUNTRY_CODE, NAME
            FROM REFERENCE{environment}.LOCATION.dim_country WHERE NAME IN (
                'Austria',
                'Czech Republic',
                'Germany',
                'Poland',
                'Romania',
                'Belgium',
                'Croatia',
                'Cyprus',
                'Estonia',
                'Greece',
                'Hungary',
                'Iceland',
                'Italy',
                'Latvia',
                'Lithuania',
                'Luxembourg',
                'Malta',
                'Slovakia',
                'Slovenia' )
        ),

        VENDORS AS (
            SELECT DISTINCT DISPLAY_PLATFORM AS PLATFORM, TITLE 
            FROM REFERENCE{environment}.TITLE.dim_title
            WHERE TITLE ilike '%Civilization VII%'
            -- AND DISPLAY_PLATFORM IN ('NSW', 'PS4', 'XB1', 'iOS', 'Android', 'Linux', 'PC', 'OSX')
        ),

        CIV_UNION AS (
            SELECT  
                  BRAND_FIRSTPARTYID
                , SALTED_PUID
                , PUID
                , CIV_CV5_PLATFORM              LAST_PLATFORM              
                , CIV_CV5_PLATFORM              CURRENT_GEN_PLATFORM
                , NULL                          NEXT_GEN_PLATFORM 
                , CIV_CV5_COUNTRY_LAST          LAST_COUNTRY               
                , NULL                          BOUGHT_VC_LAST_DATE         
                , CIV_CV5_LAST_SEEN_DATE        LAST_SEEN_DATE      
                , 'CV5'                         TAG
            FROM  sf_databricks_migration.sf_dbx.civ_cv5_summary
            UNION ALL
            SELECT                
                  BRAND_FIRSTPARTYID
                , SALTED_PUID
                , PUID
                , CIV_CV6_PLATFORM              
                , CIV_CV6_PLATFORM              CURRENT_GEN_PLATFORM
                , NULL                          NEXT_GEN_PLATFORM 
                , CIV_CV6_COUNTRY_LAST                 
                , CIV_CV6_BOUGHT_VC_LAST_DATE         
                , CIV_CV6_LAST_SEEN_DATE
                , 'CV6'                         TAG
            FROM  sf_databricks_migration.sf_dbx.civ_cv6_summary
            UNION ALL
            SELECT  
                  BRAND_FIRSTPARTYID
                , SALTED_PUID
                , PUID
                , civ_civ7_current_gen_platform
                , civ_civ7_current_gen_platform CURRENT_GEN_PLATFORM
                , civ_civ7_next_gen_platform    NEXT_GEN_PLATFORM 
                , civ_civ7_country_last                 
                , civ_civ7_bought_vc_last_date         
                , civ_civ7_last_seen_date 
                , 'CV7'                         TAG
            FROM  dataanalytics{environment}.cdp_ng.civ7_summary
        ), 

        CIV_UNION_PIVOTED AS (
            SELECT DISTINCT  
                  CIV_UNION.BRAND_FIRSTPARTYID
                , CIV_UNION.SALTED_PUID
                , CIV_UNION.PUID
                , CASE 
                    WHEN CV7.civ_civ7_last_seen_date = CV6.CIV_CV6_LAST_SEEN_DATE 
                        AND CV7.civ_civ7_country_last IN (SELECT COUNTRY_CODE FROM CC_LOOKUP) THEN CV7.civ_civ7_country_last
                    WHEN CV5.CIV_CV5_LAST_SEEN_DATE = CV6.CIV_CV6_LAST_SEEN_DATE 
                        AND CV6.CIV_CV6_COUNTRY_LAST IN (SELECT COUNTRY_CODE FROM CC_LOOKUP) THEN CV6.CIV_CV6_COUNTRY_LAST 
                    WHEN CV5.CIV_CV5_LAST_SEEN_DATE = CV6.CIV_CV6_LAST_SEEN_DATE 
                        AND CV6.CIV_CV6_COUNTRY_LAST NOT IN (SELECT COUNTRY_CODE FROM CC_LOOKUP)
                        AND CV5.CIV_CV5_COUNTRY_LAST NOT IN (SELECT COUNTRY_CODE FROM CC_LOOKUP) THEN CV5.CIV_CV5_COUNTRY_LAST
                    ELSE LAST_VALUE(CIV_UNION.LAST_COUNTRY) 
                        OVER (PARTITION BY CIV_UNION.PUID ORDER BY CIV_UNION.LAST_SEEN_DATE)
                  END AS LAST_COUNTRY
                , CASE
                    WHEN CV5.CIV_CV5_LAST_SEEN_DATE = CV6.CIV_CV6_LAST_SEEN_DATE THEN CV6.CIV_CV6_PLATFORM
                    WHEN CV6.CIV_CV6_LAST_SEEN_DATE = CV7.civ_civ7_last_seen_date THEN CV7.civ_civ7_current_gen_platform
                    ELSE LAST_VALUE(CIV_UNION.LAST_PLATFORM) 
                        OVER (PARTITION BY CIV_UNION.PUID ORDER BY CIV_UNION.LAST_SEEN_DATE)
                  END AS LAST_PLATFORM 
                , MAX(BOUGHT_VC_LAST_DATE) OVER (PARTITION BY CIV_UNION.PUID) AS BOUGHT_VC_LAST_DATE
                , MAX(LAST_SEEN_DATE) OVER (PARTITION BY CIV_UNION.PUID) AS LAST_SEEN_DATE
                , SUM(CASE WHEN TAG='CV5' THEN 1 ELSE 0 END) OVER (PARTITION BY CIV_UNION.PUID) AS HAS_PLAYED_CV5
                , SUM(CASE WHEN TAG='CV6' THEN 1 ELSE 0 END) OVER (PARTITION BY CIV_UNION.PUID) AS HAS_PLAYED_CV6
                , SUM(CASE WHEN TAG='CV7' THEN 1 ELSE 0 END) OVER (PARTITION BY CIV_UNION.PUID) AS HAS_PLAYED_CV7
                , MAX(CASE WHEN TAG='CV5' THEN BOUGHT_VC_LAST_DATE ELSE NULL END) OVER (PARTITION BY CIV_UNION.PUID) AS BOUGHT_VC_LAST_DATE_CV5
                , MAX(CASE WHEN TAG='CV6' THEN BOUGHT_VC_LAST_DATE ELSE NULL END) OVER (PARTITION BY CIV_UNION.PUID) AS BOUGHT_VC_LAST_DATE_CV6
                , MAX(CASE WHEN TAG='CV7' THEN BOUGHT_VC_LAST_DATE ELSE NULL END) OVER (PARTITION BY CIV_UNION.PUID) AS BOUGHT_VC_LAST_DATE_CV7
            FROM CIV_UNION
              LEFT JOIN sf_databricks_migration.sf_dbx.civ_cv5_summary CV5 
                ON CV5.PUID = CIV_UNION.PUID
              LEFT JOIN  sf_databricks_migration.sf_dbx.civ_cv6_summary CV6 
                ON CV6.PUID = CIV_UNION.PUID
              LEFT JOIN  dataanalytics{environment}.cdp_ng.civ7_summary CV7
                ON CV7.PUID = CIV_UNION.PUID
        )

    SELECT 
          BRAND_FIRSTPARTYID as brand_firstpartyid
        , SALTED_PUID as salted_puid
        , PUID AS puid
        , LAST_PLATFORM AS civ_platform_last
        , IFNULL(LAST_COUNTRY, 'ZZ') AS civ_country_last
        , CAST(NULL AS VARCHAR(100)) AS civ_player_type 
        , LAST_SEEN_DATE AS civ_last_seen_date
        , BOUGHT_VC_LAST_DATE AS civ_bought_vc_last_date
        , HAS_PLAYED_CV7 = 1 AS civ_has_played_cv7
        , HAS_PLAYED_CV6 = 1 AS civ_has_played_cv6
        , HAS_PLAYED_CV5 = 1 AS civ_has_played_cv5
        , CAST(NULL AS BOOLEAN) AS civ_has_played_cv4
        , CAST(NULL AS BOOLEAN) AS civ_has_played_cv3
        , CAST(NULL AS BOOLEAN) AS civ_has_played_cv2
        , CAST(NULL AS BOOLEAN) AS civ_has_played_cv1
        , CAST(NULL AS BOOLEAN) AS civ_has_played_civ_rev
        , CAST(NULL AS BOOLEAN) AS civ_has_played_civ_rev2
        , CAST(NULL AS BOOLEAN) AS civ_has_played_civ_beyond
        , CAST(NULL AS BOOLEAN) AS civ_has_played_civ_eras_allies
        , IFF(BOUGHT_VC_LAST_DATE_CV7 IS NULL, NULL, TRUE) AS civ_has_converted_cv7
        , IFF(BOUGHT_VC_LAST_DATE_CV6 IS NULL, NULL, TRUE) AS civ_has_converted_cv6
        , IFF(BOUGHT_VC_LAST_DATE_CV5 IS NULL, NULL, TRUE) AS civ_has_converted_cv5
        , CAST(NULL AS BOOLEAN) AS civ_has_converted_cv4
        , CAST(NULL AS BOOLEAN) AS civ_has_converted_cv3
        , CAST(NULL AS BOOLEAN) AS civ_has_converted_cv2
        , CAST(NULL AS BOOLEAN) AS civ_has_converted_cv1
        , CAST(NULL AS BOOLEAN) AS civ_has_converted_civ_rev
        , CAST(NULL AS BOOLEAN) AS civ_has_converted_civ_rev2
        , CAST(NULL AS BOOLEAN) AS civ_has_converted_civ_beyond
        , CAST(NULL AS BOOLEAN) AS civ_has_converted_civ_eras_allies
        , CAST(NULL AS VARCHAR(100)) AS civ_player_type_cv7
        , CAST(NULL AS VARCHAR(100)) AS civ_player_type_cv6
        , CAST(NULL AS VARCHAR(100)) AS civ_player_type_cv5
        , CAST(NULL AS VARCHAR(100)) AS civ_player_type_cv4
        , CAST(NULL AS VARCHAR(100)) AS civ_player_type_cv3
        , CAST(NULL AS VARCHAR(100)) AS civ_player_type_cv2
        , CAST(NULL AS VARCHAR(100)) AS civ_player_type_cv1
        , CAST(NULL AS VARCHAR(100)) AS civ_player_type_civ_beyond
        , CAST(NULL AS VARCHAR(100)) AS civ_player_type_civ_eras_allies
        , CAST(NULL AS VARCHAR(100)) AS civ_custom_grouping_01
        , CAST(NULL AS VARCHAR(100)) AS civ_custom_grouping_02
        , CAST(NULL AS VARCHAR(100)) AS civ_custom_grouping_03
        , CAST(NULL AS VARCHAR(100)) AS civ_custom_grouping_04
        , CAST(NULL AS VARCHAR(100)) AS civ_custom_grouping_05
        , CAST(NULL AS VARCHAR(100)) AS civ_custom_grouping_06
        , CAST(NULL AS VARCHAR(100)) AS civ_custom_grouping_07
        , CAST(NULL AS VARCHAR(100)) AS civ_custom_grouping_08
        , CAST(NULL AS VARCHAR(100)) AS civ_custom_grouping_09
        , CAST(NULL AS VARCHAR(100)) AS civ_custom_grouping_10
        , CAST(NULL AS DECIMAL(18,2)) AS civ_custom_value_01
        , CAST(NULL AS DECIMAL(18,2)) AS civ_custom_value_02
        , CAST(NULL AS DECIMAL(18,2)) AS civ_custom_value_03
        , CAST(NULL AS DECIMAL(18,2)) AS civ_custom_value_04
        , CAST(NULL AS DECIMAL(18,2)) AS civ_custom_value_05
        , CAST(NULL AS DECIMAL(18,2)) AS civ_custom_value_06
        , CAST(NULL AS DECIMAL(18,2)) AS civ_custom_value_07
        , CAST(NULL AS DECIMAL(18,2)) AS civ_custom_value_08
        , CAST(NULL AS DECIMAL(18,2)) AS civ_custom_value_09
        , CAST(NULL AS DECIMAL(18,2)) AS civ_custom_value_10
    FROM CIV_UNION_PIVOTED 
        JOIN VENDORS V
        ON LAST_PLATFORM = V.PLATFORM
    """

    # Execute the SQL query and return the result as a DataFrame
    df = spark.sql(sql_query_events)
    return df

# COMMAND ----------


def create_civ7_franchise_table(environment, spark):
    sql = f"""
    CREATE TABLE IF NOT EXISTS dataanalytics{environment}.cdp_ng.civ7_franchise (
    brand_firstpartyid STRING,
    salted_puid STRING,
    puid STRING,
    civ_platform_last STRING,
    civ_country_last STRING,
    civ_player_type STRING,
    civ_last_seen_date TIMESTAMP,
    civ_bought_vc_last_date TIMESTAMP,
    civ_has_played_cv7 BOOLEAN,
    civ_has_played_cv6 BOOLEAN,
    civ_has_played_cv5 BOOLEAN,
    civ_has_played_cv4 BOOLEAN,
    civ_has_played_cv3 BOOLEAN,
    civ_has_played_cv2 BOOLEAN,
    civ_has_played_cv1 BOOLEAN,
    civ_has_played_civ_rev BOOLEAN,
    civ_has_played_civ_rev2 BOOLEAN,
    civ_has_played_civ_beyond BOOLEAN,
    civ_has_played_civ_eras_allies BOOLEAN,
    civ_has_converted_cv7 BOOLEAN,
    civ_has_converted_cv6 BOOLEAN,
    civ_has_converted_cv5 BOOLEAN,
    civ_has_converted_cv4 BOOLEAN,
    civ_has_converted_cv3 BOOLEAN,
    civ_has_converted_cv2 BOOLEAN,
    civ_has_converted_cv1 BOOLEAN,
    civ_has_converted_civ_rev BOOLEAN,
    civ_has_converted_civ_rev2 BOOLEAN,
    civ_has_converted_civ_beyond BOOLEAN,
    civ_has_converted_civ_eras_allies BOOLEAN,
    civ_player_type_cv7 STRING,
    civ_player_type_cv6 STRING,
    civ_player_type_cv5 STRING,
    civ_player_type_cv4 STRING,
    civ_player_type_cv3 STRING,
    civ_player_type_cv2 STRING,
    civ_player_type_cv1 STRING,
    civ_player_type_civ_beyond STRING,
    civ_player_type_civ_eras_allies STRING,
    civ_custom_grouping_01 STRING,
    civ_custom_grouping_02 STRING,
    civ_custom_grouping_03 STRING,
    civ_custom_grouping_04 STRING,
    civ_custom_grouping_05 STRING,
    civ_custom_grouping_06 STRING,
    civ_custom_grouping_07 STRING,
    civ_custom_grouping_08 STRING,
    civ_custom_grouping_09 STRING,
    civ_custom_grouping_10 STRING,
    civ_custom_value_01 DECIMAL(18,2),
    civ_custom_value_02 DECIMAL(18,2),
    civ_custom_value_03 DECIMAL(18,2),
    civ_custom_value_04 DECIMAL(18,2),
    civ_custom_value_05 DECIMAL(18,2),
    civ_custom_value_06 DECIMAL(18,2),
    civ_custom_value_07 DECIMAL(18,2),
    civ_custom_value_08 DECIMAL(18,2),
    civ_custom_value_09 DECIMAL(18,2),
    civ_custom_value_10 DECIMAL(18,2)
    )
    """

    properties = {
        "delta.feature.allowColumnDefaults": "supported"  # this won't work until iceberg v3 is released
    }

    create_table(spark, sql, properties)

def run_batch():
    # Parse the environment argument
    input_param = dbutils_input_params()
    environment = input_param.get('environment', set_environment())
    spark = create_spark_session()
    
    # Create the view
    df = create_civ7_franchise_view(environment, spark)
    #create delta table
    create_civ7_franchise_table(environment ,spark)


# COMMAND ----------

if __name__ == "__main__":
    run_batch()
