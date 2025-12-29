from pyspark.sql import SparkSession
import argparse

# COMMAND ----------

def arg_parser():
    parser = argparse.ArgumentParser(description="Script to consume parameters for Databricks job")
    parser.add_argument("--environment", type=str, required=True, help="Environment name, e.g. _stg or _prod")

    args = parser.parse_args()
    environment = args.environment
    return environment

# COMMAND ----------

def create(environment, spark):
    # Define the SQL query based on the environment
    sql_query_events = f"""
    create or replace view dataanalytics{environment}.cdp_ng.vw_eth_franchise(
        parent_account_id,
        brand_firstpartyid,
        salted_puid,
        puid,
        eth_platform_last,
        eth_country_last,
        eth_player_type,
        eth_last_seen_date,
        eth_bought_vc_last_date,
        eth_has_played_et1,
        eth_has_converted_et1,
        eth_player_type_et1,
        eth_custom_grouping_01,
        eth_custom_grouping_02,
        eth_custom_grouping_03,
        eth_custom_grouping_04,
        eth_custom_grouping_05,
        eth_custom_grouping_06,
        eth_custom_grouping_07,
        eth_custom_grouping_08,
        eth_custom_grouping_09,
        eth_custom_grouping_10,
        eth_custom_grouping_11,
        eth_custom_grouping_12,
        eth_custom_grouping_13,
        eth_custom_grouping_14,
        eth_custom_grouping_15,
        eth_custom_grouping_16,
        eth_custom_grouping_17,
        eth_custom_grouping_18,
        eth_custom_grouping_19,
        eth_custom_grouping_20,
        eth_custom_value_01,
        eth_custom_value_02,
        eth_custom_value_03,
        eth_custom_value_04,
        eth_custom_value_05,
        eth_custom_value_06,
        eth_custom_value_07,
        eth_custom_value_08,
        eth_custom_value_09,
        eth_custom_value_10
        )
        as 
        with 
                vendors as (
                    select  platform, vendor 
                    from reference_customer.platform.platform
                    where   src_platform in ('PS5', 'XBSX', 'PC') -- check platforms for ETH
                ),

                player_union as (
                    select parent_account_id
                        , puid
                        , brand_firstpartyid
                        , salted_puid 
                        , coalesce(eth_next_gen_platform, eth_current_gen_platform)     platform
                        , eth_current_gen_platform                  current_gen_platform  
                        , eth_next_gen_platform                     next_gen_platform    
                        , eth_country_last                          last_country
                        , eth_last_seen_date                        last_seen 
                        , eth_bought_vc_last_date                   bought_vc_last_date  
                        , 1                                         eth_tx
                        , iff(eth_bought_vc_converted, 1, 0)        eth_vc 
                        from  dataanalytics{environment}.cdp_ng.vw_eth_summary

                ), 

                play_union_pivoted as (
                    select distinct parent_account_id
                        , puid
                        , brand_firstpartyid
                        , salted_puid
                        , last_value(platform) over(partition by parent_account_id, puid order by last_seen)       platform  
                        , last_value(last_country) over(partition by parent_account_id, puid order by last_seen)   last_country    
                        , max(last_seen) over(partition by parent_account_id, puid)                                last_seen
                        , max(bought_vc_last_date) over (partition by parent_account_id, puid)                     bought_vc_last_date
                        , sum(eth_tx) over(partition by parent_account_id, puid)                                   eth_tx
                        , sum(eth_vc) over(partition by parent_account_id, puid)                                   eth_vc

                    from  player_union

                ) 

            select  
                p.parent_account_id                                                              parent_account_id
                , p.brand_firstpartyid                                                           brand_firstpartyid
                , p.salted_puid                                                                  salted_puid
                , p.puid                                                                         puid
                , p.platform                                                                     eth_platform_last
                , last_country                                                                   eth_country_last
                , cast(null as  varchar(100))                                                    eth_player_type    
                , cast(last_seen  as date)                                                       eth_last_seen_date    
                , cast(bought_vc_last_date as date)                                              eth_bought_vc_last_date    
                , eth_tx > 0                                                                     eth_has_played_et1
                , eth_vc > 0                                                                     eth_has_converted_et1
                , cast(null as  varchar(100))                                                    eth_player_type_et1
                , cast(null as  varchar(100))                                                    eth_custom_grouping_01
                , cast(null as  varchar(100))                                                    eth_custom_grouping_02
                , cast(null as varchar(100))                                                     eth_custom_grouping_03		 
                , cast(null as  varchar(100))                                                    eth_custom_grouping_04		 
                , cast(null as  varchar(100))                                                    eth_custom_grouping_05		 
                , cast(null as  varchar(100))                                                    eth_custom_grouping_06		 
                , cast(null as  varchar(100))                                                    eth_custom_grouping_07		 
                , cast(null as  varchar(100))                                                    eth_custom_grouping_08		 
                , cast(null as  varchar(100))                                                    eth_custom_grouping_09		 
                , cast(null as  varchar(100))                                                    eth_custom_grouping_10	
                , cast(null as  varchar(100))                                                    eth_custom_grouping_11
                , cast(null as  varchar(100))                                                    eth_custom_grouping_12	
                , cast(null as  varchar(100))                                                    eth_custom_grouping_13	
                , cast(null as  varchar(100))                                                    eth_custom_grouping_14	
                , cast(null as  varchar(100))                                                    eth_custom_grouping_15	
                , cast(null as  varchar(100))                                                    eth_custom_grouping_16	
                , cast(null as  varchar(100))                                                    eth_custom_grouping_17	
                , cast(null as  varchar(100))                                                    eth_custom_grouping_18	
                , cast(null as  varchar(100))                                                    eth_custom_grouping_19	
                , cast(null as  varchar(100))                                                    eth_custom_grouping_20

                , cast(null as  decimal(18,2))                                              eth_custom_value_01		 
                , cast(null as  decimal(18,2))                                              eth_custom_value_02		 
                , cast(null as  decimal(18,2))                                              eth_custom_value_03		 
                , cast(null as  decimal(18,2))                                              eth_custom_value_04		 
                , cast(null as  decimal(18,2))                                              eth_custom_value_05		 
                , cast(null as  decimal(18,2))                                              eth_custom_value_06		 
                , cast(null as  decimal(18,2))                                              eth_custom_value_07		 
                , cast(null as  decimal(18,2))                                              eth_custom_value_08		 
                , cast(null as  decimal(18,2))                                              eth_custom_value_09		 
                , cast(null as  decimal(18,2))                                              eth_custom_value_10

            from play_union_pivoted p
            join vendors v  on p.platform = v.platform
    """
    # Execute the SQL query and return the result as a DataFrame
    df = spark.sql(sql_query_events)
    return df

# COMMAND ----------

def run_batch():
    # Parse the environment argument
    environment = arg_parser()
    
    # Create a Spark session
    spark = SparkSession.builder.appName("Hydra").getOrCreate()
    
    # Create the view
    df = create(environment, spark)
    spark.sql(f"""select * from dataanalytics{environment}.cdp_ng.vw_eth_franchise""").show()

# COMMAND ----------

if __name__ == "__main__":
    run_batch()
