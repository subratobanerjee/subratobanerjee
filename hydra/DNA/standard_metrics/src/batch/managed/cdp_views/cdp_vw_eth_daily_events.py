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
    create or replace view dataanalytics{environment}.cdp_ng.vw_eth_daily_events(
      event_date,
      brand_firstpartyid,
      salted_puid,
      puid,
      parent_account_id,
      vendor,
      game_title,
      sub_game,
      event_name,
      event_action,
      event_result,
      event_description,
      cdp_field_name
    ) 
    as
    with summary_view as (
        select eth_install_date
        , eth_last_seen_date
        , brand_firstpartyid
        , salted_puid
        , puid
        , parent_account_id
        , platform_account_id
        , eth_vendor
        from dataanalytics{environment}.cdp_ng.vw_eth_summary
      ),

      all_events as (   
          select distinct eth_install_date       event_date 
            , parent_account_id
            , platform_account_id
            , puid                      
            , eth_vendor                vendor  
            , 'eth'                     game_title 
            , 'total'                   sub_game
            , 'install'                 event_name
            , cast(null as varchar(100)) event_action 
            , 1                         event_result

          from summary_view

          union all 

          select distinct date -- this event is not required
            , sv.parent_account_id
            , platform_account_id 
            , sv.puid                   puid                                    
            , eth_vendor              
            , 'eth'                    
            , 'total'                   
            , 'reactivation'            event_name   
            ,  cast(null as varchar(100)) event_action 

            , 1                         event_result

          from horizon{environment}.managed.fact_player_match_summary s
            join summary_view sv on lower(s.full_account_id) = lower(sv.parent_account_id) 
            and lower(s.player_id) = lower(sv.platform_account_id)
            -- group by date, s.player_id, eth_vendor
            qualify datediff(day, lag(date, 1) over (partition by s.player_id order by date), date)  >= 14
        )

        select event_date                                                     
            , brand_firstpartyid
            , salted_puid
            , al.puid
            , al.parent_account_id
            , vendor
            , game_title
            , sub_game
            , event_name
            , event_action
            , cast(cast(event_result as decimal(20, 2)) as varchar(100))      event_result
            , cast(null as varchar(100))                                      event_description
            , dataanalytics{environment}.cdp_ng.mk_cdp_field2('eth' 
                    , sub_game , event_name, event_action)                    cdp_field_name
          from all_events al
          join summary_view using(parent_account_id, platform_account_id)
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
    spark.sql(f"""select * from dataanalytics{environment}.cdp_ng.vw_eth_daily_events""").show()

# COMMAND ----------

if __name__ == "__main__":
    run_batch()
