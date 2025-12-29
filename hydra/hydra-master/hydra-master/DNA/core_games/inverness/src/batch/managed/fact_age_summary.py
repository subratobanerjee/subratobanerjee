# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/table_functions

# COMMAND ----------

from pyspark.sql.functions import collect_set
from pyspark.sql.functions import array_distinct
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    when,
    col,
    first_value,
    last_value,
    row_number,
    from_json,
    size,
    array_distinct,
    expr,
    sha2,
    concat_ws,
    desc,
    asc,
    explode,
    sum
)
from delta.tables import DeltaTable


# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'inverness'
spark = create_spark_session()
key_cols = ["campaign_instance_id", "player_id", "age", "leader","player_slot","player_type"]

# COMMAND ----------

def read_fpca_table():
    """ Read the table and remove all the nulls from key columns"""

    prev_max = max_timestamp(spark, f"{database}{environment}.managed.fact_age_summary", 'dw_insert_ts')

    current_min = (
        spark.read
        .table(f"{database}{environment}.intermediate.fact_player_campaign_activity")
        .where(expr("dw_insert_ts::date between current_date - 3 and current_date"))
        .select(expr(f"ifnull(min(received_on::timestamp::date),'1999-01-01')::date as min_date"))
    ).collect()[0]['min_date']

    inc_min_date = min(prev_max,current_min)


    activity_df =  spark.read.table(f"{database}{environment}.intermediate.fact_player_campaign_activity")\
                .filter(" AND ".join([f"{col} IS NOT NULL" for col in key_cols]))\
                .where(expr("player_type='HUMAN'"))

    fpca_df=activity_df.where(expr(f"received_on::date >= to_date('{inc_min_date}')"))

    
    return fpca_df,activity_df

def read_css_table():

    css_df = spark.read.table(f"{database}{environment}.raw.campaignsetupstatus")\
            .where(expr("campaignSetupEvent = 'Complete'"))\
            .select("campaignsetupinstanceid", "campaignsetupevent", "gametype","liveevent")\
            .distinct()
    return css_df

def read_lobby_table():

    lobby_df = (
        spark.read.table(f"{database}{environment}.raw.lobbystatus")
        .select(
            col("campaigninstanceid"), 
            col("playerpublicid"), 
            col("lobbyinstanceid"),
            from_json(col("crossplayfriendrelations"), 'ARRAY<STRUCT<is_crossplay_friend: BIGINT, service: STRING>>').alias("crossplay_json")
        )
    )

    lbs_df = (
        lobby_df
        .withColumn("crossplay_info", explode(col("crossplay_json"))) 
        .select(
            col("campaigninstanceid"),
            col("playerpublicid"),
            col("lobbyinstanceid"),
            col("crossplay_info.is_crossplay_friend").alias("is_crossplay_friend"),
            col("crossplay_info.service").alias("service")
        )
        .groupBy("campaigninstanceid", "playerpublicid", "lobbyinstanceid")
        .agg(
            collect_set("service").alias("services_list"),  
            expr("max(is_crossplay_friend) as crossplay_friends")
        )
        .withColumn("num_crossplay_campaigns", size(array_distinct(col("services_list"))))
    )
    return lbs_df

def extract_data():
    fpca_df,activity_df = read_fpca_table()
    fpca_df=fpca_df.alias("pca")
    css_df = read_css_table().alias("css")
    lbs_df= read_lobby_table().alias("lbs")

    joined_df = (
        fpca_df
        .join(css_df, on=expr("pca.campaign_setup_instance_id = css.campaignsetupinstanceid"), how="left")
        .join(lbs_df, on=expr("pca.player_id=lbs.playerpublicid and pca.campaign_instance_id = lbs.campaigninstanceid and pca.lobby_instance_id=lbs.lobbyinstanceid"), how="left")
        .select("pca.age",
                "pca.leader",
                "civilization",
                "player_id",
                "player_type",
                "player_slot",
                "received_on",
                "campaign_instance_id",
                "event_trigger",
                "gametype",
                "milestone_type",
                "policy_cards",
                "government_selected",
                "victory_type",
                "production_item",
                "campaign_exit_type",
                "commander_action",
                "commander_id",
                "source_table",
                "diplomacy_action_instance_id",
                "world_event_name",
                "application_session_instance_id",
                "attacks_made",
                "turn_number",
                "campaignsetupevent",
                "lbs.crossplay_friends",
                "lbs.num_crossplay_campaigns",
                "culture_per_turn",
                "food_per_turn",
                "gold_per_turn",
                "happiness_per_turn",
                "influence_per_turn",
                "production_per_turn",
                "science_per_turn",
                "liveevent",
                "node_id"
                )
    )

    return joined_df,activity_df

# COMMAND ----------

def transform_data(joined_df,activity_df):
    # Define the window specifications
    window_spec = Window.partitionBy("campaign_instance_id", "player_id").orderBy(
        "received_on"
    )

    window_spec_full = Window.partitionBy(
        "pca.campaign_instance_id", "pca.player_id", "pca.age", "pca.leader",
    ).orderBy("received_on")

    window_spec_yield = Window.partitionBy(
        "pca.campaign_instance_id", "pca.player_id", "pca.age", "pca.leader"
    ).orderBy(desc("received_on"))


    age_num_df = (
        joined_df
        .groupBy(
            "campaign_instance_id",
            "player_id",
            "age"
        )
        .agg(
            expr("min(received_on) as min_ts")
        )
        .select(
            "campaign_instance_id",
            "player_id",
            "age",
            expr("row_number() over (partition by campaign_instance_id, player_id order by min_ts) as age_num")
        )
        .groupBy(
            "campaign_instance_id",
            "player_id"
        )
        .agg
        (
            expr("max(age_num) as age_num"),
        )
    )

    #application_session_instance_id
    session_instance_id = (
        activity_df
        .groupBy(
            "campaign_instance_id",
            "player_id",
            "age",
            "player_type",
            "player_slot",
            "leader"
        )
        .agg(
            expr("count(distinct application_session_instance_id) as num_sessions_played")
        )
    )

    #num_days_played
    num_days_played = (
        activity_df
        .groupBy(
            "campaign_instance_id",
            "player_id",
            "age",
            "player_type",
            "player_slot",
            "leader"
        )
        .agg(
            expr("count(distinct to_date(received_on)) as num_days_played")
        )
    )

    #num_minutes_played
    minutes_played_df = (
        activity_df
        .groupBy(
            "campaign_instance_id",
            "player_id",
            "age",
            "application_session_instance_id",
            "player_type",
            "player_slot",
            "leader"
        )
        .agg(
            expr("timestampdiff(MINUTE, min(received_on), max(received_on)) as num_minutes_played")
        )
    )

    minutes_played = minutes_played_df.groupBy(*key_cols).agg(expr("SUM(num_minutes_played)").alias("num_minutes_played"))
        

    # Apply window functions
    window_df = (
        joined_df.alias("pca")
        .join(age_num_df.alias("age_n"), 
              on=expr("age_n.player_id = pca.player_id and age_n.campaign_instance_id = pca.campaign_instance_id"), 
              how="left")
        .withColumns(
            {
                "government_selected": first_value(when(col("event_trigger") == "Government Selected",col("government_selected"),),True,).over(window_spec_full),
                "ending_yield_culture": first_value(expr("culture_per_turn::float"), True).over(window_spec_yield),
                "ending_yield_diplomacy": first_value(expr("influence_per_turn::float"), True).over(window_spec_yield),
                "ending_yield_food": first_value(expr("food_per_turn::float"), True).over(window_spec_yield),
                "ending_yield_gold": first_value(expr("gold_per_turn::float"), True).over(window_spec_yield),
                "ending_yield_happiness": first_value(expr("happiness_per_turn::float"), True).over(window_spec_yield),
                "ending_yield_production": first_value(expr("production_per_turn::float"), True).over(window_spec_yield),
                "ending_yield_science": first_value(expr("science_per_turn::float"), True).over(window_spec_yield),
                "victory_type": last_value(col("victory_type"), True).over(window_spec_full),
                "PolicyCards_Json": from_json(col("policy_cards"), "struct<PolicyCards:array<string>>"),
                "num_policy_cards": size(array_distinct(col("PolicyCards_Json.PolicyCards")))
            }
        )
        .select(
            "pca.*",
            "government_selected",
            "ending_yield_culture",
            "ending_yield_diplomacy",
            "ending_yield_food",
            "ending_yield_gold",
            "ending_yield_happiness",
            "ending_yield_production",
            "ending_yield_science",
            "victory_type",
            "num_policy_cards",
            "age_num"
        )
    )

    # Perform the transformations
    aggregated_df = (
        window_df.groupBy(*key_cols)
        .agg(
            expr("max(age_num) as age_num"),
            expr("max(government_selected) as government_selected"),
            expr("coalesce(max(ending_yield_culture), 0) as ending_yield_culture"),
            expr("coalesce(max(ending_yield_diplomacy), 0) as ending_yield_diplomacy"),
            expr("coalesce(max(ending_yield_food), 0) as ending_yield_food"),
            expr("coalesce(max(ending_yield_gold), 0) as ending_yield_gold"),
            expr("coalesce(max(ending_yield_happiness), 0) as ending_yield_happiness"),
            expr("coalesce(max(ending_yield_production), 0) as ending_yield_production"),
            expr("coalesce(max(ending_yield_science), 0) as ending_yield_science"),
            expr("max(victory_type) as victory_type"),
            expr("min(received_on) as age_start_ts"),
            expr("max(case when num_policy_cards > 0 then num_policy_cards else 0 end) as num_policy_cards"),
            expr(
                "max(case when event_trigger = 'CampaignExit' and campaign_exit_type = 'AgeEnd' then received_on end) as age_end_ts"
            ),
            expr(
                "max(case when civilization != 'NA' then civilization end) as civilization"
            ),
            expr(
                "max(case when lower(gametype) in ('internet','lan','wireless') and lower(campaignsetupevent) = 'complete' then true else false end) as is_multiplayer"
            ),
            expr(
                "max(case when element_at(split(milestone_type, '_'), size(split(milestone_type, '_'))) >= 1 and source_table = 'victorystatus' then true else false end) as qualified_attempt"
            ),
            expr("max(cast(turn_number as int)) as num_turns_played"),
            expr(
                "max(case when event_trigger = 'CampaignExit' and campaign_exit_type = 'AgeEnd' then true else false end) as is_player_completed_age"
            ),
            expr(
                "sum(case when lower(production_item) like 'building%' and event_trigger in ('ProductionItemCompleted','ProductionItemPurchased') then 1 else 0 end) as total_buildings"
            ),
            expr(
                "sum(case when event_trigger = 'SettlementFounded' then 1 else 0 end) as settlements_founded"
            ),
            expr(
                "sum(case when event_trigger = 'SettlementAcquired' then 1 else 0 end) as settlements_captured"
            ),
            expr(
                "sum(case when event_trigger = 'ConvertedToCity' then 1 else 0 end) as total_cities"
            ),
            expr("sum(attacks_made) as total_combats"),
            expr(
                "count(DISTINCT case when lower(milestone_type) like '%science%' and event_trigger = 'Milestone Achieved' then milestone_type else null end) as science_milestones"
            ),
            expr(
                "count(DISTINCT case when lower(milestone_type) like '%culture%' and event_trigger = 'Milestone Achieved' then milestone_type else null end) as culture_milestones"
            ),
            expr(
                "count(DISTINCT case when lower(milestone_type) like '%military%' and event_trigger = 'Milestone Achieved' then milestone_type else null end) as military_milestones"
            ),
            expr(
                "count(DISTINCT case when lower(milestone_type) like '%economic%' and event_trigger = 'Milestone Achieved' then milestone_type else null end) as economic_milestone"
            ),
            expr("count(distinct diplomacy_action_instance_id) as num_diplomatic_actions"),
            expr(
                "sum(case when event_trigger = 'Respond' then 1 else 0 end) as num_diplomatic_responses"
            ),
            expr(
                "count(distinct case when event_trigger = 'Completed' and source_table = 'techtreestatus' then node_id else null end) as num_tech_researched"
            ),
            expr(
                "count(distinct case when event_trigger = 'Completed' and source_table = 'civictreestatus' then node_id else null end) as num_civics_researched"
            ),
            expr("count(distinct commander_id) as num_commanders_earned"),
            expr(
                "sum(case when commander_action = 'promote' then 1 else 0 end) as num_commander_promotions"
            ),
            expr(
                "count(case when event_trigger = 'Natural Disaster' then world_event_name else null end) as num_natural_disasters"
            ),
            expr(
                "sum(case when event_trigger = 'Point Earned' then 1 else 0 end) as leader_attributes_earned"
            ),
            expr(
                "sum(case when event_trigger = 'Node Aquired' then 1 else 0 end) as leader_attributes_spent"
            ),
             expr(
                "max(case when crossplay_friends=1 then true else false end) as is_crossplay_friend"
            ),
            expr(
                "max(case when  num_crossplay_campaigns > 1 then true else false end) as is_crossplay_campaign"
                ),
            expr(
                "max(case when  liveevent is not null then true else false end) as is_lte"
                ),
            expr("current_timestamp() as dw_insert_ts"),
            expr("current_timestamp() as dw_update_ts"),
        )
        .withColumn("merge_key", sha2(concat_ws("||", *key_cols), 256))
    )
    aggregated_df = aggregated_df.filter(expr("player_id != 'anonymous'"))
    
    aggregated_df = (
        aggregated_df.alias("pcd")
        .join(minutes_played.alias("mp"),
               on=expr("mp.player_id = pcd.player_id and mp.campaign_instance_id = pcd.campaign_instance_id and mp.age = pcd.age and mp.player_type = pcd.player_type and mp.player_slot = pcd.player_slot and mp.leader = pcd.leader"),
               how="left")
        .join(session_instance_id.alias("sp"),
               on=expr("sp.player_id = pcd.player_id and sp.campaign_instance_id = pcd.campaign_instance_id and sp.age = pcd.age and sp.player_type = pcd.player_type and sp.player_slot = pcd.player_slot and sp.leader = pcd.leader"),
               how="left")
        .join(num_days_played.alias("nd"),
               on=expr("nd.player_id = pcd.player_id and nd.campaign_instance_id = pcd.campaign_instance_id and nd.age = pcd.age and nd.player_type = pcd.player_type and nd.player_slot = pcd.player_slot and nd.leader = pcd.leader"),
               how="left")
    .select(
        expr("pcd.*"),
        expr("mp.num_minutes_played"),
        expr("sp.num_sessions_played"),
        expr("nd.num_days_played")
    ))

    return aggregated_df

# COMMAND ----------

def load_data(aggregated_df):
    target_table = f"{database}{environment}.managed.fact_age_summary"
    create_fact_age_summary_table(target_table)
    delta_table = DeltaTable.forName(spark, target_table)

    insert_cols = key_cols + ["dw_insert_ts", "merge_key"]
    update_cols = {}
    for col_name in aggregated_df.columns:
        if col_name in insert_cols:
            continue

        if col_name in ["num_turns_played"]:
            update_cols[col_name] = f"greatest(src.{col_name}, tgt.{col_name})"

        else:
            update_cols[col_name] = f"src.{col_name}"

    delta_table.alias("tgt").merge(
        aggregated_df.alias("src"), "tgt.merge_key = src.merge_key"
    ).whenMatchedUpdate(set = update_cols).whenNotMatchedInsertAll().withSchemaEvolution().execute()

# COMMAND ----------

def create_fact_age_summary_table(target_table):
    sql = f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            campaign_instance_id STRING,
            player_id STRING,
            age STRING,
            leader STRING,
            player_slot STRING,
            player_type STRING,
            civilization STRING,
            age_start_ts TIMESTAMP,
            age_end_ts TIMESTAMP,
            is_multiplayer BOOLEAN,
            qualified_attempt BOOLEAN,
            num_turns_played INT,
            num_sessions_played INT,
            num_days_played INT,
            num_minutes_played INT,
            num_policy_cards INT,
            is_player_completed_age BOOLEAN,
            total_buildings INT,
            settlements_founded INT,
            settlements_captured INT,
            total_cities INT,
            total_combats INT,
            science_milestones INT,
            culture_milestones INT,
            military_milestones INT,
            economic_milestone INT,
            num_diplomatic_actions INT,
            num_diplomatic_responses INT,
            num_tech_researched INT,
            num_civics_researched INT,
            num_commanders_earned INT,
            num_commander_promotions INT,
            num_natural_disasters INT,
            leader_attributes_earned INT,
            leader_attributes_spent INT,
            government_selected STRING,
            victory_type STRING,
            age_num INT,
            ending_yield_culture FLOAT,
            ending_yield_diplomacy FLOAT,
            ending_yield_food FLOAT,
            ending_yield_gold FLOAT,
            ending_yield_happiness FLOAT,
            ending_yield_production FLOAT,
            ending_yield_science FLOAT,
            is_crossplay_friend BOOLEAN,
            is_crossplay_campaign BOOLEAN,
            is_lte BOOLEAN,
            merge_key STRING,
            dw_insert_ts TIMESTAMP,
            dw_update_ts TIMESTAMP
        )
    """

    properties = {
        "delta.feature.allowColumnDefaults": "supported"  # this won't work until iceberg v3 is released
    }

    create_table(spark, sql, properties)

# COMMAND ----------

# Main execution
joined_df,activity_df =  extract_data()
aggregated_df = transform_data(joined_df,activity_df)
load_data(aggregated_df)