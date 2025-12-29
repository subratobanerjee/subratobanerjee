# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

import json
from pyspark.sql.functions import expr,col
from delta.tables import DeltaTable


# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/table_functions

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'inverness'
data_source ='dna'
title = 'Civilization VII'
key_cols = ["campaign_instance_id", "player_id", "age", "leader","player_slot","player_type"]

# COMMAND ----------

def read_age_summary(spark,environment):

    prev_max = max_timestamp(spark, f"inverness{environment}.managed.fact_player_campaign_summary", 'dw_insert_ts')

    current_min = (
        spark.read
        .table(f"inverness{environment}.managed.fact_age_summary")
        .where(expr("dw_insert_ts::date between current_date - 3 and current_date"))
        .select(expr(f"ifnull(min(age_start_ts::timestamp::date),'1999-01-01')::date as min_date"))
    ).collect()[0]['min_date']

    print(prev_max)
    print(current_min)
    inc_min_date = min(prev_max,current_min)
    print(inc_min_date)

    return (
        spark
        .read
        .table(f"inverness{environment}.managed.fact_age_summary")
        .where(expr(f"""age_start_ts::date >= to_date('{inc_min_date}')
                    and player_type in ('HUMAN')"""))
    )

# COMMAND ----------
def read_fpca_table(spark,environment):
    """ Read the table and remove all the nulls from key columns"""


    fpca_df =  spark.read.table(f"{database}{environment}.intermediate.fact_player_campaign_activity")\
                .filter(" AND ".join([f"{col} IS NOT NULL" for col in key_cols]))\
                .where(expr(f"player_type='HUMAN' and player_id !='anonymous'"))
    return fpca_df 

def read_victory_status(spark,environment):
    return (
        spark
        .read
        .table(f"inverness{environment}.raw.victorystatus")
        .where(expr("""playertype in ('HUMAN')
                    and victoryevent = 'Victory Achieved'"""))
        .select(
            "playerPublicId",
            "campaigninstanceid",
            "age",
            "leader",
            "playerslot",
            "playertype",
            "victoryevent"
        )
        .distinct()
    )

# COMMAND ----------

def extract(spark, environment):
    age_df = read_age_summary(spark, environment).alias("age")
    vict_df = read_victory_status(spark, environment).alias("victory")
    comp_df = (
        vict_df
        .groupBy(
            "campaigninstanceid"
        )
        .agg(
            expr("max(case when victoryevent = 'Victory Achieved' then 1 else 0 end) as camp_complete")
        )
    ).alias("completes")

    joined_df = (
        age_df
        .join(
            vict_df,
            on = expr("""age.player_id = victory.playerPublicId 
                    and age.campaign_instance_id = victory.campaigninstanceid 
                    and age.age = victory.age
                    and age.leader = victory.leader
                    and age.player_slot = victory.playerslot 
                    and age.player_type = victory.playertype"""),
            how = "left"
        )
        .join(
            comp_df,
            on = expr("""age.campaign_instance_id = completes.campaigninstanceid"""),
            how = "left"
        )
    )

    return joined_df

# COMMAND ----------

def transform(df, environment, spark):
    fpca_df = read_fpca_table(spark,environment)
    fpca_df = (
    fpca_df
    .groupBy(
        'player_id',
        'campaign_instance_id',
        'player_slot',
        'player_type'
    )
    .agg(
        expr("count(distinct to_date(received_on)) as num_days_played"),
        expr("count(distinct application_session_instance_id) as num_sessions_played")
    )
    )
    transformed_df = (
        df
        .where(col("age.player_id")!='anonymous')
        .groupBy(
                "age.player_id",
                "age.campaign_instance_id",
                "age.player_slot",
                "age.player_type"
                )
        .agg(expr("min(age_start_ts) as campaign_start_ts"), 
             expr("max(age_end_ts) as campaign_end_ts"), 
             expr("max(is_multiplayer) as is_multiplayer"), 
             expr("count(distinct age.age) as num_ages_played"),
             expr("sum(num_turns_played) as num_turns_played"),
             expr("sum(num_minutes_played) as num_minutes_played"),
             expr("max(camp_complete)::boolean as is_complete"),
             expr("max(victory_type) as victory_type"), 
             expr("max(case when victoryevent = 'Victory Achieved' then playertype end) as victory_player_type"),
             expr("max(case when victoryevent = 'Victory Achieved' then true else false end) as is_campaign_won")
        )
        .withColumns({
                    "campaign_number": expr("row_number() over (partition by age.player_id, campaign_instance_id order by campaign_start_ts)"),
                    "dw_insert_ts": expr("current_timestamp()"),
                    "dw_update_ts": expr("current_timestamp()"),
                    "merge_key" : expr("sha2(concat_ws('|', player_id, campaign_instance_id, age.player_slot,age.player_type), 256)")

                })
        .select
        (
         "player_id",
         expr("campaign_instance_id as campaign_id"),
         "player_slot",
         "player_type",
         "campaign_start_ts",
         "campaign_end_ts",
         "is_multiplayer",
         "campaign_number",
         "num_ages_played",
         "num_turns_played",
         "num_minutes_played",
         expr("ifnull(is_complete,false) as is_complete"),
         "victory_type",
         "victory_player_type",
         expr("ifnull(is_campaign_won,false) as is_campaign_won"),
         "dw_insert_ts",
         "dw_update_ts",
         "merge_key")
    )

    transformed_df = (
        transformed_df.alias("pcd")
        .join(fpca_df.alias("mp"),
               on=expr("mp.player_id = pcd.player_id and mp.campaign_instance_id = pcd.campaign_id and mp.player_type = pcd.player_type and mp.player_slot = pcd.player_slot"),
               how="left")
    .select(
        expr("pcd.*"),
        expr("mp.num_days_played"),
        expr("mp.num_sessions_played")
    ))

    transformed_df=transformed_df.select(
         "player_id",
         "campaign_id",
         "player_slot",
         "player_type",
         "campaign_start_ts",
         "campaign_end_ts",
         "is_multiplayer",
         "campaign_number",
         "num_ages_played",
         "num_turns_played",
         "num_minutes_played",
         "num_sessions_played",
         "num_days_played",
         "is_complete",
         "victory_type",
         "victory_player_type",
         "is_campaign_won",
         "dw_insert_ts",
         "dw_update_ts",
         "merge_key"
     )

    
    return transformed_df


# COMMAND ----------

def load(df, environment, spark):
    (
        DeltaTable.createIfNotExists(spark)
        .tableName(f"inverness{environment}.managed.fact_player_campaign_summary")
        .addColumn("player_id", "string")
        .addColumn("campaign_id", "string")
        .addColumn("player_slot", "string")
        .addColumn("player_type", "string")
        .addColumn("campaign_start_ts", "timestamp")
        .addColumn("campaign_end_ts", "timestamp")
        .addColumn("is_multiplayer", "boolean")
        .addColumn("campaign_number", "int")
        .addColumn("num_ages_played", "int")
        .addColumn("num_turns_played", "int")
        .addColumn("num_minutes_played", "int")
        .addColumn("num_sessions_played", "int")
        .addColumn("num_days_played", "int")
        .addColumn("is_complete", "boolean")
        .addColumn("victory_type", "string")
        .addColumn("victory_player_type", "string")
        .addColumn("is_campaign_won", "boolean")
        .addColumn("dw_insert_ts", "timestamp")
        .addColumn("dw_update_ts", "timestamp")
        .addColumn("merge_key", "string")
        .property('delta.enableIcebergCompatV2', 'true')
        .property('delta.universalFormat.enabledFormats', 'iceberg')
        .execute()
    )

    target_table = DeltaTable.forName(spark, f"inverness{environment}.managed.fact_player_campaign_summary")
    (
        target_table
        .alias("target")
        .merge(
            df.alias("source"),
            "target.merge_key = source.merge_key")
        .withSchemaEvolution()
        .whenMatchedUpdate(set = 
                           {
                                "target.campaign_start_ts": "source.campaign_start_ts",
                                "target.campaign_end_ts":"source.campaign_end_ts",
                                "target.is_multiplayer":"source.is_multiplayer",
                                "target.campaign_number": "source.campaign_number" ,
                                "target.num_ages_played": "source.num_ages_played",
                                "target.num_turns_played": "source.num_turns_played",
                                "target.num_minutes_played": "source.num_minutes_played",
                                "target.num_sessions_played": "source.num_sessions_played",
                                "target.num_days_played": "source.num_days_played",
                                "target.is_complete": "source.is_complete",
                                "target.victory_type": "source.victory_type",
                                "target.victory_player_type": "source.victory_player_type",
                                "target.is_campaign_won": "source.is_campaign_won",
                                "target.dw_update_ts": "current_timestamp()"
                           })
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

def process_fact_player_campaign_summary():
    
    spark = create_spark_session(name=f"{database}")

    df = extract(spark,environment)
    
    df = transform(df, environment, spark)

    load(df, environment, spark)


# COMMAND ----------

if __name__ == "__main__":
    process_fact_player_campaign_summary()
