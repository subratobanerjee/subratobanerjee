# Databricks notebook source
# MAGIC %run ../../../../../../utils/set_spark_session

# COMMAND ----------

# MAGIC %run ../../../../../../utils/helpers

# COMMAND ----------

# MAGIC %run ../../../../../../utils/ddl/intermediate/fact_player_progression

# COMMAND ----------

from pyspark.sql.functions import expr, col, lit, explode

# COMMAND ----------

input_param = dbutils_input_params()
environment = input_param.get('environment',set_environment())
database  = 'bluenose'
data_source ='dna'
title = 'PGA Tour 2K25'

# COMMAND ----------

def read_title_df(environment, spark):
    title_df= (
                spark.
                read.
                table(f"reference{environment}.title.dim_title")
                .select(
                    expr("display_platform as platform"),
                    "app_id",
                    expr("display_service as service")
                )
                .alias("title")
                )
    return title_df

# COMMAND ----------

def read_player_progression(environment, spark, prev_max_ts):
    characterprogression_df = (
    spark
    .read
    .table(f"bluenose{environment}.raw.characterprogression")
    .select("receivedOn", 
            "playerPublicId", 
            "myplayerid", 
            "characterprogressioninstanceid", 
            "attributecontact", 
            "attributelierange", 
            "attributepower", 
            "attributeputtcontact", 
            "attributeputtpath", 
            "attributeshaping", 
            "attributeswingbiascorrection", 
            "attributeswingpath", 
            "attributetiming", 
            "attributetransition", 
            "attributputttiming",
            "attributputtweight", 
            "skillsallocated", 
            "myplayerlevel", 
            "ovr",
            "characterprogressionstatus",
            "skillpointsremaining",
            "skillpointsspent",
            "attributepointsremaining",
            "attributepointsspent",
            "appPublicId", 
            "transactionId",
            "appGroupId")
    .where(
        (expr(f"insert_ts::date >= to_date({prev_max_ts}) - interval '2 day'")) &
        (expr("playerPublicId is not null and playerPublicId != 'anonymous'")) &
        (expr("myplayerid is not null or myplayerid != ''")) &
        (expr("characterprogressionstatus != 'DeleteMyPlayer'")) &
        (expr("buildenvironment = 'RELEtASE'"))
    )

    )

    return characterprogression_df

# COMMAND ----------

def read_last_progression(environment, distinct_players_df):
    distinct_players_df = distinct_players_df.alias('dp')
    # Load the last player progression for each playerid, myplayerid, for each subject_id to perform lag with last progression and new incoming data
    last_player_progression = (
    spark
    .read
    .table(f"bluenose{environment}.intermediate.fact_player_progression")
    .select(
        expr('received_on as receivedOn'),
        expr('player_id AS playerpublicid'),
        expr('primary_instance_id AS myplayerid'),
        expr('secondary_instance_id AS characterprogressioninstanceid'),
        expr('action as characterprogressionstatus'),
        expr('extra_info_1 as transactionId' ),
        expr('NULL as appGroupId'),
        'platform',
        'service',
        expr('case when subject_id like "%Skills%" then "skillsallocated" else subject_id end as subject_id'),
        expr('extra_info_2 AS current_value'),
        expr('extra_info_7 as myplayerlevel'),
        expr('extra_info_8 as ovr'),
        expr('ROW_NUMBER() OVER (PARTITION BY player_id, primary_instance_id, subject_id  ORDER BY received_on DESC) AS row_num')
    ).alias('fp').join(distinct_players_df, expr("fp.playerpublicid = dp.playerpublicid"), "inner")
    )   

    last_player_progression = last_player_progression.select("fp.*")


    return last_player_progression

# COMMAND ----------

def read_skill_tree_points(environment, spark):
    skill_points_df = (
        spark
        .read
        .table(f"bluenose{environment}.reference.skill_point_mapping")
        .select(
            expr("slot as subject_id"),
            expr("cost as points")
        )
    )
    return skill_points_df

# COMMAND ----------

def extract(environment):
    prev_max_ts = max_timestamp(spark, f"{database}{environment}.intermediate.fact_player_progression")
    print(prev_max_ts)
    char_prog_df = read_player_progression(environment, spark, prev_max_ts).alias("cp")
    distinct_playerid_df = char_prog_df.select("playerPublicId").distinct()
    title_df = read_title_df(environment, spark).alias("title")
    skill_points_df = read_skill_tree_points(environment, spark)
    joined_df = (
        char_prog_df.join(title_df, expr("cp.appPublicId = title.app_id"), "left")
    ).select(
            expr("receivedOn::timestamp as receivedOn"), 
            "playerPublicId", 
            "myplayerid", 
            "characterprogressioninstanceid", 
            "attributecontact", 
            "attributelierange", 
            "attributepower", 
            "attributeputtcontact", 
            "attributeputtpath", 
            "attributeshaping", 
            "attributeswingbiascorrection", 
            "attributeswingpath", 
            "attributetiming", 
            "attributetransition", 
            "attributputttiming",
            "attributputtweight", 
            "skillsallocated", 
            "myplayerlevel",
            "ovr",
            "skillpointsremaining",
            "skillpointsspent",
            "attributepointsremaining",
            "attributepointsspent",
            "characterprogressionstatus",
            "transactionId",
            "appGroupId",
            "platform",
            "service"
    )

    last_player_progression = read_last_progression(environment, distinct_playerid_df)
    return joined_df, last_player_progression, skill_points_df

# COMMAND ----------

# Unpivot the specified columns
def transform(df, last_player_prog_df, skill_points_df):
    # Filter for last progression for each playerPublicId, myplayerid and subject_id
    last_player_prog_df = last_player_prog_df.select(
        "receivedOn", 
        "playerPublicId", 
        "myplayerid", 
        "characterprogressioninstanceid", 
        "characterprogressionstatus",
        "transactionId",
        "appGroupId",
        "platform",
        "service",
        "myplayerlevel",
        "ovr",
        "subject_id",
        "current_value",
        expr("'old_data' as data_type")
    ).where((col("row_num") == 1))
    
    # Unpivot all the progression values and assign them as subject_id
    unpivoted_df = df.unpivot(
        ids=["receivedOn", 
             "playerPublicId", 
             "myplayerid", 
             "characterprogressioninstanceid", 
             "characterprogressionstatus",
             "transactionId",
             "appGroupId",
             "platform",
             "service",
             "myplayerlevel",
             "ovr"
             ],
        
        values=["attributecontact", 
                "attributelierange", 
                "attributepower",
                 "attributeputtcontact", 
                 "attributeputtpath", 
                 "attributeshaping", 
                 "attributeswingbiascorrection", 
                 "attributeswingpath", 
                 "attributetiming", 
                 "attributetransition", 
                 "attributputttiming",
                "attributputtweight", 
                "skillsallocated", 
                "myplayerlevel",
                "ovr",
                "skillpointsremaining", 
                "skillpointsspent", 
                "attributepointsremaining", 
                "attributepointsspent"],
        variableColumnName="subject_id",
        valueColumnName="current_value"
    )

    unpivoted_df = unpivoted_df.select("*", expr("'new_data' as data_type"))

    #Union with last player progressions so we can compare the previous and new values to perform LAG operation

    combined_df = unpivoted_df.unionByName(last_player_prog_df)

    final_unpivoted_df = combined_df.withColumn(
        "prev_value",
        expr("LAG(current_value) OVER (PARTITION BY playerPublicId, myplayerid, subject_id ORDER BY receivedOn)")
    )


    # Consider only those progressions values where we see a change in any subject_id values comparing previous and new values
    filtered_df = final_unpivoted_df.select(
        "receivedOn", 
        "playerPublicId", 
        "myplayerid", 
        "characterprogressioninstanceid", 
        "characterprogressionstatus", 
        "transactionId",
        "appGroupId",
        "subject_id",
        "current_value",
        "prev_value",
        "platform",
        "service",
        "myplayerlevel",
        "ovr"
    ).where(expr("(prev_value IS NULL OR prev_value != current_value) AND data_type = 'new_data'"))

    # For skills, since we get an array of values, we only consider the new skills added to an array as previous skills already exists and then explode the array for each skill to be considered as a unique subject_id
    skills_data = filtered_df.select(
        "receivedOn",
        "playerPublicId",
        "myplayerid",
        "characterprogressioninstanceid",
        "characterprogressionstatus",
        "transactionId",
        "appGroupId",
        "platform",
        "service",
        "myplayerlevel",
        "ovr",
        expr("""
            CASE 
                WHEN prev_value IS NOT NULL THEN 
                    array_except(
                        from_json(current_value, 'array<string>'), 
                        from_json(prev_value, 'array<string>') 
                    )
                ELSE 
                    from_json(current_value, 'array<string>')
            END AS new_skills_array
        """),
        "prev_value",
        "current_value"
    ).where(expr("subject_id = 'skillsallocated'"))

    exploded_skills_data = skills_data.withColumn("exploded_value", explode(col("new_skills_array"))).select(
        "receivedOn",
        "playerPublicId",
        "myplayerid",
        "characterprogressioninstanceid",
        "characterprogressionstatus",
        "transactionId",
        "appGroupId",
        col("exploded_value").alias("subject_id"),
        "current_value",
        "prev_value",
        "platform",
        "service",
        "myplayerlevel",
        "ovr"
    )

    # Combine exploded skills dataframe with subject_ids other than skills
    joined_df = filtered_df.filter(col("subject_id") != "skillsallocated").union(
        exploded_skills_data                                                  
        )

    skill_points_df = skill_points_df.alias('skp')
    final_df = joined_df.alias('prog').join(
        skill_points_df,
        expr('prog.subject_id = skp.subject_id'),
        'left'
    ).select(
            expr("receivedOn as received_on"),
            expr("playerPublicId as player_id"),
            "platform",
            "service",
            expr("myplayerid as primary_instance_id"),
            expr("characterprogressioninstanceid as secondary_instance_id"),
            expr("case when characterprogressionstatus ilike any ('%myplayer%', '%SpendAttributePoints%') then 'Player' when characterprogressionstatus ilike '%skill%' then 'Skills' end as category"),
            "prog.subject_id",
            expr("characterprogressionstatus as action"),
            expr("transactionId as extra_info_1"),
            expr("current_value as extra_info_2"),
            expr("prev_value as extra_info_3"),
            expr("(CASE WHEN characterprogressionstatus ilike '%Levelup%' AND prog.subject_id = 'attributepointsremaining' THEN current_value - prev_value ELSE 0 END)::string as extra_info_4"),
            expr("(CASE WHEN characterprogressionstatus ilike '%Levelup%' AND prog.subject_id = 'skillpointsremaining' THEN current_value - prev_value ELSE NULL END)::string as extra_info_5"),
            expr("case when (action = 'SpendSkillTreePoints' or action = 'SpendAttributePoints') AND prog.subject_id in ('attributecontact', 'attributelierange', 'attributepower', 'attributeputtcontact', 'attributeputtpath', 'attributeshaping', 'attributeswingbiascorrection', 'attributeswingpath', 'attributetiming', 'attributetransition', 'attributputttiming', 'attributputtweight') AND (current_value is not null and prev_value is not null) AND (current_value > prev_value) then ROUND((CAST(current_value AS DOUBLE) - CAST(prev_value AS DOUBLE)) * 100, 2) else 0 END as extra_info_6"),
            expr("case when action = 'SpendSkillTreePoints' then skp.points else 0 END as extra_info_7"),
            expr("CASE WHEN appGroupId IN ('d3963fb6e4f54a658e1a14846f0c9a8c', '317c0552032c4804bb10d81b89f4c37e') THEN 'full game' WHEN appGroupId = '886bcab8848046d0bd89f5f1ce3b057b' THEN 'demo' END as extra_info_8"),
            expr("myplayerlevel as extra_info_9"),
            expr("ovr as extra_info_10")
        )
    
    return final_df


# COMMAND ----------

def run_batch():
    spark = create_spark_session(name=f"{database}")
    checkpoint_location = create_fact_player_progression(spark, database)
    df, last_player_progression, skills_df = extract(environment)
    df_transformed = transform(df, last_player_progression, skills_df)
    load_fact_player_progression(spark, df_transformed, database, environment)

# COMMAND ----------

run_batch()
