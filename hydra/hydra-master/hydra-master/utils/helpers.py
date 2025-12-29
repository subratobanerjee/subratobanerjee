# Databricks notebook source
import json
import argparse
import logging

# COMMAND ----------

# Configure logging
def setup_logger():

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    return logger

def arg_parser():

    parser = argparse.ArgumentParser(description="Consume parameters from Databricks job")
    parser.add_argument("--environment", type=str, required=True, help="Description for environment")
    parser.add_argument("--checkpoint_location", type=str, required=True, help="Description for checkpoint_location")
    
    args = parser.parse_args()

    environment = args.environment
    checkpoint_location = args.checkpoint_location

    return environment,checkpoint_location

def dbutils_input_params():
    #Creating a widget for inputParams
    dbutils.widgets.text("inputParam", '{"key":"default"}', "Input Parameter")
    # Get the value of the widget as a dict
    input_param_dict = json.loads(dbutils.widgets.get("inputParam"))
    return input_param_dict

# COMMAND ----------

def set_environment(environment=None):
    workspace_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().workspaceId().get()

    if workspace_id == 74887402257591:
        return '_prod'
    elif environment:
        if  environment in ('_stg','_dev'):
            return environment
    else:
        return '_dev'

# COMMAND ----------
def set_merge_update_condition (merge_df, merge_update_conditions):
    # Dynamically generate the multiple WHEN MATCHED UPDATE clauses
    for update_condition in merge_update_conditions:
        condition_expr = update_condition.get("condition", None)
        update_set = update_condition.get("set_fields", None)

        if not condition_expr or not update_set:
            continue 

        # Dynamically build the WHEN MATCHED UPDATE expressions and applying it
        update_exprs = {}
        for column, logic in update_set.items():
            update_exprs[f"target.{column}"] = expr(logic)
        merge_df.whenMatchedUpdate(condition=condition_expr,set=update_exprs)

    return merge_df

# COMMAND ----------
def set_merge_insert_condition (merge_df,df):
    # Dynamically generate the insert columns based on micro_batch_df columns
    insert_columns = {f"target.{col}": f"source.{col}" for col in df.columns}
    return merge_df.whenNotMatchedInsert(values=insert_columns)

# COMMAND ---------- 
def max_timestamp(spark, table_name, column_name='dw_insert_ts', filters='1=1'):
    latest_ts_value = spark.sql(f"select ifnull(max({column_name}) , '1999-01-01')::date as latest_value from {table_name} where {filters}").collect()[0]['latest_value']
    return latest_ts_value

# COMMAND ----------

def merge_to_table(df, target_table, table_name, min_cols=[], max_cols=[], exclude_cols=[], merge_key_logic='', filters='1=1'):
    
    # get schema of the table
    table_schema = spark.read.table(f"{table_name}").schema

    # create empty dataframe using the schema
    out_df = spark.createDataFrame([], table_schema)

    # union the incoming dataframe with the empty dataframe, we can set allowMissingColumns to False to be more strict
    df = df.selectExpr(
        "*", 
        "current_timestamp() as dw_insert_ts", 
        "current_timestamp() as dw_update_ts", 
        f"{merge_key_logic} as merge_key"
    ).where(filters)
    out_df = out_df.unionByName(df, allowMissingColumns=True)

    # dynamically setup merge condition based on the aggregate columns
    max_cols = [col_name for col_name in out_df.columns if ('agg_' in col_name.lower() or col_name in max_cols) and col_name not in exclude_cols]
    min_cols = [col_name for col_name in out_df.columns if col_name in min_cols and col_name not in exclude_cols]
    other_cols = [col_name for col_name in out_df.columns if col_name not in max_cols and col_name not in min_cols and col_name not in ['merge_key'] and col_name not in exclude_cols]
    merge_condition = " OR ".join(f"old.{col_name} <> new.{col_name}" for col_name in max_cols + min_cols + other_cols)

    # create the update column dict
    update_set = {}
    for col_name in max_cols:
        update_set[f"old.{col_name}"] = f"greatest(new.{col_name}, old.{col_name})"
    for col_name in min_cols:
        if f"(new.{col_name} is not null) and (old.{col_name} is not null)":
            update_set[f"old.{col_name}"] = f"least(new.{col_name}, old.{col_name})"
        else:
            update_set[f"old.{col_name}"] = f"greatest(new.{col_name}, old.{col_name})"
    for col_name in other_cols:
        update_set[f"old.{col_name}"] = f"new.{col_name}"

    update_set[f"old.dw_update_ts"] = "CURRENT_TIMESTAMP()"

    # merge the table
    (
        target_table.alias('old')
        .merge(
            out_df.alias('new'),
            "new.merge_key = old.merge_key"
        )
        .whenMatchedUpdate(condition=merge_condition, set=update_set)
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------
def merge_data(target, source, merge_condition, df , merge_update_conditions={} , requried_insert=True):

    # Dynamically generate the insert columns based on micro_batch_df columns
    insert_columns = {f"target.{col}": f"source.{col}" for col in df.columns}

    df.createOrReplaceTempView(f"{source}")
    
    sql = f"""MERGE INTO {target} target
              USING {source} source
              ON {merge_condition}""" 

    for update_condition in merge_update_conditions:
        condition_expr = update_condition.get("condition", None)
        update_set = update_condition.get("set_fields", None)

        if not condition_expr or not update_set:
            continue 

        # Dynamically build the WHEN MATCHED UPDATE expressions and applying it
        update_exprs = {f"target.{column}": logic for column, logic in update_set.items()}
        update_exprs_str = ', '.join([f"{k}={v}" for k, v in update_exprs.items()])
        sql += f"\n WHEN MATCHED AND ( {condition_expr} ) \n THEN UPDATE SET {update_exprs_str}"

    if requried_insert:
        sql +=  f"""\nWHEN NOT MATCHED THEN
                    INSERT ({', '.join(insert_columns.keys())})
                    VALUES ({', '.join(insert_columns.values())})"""
    spark.sql(sql)
    spark.sql(f'drop table if exists {source}')
    return 'Merged Data'