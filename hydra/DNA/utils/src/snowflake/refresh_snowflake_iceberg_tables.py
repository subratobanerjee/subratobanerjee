# Databricks notebook source
# MAGIC %run ../../../../utils/helpers

# COMMAND ----------

# MAGIC %pip install snowflake

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

user = dbutils.secrets.get("data-engineering", "snowflake_svc_acc_userid")
password = dbutils.secrets.get("data-engineering", "snowflake_svc_acc_password")

# COMMAND ----------

import snowflake.connector
conn = snowflake.connector.connect(
    user=user,
    password=password,
    account="twokgames",
    warehouse="wh_team_de_databricks",
    database="databricks_migration"
    )

# COMMAND ----------

import snowflake.connector

def refresh_iceberg_tables(dbx_catalog: str, dbx_schema: str, title: str, environment: str = '_stg'):
    """
    Refreshes Snowflake Iceberg tables by reading the config from Databricks first.

    Args:
        dbx_catalog (str): The source Databricks catalog to filter configurations by.
        dbx_schema (str): The source Databricks schema to filter configurations by.
        title (str): Title name as per config table
        environment (str): The environment suffix for the config table name (e.g., '_stg').
    """
    print(f"--- 🚀 Starting refresh for source {dbx_catalog}.{dbx_schema} ---")

    # 1. Query the config table in Databricks to get the list of tables
    config_table = f"reference{environment}.utils.snowflake_iceberg_integration_configs"
    query = f"""
    SELECT snowflake_db, snowflake_schema, snowflake_table_name 
    FROM {config_table}
    WHERE is_active = true 
      AND dbx_db = '{dbx_catalog}'
      AND dbx_schema = '{dbx_schema}'
      AND title = '{title}'
    """
    
    print(f"Finding tables to refresh from config table: {config_table}...")
    tables_to_refresh_df = spark.sql(query)
    
    if tables_to_refresh_df.isEmpty():
        print("ℹ️ No active tables found for the specified source. Exiting.")
        return
        
    print(f"Found {(tables_to_refresh_df.count())} tables to refresh in Snowflake.")

    # 2. Get Snowflake credentials
    try:
        user = dbutils.secrets.get("data-engineering", "snowflake_svc_acc_userid")
        password = dbutils.secrets.get("data-engineering", "snowflake_svc_acc_password")
    except Exception as e:
        print("⛔️ ERROR: Could not retrieve secrets. Halting execution.")
        raise e

    # 3. Connect to Snowflake and execute refreshes
    try:
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account='twokgames',
            warehouse="wh_team_de_databricks"
        )
        cur = conn.cursor()
    except Exception as e:
        print("⛔️ ERROR: Failed to connect to Snowflake. Halting execution.")
        raise e

    tables_list = tables_to_refresh_df.collect()
    
    for row in tables_list:
        table_full_name = f"{row['snowflake_db']}.{row['snowflake_schema']}.{row['snowflake_table_name']}"
        try:
            refresh_sql = f"ALTER ICEBERG TABLE {table_full_name} REFRESH;"
            print(" Executing COMMAND: ", refresh_sql)
            print(f"▶️ Refreshing {table_full_name}...")
            cur.execute(refresh_sql)
            print(f"✅ Snowflake table {table_full_name} refresh complete\n")
        except Exception as e:
            print(f"   ⛔️ ERROR refreshing {table_full_name}: {e}")

    cur.close()
    conn.close()
    print("\n--- ✅ Refresh process finished ---")

# COMMAND ----------

title = dbutils.widgets.get("title")
environment = dbutils.widgets.get("environment")
dbx_catalog = dbutils.widgets.get("dbx_catalog")
dbx_schema = dbutils.widgets.get("dbx_schema")

# COMMAND ----------

refresh_iceberg_tables(dbx_catalog, dbx_schema, title, environment)
