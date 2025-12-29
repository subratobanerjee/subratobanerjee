# Databricks notebook source
# Use dbutils secrets to get Snowflake credentials.
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

dbutils.widgets.text(name="environment", defaultValue="_dev")
environment = dbutils.widgets.get("environment")

# COMMAND ----------

table_list = [f"reference{environment}.title.dim_title", f"reference{environment}.location.dim_country", f"reference{environment}.calendar.dim_date"]
table_map = {
    f"reference{environment}.title.dim_title": "databricks_migration.reference.dim_title",
    f"reference{environment}.location.dim_country": "databricks_migration.reference.dim_country",
    f"reference{environment}.calendar.dim_date": "databricks_migration.reference.dim_date",

}

create_table_sql = """
CREATE ICEBERG TABLE IF NOT EXISTS {sf_table}
    EXTERNAL_VOLUME='reference_uniform_demo_vol'
    CATALOG='uniform_demo_catalog_int'
    METADATA_FILE_PATH='{meta_path}';
"""

replace_table_sql = """
CREATE OR REPLACE ICEBERG TABLE] {sf_table}
    EXTERNAL_VOLUME='reference_uniform_demo_vol'
    CATALOG='uniform_demo_catalog_int'
    METADATA_FILE_PATH='{meta_path}';
"""

# COMMAND ----------

for table in table_list:
    print(table)
    metadata_table = spark.sql(f"DESCRIBE EXTENDED {table}")
    print(metadata_table)
    metadata_df = spark.sql(f"DESCRIBE EXTENDED {table}").filter('col_name = "Metadata location"').select('data_type')
    if metadata_df.count() > 0:
        metadata_location = metadata_df.collect()[0][0]
        latest = "/".join(metadata_location.split("/")[8:])
        print(latest)
        conn.cursor().execute(create_table_sql.format(sf_table=table_map[table], meta_path=latest))
        try:
            conn.cursor().execute(f"ALTER ICEBERG TABLE {table_map[table]} REFRESH '{latest}'")
        except BaseException as e:
            if "Invalid type modification" in str(e):
                print("invalid")
            else:
                print(e)
    else:
        print(f"No metadata location found for table {table}")
