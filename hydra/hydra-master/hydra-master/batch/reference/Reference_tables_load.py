# Databricks notebook source
from delta.tables import DeltaTable

# COMMAND ----------

# Use dbutils secrets to get Snowflake credentials.
user = dbutils.secrets.get("data-engineering", "snowflake_svc_acc_userid")
password = dbutils.secrets.get("data-engineering", "snowflake_svc_acc_password")

# COMMAND ----------

dbutils.widgets.text(name="environment", defaultValue="_dev")
environment = dbutils.widgets.get("environment")

# COMMAND ----------

options = {
  "sfUrl": "twokgames.snowflakecomputing.com",
  "sfUser": user,
  "sfPassword": password,
  "sfDatabase": "REFERENCE",
  "sfSchema": "TITLE",
  "sfWarehouse": "wh_team_de_databricks"
}

# COMMAND ----------

dim_title = (spark.read
       .format("snowflake")
       .options(**options)
       .option("dbtable","REFERENCE.TITLE.DIM_TITLE")
       .load())

#define target table as iceberg
(
    DeltaTable.createIfNotExists(spark)
    .tableName(f"reference{environment}.TITLE.DIM_TITLE")
    .addColumns(dim_title.schema)
    .property('delta.enableIcebergCompatV2', 'true')
    .property('delta.universalFormat.enabledFormats', 'iceberg')
    .execute()
)

# Overwrite to Databricks table
(dim_title.write
 .mode("overwrite")
 .format("delta")
 .saveAsTable(f"reference{environment}.title.dim_title")
)



# COMMAND ----------




dim_country = (spark.read
       .format("snowflake")
       .options(**options)
       .option("dbtable","REFERENCE.LOCATION.DIM_COUNTRY")
       .load())

#define target table as iceberg
(
    DeltaTable.createIfNotExists(spark)
    .tableName(f"reference{environment}.LOCATION.DIM_COUNTRY")
    .addColumns(dim_country.schema)
    .property('delta.enableIcebergCompatV2', 'true')
    .property('delta.universalFormat.enabledFormats', 'iceberg')
    .execute()
)

# Overwrite to Databricks table
(dim_country.write
 .mode("overwrite")
 .format("delta")
 .saveAsTable(f"reference{environment}.LOCATION.DIM_COUNTRY")
)

# COMMAND ----------

dim_date = (spark.read
       .format("snowflake")
       .options(**options)
       .option("dbtable","REFERENCE.CALENDAR.DIM_DATE")
       .load())

#define target table as iceberg
(
    DeltaTable.createIfNotExists(spark)
    .tableName(f"reference{environment}.calendar.dim_date")
    .addColumns(dim_date.schema)
    .property('delta.enableIcebergCompatV2', 'true')
    .property('delta.universalFormat.enabledFormats', 'iceberg')
    .execute()
)


# # Overwrite to Databricks table
(dim_date.write
 .mode("overwrite")
 .format("delta")
 .saveAsTable(f"reference{environment}.calendar.dim_date")
)
