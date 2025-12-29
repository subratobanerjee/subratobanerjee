# Databricks notebook source
from delta.tables import DeltaTable

#Global Table Properties
global_properties = {
        'delta.enableIcebergCompatV2': 'true',
        'delta.universalFormat.enabledFormats': 'iceberg',
        'write.metadata.delete.current.file.enabled': 'true',
        'write.metadata.check.interval-ms': '10000',
        'snapshot.expiration.interval-ms': '604800000',
        'write.target.file.size': '52428800',
        'delta.autoOptimize.optimizeWrite': 'true',
        'delta.autoOptimize.autoCompact': 'true'
    }

# COMMAND ----------

def create_table(spark, sql, properties={}):
    """
    Create a table in Spark using a given SQL command and properties.

    Parameters:
    spark (SparkSession): The Spark session for executing the SQL command.
    sql (str): The SQL command for creating the table (excluding TBLPROPERTIES).
    properties (dict, optional): A dictionary of properties to add to the table.

    Example:
    sql_command = "
    CREATE TABLE IF NOT EXISTS my_table (
        id STRING,
        name STRING
    )
    TBLPROPERTIES (
    "

    optional_properties = {
        'delta.param.1': 'true'
    }

    properties are optinal we have gobal properties and any properties that are 
    passed will be added to the sql statment by merging the gobal with the user
    defined properties.


    create_table(spark, sql_command, optional_properties)
    or 
    create_table(spark, sql_command)

    """
    
    # Combine global and local properties
    global_properties.update(properties)

    # Adding the TBLPROPERTIES opening block
    sql =  sql+'\nTBLPROPERTIES ( \n'
    # Add properties to the SQL command
    for key, value in global_properties.items():
        sql += f"    '{key}' = '{value}',\n"
    
    # Remove the last comma and add closing parenthesis
    sql = sql.rstrip(',\n') + "\n    );"

    # Execute the SQL command
    return spark.sql(sql)

# COMMAND ----------

def create_table_raw(spark, table_path, schema, properties={}):
    """
    Create a table in Spark using a given table path and properties.

    Parameters:
    spark (SparkSession): The Spark session for executing the SQL command.
    table_path (str): The path to the table to be created.
    schema (str): The schema of the table.
    properties (dict, optional): A dictionary of properties to add to the table.

    Example:
    table_path = 'gbx_dev.raw.logins'
    schema = <df.schema>
    properties = {
        'delta.param1':true
    }

    create_table_raw(spark, table_path, df.schema, properties)
    """
    # Combine global and local properties
    global_properties.update(properties)

    # Create the Delta table with the combined properties
    builder = (
        DeltaTable.createIfNotExists(spark)
        .tableName(table_path)
        .addColumns(schema)
    )
    for key, value in global_properties.items():
        builder = builder.property(key, value)
    builder.execute()
