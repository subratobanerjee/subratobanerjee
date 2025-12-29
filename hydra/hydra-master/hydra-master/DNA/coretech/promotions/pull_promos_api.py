# Databricks notebook source
import requests
import json
import logging
from datetime import datetime, timedelta
from time import sleep
from pyspark.sql.types import StringType
from delta.tables import DeltaTable

# COMMAND ----------

dbutils.widgets.text(name="title", defaultValue="inverness")
dbutils.widgets.text(name="product_id", defaultValue="d885935758854604911bbeb027f15dca")
# dbutils.widgets.text(name="product_group_id", defaultValue="9bd01e5b8d924235aeb50423c88a70f5")
dbutils.widgets.text(name="sandbox_id", defaultValue="d33014dcc58347199be674b14dd41467")
dbutils.widgets.text(name="environment", defaultValue="_dev")
dbutils.widgets.text(name="backfill", defaultValue="false")

# COMMAND ----------

title = dbutils.widgets.get("title")
product_id = dbutils.widgets.get("product_id")
sandbox_id = dbutils.widgets.get("sandbox_id")
environment = dbutils.widgets.get("environment")
backfill = dbutils.widgets.get("backfill")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# COMMAND ----------

def get_auth(title):
    """
    This function posts to the sso endpoint to retrieve a token for authentication on the e-commerce catalog API
    Returns the auth string 'Bearer <token>'
    """
    logger.debug(f"Pulling auth for {title}")
    app_id = dbutils.secrets.get(scope="data-engineering", key="sso_api_app_id")
    instance_id = dbutils.secrets.get(scope="data-engineering", key="sso_api_instance_id")
    token = dbutils.secrets.get(scope="data-engineering", key=f"{title}_promos_api_token")
    base_url = 'https://sso.api.2kcoretech.online/sso/v2.0/auth/tokens'

    headers = {
        'Authorization': 'Basic {}'.format(token), 
        'Content-Type': 'application/json'
    }

    data = {
        'locale': 'en-US',
        'accountType': 'server',
        'credentials': {
            'type': 'server',
            'instanceId': '{}'.format(instance_id)
        }
    }

    token = requests.post(base_url, headers=headers, data=json.dumps(data)).json()
    authorization = "Bearer " + token['accessToken']

    return authorization

# COMMAND ----------

def retrieve_promotions(title, product_id, sandbox_id, backfill, **kwargs):
    """
    retrieves the entire promotions list  for the given product id
    pulls 100 records per page and sleeps 3 seconds in between each page to avoid overloading the api
    NOTE: every product will have its own connection with different credentials, so it is required to add those to airflow
    """
    dt = datetime.today().date()
    next_dt = dt + timedelta(days=1)
    prev_dt = dt - timedelta(days=2)
    logger.info(f"Pulling promos data from the api for {title} for the range {prev_dt} to {next_dt}")
    if backfill == "true":
        url = f'https://promotions-ew.2kcoretech.online/products/{product_id}/sandboxes/{sandbox_id}/promos'
    else:
        url = f'https://promotions-ew.2kcoretech.online/products/{product_id}/sandboxes/{sandbox_id}/promos?includeFilter={{%0A%20%20%22modifiedAt_gte%22%3A%20%22{prev_dt}T07%3A59%3A00Z%22%2C%0A%20%20%22modifiedAt_lte%22%3A%20%22{next_dt}T08%3A00%3A00Z%22%0A}}'

    logger.debug(f"Promos endpoint: {url}")
    authorization = get_auth(title)
    promos = {}
    new_results = True
    page = 0

    while new_results:
        headers = {'Content-Type': 'application/json', 'Authorization': authorization,
                   'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36'}
        res = requests.request("GET", url, headers=headers, data={})
        new_results = res.headers.get('X-2k-Result-More') == 'true'
        promos.update(res.json())
        page += 1
        sleep(3)

    return promos

# COMMAND ----------

def create_promos_table(title, environment):
    (
        DeltaTable.createIfNotExists(spark)
        .tableName(f"{title}{environment}.reference.promotions")
        .addColumn("id", "string")
        .addColumn("offer_id", "string")
        .addColumn("name", "string")
        .addColumn("promo_type", "string")
        .addColumn("product_id", "string")
        .addColumn("created_at", "timestamp")
        .addColumn("modified_at", "timestamp")
        .addColumn("json", "string")
        .addColumn("dw_insert_ts", "timestamp")
        .addColumn("dw_update_ts", "timestamp")
        .property('delta.enableIcebergCompatV2', 'true')
        .property('delta.universalFormat.enabledFormats', 'iceberg')
        .execute()
    )

# COMMAND ----------

def load_promotions(title, environment, promo_dict):
    logger.info(f"Loading promos data for {title} into {title}{environment}.reference.promotions")

    # iterate over the data array and dump the json
    rows = []
    for rec in promo_dict['data']:
        rows.append(json.dumps(rec))
    
    df = spark.createDataFrame(rows, StringType())
    ndf = (
        df
        .selectExpr(
            "get_json_object(value, '$.id') as id",
            "get_json_object(value, '$.offerId') as offer_id",
            "get_json_object(value, '$.name') as name",
            "get_json_object(value, '$.promoType') as promo_type",
            "get_json_object(value, '$.productId') as product_id",
            "get_json_object(value, '$.createdAt')::timestamp as created_at",
            "get_json_object(value, '$.modifiedAt')::timestamp as modified_at",
            "value as json",
            "current_timestamp() as dw_insert_ts",
            "current_timestamp() as dw_update_ts"
        )
    )

    final_table = DeltaTable.forName(spark, f"{title}{environment}.reference.promotions")

    (
        final_table.alias("tgt")
        .merge(ndf.alias("src"), "tgt.id = src.id")
        .whenMatchedUpdate(set = {
            "tgt.offer_id": "src.offer_id",
            "tgt.name": "src.name",
            "tgt.promo_type": "src.promo_type",
            "tgt.product_id": "src.product_id",
            "tgt.created_at": "src.created_at",
            "tgt.modified_at": "src.modified_at",
            "tgt.json": "src.json",
            "tgt.dw_update_ts": "current_timestamp()"
        })
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

def run():
    create_promos_table(title, environment)
    promo_dict = retrieve_promotions(title, product_id, sandbox_id, backfill)
    if promo_dict['data'] is not None:
        load_promotions(title, environment, promo_dict)
        logger.info(f"Finished loading promos data for {title} into {title}{environment}.reference.promotions")
    else:
        logger.info(f"No new or modified promotions found for {title}")

# COMMAND ----------

run()
