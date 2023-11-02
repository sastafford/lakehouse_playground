from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Format, Disposition
from datetime import date

import argparse
import logging
import os
import pandas as pd
import requests
import sys

log = logging.getLogger("to_excel")
today = date.today()

logging.basicConfig(
    handlers=[
        logging.FileHandler(
            filename="{}-to_excel.log".format(today), 
            encoding='utf-8', 
            mode='a+'
        ),
        logging.StreamHandler()
    ],
    format="%(asctime)s:%(name)s:%(levelname)s:%(message)s", 
    datefmt="%Y-%m-%dT%H-%M-%S",
    level=logging.DEBUG
)

parser = argparse.ArgumentParser()
parser.add_argument('catalog', type=str, help='Name of catalog')
parser.add_argument('schema', type=str, help='Name of schema' )
parser.add_argument('sql', type=str, help="SELECT SQL statement")
parser.add_argument("warehouse_id", type=str, help="Databricks SQL Warehouse ID")
args = parser.parse_args()

catalog = args.catalog
schema = args.schema
sql = args.sql
warehouse_id = args.warehouse_id

log.debug(catalog)
log.debug(schema)
log.debug(sql)
log.debug(warehouse_id)

if (not sql.startswith("SELECT")):
    log.info(sql)
    sys.exit("SQL statement must be SELECT")

w = WorkspaceClient(profile="fe")

statement = w.statement_execution

response = statement.execute_statement(
    sql,
    warehouse_id=warehouse_id,
    catalog=catalog,
    schema=schema,
    format=Format.CSV,
    disposition=Disposition.EXTERNAL_LINKS
)

url = response.result.external_links[0].external_link

csv_response = requests.get(url)
f = open("out.csv", "w")
f.write(csv_response.text)
f.close()

df = pd.read_csv("out.csv")
df.to_excel("out.xlsx", "Sheet1")
os.remove("out.csv")
log.info("Data exported to out.xlsx")