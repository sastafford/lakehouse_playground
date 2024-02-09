# Databricks notebook source
from IPython.display import display
from ipywidgets import interact, interactive, fixed, interact_manual
import ipywidgets as widgets

catalogs = [row["catalog"] for row in spark.sql("SHOW CATALOGS").collect()]

catalog_w = widgets.Dropdown(
    options=catalogs,
    placeholder='Catalog',
    description='Catalog:',
    disabled=False   
)

schema_w = widgets.Dropdown(
    options=[""],
    placeholder='Schema',
    description='Schema:',
    disabled=False   
)

table_w = widgets.Dropdown(
    options=[""],
    placeholder='Table',
    description='Table:',
    disabled=False   
)

schema_sql = lambda catalog_name: "SHOW SCHEMAS in {catalog}".format(catalog = catalog_name)
schemas = lambda catalog_name: [row["databaseName"] for row in spark.sql(schema_sql(catalog_name)).collect()]

def update_schema_dropdown(*args):
  schema_w.options = schemas(catalog_w.value)

catalog_w.observe(update_schema_dropdown, "value")

table_sql = lambda catalog_name, schema_name: "SHOW TABLES in {catalog}.{schema}".format(catalog = catalog_name, schema = schema_name)
tables = lambda catalog_name, schema_name: [row["tableName"] for row in spark.sql(table_sql(catalog_name, schema_name)).collect()]

def update_table_dropdown(*args):
  table_w.options = tables(catalog_w.value, schema_w.value)

schema_w.observe(update_table_dropdown, "value")

display(catalog_w, schema_w, table_w)


# COMMAND ----------

source_catalog = catalog_w.value
source_schema = schema_w.value
source_table = table_w.value
source_table_fullname = f"{source_catalog}.{source_schema}.{source_table}"


# COMMAND ----------

display(spark.sql(f"SELECT * FROM {source_table_fullname}"))

# COMMAND ----------


