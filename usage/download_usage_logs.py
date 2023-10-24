# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Tracking Databricks Units (DBU's)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Option 1: System Tables (Best Option)
# MAGIC
# MAGIC [Instructions](https://docs.databricks.com/en/administration-guide/system-tables/index.html) 
# MAGIC
# MAGIC [Sample Notebooks](https://www.databricks.com/resources/demos/tutorials/governance/system-tables)
# MAGIC
# MAGIC [Billable Usage Schema](https://docs.databricks.com/en/administration-guide/system-tables/billing.html)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Option 2: View billable usage using the account console 
# MAGIC
# MAGIC [Instructions](https://docs.databricks.com/en/administration-guide/account-settings/usage.html)
# MAGIC
# MAGIC This includes [instructions on downloading usage data](https://docs.databricks.com/en/administration-guide/account-settings/usage.html#usage-downloads).  

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Option 3: Download usage logs with the Account API
# MAGIC
# MAGIC Follow the guided instructions below.  
# MAGIC
# MAGIC [GET /api/2.0/accounts/{account_id}/usage/download](https://docs.databricks.com/api/account/billableusage/download)
# MAGIC
# MAGIC To authenticate to the Account API, you can use Databricks OAuth tokens for [service principals](https://docs.databricks.com/dev-tools/service-principals.html) or an account admin’s username and password. [Databricks strongly recommends that you use OAuth tokens for service principals.](https://docs.databricks.com/dev-tools/authentication-oauth.html)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Retrieve your Databricks Account ID
# MAGIC
# MAGIC [Instructions](https://docs.databricks.com/en/administration-guide/account-settings/index.html#locate-your-account-id)

# COMMAND ----------

# DBTITLE 1,Run to activate widgets
from ipywidgets import Text, DatePicker, Checkbox, Layout
style = {'description_width': 'initial'}
layout = Layout(width='75%')

# COMMAND ----------

accountId = Text(
  value="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
  description="Databricks Account Id",
  disabled=False,
  style=style, 
  layout=layout  
)

display(accountId)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Identify a scope to store secrets
# MAGIC
# MAGIC Option 1: Reuse an existing secret scope to store the secret
# MAGIC
# MAGIC Option 2: [Create a scope if one does not exist](https://docs.databricks.com/en/security/secrets/secret-scopes.html)

# COMMAND ----------

secretScope = Text(
  value="x",
  description="Secret Scope",
  disabled=False,
  style=style  
)

display(secretScope)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Create a Service Principal and OAuth credentials
# MAGIC
# MAGIC You must be an account admin to create a service principal and manage OAuth credentials for service principals. 
# MAGIC
# MAGIC [Instructions](https://docs.databricks.com/en/dev-tools/authentication-oauth.html)
# MAGIC
# MAGIC The service principal will need to assume the role of Account Admin.  
# MAGIC
# MAGIC The secret will only be revealed once during creation. The client ID is the same as the service principal’s application ID.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Store OAuth Secret ID and Token into the Databricks Secret Vault
# MAGIC
# MAGIC [Instructions](https://docs.databricks.com/en/security/secrets/secrets.html)
# MAGIC
# MAGIC This step requires use of the [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/index.html). 

# COMMAND ----------

spOAuthSecretId = Text(
  value="observeServicePrincipalOAuthSecretId",
  description="Observe Service Principal OAuth Secret Id",
  disabled=False,
  style=style,
  layout=layout
)

spOAuthSecretToken = Text(
  value="observeServicePrincipalOAuthSecretToken",
  description = "Observe Service Principal OAuth Secret Token",
  disabled=False,
  style=style,
  layout=layout  
)

display(spOAuthSecretId, spOAuthSecretToken)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Select Start and End Date

# COMMAND ----------

startDate = DatePicker(
    description='Start Date',
    disabled=False
)

endDate = DatePicker(
    description='End Date',
    disabled=False
)

display(startDate, endDate)

# COMMAND ----------

import requests
from requests.auth import HTTPBasicAuth

from pyspark.sql import Row 
from pyspark.sql.functions import hash, col
from datetime import date, datetime

import csv

# COMMAND ----------

servicePrincipalOAuthSecretClientId = dbutils.secrets.get(secretScope.value, spOAuthSecretId.value)
servicePrincipalOAuthSecretToken = dbutils.secrets.get(secretScope.value, spOAuthSecretToken.value)
basic = HTTPBasicAuth(servicePrincipalOAuthSecretClientId, servicePrincipalOAuthSecretToken)

url = "https://accounts.cloud.databricks.com/oidc/accounts/{myAccountId}/v1/token".format(myAccountId=accountId.value)

data = {'grant_type': 'client_credentials', 'scope': 'all-apis'}
headers = {'Content-Type': 'application/x-www-form-urlencoded'}

# COMMAND ----------

# DBTITLE 1,Get OAuth token for Service Principal
r = requests.post(url, data=data, headers=headers, auth=basic)
print(r.status_code)

# COMMAND ----------

access_token = r.json()["access_token"]

# COMMAND ----------

queryString = "start_month={startMonth}&end_month={endMonth}&personal_data=false".format(startMonth=str(startDate.value)[0:7], endMonth=str(endDate.value)[0:7])
url = "https://accounts.cloud.databricks.com/api/2.0/accounts/{myAccountId}/usage/download?{queryString}".format(myAccountId=accountId.value, queryString=queryString)
headers = {"Authorization": "Bearer {accessToken}".format(accessToken = access_token)}

# COMMAND ----------

r = requests.get(url, headers=headers)
print(r.status_code)

# COMMAND ----------

out = r.text
lines = out.split("\n")

rows = []
numberOfValidRows = len(lines) - 1 
for line in lines[1:numberOfValidRows]:
  csvRows = csv.reader([line], delimiter=',', quoting=csv.QUOTE_MINIMAL)
  for item in csvRows:
    row = Row(
      workspaceId=item[0], 
      timestamp=item[1], 
      clusterId=item[2], 
      clusterName=item[3], 
      clusterNodeType=item[4],
      clusterOwnerUserId=item[5],
      clusterCustomTags=item[6],
      sku=item[7],
      dbus=item[8],
      machineHours=item[9]
    )
  rows.append(row)

df = spark.createDataFrame(x for x in rows)

display(df)



# COMMAND ----------

df.createOrReplaceTempView("usage")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT date_format(date_trunc("DAY", timestamp), "yyyy-MM-dd") as dt, sku, SUM(dbus) as dbus
# MAGIC FROM usage
# MAGIC GROUP BY ALL
# MAGIC ORDER BY dt
