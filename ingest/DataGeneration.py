# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

import csv
from faker import Faker

# COMMAND ----------

number_of_records = 100

# COMMAND ----------

fake = Faker()

fake_records = []
i = 0
while i < number_of_records:
  record = {"name": fake.name(), "home address": fake.address().replace('\n', ', ')}
  fake_records.append(record)
  i += 1

fake_records

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC mkdir /dbfs/tmp/fake_records/raw

# COMMAND ----------

# DBTITLE 1,Writing the CSV file to the driver node, but writing to a DBFS mount point '/dbfs'
fields = ["name", "home address"]

with open('/dbfs/tmp/fake_records/raw/fake_records.csv', 'w', newline='') as file: 
    writer = csv.DictWriter(file, fieldnames = fields)
    writer.writeheader() 
    writer.writerows(fake_records)


# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls /tmp/fake_records/raw/fake_records.csv

# COMMAND ----------

# DBTITLE 1,Show content from the driver node point of view
# MAGIC %sh
# MAGIC 
# MAGIC less /dbfs/tmp/fake_records/raw/fake_records.csv

# COMMAND ----------

display(spark.read.option("header", "true").csv('/tmp/fake_records/raw/fake_records.csv'))
