# Specify a Databricks configuration profile and
# the cluster_id field separately:
from databricks.connect import DatabricksSession
from databricks.sdk.core import Config

config = Config(
  profile    = "Databricks Connect Config"
  cluster_id = "get_cluster_id"
)

spark = DatabricksSession.builder.sdkConfig(config).getOrCreate()

spark.createDataFrame([("Alice", 6)], ['name', 'age']).createOrReplaceTempView("person")


spark.sql("SELECT * FROM person").show()