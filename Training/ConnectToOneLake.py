# Databricks notebook source
# MAGIC %md
# MAGIC ### OPTION 1 - Straight to Lakehouse

# COMMAND ----------

# file_path = "dbfs:/FileStore/2019.csv"
one_lake_url = "abfss://Dev@onelake.dfs.fabric.microsoft.com/TrainingLakeHouse.Lakehouse"
df = spark.read.csv(one_lake_url + '/Files/home_school_district.csv', header=True, inferSchema=True)
df.show(5)

# COMMAND ----------

df.write.format("delta").mode("append").save(one_lake_url + "/Tables/SchoolDistricts")

# COMMAND ----------

# MAGIC %md
# MAGIC ### OPTION 2 - Via Storage Account

# COMMAND ----------

# file_path = "dbfs:/FileStore/2019.csv"
one_lake_url = "abfss://Dev@onelake.dfs.fabric.microsoft.com/TrainingLakeHouse.Lakehouse"
df = spark.read.csv(one_lake_url + '/Files/home_school_district.csv', header=True, inferSchema=True)
df.show(5)

# COMMAND ----------

secret = dbutils.secrets.get(scope="dev", key="Secret")
tenant_id = dbutils.secrets.get(scope="dev", key="TenantId")
client_id = dbutils.secrets.get(scope="dev", key="ClientId")


spark.conf.set("fs.azure.account.auth.type.medallionzanorth.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.medallionzanorth.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.medallionzanorth.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.medallionzanorth.dfs.core.windows.net", secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.medallionzanorth.dfs.core.windows.net", "https://login.microsoftonline.com/" + tenant_id + "/oauth2/token")

blob_url = "abfss://bronze@medallionzanorth.dfs.core.windows.net/home_school_district"

df.write.format("delta").mode("overwrite").save(blob_url)
