# Databricks notebook source
# Import libraries
from pyspark.sql.types import *
from contrib.BlobStorageOps import BlobStorage

# Get keys from keyvault
SECRET = dbutils.secrets.get(scope="dev", key="Secret")
TENANT_ID = dbutils.secrets.get(scope="dev", key="TenantId")
CLIENT_ID = dbutils.secrets.get(scope="dev", key="ClientId")

# set the spark context
spark.conf.set("fs.azure.account.auth.type.medallionzanorth.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.medallionzanorth.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.medallionzanorth.dfs.core.windows.net", CLIENT_ID)
spark.conf.set("fs.azure.account.oauth2.client.secret.medallionzanorth.dfs.core.windows.net", SECRET)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.medallionzanorth.dfs.core.windows.net", "https://login.microsoftonline.com/" + TENANT_ID + "/oauth2/token")

# Setting the constant Variables
BLOB_ACCOUNT_NAME = dbutils.secrets.get(scope="dev", key="BlobAccountName")
BLOB_PRIMARY_KEY = dbutils.secrets.get(scope="dev", key="BlobPrimaryKey")
BLOB_CONNECTION_STRING = dbutils.secrets.get(scope="dev", key="BlobConnectionString")
ONE_LAKE_URL = "abfss://Dev@onelake.dfs.fabric.microsoft.com/TrainingLakeHouse.Lakehouse"
CONTAINER = 'bronze'



# COMMAND ----------

# Create the connections
blob = BlobStorage(BLOB_ACCOUNT_NAME, BLOB_PRIMARY_KEY, BLOB_CONNECTION_STRING)
blob.connect_lake_blob()
blob.connect_service_blob()
blob_url = blob.connection_url(CONTAINER)
blob_lst = blob.list_directory_contents(CONTAINER, "orders")

# Create the schema
order_schema = StructType(
    [
        StructField("SalesOrderNumber", StringType()),
        StructField("SalesOrderLineNumber", StringType()),
        StructField("OrderDate", StringType()),
        StructField("CustomerName", StringType()),
        StructField("Email", StringType()),
        StructField("Item", StringType()),
        StructField("Quantity", StringType()),
        StructField("UnitPrice", StringType()),
        StructField("Tax", StringType()),

    ]
)

# Iterate through the list of blobs and write csv to delta parquet
for i in range(len(blob_lst)):
    print(blob_url + '/' + blob_lst[i])
    df = spark.read.schema(order_schema).csv(blob_url + '/' + blob_lst[i])
    df.write.format("delta").mode("append").save(ONE_LAKE_URL + "/Tables/Orders")


# COMMAND ----------


