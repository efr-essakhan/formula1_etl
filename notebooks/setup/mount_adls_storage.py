# Databricks notebook source
#configuration - note you should put these credentials into azure secrets instead, rather then exposing it like this.

storage_account_name = "formula1strg"
client_id = "17136f9e-cbfa-4417-ba48-ab330f5d4172"
tenant_id = "f262f29c-0252-4f00-bcd2-e5f1428a9b9c"
client_secret = "4Xt7Q~xBJ46ItSdrxyTfDV6erVvIEueJn_7dw"


# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret" : f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint" : f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

def mount_adls(container_name):
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)

# COMMAND ----------

#Mounting our storage raw
mount_adls("raw")

# COMMAND ----------

#checking if file exists now
dbutils.fs.ls("/mnt/formula1strg/raw")

# COMMAND ----------

#list all the mount sin our workspace
dbutils.fs.mounts()

# COMMAND ----------

#Mounting our storage processed
mount_adls("processed")

# COMMAND ----------


