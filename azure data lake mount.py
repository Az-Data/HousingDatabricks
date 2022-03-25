# Databricks notebook source


# COMMAND ----------

# Gather relevant keys
ServicePrincipalID = ""
ServivePrincipalKey = ""
DirectoryID = ""

# Combine DirectoryID into full string
Directory = f"http://login.microsoftonlin.com/{DirectoryID}/oauth2/token"

# Create configurations for our connections 
configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": ServicePrincipalID,
    "fs.azure.account.oauth2.client.id": ServicePrincipalKey,
    "fs.azure.account.oauth2.client.endpoint": Directory
}

# Mount the Data Lake onto DBFS at the /mnt/Lake location
dbutils.fs.mount(
    source = "adfss://deltalake@deltalake.dfs.core.windows.net/",
    mount_point = "/mnt/Lake",
    extra_configs = configs
)
