# Databricks notebook source
azureStorageAccountName = "wannadream"
azureStorageContainerName = "validated"
azureFolderName = "data"
mountPath = "/mnt/files/validated"
# Application (Client) ID
applicationId = dbutils.secrects.get(scope="DatabricksResearch",key="clientId")
# Application (Client) Secret
applicationSecret = dbutils.secrets.get(scope="DatabricksResearch",key="clientSecret")
# Directory (Tenant) ID
tenantId = dbutils.secrets.get(scope="DatabricksResearch",key="tenantId")
tokenEndpoint = "https://login.microsoftonline.com/" + tenantId + "/oauth2/token"
source = "abfss://" + azureStorageContainerName + "@" + azureStorageAccountName + ".dfs.core.windows.net/" + azureFolderName
configs = {
    "fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net": "OAuth",
    "fs.azure.account.oauth.provider.type.<storage-account>.dfs.core.windows.net": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id.<storage-account>.dfs.core.windows.net": applicationId,
    "fs.azure.account.oauth2.client.secret.<storage-account>.dfs.core.windows.net", applicationSecret,
    "fs.azure.account.oauth2.client.endpoint.<storage-account>.dfs.core.windows.net", tokenEndpoint
}
# Mount
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts())
    dbutils.fs.mount(
        source=source,
        mount_point=mountPoint,
        extra_configs=configs
    )


# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/files/validated
