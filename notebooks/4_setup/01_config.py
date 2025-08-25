# Databricks notebook source
# MAGIC %md
# MAGIC # Configuration Setup
# MAGIC 
# MAGIC This notebook sets up configuration for all other notebooks.

# COMMAND ----------

# Load Azure configuration
import json

with open('/Workspace/config/azure-config-dev.json', 'r') as f:
    azure_config = json.load(f)

# Set Spark configurations
spark.conf.set("storage_account", azure_config["storageAccount"])
spark.conf.set("resource_group", azure_config["resourceGroup"])
spark.conf.set("databricks_workspace", azure_config["databricksWorkspace"])

# For storage key, use Azure Key Vault or Databricks secrets
# spark.conf.set("storage_key", dbutils.secrets.get(scope="azure-kv", key="storage-key"))

# Alternatively, set manually (for development only)
spark.conf.set("storage_key", "your-storage-account-key-here")

print("Configuration loaded successfully!")
print(f"Storage Account: {azure_config['storageAccount']}")
print(f"Resource Group: {azure_config['resourceGroup']}")