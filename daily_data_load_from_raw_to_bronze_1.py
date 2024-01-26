# Databricks notebook source
scope = 'scope-cric-analysis'
storage_account_name = dbutils.secrets.get(scope='scope-cric-analysis', key='storage-account')
sas_token = dbutils.secrets.get(scope, 'storage-account-sas-token')

spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account_name}.dfs.core.windows.net", sas_token)

# COMMAND ----------


