# Databricks notebook source
import hashlib
import pyspark.sql.functions as F
import re

scope = 'scope-cric-analysis'
storage_account_name = dbutils.secrets.get(scope='scope-cric-analysis', key='storage-account')
sas_token = dbutils.secrets.get(scope, 'storage-account-sas-token')

spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account_name}.dfs.core.windows.net", sas_token)

# COMMAND ----------

container_name = 'cricsheet-latest-2-day-dump'
base_path = f'abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/latest_2_day/data'

# COMMAND ----------

dbutils.fs.mkdirs(f'abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/bad_records/data/')

# COMMAND ----------

for folder in ['2024-01-26', '2024-01-27', '2024-01-28']:
    dbutils.fs.cp(
        f'abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/latest_2_day/data/{folder}/',
        f'abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/bad_records/data/{folder}/',
        True
    )

# COMMAND ----------

dbutils.fs.ls(f'abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/bad_records/data/{folder}/')
