# Databricks notebook source
import hashlib
import pyspark.sql.functions as F
import pyspark.sql.types as T
import re
from datetime import datetime, timedelta

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

incremental_load_lookup = spark.read.load(f'abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/incremental_load_lookup/data')
max_ingested_date = incremental_load_lookup.select(F.max("ingested_date")).collect()[0][0]

# COMMAND ----------

folders = (folder for folder in dbutils.fs.ls(base_path) if datetime.strptime(folder.name[:-1], "%Y-%m-%d").date() > max_ingested_date)
zip_roots = (dbutils.fs.ls(folder.path)[0].path for folder in folders)
read_mes = (list(filter(lambda x: 'README' in x.name, dbutils.fs.ls(zip_root)))[0] for zip_root in zip_roots)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read all new folders

# COMMAND ----------

read_me_fields = ['start_date', 'team_type', 'match_type', 'gender', 'match_id', 'match_name']
read_me_df = None
read_me_paths = []
i = 0

while True:
    try:
        read_me = next(read_mes).path
        df = spark.read.schema("line string").csv(read_me).filter(
            "line regexp '[0-9]{4}\-[0-9]{2}\-[0-9]{2} \- .*'"
        ).select(
            F.split('line', ' - ').alias("deets")
        ).select(
            F.lit(read_me.split('data/')[1].split('/')[0]).cast('date').alias('ingested_date'),
            *[F.col("deets")[i].alias(v) for i, v in enumerate(read_me_fields)]
        )

        if read_me_df:
            read_me_df = read_me_df.union(df)
        else:
            read_me_df = df
        read_me_paths += [read_me.replace('README.txt', '*.json')]
        print(read_me)
    except Exception as e:
        print(e)
        break

# COMMAND ----------

data_df = spark.read.option("multiLine", True).json(read_me_paths)

# COMMAND ----------

transformed_df = data_df.select(
    F.col("info.dates")[0].alias("start_date"),
    "info.team_type",
    "info.match_type",
    "info.gender",
    F.array_join("info.teams", " vs ").alias("match_name"),
    "*"
)

# COMMAND ----------

joined_df = transformed_df.alias("t").join(
    read_me_df.alias('r'),
    on=[i for i in read_me_fields if i not in ('match_type', 'match_id')]
).select(
    "r.ingested_date",
    "r.match_id",
    "t.*"
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### exploring info field

# COMMAND ----------

get_team_players = F.udf(lambda team, players: players[team], T.ArrayType(T.StringType()))

# COMMAND ----------

info_df = joined_df.select(
    F.current_timestamp().alias("processed_timestamp"),
    "ingested_date",
    'match_id',
    'start_date',
    'team_type',
    'gender',
    'match_name',
    'match_type',
    F.col("info.teams")[0].alias("team_1"),
    F.col("info.teams")[1].alias("team_2"),
    get_team_players(F.split("match_name", " vs ")[0], "info.players").alias("players_team_1"),
    get_team_players(F.split("match_name", " vs ")[1], "info.players").alias("players_team_2"),
    F.col("info").dropFields("players", "registry").alias("info")
)

# COMMAND ----------

info_df.write.format('delta').mode('append').save(f'abfss://bronze0@{storage_account_name}.dfs.core.windows.net/info/data')

# COMMAND ----------

# MAGIC %md
# MAGIC #### exploring innings field

# COMMAND ----------

innings_df = joined_df.select(
    F.current_timestamp().alias("processed_timestamp"),
    *joined_df.columns[:6],
    "innings"
)

# COMMAND ----------

# innings_df.write.format('delta').mode('append').save(f'abfss://bronze0@{storage_account_name}.dfs.core.windows.net/innings/data')

# COMMAND ----------

# MAGIC %md
# MAGIC #### exploring registry

# COMMAND ----------

registry_df = spark.createDataFrame([[i.name] for i in joined_df.select("info.registry.people.*").schema], ['name'])

# COMMAND ----------

registry_df.select("name").distinct().write.format('delta').mode('append').save(f'abfss://bronze0@{storage_account_name}.dfs.core.windows.net/registry/data')

# COMMAND ----------

registry_target = spark.read.load(f'abfss://bronze0@{storage_account_name}.dfs.core.windows.net/registry/data')

# COMMAND ----------

# MAGIC %md
# MAGIC #### update lookup

# COMMAND ----------

try:
    spark.createDataFrame(
        [[i.split("data/")[1].split("/")[0]] for i in read_me_paths], 
        ["ingested_date"]
    ).select(
        F.col("ingested_date").cast("date")
    ).write.format("delta").mode("append").save(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/incremental_load_lookup/data")
except Exception as e:
    print(e)
