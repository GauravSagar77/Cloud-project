# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://olympic@storagepro.blob.core.windows.net",
  mount_point = "/mnt/olympic/bronze",
  extra_configs = {"fs.azure.account.key.storagepro.blob.core.windows.net":"DSfoJTrdbhsWVXQms5sdwwzyd8fPnmaIS/aakPPULeW7QZBdVclnDWx9iC/PfSQk9t+NEYWmSg3M+AStPw7k7w=="})

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/olympic/bronze/bronze/

# COMMAND ----------

athletes = spark.read.format("csv").option("header","true").option("inferSchema","true").option("inferschema",True).load("dbfs:/mnt/olympic/bronze/bronze/Athelets.csv")
coaches = spark.read.format("csv").option("header","true").option("inferSchema","true").option("inferschema",True).load("dbfs:/mnt/olympic/bronze/bronze/coaches.csv")
genders = spark.read.format("csv").option("header","true").option("inferSchema","true").option("inferschema",True).load("dbfs:/mnt/olympic/bronze/bronze/entriesgender.csv")
medals = spark.read.format("csv").option("header","true").option("inferSchema","true").option("inferschema",True).load("dbfs:/mnt/olympic/bronze/bronze/medals.csv")
teams = spark.read.format("csv").option("header","true").option("inferSchema","true").option("inferschema",True).load("dbfs:/mnt/olympic/bronze/bronze/teams.csv")

# COMMAND ----------

athletes.show()

# COMMAND ----------

athletes.printSchema()

# COMMAND ----------


