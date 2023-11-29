# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://olympic@storagepro.blob.core.windows.net",
  mount_point = "/mnt/olympic/bronze",
  extra_configs = {"fs.azure.account.key.storagepro.blob.core.windows.net":"DSfoJTrdbhsWVXQms5sdwwzyd8fPnmaIS/aakPPULeW7QZBdVclnDWx9iC/PfSQk9t+NEYWmSg3M+AStPw7k7w=="})

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://silver@storagepro.blob.core.windows.net",
  mount_point = "/mnt/silver",
  extra_configs = {"fs.azure.account.key.storagepro.blob.core.windows.net":"DSfoJTrdbhsWVXQms5sdwwzyd8fPnmaIS/aakPPULeW7QZBdVclnDWx9iC/PfSQk9t+NEYWmSg3M+AStPw7k7w=="})

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/olympic/bronze/

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

genders.show()

# COMMAND ----------

genders.printSchema()

# COMMAND ----------



genders = genders.withColumn("Female",col("Female").cast(IntegerType()))\
    .withColumn("Male",col("Male").cast(IntegerType()))\
    .withColumn("Total",col("Total").cast(IntegerType()))

# COMMAND ----------

genders.printSchema()

# COMMAND ----------

medals.show(5)

# COMMAND ----------

medals.printSchema()

# COMMAND ----------

teams.show()

# COMMAND ----------

teams.printSchema()

# COMMAND ----------

# Find the top countries with the highest number of gold medals
top_countries_with_goldmedals = medals.orderBy("Gold", ascending=False).select("Team_Country", col("Gold").alias("Gold_medals")).show()

# COMMAND ----------

# Calculate the average number of entries by gender for each sports
average_entries_by_gender = genders.withColumn(
    'Avg_Female', genders['Female'] / genders['Total']
).withColumn(
    'Avg_Male', genders['Male'] / genders['Total']
)
average_entries_by_gender.show()

# COMMAND ----------

athletes.repartition(1).write.mode("overwrite").option("headers",True).csv("/mnt/silver/Athletes")

# COMMAND ----------

genders.repartition(1).write.mode("overwrite").option("headers",True).csv("/mnt/silver/EntiresGender")
coaches.repartition(1).write.mode("overwrite").option("headers",True).csv("/mnt/silver/Coaches")
medals.repartition(1).write.mode("overwrite").option("headers",True).csv("/mnt/silver/medals")
teams.repartition(1).write.mode("overwrite").option("headers",True).csv("/mnt/silver/Teams")

# COMMAND ----------



# COMMAND ----------


