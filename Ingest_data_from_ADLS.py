# Databricks notebook source
dbutils.widgets.text("city","Boston","City Name")
dbutils.widgets.text("date","03242024","Date of Load")

# COMMAND ----------

city=dbutils.widgets.get("city")
date=dbutils.widgets.get("date")
print(f"Loading data for {city} on {date}")

# COMMAND ----------

core_input_path= 'abfss://inbounddatacontainer@airbnbstorageaccountjv.dfs.core.windows.net/AirbnbData'
input_path= f'{core_input_path}/{city}/{date}'
print(f"Loading data from {input_path}")

# COMMAND ----------

acc_name= dbutils.secrets.get(scope="airbnb_az",key='storage_account_name')
storage_key = dbutils.secrets.get(scope="airbnb_az",key='storage_key')
spark.conf.set("fs.azure.account.key.{acc_name}.dfs.core.windows.net", storage_key
)

# COMMAND ----------

dbutils.fs.ls(input_path)

# COMMAND ----------

listings= f'{input_path}/listings (1).csv'
listings_df= spark.read.format("csv").option("header", "true").load(listings)

# COMMAND ----------

display(listings_df)

# COMMAND ----------

from pyspark.sql import functions as F

listings_df= listings_df.withColumn("city_name", F.lit(city))
display(listings_df)

# COMMAND ----------

listings_df= listings_df.withColumn("load_date", F.lit(date))
display(listings_df)

# COMMAND ----------

listings_df.write.mode("overwrite").saveAsTable("taproot_training.bronze.listings")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from taproot_training.bronze.listings