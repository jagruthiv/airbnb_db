# Databricks notebook source
# MAGIC %md
# MAGIC ##Notebook for ADLS Connectivity
# MAGIC - set up ADLS credentials
# MAGIC - use spark to read from storage container
# MAGIC - write output to storage container

# COMMAND ----------

# DBTITLE 1,Establish connection to storage
spark.conf.set(
    "fs.azure.account.key.jvtaproottraining.dfs.core.windows.net", "ntJxuX2jKfsU4KLL12ezv7dWhj4NxOEWmS6oeDjPIaKWKPxrIIwYSOyUtzoQ3GGnA5I5XifcalEC+ASt3dyseA=="
    )

# COMMAND ----------

dbutils.fs.ls("abfss://inputdatacontainer@jvtaproottraining.dfs.core.windows.net/AirbnbData")

# COMMAND ----------

# MAGIC %fs ls abfss://inputdatacontainer@jvtaproottraining.dfs.core.windows.net/AirbnbData/Boston/03242024/

# COMMAND ----------

spark

# COMMAND ----------

file_path = "abfss://inputdatacontainer@jvtaproottraining.dfs.core.windows.net/AirbnbData/Boston/03242024/listings (1).csv"
l_df= spark.read.csv(file_path, header=True)
display(l_df)

# COMMAND ----------

