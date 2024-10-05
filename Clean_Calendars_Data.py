# Databricks notebook source
calendars= spark.read.table("taproot_training.bronze.calendars")
display(calendars)

# COMMAND ----------

from pyspark.sql.functions import col, translate
calendars = calendars.withColumn("price", translate(col("price"), "$", "").cast("double")).withColumn("minimum_nights", col("minimum_nights").cast("int")).withColumn("maximum_nights", col("maximum_nights").cast("int")).drop("adjusted_price")
display(calendars)

# COMMAND ----------

display(calendars.describe())

# COMMAND ----------

calendars= calendars.filter(col("minimum_nights") < 365)

# COMMAND ----------

calendars.write.mode("overwrite").saveAsTable("taproot_training.silver.calendars_clean")