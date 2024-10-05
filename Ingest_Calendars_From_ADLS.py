# Databricks notebook source
# MAGIC %run ./common

# COMMAND ----------

calendars = f'{input_path}/calendar.csv.gz'
calendars_df = spark.read.format('csv').option("multiline",True).option("header",True).load(calendars)

# COMMAND ----------

display(calendars_df)

# COMMAND ----------

import pyspark.sql.functions as F
calendars_df = calendars_df.withColumn('city_name',F.lit(city)).withColumn('load_date',F.lit(date))

calendars_df.write.mode('overwrite').saveAsTable('taproot_training.bronze.calendars')

# COMMAND ----------

