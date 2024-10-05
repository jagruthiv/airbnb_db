# Databricks notebook source
# MAGIC %run ./common

# COMMAND ----------

reviews = f'{input_path}/reviews_uncomp.csv'
reviews_df = spark.read.format('csv').option("multiline",True).option("header",True).load(reviews)

# COMMAND ----------

display(reviews_df)

# COMMAND ----------

import pyspark.sql.functions as F
reviews_df = reviews_df.withColumn('city_name',F.lit(city)).withColumn('load_date',F.lit(date))

reviews_df.write.mode('overwrite').saveAsTable('taproot_training.bronze.reviews')