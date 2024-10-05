# Databricks notebook source
reviews= spark.read.table("taproot_training.bronze.reviews")

# COMMAND ----------

display(reviews)

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, lower
reviews= reviews.withColumn("comments", regexp_replace(reviews["comments"], "<br/>", ""))
reviews = reviews.withColumn("comments", lower(regexp_replace(reviews["comments"], "[^a-zA-Z0-9 ]", "")))



# COMMAND ----------

display(reviews)

# COMMAND ----------

reviews.write.mode("overwrite").saveAsTable('taproot_training.silver.reviews_clean')