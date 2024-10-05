# Databricks notebook source
listings = spark.read.table('taproot_training.silver.listings_clean')
display(listings)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT neighbourhood, COUNT(*) AS total_listings
# MAGIC FROM taproot_training.silver.listings_clean
# MAGIC GROUP BY neighbourhood
# MAGIC ORDER BY total_listings DESC;
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC select month(last_review) as month_of_review from taproot_training.silver.listings_clean;