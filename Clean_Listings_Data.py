# Databricks notebook source
# MAGIC %md 
# MAGIC ## Clean the Data that we ingested from the inbound files, Check for null, fix types, join/clean etc.

# COMMAND ----------

# DBTITLE 1,Load the data from the table
listings = spark.read.table('taproot_training.bronze.listings')
display(listings)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Clean up & Fix Data 
# MAGIC - Getting Rid of Extreme values for price, rooms, min nights and other numerical columns
# MAGIC - Handle null values
# MAGIC - Cast to Double all numeric values.
# MAGIC

# COMMAND ----------

display(listings.select('price').describe())

# COMMAND ----------

from pyspark.sql.functions import col,translate

listings = listings.withColumn('price',translate(col('price'), "$",'').cast("double"))
display(listings)

# COMMAND ----------

from pyspark.sql.functions import col

display(listings.filter(col('price').isNull()))

# COMMAND ----------

from pyspark.sql.functions import col
listings = listings.filter(col('price').isNotNull()).filter(col('price') > 0).filter(col('price')<1000)
display(listings)

# COMMAND ----------

display(listings.select('minimum_nights').describe())

# COMMAND ----------

display(listings.groupBy("minimum_nights").count()
        .orderBy(col("count").desc(),col("minimum_nights")))

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC Lets fix the minimum nights to 365 days or less.

# COMMAND ----------

listings = listings.filter(col("minimum_nights") < 365)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T

# Assuming 'spark' is your SparkSession object
listings = spark.sql("""
SELECT *
FROM taproot_training.bronze.listings
WHERE neighbourhood IS NOT NULL
  AND neighbourhood NOT REGEXP '[0-9]'
  AND id NOT REGEXP '[a-zA-Z]'  -- Exclude rows where 'id' contains any alphabetic characters
""")

listings = listings.withColumn('load_date', F.to_date(F.col("load_date"), "yyyy-MM-dd")) \
                   .withColumn('last_review', F.to_date(F.col("last_review"), "yyyy-MM-dd")) \
                   .drop("neighbourhood_group")

# Display the final DataFrame
display(listings)


# COMMAND ----------

from pyspark.sql import Window

# Step 1: Define a window specification to partition by 'room_type' and 'neighbourhood'
window_spec = Window.partitionBy("room_type", "neighbourhood")

# Step 2: Calculate the average price within the window
listings = listings.withColumn(
    "avg_price", F.avg("price").over(window_spec)
)

# Step 3: Replace the NULL values in the 'price' column with the average price
listings = listings.withColumn(
    "price",
    F.when(F.col("price").isNull(), F.col("avg_price"))  # Replace NULLs with avg_price
     .otherwise(F.col("price"))  # Keep the original price if not NULL
)

# Step 4: Drop the temporary 'avg_price' column (since it's no longer needed)
listings = listings.drop("avg_price")

# Display the updated DataFrame
display(listings)



# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType

# Cast the 'price' column to DoubleType after the SQL query
listings = listings.withColumn("price", col("price").cast(DoubleType()))

# COMMAND ----------

listings.write.mode('overwrite').saveAsTable('taproot_training.silver.listings_clean')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from taproot_training.silver.listings_clean