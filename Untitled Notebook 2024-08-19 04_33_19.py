# Databricks notebook source
df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("cloudFiles.schemaLocation", "/dbfs/FileStore/checkpoint") \
    .load("dbfs:/FileStore/Stream_output/")

# COMMAND ----------

result_df = df.writeStream.format("console").start()
display(df)

# COMMAND ----------

result_df.stop()

# COMMAND ----------


