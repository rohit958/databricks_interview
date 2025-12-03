# Databricks notebook source
path="dbfs:/FileStore/sample3.txt"

from pyspark.sql.functions import *
df=spark.read.text(path)
df.show()

# COMMAND ----------

#count number of words
df.printSchema()

# COMMAND ----------

df1=df.select(split(col("value")," ").alias("words"))
df1.show()

# COMMAND ----------

df2=df1.select(explode(col("words")).alias("wordtemp"))
df2=df2.withColumn("words",regexp_replace(col("wordtemp"),r"[^a-zA-Z0-9]",""))
df3=df2.drop("wordtemp")
df3.groupby(col("words")).agg(count("*").alias("counts")).orderBy(col("counts").desc()).show()

# COMMAND ----------


