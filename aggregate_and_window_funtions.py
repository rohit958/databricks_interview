# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# creating sample dataframe
simpleData = (("James", "Sales", 3000), \
    ("Michael", "Sales", 4600),  \
    ("Robert", "Sales", 4100),   \
    ("Maria", "Finance", 3000),  \
    ("James", "Sales", 3000),    \
    ("Scott", "Finance", 3300),  \
    ("Jen", "Finance", 3900),    \
    ("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000),\
    ("Saif", "Sales", 4100) \
  )
 
columns= ["employee_name", "department", "salary"]
df = spark.createDataFrame(data = simpleData, schema = columns)
#group by and aggregation
from pyspark.sql.functions import sum,avg,max,count
df2=df.select.groupBy("department").agg(avg("salary")).show()


# COMMAND ----------

# filtering
df1= df.filter(col("salary")>3000)


# COMMAND ----------

# 3rd highest salary in each department
from pyspark.sql.window import *
from pyspark.sql.functions import *

windowSpec= Window.partitionBy("department").orderBy(desc(col("salary")))
df2=df.withColumn("rank",dense_rank().over(windowSpec))
df2.select("*").where(df2.rank==3).show()

# COMMAND ----------

#group by and aggregation
from pyspark.sql.functions import sum,avg,max,count
df2=df.select.groupBy("department").agg(avg("salary")).show()

# COMMAND ----------

#cummalative sum

windowSpec1= Window.partitionBy("department").orderBy("salary").rangeBetween(Window.unboundedPreceding, 0)
df3=df.withColumn("CummSum",sum("salary").over(windowSpec1))
df3.show()

# COMMAND ----------


