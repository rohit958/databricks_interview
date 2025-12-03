# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
schema1= StructType([StructField("Order_ID", IntegerType()),
    StructField("Order_Date",DateType()),
    StructField("Order_value",IntegerType())
])

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

path="dbfs:/FileStore/file-1.csv"
df=spark.read.option("sep","\t").option("header","false").option("mode","permissive").option("dateFormat","dd-MM-YYYY").csv(path,schema1)

# COMMAND ----------

df.show()

# COMMAND ----------

df.createOrReplaceTempView("temp_tbl")
sql="""with cte as(select Order_Date,sum(order_value)as total from temp_tbl group by Order_Date order by Order_Date)
    select Order_Date, sum(total) over(rows between unbounded preceding and current row) as running_total from cte
    ;
    """
spark.sql(sql).show()

# COMMAND ----------

from pyspark.sql.window import Window

windowSpec=Window.partitionBy().rowsBetween(Window.unboundedPreceding,Window.currentRow)

df1=df.groupBy(col("Order_Date")).agg(sum(col("Order_value")).alias("Total")).orderBy(col("Order_Date"))
df1.withColumn("Running_Total",sum("Total").over(windowSpec)).show()

# COMMAND ----------

from pyspark.sql.window import Window

windowSpec=Window.partitionBy().orderBy(col("Order_Date")).rowsBetween(Window.unboundedPreceding,0)
windowTotal=Window.partitionBy("Order_Date")

df1=df.withColumn("Total",sum("Order_value").over(windowTotal)).cache()
df2=df1.select("Order_date","Total").distinct()



df3=df2.withColumn("Running_Total",sum("Total").over(windowSpec))
df3.show()
