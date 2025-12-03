# Databricks notebook source
# MAGIC %md
# MAGIC Intergrating Pytest for unit testing of functions in Pyspark

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------



sales_data = [

(1,1,'manish','2023-01-16','gurgaon','india',380),
(77,1,'manish','2023-03-11','bangalore','india',300),
(12,3,'nikita','2023-09-20','delhi','india',127),
(54,4,'rakesh','2023-08-10','jaipur','india',321),
(65,5,'ayush','2023-09-07','mosco','russia',765),
(89,6,'rajat','2023-08-10','jaipur','india',321)
]

sales_schema = ['sales_id', 'customer_id','customer_name', 'sales_date', 'food_delivery_address','food_delivery_country', 'food_cost']

sales_df = spark.createDataFrame(data=sales_data,schema=sales_schema)

# sales- fact table

# COMMAND ----------

#creating function to add 5% GST on food cost and then calculate total cost

def add_gst(df,gst_rate):
    total_gst_rate=1+ (gst_rate/100)
    result_df=df.withColumn('food_cost_with_gst',df['food_cost'] * total_gst_rate)
    return result_df

  
added_gst_df=add_gst(sales_df,5)
display(added_gst_df)


# COMMAND ----------

# MAGIC %md
# MAGIC defining and testing UDF

# COMMAND ----------

#test gst_rate_function

from pyspark.errors import PySparkException

def test_gst_rate_function():
    try:
        test_df = spark.createDataFrame([(1,10000),(2,20000),(3,30000),(4,40000)], ['sales_id','food_cost'])
        expected_df = spark.createDataFrame([(1,10000,10500),(2,20000,21000),(3,30000,31500),(4,40000,42000)], ['sales_id','food_cost','food_cost_with_gst'])
        actual_df = add_gst(test_df, 5)
        print("Input DataFrame:")
        display(test_df) #optional display to see the input dataframe
        
        print("Expected DataFrame:")
        display(expected_df) #optional display to see the expected dataframe
        print("Actual DataFrame after GST applied:")
        display(actual_df) #optional display to see the actual dataframe
        assert actual_df.collect() == expected_df.collect()
        print("Test passed: actual_df matches expected_df")
    except PySparkException as ex:
        print("Error Condition   : " + ex.getErrorClass())
        print("Message arguments : " + str(ex.getMessageParameters()))
        print("SQLSTATE          : " + ex.getSqlState())
        print(ex)
    except AssertionError:
        print("Test failed: actual_df does not match expected_df")
    except Exception as e:
        print("Unexpected error:", str(e))

test_gst_rate_function()

# COMMAND ----------


