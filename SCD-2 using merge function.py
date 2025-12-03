# Databricks notebook source
# MAGIC %sql
# MAGIC drop table dim_customer;
# MAGIC  CREATE TABLE IF NOT EXISTS dim_customer (
# MAGIC     customer_id INT,
# MAGIC     customer_name STRING,
# MAGIC     city STRING,
# MAGIC     state STRING,
# MAGIC     start_date DATE,
# MAGIC     end_date DATE,
# MAGIC     is_current BOOLEAN
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS stg_customer (
# MAGIC     customer_id INT,
# MAGIC     customer_name STRING,
# MAGIC     city STRING,
# MAGIC     state STRING,
# MAGIC     load_date DATE
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC --delete from stg_customer;
# MAGIC INSERT INTO stg_customer VALUES
# MAGIC (101, 'John Smith', 'Pune', 'Maharashtra', DATE '2024-01-01'),
# MAGIC (102, 'Priya Patel', 'Bangalore', 'Karnataka', DATE '2024-01-01'),
# MAGIC (103, 'Amit Verma', 'Delhi', 'Delhi', DATE '2024-01-01');

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC -- Initial insert into dimension
# MAGIC --delete from dim_customer;
# MAGIC INSERT INTO dim_customer
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   customer_name,
# MAGIC   city,
# MAGIC   state,
# MAGIC   load_date AS start_date,
# MAGIC   NULL AS end_date,
# MAGIC   TRUE AS is_current
# MAGIC FROM stg_customer;

# COMMAND ----------

# MAGIC %md
# MAGIC new day load detected
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE stg_customer;
# MAGIC
# MAGIC INSERT INTO stg_customer VALUES
# MAGIC (101, 'John Smith', 'Mumbai', 'Maharashtra', DATE '2024-03-10'),  -- changed
# MAGIC (102, 'Priya Patel', 'Bangalore', 'Karnataka', DATE '2024-03-10'), -- no change
# MAGIC (104, 'Neha Sharma', 'Hyderabad', 'Telangana', DATE '2024-03-10'); -- new

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO dim_customer AS tgt
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         customer_id,
# MAGIC         customer_name,
# MAGIC         city,
# MAGIC         state,
# MAGIC         load_date AS start_date
# MAGIC     FROM stg_customer
# MAGIC ) AS src
# MAGIC ON tgt.customer_id = src.customer_id AND tgt.is_current = TRUE
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC        tgt.customer_name <> src.customer_name
# MAGIC     OR tgt.city <> src.city
# MAGIC     OR tgt.state <> src.state
# MAGIC ) THEN
# MAGIC   -- 1️⃣ Close the old record
# MAGIC   UPDATE SET
# MAGIC     tgt.end_date = DATEADD(day, -1, src.start_date),
# MAGIC     tgt.is_current = FALSE
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   -- 2️⃣ Insert new or changed record
# MAGIC   INSERT (
# MAGIC       customer_id,
# MAGIC       customer_name,
# MAGIC       city,
# MAGIC       state,
# MAGIC       start_date,
# MAGIC       end_date,
# MAGIC       is_current
# MAGIC   )
# MAGIC   VALUES (
# MAGIC       src.customer_id,
# MAGIC       src.customer_name,
# MAGIC       src.city,
# MAGIC       src.state,
# MAGIC       src.start_date,
# MAGIC       NULL,
# MAGIC       TRUE
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dim_customer ORDER BY customer_id, start_date;

# COMMAND ----------


