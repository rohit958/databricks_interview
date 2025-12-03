# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, DateType, IntegerType

#spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ##covid cases

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, DateType, IntegerType

spark = SparkSession.builder.getOrCreate()

# Schema
schema = ['date','cases_count']

# Data (same as your SQL INSERT statements)
data = [
('2021-01-01',66),('2021-01-02',41),('2021-01-03',54),('2021-01-04',68),('2021-01-05',16),
('2021-01-06',90),('2021-01-07',34),('2021-01-08',84),('2021-01-09',71),('2021-01-10',14),
('2021-01-11',48),('2021-01-12',72),('2021-01-13',55),

('2021-02-01',38),('2021-02-02',57),('2021-02-03',42),('2021-02-04',61),('2021-02-05',25),
('2021-02-06',78),('2021-02-07',33),('2021-02-08',93),('2021-02-09',62),('2021-02-10',15),
('2021-02-11',52),('2021-02-12',76),('2021-02-13',45),

('2021-03-01',27),('2021-03-02',47),('2021-03-03',36),('2021-03-04',64),('2021-03-05',29),
('2021-03-06',81),('2021-03-07',32),('2021-03-08',89),('2021-03-09',63),('2021-03-10',19),
('2021-03-11',53),('2021-03-12',78),('2021-03-13',49),

('2021-04-01',39),('2021-04-02',58),('2021-04-03',44),('2021-04-04',65),('2021-04-05',30),
('2021-04-06',87),('2021-04-07',37),('2021-04-08',95),('2021-04-09',60),('2021-04-10',13),
('2021-04-11',50),('2021-04-12',74),('2021-04-13',46),

('2021-05-01',28),('2021-05-02',49),('2021-05-03',35),('2021-05-04',67),('2021-05-05',26),
('2021-05-06',82),('2021-05-07',31),('2021-05-08',92),('2021-05-09',61),('2021-05-10',18),
('2021-05-11',54),('2021-05-12',79),('2021-05-13',51),

('2021-06-01',40),('2021-06-02',59),('2021-06-03',43),('2021-06-04',66),('2021-06-05',27),
('2021-06-06',85),('2021-06-07',38),('2021-06-08',94),('2021-06-09',64),('2021-06-10',17),
('2021-06-11',55),('2021-06-12',77),('2021-06-13',48),

('2021-07-01',34),('2021-07-02',50),('2021-07-03',37),('2021-07-04',69),('2021-07-05',32),
('2021-07-06',80),('2021-07-07',33),('2021-07-08',88),('2021-07-09',57),('2021-07-10',21),
('2021-07-11',56),('2021-07-12',73),('2021-07-13',42),

('2021-08-01',41),('2021-08-02',53),('2021-08-03',39),('2021-08-04',62),('2021-08-05',23),
('2021-08-06',83),('2021-08-07',29),('2021-08-08',91),('2021-08-09',59),('2021-08-10',22),
('2021-08-11',51),('2021-08-12',75),('2021-08-13',44),

('2021-09-01',36),('2021-09-02',45),('2021-09-03',40),('2021-09-04',68),('2021-09-05',28),
('2021-09-06',84),('2021-09-07',30),('2021-09-08',90),('2021-09-09',61),('2021-09-10',20),
('2021-09-11',52),('2021-09-12',71),('2021-09-13',43),

('2021-10-01',46),('2021-10-02',58),('2021-10-03',41),('2021-10-04',63),('2021-10-05',24),
('2021-10-06',82),('2021-10-07',34),('2021-10-08',86),('2021-10-09',56),('2021-10-10',14),
('2021-10-11',57),('2021-10-12',70),('2021-10-13',47),

('2021-11-01',31),('2021-11-02',44),('2021-11-03',38),('2021-11-04',67),('2021-11-05',22),
('2021-11-06',79),('2021-11-07',32),('2021-11-08',94),('2021-11-09',60),('2021-11-10',15),
('2021-11-11',54),('2021-11-12',73),('2021-11-13',46),

('2021-12-01',29),('2021-12-02',50),('2021-12-03',42),('2021-12-04',65),('2021-12-05',25),
('2021-12-06',83),('2021-12-07',30),('2021-12-08',93),('2021-12-09',58),('2021-12-10',19),
('2021-12-11',52),('2021-12-12',75),('2021-12-13',48)
]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)


#converting date column to date type
df_cleaned=df.withColumn("date", to_date(col("date")).cast(DateType()))


#converting date column to month
df2=df.withColumn('month',month(df['date']))
#grouping by month
df3=df2.groupBy('month').agg(sum('cases_count').alias('cases_count'))


#cumulative sum
df4=df3.withColumn('Cum_sum',sum(col("cases_count")).over(Window.orderBy(col("month")).rangeBetween(Window.unboundedPreceding, -1)))
display(df4)


# COMMAND ----------

# MAGIC %md
# MAGIC ##Start and end location

# COMMAND ----------

data = [
    ("c1", "New York", "Lima"),
    ("c1", "London", "New York"),
    ("c1", "Lima", "Sao Paulo"),
    ("c1", "Sao Paulo", "New Delhi"),
    ("c2", "Mumbai", "Hyderabad"),
    ("c2", "Surat", "Pune"),
    ("c2", "Hyderabad", "Surat"),
    ("c3", "Kochi", "Kurnool"),
    ("c3", "Lucknow", "Agra"),
    ("c3", "Agra", "Jaipur"),
    ("c3", "Jaipur", "Kochi")
]

columns = ["customer", "start_loc", "end_loc"]

df_loc = spark.createDataFrame(data, columns)
from pyspark.sql import functions as F
from pyspark.sql.window import Window


df_end_locs = df_loc.select(F.col("end_loc").alias("loc")).distinct()

# Find the 'start_loc' that are NOT in the 'end_loc' list (Anti-Join)
df_origin = (
    df_loc.select("customer", F.col("start_loc").alias("initial_location"))
    .distinct()
    .join(
        df_end_locs,
        (F.col("initial_location") == F.col("loc")),
        "left_anti"  
    )
)

# --- 2. Find the True Destination (Final Location) ---

df_start_locs = df_loc.select(F.col("start_loc").alias("loc")).distinct()

# Find the 'end_loc' that are NOT in the 'start_loc' list (Anti-Join)
df_destination = (
    df_loc.select("customer", F.col("end_loc").alias("final_location"))
    .distinct()
    .join(
        df_start_locs,
        (F.col("final_location") == F.col("loc")),
        "left_anti"  
    )
)

# --- 3. Combine Results ---
# Join the origin and destination DataFrames back together by customer.

df_final_robust = df_origin.join(
    df_destination,
    on="customer",
    how="inner"
).select("customer", "initial_location", "final_location")

display(df_final_robust)

# COMMAND ----------

# MAGIC %md
# MAGIC ##discounted families

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark Session (if not already running)
# spark = SparkSession.builder.appName("SQLtoPySpark").getOrCreate()

# --- Create FAMILIES DataFrame ---

# Define the schema for the FAMILIES table
families_schema = StructType([
    StructField("ID", StringType(), True),
    StructField("NAME", StringType(), True),
    StructField("FAMILY_SIZE", IntegerType(), True)
])

# Define the data for the FAMILIES table
families_data = [
    ('c00dac11bde74750b4d207b9c182a85f', 'Alex Thomas', 9),
    ('eb6f2d3426694667ae3e79d6274114a4', 'Chris Gray', 2),
    ('3f7b5b8e835d4e1c8b3e12e964a741f3', 'Emily Johnson', 4),
    ('9a345b079d9f4d3cafb2d4c11d20f8ce', 'Michael Brown', 6),
    ('e0a5f57516024de2a231d09de2cbe9d1', 'Jessica Wilson', 3)
]

# Create the FAMILIES DataFrame
df_families = spark.createDataFrame(families_data, schema=families_schema)

# Show the result (optional)
print("FAMILIES DataFrame:")
df_families.show()
df_families.printSchema()

# --- Create COUNTRIES DataFrame ---

# Define the schema for the COUNTRIES table
countries_schema = StructType([
    StructField("ID", StringType(), True),
    StructField("NAME", StringType(), True),
    StructField("MIN_SIZE", IntegerType(), True),
    StructField("MAX_SIZE", IntegerType(), True)
])

# Define the data for the COUNTRIES table
countries_data = [
    ('023fd23615bd4ff4b2ae0a13ed7efec9', 'Bolivia', 2 , 4),
    ('be247f73de0f4b2d810367cb26941fb9', 'Cook Islands', 4, 8),
    ('3e85ab80a6f84ef3b9068b21dbcc54b3', 'Brazil', 4, 7),
    ('e571e164152c4f7c8413e2734f67b146', 'Australia', 5, 9),
    ('f35a7bb7d44342f7a8a42a53115294a8', 'Canada', 3, 5),
    ('a1b5a4b5fc5f46f891d9040566a78f27', 'Japan', 10, 12)
]

# Create the COUNTRIES DataFrame
df_countries = spark.createDataFrame(countries_data, schema=countries_schema)

# Show the result (optional)
print("\nCOUNTRIES DataFrame:")
df_countries.show()
df_countries.printSchema()


# COMMAND ----------

df_matched=df_families.join(df_countries,df_families.FAMILY_SIZE.between(df_countries.MIN_SIZE,df_countries.MAX_SIZE),how='inner').groupBy(df_families.NAME).agg(count("*").alias("count"))
df_matched.printSchema()
df_matched.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##movie rating

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType

# Initialize Spark Session (if not already running)
# spark = SparkSession.builder.appName("MoviesAndReviews").getOrCreate()

# --- Create MOVIES DataFrame ---

# Define the schema for the MOVIES table
# Note: Primary key/Foreign key constraints are logical in SQL; PySpark handles this via join logic, not schema constraints.
movies_schema = StructType([
    StructField("id", IntegerType(), False), # Assuming 'id' is NOT NULL
    StructField("genre", StringType(), True),
    StructField("title", StringType(), True)
])

# Define the data for the MOVIES table
movies_data = [
    (1, 'Action', 'The Dark Knight'),
    (2, 'Action', 'Avengers: Infinity War'),
    (3, 'Action', 'Gladiator'),
    (4, 'Action', 'Die Hard'),
    (5, 'Action', 'Mad Max: Fury Road'),
    (6, 'Drama', 'The Shawshank Redemption'),
    (7, 'Drama', 'Forrest Gump'),
    (8, 'Drama', 'The Godfather'),
    (9, 'Drama', 'Schindler\'s List'),
    (10, 'Drama', 'Fight Club'),
    (11, 'Comedy', 'The Hangover'),
    (12, 'Comedy', 'Superbad'),
    (13, 'Comedy', 'Dumb and Dumber'),
    (14, 'Comedy', 'Bridesmaids'),
    (15, 'Comedy', 'Anchorman: The Legend of Ron Burgundy')
]

# Create the MOVIES DataFrame
df_movies = spark.createDataFrame(movies_data, schema=movies_schema)

# Show the result (optional)
print("MOVIES DataFrame:")
df_movies.show(truncate=False)
df_movies.printSchema()

# --- Create REVIEWS DataFrame ---

# Define the schema for the REVIEWS table
reviews_schema =['movie_id','rating']

# Define the data for the REVIEWS table
reviews_data = [
    (1, 4.5), (1, 4.0), (1, 5.0), (2, 4.2), (2, 4.8), (2, 3.9), (3, 4.6),
    (3, 3.8), (3, 4.3), (4, 4.1), (4, 3.7), (4, 4.4), (5, 3.9), (5, 4.5),
    (5, 4.2), (6, 4.8), (6, 4.7), (6, 4.9), (7, 4.6), (7, 4.9), (7, 4.3),
    (8, 4.9), (8, 5.0), (8, 4.8), (9, 4.7), (9, 4.9), (9, 4.5), (10, 4.6),
    (10, 4.3), (10, 4.7), (11, 3.9), (11, 4.0), (11, 3.5), (12, 3.7),
    (12, 3.8), (12, 4.2), (13, 3.2), (13, 3.5), (13, 3.8), (14, 3.8),
    (14, 4.0), (14, 4.2), (15, 3.9), (15, 4.0), (15, 4.1)
]

# Create the REVIEWS DataFrame
df_reviews = spark.createDataFrame(reviews_data, schema=reviews_schema)

# Show the result (optional, limited to first 20 rows by default)
print("\nREVIEWS DataFrame:")
df_reviews.show()
df_reviews.printSchema()


# COMMAND ----------

df_combined= df_movies.join(df_reviews, df_movies.id==df_reviews.movie_id, how="inner").groupBy(df_movies.genre).agg(avg(col("rating")).alias("avg_rating"))
df_final=df_combined.withColumn('rating_stars', when(col("avg_rating").between (0,1.0),"").when(col("avg_rating").between (1.1 , 1.9),"*").when(col("avg_rating").between(2.0 ,2.9),"**").when(col("avg_rating").between(3.0, 3.9),"***").when(col("avg_rating").between (4.0 , 4.9),"****").when(col("avg_rating")==5, "*****"))
display(df_final)
