from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import when, col, avg
import os

os.environ["PYSPARK_PYTHON"]="C:/Users/hp/Desktop/bigdata/Python/Python/Python37/python.exe"

conf=SparkConf()
conf.set("spark.app.name","product_return")
conf.set("spark.master","local[4]")

spark=SparkSession.Builder()\
    .config(conf=conf)\
    .getOrCreate()

customers = [
("karthik", "Premium", 1050, 32),
("neha", "Standard", 800, 28),
("priya", "Premium", 1200, 40),
("mohan", "Basic", 300, 35),
("ajay", "Standard", 700, 25),
("vijay", "Premium", 500, 45),
("veer", "Basic", 450, 33),
("aatish", "Standard", 600, 29),
("animesh", "Premium", 1500, 60),
("nishad", "Basic", 200, 21)
]
customers_df = spark.createDataFrame(customers, ["name", "membership", "spending", "age"])
customers_df.show()

category_df=customers_df.withColumn("category",when((col("spending")>1000) & (col("membership")=="Premium"),"High Spender")\
                                    .when(((col("spending")>500) & (col("spending")<1000)) & (col("membership")=="Standard"),"Average Spender")\
                                    .otherwise("Low Spender"))
category_df.show()

avg_spend_df=category_df.groupBy(col("membership")).agg(avg(col("spending")).alias("avg_spend"))
avg_spend_df.show()

