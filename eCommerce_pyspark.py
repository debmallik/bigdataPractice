from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import when, col, count
import os

os.environ["PYSPARK_PYTHON"]="C:/Users/hp/Desktop/bigdata/Python/Python/Python37/python.exe"

conf=SparkConf()
conf.set("spark.app.name","eCommerce")
conf.set("spark.master","local[4]")

spark=SparkSession.Builder()\
    .config(conf=conf)\
    .getOrCreate()

orders = [
("Order1", "Laptop", "Domestic", 2),
("Order2", "Shoes", "International", 8),
("Order3", "Smartphone", "Domestic", 3),
("Order4", "Tablet", "International", 5),
("Order5", "Watch", "Domestic", 7),
("Order6", "Headphones", "International", 10),
("Order7", "Camera", "Domestic", 1),
("Order8", "Shoes", "International", 9),
("Order9", "Laptop", "Domestic", 6),
("Order10", "Tablet", "International", 4)
]
orders_df = spark.createDataFrame(orders, ["order_id", "product_type", "origin", "delivery_days"])
orders_df.show()

classification_df=orders_df.withColumn("speed_category",\
                                       when((col("delivery_days")>7) & (col("origin")=="International"),"Delayed")\
                                       .when((col("delivery_days")>=3) & (col("delivery_days")<=7),"On-Time")\
                                       .otherwise("Fast"))
classification_df.show()

count_df=classification_df.groupBy("product_type", "speed_category").agg(count(col("speed_category")).alias("count"))
count_df.show()