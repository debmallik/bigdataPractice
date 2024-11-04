from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import when, col, count
import os

os.environ["PYSPARK_PYTHON"]="C:/Users/hp/Desktop/bigdata/Python/Python/Python37/python.exe"

conf=SparkConf()
conf.set("spark.app.name","product_return")
conf.set("spark.master","local[4]")

spark=SparkSession.Builder()\
    .config(conf=conf)\
    .getOrCreate()

products = [
("Laptop", "Electronics", 120, 45),
("Smartphone", "Electronics", 80, 60),
("Tablet", "Electronics", 50, 72),
("Headphones", "Accessories", 110, 47),
("Shoes", "Clothing", 90, 55),
("Jacket", "Clothing", 30, 80),
("TV", "Electronics", 150, 40),
("Watch", "Accessories", 60, 65),
("Pants", "Clothing", 25, 75),
("Camera", "Electronics", 95, 58)
]
products_df = spark.createDataFrame(products, ["product_name", "category", "return_count","satisfaction_score"])
products_df.show()

return_df=products_df.withColumn("return_score",when((col("return_count")>100) & (col("satisfaction_score")<50),"High Return Rate")\
                                 .when(((col("return_count")>50) & (col("return_count")<100)) & ((col("satisfaction_score")>50) & (col("satisfaction_score")<70)),"Moderate Return Rat")\
                                 .otherwise("Low Return Rate")\
                                 )
return_df.show()

return_count_df=return_df.groupBy("return_score").agg(count(col("return_score")).alias("return_count"))
return_count_df.show()