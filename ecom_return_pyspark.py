from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import when, col, min, max, count,avg
import os

os.environ["PYSPARK_PYTHON"]="C:/Users/hp/Desktop/bigdata/Python/Python/Python37/python.exe"

conf=SparkConf()
conf.set("spark.app.name","ecom_return")
conf.set("spark.master","local[4]")

spark=SparkSession.Builder()\
    .config(conf=conf)\
    .getOrCreate()

ecommerce_return = [
("Product1", 75, 25),
("Product2", 40, 15),
("Product3", 30, 5),
("Product4", 60, 18),
("Product5", 100, 30),
("Product6", 45, 10),
("Product7", 80, 22),
("Product8", 35, 8),
("Product9", 25, 3),
("Product10", 90, 12)
]
ecommerce_return_df = spark.createDataFrame(ecommerce_return, ["product_name", "sale_price","return_rate"])
ecommerce_return_df.show()

return_rate_df=ecommerce_return_df.withColumn("classification",when((col("return_rate")>20),"High Return")\
                                              .when(col("return_rate").between(10,20),"Medium Return")\
                                              .otherwise("Low return"))
return_rate_df.show()

count_prod_df=return_rate_df.groupBy(col("classification")).agg(count(col("product_name")).alias("prod_count"))
count_prod_df.show()

avg_df=return_rate_df.groupBy("classification").agg(avg(col("sale_price")).alias("avg_price")).filter(col("classification")=="High Return")
avg_df.show()

max_ret_df=return_rate_df.groupBy("classification").agg(max(col("return_rate")).alias("max_ret_rate")).filter(col("classification")=="Medium Return")
max_ret_df.show()

low_ret_prod_df=return_rate_df.filter((col("classification")=="Low return") & (col("sale_price")<50) & (col("return_rate")<5))
low_ret_prod_df.show()