from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import when, col, min, max, count
import os

os.environ["PYSPARK_PYTHON"]="C:/Users/hp/Desktop/bigdata/Python/Python/Python37/python.exe"

conf=SparkConf()
conf.set("spark.app.name","prod_sales")
conf.set("spark.master","local[4]")

spark=SparkSession.Builder()\
    .config(conf=conf)\
    .getOrCreate()

product_sales = [
("Product1", 250000, 5),
("Product2", 150000, 8),
("Product3", 50000, 20),
("Product4", 120000, 10),
("Product5", 300000, 7),
("Product6", 60000, 18),
("Product7", 180000, 9),
("Product8", 45000, 25),
("Product9", 70000, 15),
("Product10", 10000, 30)
]
product_sales_df = spark.createDataFrame(product_sales, ["product_name", "total_sales","discount"])
product_sales_df.show()

category_df=product_sales_df.select(col("product_name"),col("total_sales"),col("discount"),\
                                    when(((col("total_sales")>200000) & (col("discount")<10)),"Top Seller")\
                                    .when(col("total_sales").between(100000,200000),"Moderate Seller")\
                                    .otherwise("Low Seller")\
                                    .alias("category")
                                    )
category_df.show()

prod_count_df=category_df.groupBy(col("category")).agg(count(col("product_name")).alias("prod_count"))
prod_count_df.show()

max_sales_df=category_df.filter(col("category")=="Top Seller").agg(max(col("total_sales")).alias("max_sales"))
max_sales_df.show()

min_dis_df=category_df.filter(col("category")=="Moderate Seller").agg(min(col("discount")).alias("min_discount"))
min_dis_df.show()

tot_sales_range_df=category_df.filter(col("category")=="Low Seller").filter((col("total_sales")<50000) & (col("discount")>15).alias("total_sales_range"))
tot_sales_range_df.show()