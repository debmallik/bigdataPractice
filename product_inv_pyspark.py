from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import when, col, count
import os

os.environ["PYSPARK_PYTHON"]="C:/Users/hp/Desktop/bigdata/Python/Python/Python37/python.exe"

conf=SparkConf()
conf.set("spark.app.name","product_inv")
conf.set("spark.master","local[4]")

spark=SparkSession.Builder()\
    .config(conf=conf)\
    .getOrCreate()

inventory = [
("ProductA", 120),
("ProductB", 95),
("ProductC", 45),
("ProductD", 200),
("ProductE", 75),
("ProductF", 30),
("ProductG", 85),
("ProductH", 100),
("ProductI", 60),
("ProductJ", 20)
]
inventory_df = spark.createDataFrame(inventory, ["product_name", "stock_quantity"])

inv_status_df=inventory_df.withColumn("inv_status"\
                                      ,when(col("stock_quantity")>100,"Overstocked")\
                                      .when((col("stock_quantity")>50)&(col("stock_quantity")<100),"Normal")\
                                      .otherwise("Low stock"))

inv_status_df.show()
stock_count_df=inv_status_df.groupby(col("inv_status")).agg(count(col("inv_status")).alias("inv_categoty_count"))
stock_count_df.show()