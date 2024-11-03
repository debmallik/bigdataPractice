from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import when, initcap
import os

os.environ["PYSPARK_PYTHON"]="C:/Users/hp/Desktop/bigdata/Python/Python/Python37/python.exe"

conf=SparkConf()
conf.set("spark.app.name","sales_performance")
conf.set("spark.master","local[4]")

spark=SparkSession.Builder()\
    .config(conf=conf)\
    .getOrCreate()

sales = [
("karthik", 60000),
("neha", 48000),
("priya", 30000),
("mohan", 24000),
("ajay", 52000),
("vijay", 45000),
("veer", 70000),
("aatish", 23000),
("animesh", 15000),
("nishad", 8000),
("varun", 29000),
("aadil", 32000)
]

sales_df = spark.createDataFrame(sales, ["name", "total_sales"])
perf_df=sales_df.withColumn("Performance",when(sales_df["total_sales"]>50000,"Excellent")\
                            .when((sales_df["total_sales"]>25000) & (sales_df["total_sales"]<50000),"Good")\
                            .otherwise("Needs Improvment"))
perf_df=perf_df.withColumn("name",initcap("name"))

perf_df.show()
