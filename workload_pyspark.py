from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import when, initcap, col, count
import os

os.environ["PYSPARK_PYTHON"]="C:/Users/hp/Desktop/bigdata/Python/Python/Python37/python.exe"

conf=SparkConf()
conf.set("spark.app.name","workload_analysis")
conf.set("spark.master","local[4]")

spark=SparkSession.Builder()\
    .config(conf=conf)\
    .getOrCreate()

workload = [
("karthik", "ProjectA", 120),
("karthik", "ProjectB", 100),
("neha", "ProjectC", 80),
("neha", "ProjectD", 30),
("priya", "ProjectE", 110),
("mohan", "ProjectF", 40),
("ajay", "ProjectG", 70),
("vijay", "ProjectH", 150),
("veer", "ProjectI", 190),
("aatish", "ProjectJ", 60),
("animesh", "ProjectK", 95),
("nishad", "ProjectL", 210),
("varun", "ProjectM", 50),
("aadil", "ProjectN", 90)
]
workload_df = spark.createDataFrame(workload, ["name", "project", "hours"])
workload_cate_df=workload_df.select(col("name"),col("project"),col("hours"),when(col("hours")>200,"Overloaded").when((col("hours")>100) & (col("hours")<200),"Balanced").otherwise("Underutilized").alias("Category"))
workload_cate_df=workload_cate_df.groupBy(initcap(col("name"))).agg(count(col("category")).alias("count"))

workload_cate_df.show()