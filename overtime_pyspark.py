from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import when, initcap, col, count
import os

os.environ["PYSPARK_PYTHON"]="C:/Users/hp/Desktop/bigdata/Python/Python/Python37/python.exe"

conf=SparkConf()
conf.set("spark.app.name","overtime")
conf.set("spark.master","local[4]")

spark=SparkSession.Builder()\
    .config(conf=conf)\
    .getOrCreate()

employees = [
("karthik", 62),
("neha", 50),
("priya", 30),
("mohan", 65),
("ajay", 40),
("vijay", 47),
("veer", 55),
("aatish", 30),
("animesh", 75),
("nishad", 60)
]
employees_df = spark.createDataFrame(employees, ["name", "hours_worked"])
overtime_df=employees_df.select(initcap(col("name")),col("hours_worked"),\
                                when(col("hours_worked")>=60,"Overtime")\
                                .when((col("hours_worked")>45) & (col("hours_worked")<60),"Standard overtime")\
                                .otherwise("No overtime").alias("status"))
overtime_df.show()

overtime_df=overtime_df.groupBy(col("status")).agg(count(col("status")).alias("count"))
overtime_df.show()