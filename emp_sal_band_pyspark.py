from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import when, col, avg, max, count
import os

os.environ["PYSPARK_PYTHON"]="C:/Users/hp/Desktop/bigdata/Python/Python/Python37/python.exe"

conf=SparkConf()
conf.set("spark.app.name","elec-consumption")
conf.set("spark.master","local[4]")

spark=SparkSession.Builder()\
    .config(conf=conf)\
    .getOrCreate()

employees =[
("karthik", "IT", 110000, 12, 88),
("neha", "Finance", 75000, 8, 70),
("priya", "IT", 50000, 5, 65),
("mohan", "HR", 120000, 15, 92),
("ajay", "IT", 45000, 3, 50),
("vijay", "Finance", 80000, 7, 78),
("veer", "Marketing", 95000, 6, 85),
("aatish", "HR", 100000, 9, 82),
("animesh", "Finance", 105000, 11, 88),
("nishad", "IT", 30000, 2, 55)
]
df = spark.createDataFrame(employees,["name", "department", "salary", "experience", "performance_score"])
df.show()

band_df=df.withColumn("band",when(((col("salary")>100000) & (col("experience")>10)),"Senior")\
    .when(((col("salary").between(50000,100000)) & (col("experience").between(5,10))),"Mid_level")\
    .otherwise("Junior"))
band_df.show()

count_sal_band_df=band_df.groupBy(col("department"),col("band")).agg(count(col("band")).alias("sal_band_count"))
count_sal_band_df.show()

avg_perf_df=band_df.groupBy(col("band")).agg(avg(col("performance_score")).alias("avg_perf")).filter(col("avg_perf")>80)
avg_perf_df.show()

mid_band_df=band_df.filter((col("performance_score")>85) & (col("experience")>7).alias("mid_band_perf")).filter(col("band")=="Mid_level")
mid_band_df.show()
