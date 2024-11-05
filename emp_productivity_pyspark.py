from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import when, col, min, max, count,avg
import os

os.environ["PYSPARK_PYTHON"]="C:/Users/hp/Desktop/bigdata/Python/Python/Python37/python.exe"

conf=SparkConf()
conf.set("spark.app.name","emp_productivity")
conf.set("spark.master","local[4]")

spark=SparkSession.Builder()\
    .config(conf=conf)\
    .getOrCreate()

employee_productivity = [
("Emp1", 85, 6),
("Emp2", 75, 4),
("Emp3", 40, 1),
("Emp4", 78, 5),
("Emp5", 90, 7),
("Emp6", 55, 3),
("Emp7", 80, 5),
("Emp8", 42, 2),
("Emp9", 30, 1),
("Emp10", 68, 4)
]
employee_productivity_df = spark.createDataFrame(employee_productivity, ["employee_id","productivity_score", "project_count"])
employee_productivity_df.show()

classify_df=employee_productivity_df.withColumn("classification",when(((col("productivity_score")>80) & (col("project_count")>5)),"High Performer")\
                                                .when(col("productivity_score").between(60,80),"Average Performer")\
                                                .otherwise("Low Performer"))
classify_df.show()

avg_df=classify_df.groupBy(col("classification")).agg(avg(col("productivity_score")).alias("avg_product_score"))\
    .filter(col("classification")=="High Performer")
avg_df.show()

min_score_df=classify_df.groupBy(col("classification")).agg(min(col("productivity_score")).alias("min_product_score"))\
    .filter(col("classification")=="Average Performer")
min_score_df.show()

low_perf_df=classify_df.filter((col("classification")=="Low Performer") & (col("productivity_score")<50) & (col("project_count")<2))
low_perf_df.show()