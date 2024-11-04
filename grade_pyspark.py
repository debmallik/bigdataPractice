from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import when, col, count
import os

os.environ["PYSPARK_PYTHON"]="C:/Users/hp/Desktop/bigdata/Python/Python/Python37/python.exe"

conf=SparkConf()
conf.set("spark.app.name","student_grade")
conf.set("spark.master","local[4]")

spark=SparkSession.Builder()\
    .config(conf=conf)\
    .getOrCreate()

students = [
("karthik", 95),
("neha", 82),
("priya", 74),
("mohan", 91),
("ajay", 67),
("vijay", 80),
("veer", 85),
("aatish", 72),
("animesh", 90),
("nishad", 60)
]
students_df = spark.createDataFrame(students, ["name", "score"])

grade_df=students_df.withColumn("grade",when(col("score")>90,"excellent").when((col("score")>75) & (col("score")<=89),"Good").otherwise("Need Improvement"))
grade_count_df=grade_df.groupBy(col("grade")).agg(count(col("grade")).alias("count"))

grade_df.show()
grade_count_df.show()