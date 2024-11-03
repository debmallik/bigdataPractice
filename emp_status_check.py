from pyspark.sql import SparkSession
from pyspark import SparkConf
import os

from pyspark.sql.functions import datediff, current_date, when

os.environ["PYSPARK_PYTHON"]="C:/Users/hp/Desktop/bigdata/Python/Python/Python37/python.exe"

conf=SparkConf()
conf.set("spark.app.name","emp_status_check")
conf.set("spark.master","local[4]")

spark=SparkSession.Builder()\
    .config(conf=conf)\
    .getOrCreate()

employees = [
("karthik", "2024-11-01"),
("neha", "2024-10-20"),
("priya", "2024-10-28"),
("mohan", "2024-11-02"),
("ajay", "2024-09-15"),
("vijay", "2024-10-30"),
("veer", "2024-10-25"),
("aatish", "2024-10-10"),
("animesh", "2024-10-15"),
("nishad", "2024-11-01"),
("varun", "2024-10-05"),
("aadil", "2024-09-30")
]

employees_df=spark.createDataFrame(employees,["name","last_checkin"])

checkin_duration_df=employees_df.withColumn("checkin_duration",datediff(current_date(),"last_checkin"))

status_df=checkin_duration_df.withColumn("status",when(checkin_duration_df["checkin_duration"]<7,"active").otherwise("Inactive"))

status_df.show()