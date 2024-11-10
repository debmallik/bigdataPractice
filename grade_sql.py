from pyspark.sql import SparkSession
from pyspark import SparkConf
import os

os.environ["PYSPARK_PYTHON"]="C:/Users/hp/Desktop/bigdata/Python/Python/Python37/python.exe"

conf=SparkConf()
conf.set("spark.app.name","student_grade")
conf.set("spark.master","local[4]")
conf.set("spark.local.dir","C:/temp")

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
students_df.show()
students_df.createTempView("main_table")
score_category_df=spark.sql("""
select *,
case
    when score>90 then "excellent"
    when score between 75 and 89 then "Good"
    else "Needs Improvement"
end as category
from main_table
""")
print("=== score_category_df ===")
score_category_df.show()

score_category_df.createTempView("category_table")
count_student_df=spark.sql("""
select 
category,
count(name) as count_student
from category_table
group by category
""")
print("=== count_student_df ===")
count_student_df.show()