from pyspark.sql import SparkSession
from pyspark import SparkConf
import os

os.environ["PYSPARK_PYTHON"]="C:/Users/hp/Desktop/bigdata/Python/Python/Python37/python.exe"

conf=SparkConf()
conf.set("spark.app.name","bonus")
conf.set("spark.master","local[4]")
conf.set("spark.local.dir","C:/temp")

spark=SparkSession.Builder()\
    .config(conf=conf)\
    .getOrCreate()

employees = [
("karthik", "Sales", 85),
("neha", "Marketing", 78),
("priya", "IT", 90),
("mohan", "Finance", 65),
("ajay", "Sales", 55),
("vijay", "Marketing", 82),
("veer", "HR", 72),
("aatish", "Sales", 88),
("animesh", "Finance", 95),
("nishad", "IT", 60)
]
employees_df = spark.createDataFrame(employees, ["name", "department", "performance_score"])
employees_df.show()
employees_df.createTempView("main_table")

bonus_category_df=spark.sql("""
select *,
case
    when department in('Sales','Marketing') and performance_score>80 then 20
    when department not in('Sales','Marketing') and performance_score>70 then 15
    else "no_bonus"
end as bonus_category
from main_table
""")
bonus_category_df.show()
bonus_category_df. createTempView("bonus_category_temple")

###Group by department and calculate total bonus allocation.
total_bonus_df=spark.sql("""
select
department,
sum(bonus_category) as total_bonus
from bonus_category_temple
group by department
""")
total_bonus_df.show()
