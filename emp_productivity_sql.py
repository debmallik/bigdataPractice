from pyspark.sql import SparkSession
from pyspark import SparkConf
import os

os.environ["PYSPARK_PYTHON"]="C:/Users/hp/Desktop/bigdata/Python/Python/Python37/python.exe"

conf=SparkConf()
conf.set("spark.app.name","emp_productivity")
conf.set("spark.master","local[4]")
conf.set("spark.local.dir","C:/temp")

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

employee_productivity_df.createTempView("main_table")
perform_category_df=spark.sql("""
select *,
case
    when productivity_score>80 and project_count>5 then "High Performer"
    when productivity_score between 60 and 80 then "Average Performer"
    else "Low Performer"
end as performance_category
from main_table
""")
print("=== perform_category_df ===")
perform_category_df.show()

perform_category_df.createTempView("emp_productivity_table")
emp_count_df=spark.sql("""
select 
performance_category,
count(employee_id) as emp_count
from emp_productivity_table
group by performance_category
""")
print("=== emp_count_df ===")
emp_count_df.show()

avg_productivity_df=spark.sql("""
select 
    performance_category,
    avg(productivity_score) as avg_prod_score
from emp_productivity_table
group by performance_category
having performance_category='High Performer'
""")
print("=== avg_productivity_df ===")
avg_productivity_df.show()

min_score_df=spark.sql("""
select 
    performance_category,
    min(productivity_score) as min_prod_score
from emp_productivity_table
group by performance_category
having performance_category='Average Performer'
""")

low_perform_df=spark.sql("""
select *
from emp_productivity_table
    where performance_category="Low Performer" and productivity_score<50 and project_count<2
""")
print("=== low_perform_df ===")
low_perform_df.show()