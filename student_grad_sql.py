from pyspark.sql import SparkSession
from pyspark import SparkConf
import os

os.environ["PYSPARK_PYTHON"]="C:/Users/hp/Desktop/bigdata/Python/Python/Python37/python.exe"

conf=SparkConf()
conf.set("spark.app.name","student_grad")
conf.set("spark.master","local[4]")
conf.set("spark.local.dir","C:/temp")

spark=SparkSession.Builder()\
    .config(conf=conf)\
    .getOrCreate()

students = [
("Student1", 70, 45, 60, 65, 75),
("Student2", 80, 55, 58, 62, 67),
("Student3", 65, 30, 45, 70, 55),
("Student4", 90, 85, 80, 78, 76),
("Student5", 72, 40, 50, 48, 52),
("Student6", 88, 60, 72, 70, 68),
("Student7", 74, 48, 62, 66, 70),
("Student8", 82, 56, 64, 60, 66),
("Student9", 78, 50, 48, 58, 55),
("Student10", 68, 35, 42, 52, 45)
]
students_df = spark.createDataFrame(students, ["student_id", "attendance_percentage","math_score", "science_score", "english_score", "history_score"])
students_df.show()

students_df.createTempView("main_table")
risk_category_df=spark.sql("""
select *,
(math_score + science_score + english_score + history_score) / 4 as avg_score,
((case when math_score>70 then 1 else 0 end)
+(case when science_score>70 then 1 else 0 end)
+(case when english_score>70 then 1 else 0 end)
+(case when history_score>70 then 1 else 0 end)
) as count_gt_70,
case
    when attendance_percentage<75 and ((math_score+science_score+english_score+history_score)/4)<50 then "At-Risk"
    when attendance_percentage between 75 and 85 then "Moderate Risk"
    else "Low Risk"
end as risk_category
from main_table
""")
print("=== risk_category_df ===")
risk_category_df.show()

###Calculate the number of students in each risk category
risk_category_df.createTempView("risk_category_table")
num_student_df=spark.sql("""
select 
risk_category,
count(student_id) as student_count
from risk_category_table
group by risk_category
""")
print("=== num_student_df ===")
num_student_df.show()

###Find the average score for students in the "At-Risk" category
avg_score_df=spark.sql("""
select
risk_category,
avg(avg_score) as avg_risk_per_cat
from risk_category_table
group by risk_category
having risk_category='At-Risk'
""")
print("=== avg_score_df ===")
avg_score_df.show()

###Identify "Moderate Risk" students who have scored above 70 in at least three subjects6
mod_risk_stu_gt_70_df=spark.sql("""
select
student_id,
risk_category,
count_gt_70
from risk_category_table
    where count_gt_70>=3
""")
print("=== mod_risk_stu_gt_70_df ===")
mod_risk_stu_gt_70_df.show()