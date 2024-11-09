from pyspark.sql import SparkSession
from pyspark import SparkConf
import os

os.environ["PYSPARK_PYTHON"]="C:/Users/hp/Desktop/bigdata/Python/Python/Python37/python.exe"

conf=SparkConf()
conf.set("spark.app.name","elec-consumption")
conf.set("spark.master","local[4]")
conf.set("spark.local.dir","C:/temp")

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

df.createTempView("main_table")
band_df=spark.sql("""
select *,
case
    when salary>100000 and experience>10 then "Senior"
    when (salary between 50000 and 100000) and (experience between 5 and 10) then "Mid-level"
    else "Junior"
end as band_category
from main_table
""")
print("==== band dataframe ====")
band_df.show()

band_df.createTempView("band_category_table")
sal_band_count_df=spark.sql("""
select
band_category,
count(*) as band_cat_count
from band_category_table
group by band_category
""")
print("==== sal_band_count_df ====")
sal_band_count_df.show()

avg_perf_score_df=spark.sql("""
select 
    band_category,
    avg(performance_score) as avg_perf_score
from band_category_table
group by band_category
having avg(performance_score)>80
""")
print("=== avg_perf_score_df ===")
avg_perf_score_df.show()

mid_level_band_df=spark.sql("""
select * from band_category_table
where performance_score>85 and experience>7
""")
print("===mid_level_band_df===")
mid_level_band_df.show()