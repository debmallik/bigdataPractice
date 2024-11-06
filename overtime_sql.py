from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import when, initcap, col, count
import os

os.environ["PYSPARK_PYTHON"]="C:/Users/hp/Desktop/bigdata/Python/Python/Python37/python.exe"

conf=SparkConf()
conf.set("spark.app.name","overtime")
conf.set("spark.master","local[4]")
conf.set("spark.local.dir","C:/temp")

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
employees_df.show()

employees_df.createTempView("overtime")
overtime_df=spark.sql("""
select
initcap(name),
hours_worked,
case
when hours_worked>60 then "Excessive Overtime"
when hours_worked between 45 and 60 then "Standard Overtime"
else "No Overtime"
end as status
from overtime
""")
overtime_df.show()

overtime_df.createTempView("overtime_status")
query="""
select
status,
count(*) as status_count
from overtime_status
group by(status)
"""
overtime_status_df=spark.sql(query)
overtime_status_df.show()