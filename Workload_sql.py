from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import initcap,when
import os

os.environ["PYSPARK_PYTHON"]="C:/Users/hp/Desktop/bigdata/Python/Python/Python37/python.exe"

conf=SparkConf()
conf.set("spark.app.name","Workload")
conf.set("spark.master","local[4]")
conf.set("spark.local.dir","C:/temp")

spark=SparkSession.Builder()\
    .config(conf=conf)\
    .getOrCreate()

workload = [
("karthik", "ProjectA", 120),
("karthik", "ProjectB", 100),
("neha", "ProjectC", 80),
("neha", "ProjectD", 30),
("priya", "ProjectE", 110),
("mohan", "ProjectF", 40),
("ajay", "ProjectG", 70),
("vijay", "ProjectH", 150),
("veer", "ProjectI", 190),
("aatish", "ProjectJ", 60),
("animesh", "ProjectK", 95),
("nishad", "ProjectL", 210),
("varun", "ProjectM", 50),
("aadil", "ProjectN", 90)
]
df = spark.createDataFrame(workload, ["name", "project", "hours"])
df.show()

df.createTempView("Workload")
workload_df=spark.sql("""
select
initcap(name),
project,
hours,
case
when hours>200 then "Overloaded"
when hours between 100 and 200 then "Balanced"
else "Underutilized"
end as workload_status
from Workload
""")
workload_df.show()

workload_df.createTempView("agg_workload")
agg_workload_df=spark.sql("""
select
workload_status,
count(*) as status_count
from agg_workload
group by(workload_status)
""")

agg_workload_df.show()