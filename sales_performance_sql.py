from pyspark.sql import SparkSession
from pyspark import SparkConf
import os

from pyspark.sql.functions import datediff, current_date, when, initcap

os.environ["PYSPARK_PYTHON"]="C:/Users/hp/Desktop/bigdata/Python/Python/Python37/python.exe"

conf=SparkConf()
conf.set("spark.app.name","sales_performance")
conf.set("spark.master","local[4]")
conf.set("spark.local.dir","C:/temp")

spark=SparkSession.Builder()\
    .config(conf=conf)\
    .getOrCreate()

sales = [
("karthik", 60000),
("neha", 48000),
("priya", 30000),
("mohan", 24000),
("ajay", 52000),
("vijay", 45000),
("veer", 70000),
("aatish", 23000),
("animesh", 15000),
("nishad", 8000),
("varun", 29000),
("aadil", 32000)
]

sales_df = spark.createDataFrame(sales, ["name", "total_sales"])
sales_df.show()

sales_df.createTempView("sales_performance")

sales_performance_df=spark.sql("""
select
initcap(name),
total_sales,
case
when total_sales>50000 then "Excellent"
when total_sales between 25000 and 50000 then "Good"
else "Needs Improvment"
end as Performance
from sales_performance
""")

sales_performance_df.show()