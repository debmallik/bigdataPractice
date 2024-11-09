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

electricity_usage = [
("House1", 550, 250),
("House2", 400, 180),
("House3", 150, 50),
("House4", 500, 200),
("House5", 600, 220),
("House6", 350, 120),
("House7", 100, 30),
("House8", 480, 190),
("House9", 220, 105),
("House10", 150, 60)
]
electricity_usage_df = spark.createDataFrame(electricity_usage, ["household", "kwh_usage","total_bill"])
electricity_usage_df.show()

electricity_usage_df.createTempView("main_table")
usage_category_df=spark.sql("""
select *,
case
    when kwh_usage>500 and total_bill>200 then "High Usage"
    when kwh_usage between 200 and 500 then "Medium Usage"
    else "Low Usage"
end as category
from main_table
""")
print("=====usage_category_df====")
usage_category_df.show()

usage_category_df.createTempView("usage_category_table")
max_bill_df=spark.sql("""
select
category,
max(total_bill) as max_bill
from usage_category_table
where category='High Usage'
group by category
""")
print("=== max_bill_df ===")
max_bill_df.show()

avg_kwh_df=spark.sql("""
select 
category,
avg(kwh_usage) as avg_kwh_num
from usage_category_table
where category='Medium Usage'
group by category
""")
print("====avg_kwh_df====")
avg_kwh_df.show()

household_count_df=spark.sql("""
select 
category,
count(*) as household_count
from usage_category_table
where (category='Low Usage') and (kwh_usage>300)
group by category
""")
print("=====household_count_df====")
household_count_df.show()