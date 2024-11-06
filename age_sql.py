from pyspark.sql import SparkSession
from pyspark import SparkConf
import os

from pyspark.sql.functions import initcap, col

os.environ["PYSPARK_PYTHON"]="C:/Users/hp/Desktop/bigdata/Python/Python/Python37/python.exe"

conf=SparkConf()
conf.set("spark.app.name","overtime")
conf.set("spark.master","local[4]")
conf.set("spark.local.dir","C:/temp")

spark=SparkSession.Builder()\
    .config(conf=conf)\
    .getOrCreate()

customers = [
("karthik", 22),
("neha", 28),
("priya", 40),
("mohan", 55),
("ajay", 32),
("vijay", 18),
("veer", 47),
("aatish", 38),
("animesh", 60),
("nishad", 25)
]

customers_df = spark.createDataFrame(customers, ["name", "age"])
customers_df=customers_df.withColumn("name",initcap(col("name")))
customers_df.show()

customers_df.createTempView("age_table")
age_status_df=spark.sql("""
select
    case
        when age < 25 then "Youth"
        when age between 25 and 45 then "Adult"
        else "Senior"
    end as age_status,
    count(*) as tot_customers
from age_table
group by age_status
""")

age_status_df.show()
