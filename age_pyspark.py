from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import when, initcap, col, count
import os

os.environ["PYSPARK_PYTHON"]="C:/Users/hp/Desktop/bigdata/Python/Python/Python37/python.exe"

conf=SparkConf()
conf.set("spark.app.name","overtime")
conf.set("spark.master","local[4]")

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
# age_df=customers_df.withColumn("age_status",\
#                                when(col("age")<25,"Youth")\
#                                .when((col("age")>25) & (col("age")<45),"Adult")\
#                                .otherwise("Senior"))

age_df=customers_df.select(initcap(col("name")),col("age"),when(col("age")<25,"Youth")\
                               .when((col("age")>25) & (col("age")<45),"Adult")\
                               .otherwise("Senior").alias("age_status"))
age_df.show()
age_df=age_df.groupBy(col("age_status")).agg(count(col("age_status")).alias("count"))
age_df.show()



