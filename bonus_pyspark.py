from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import when, col, sum
import os

os.environ["PYSPARK_PYTHON"]="C:/Users/hp/Desktop/bigdata/Python/Python/Python37/python.exe"

conf=SparkConf()
conf.set("spark.app.name","bonus")
conf.set("spark.master","local[4]")

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

bonus_df=employees_df.select(col("name"),col("department"),col("performance_score"),\
                          when((col("department").startswith("Sales") | col("department").startswith("Marketing")) & (col("performance_score")>80),20)\
                          .when((~col("department").startswith("Sales") | ~col("department").startswith("Marketing")) & (col("performance_score")>70),15)\
                          .otherwise("No Bonus")\
                          .alias("bonus_perc")
)
# bonus_df=employees_df.select(col("name"),col("department"),col("performance_score")\
#                              ,when(col("department").isin("Sales","Marketing") & (col("performance_score")>80),20)\
#                              .when(~col("department").isin("Sales","Marketing") & (col("performance_score")>70),15)\
#                              .otherwise("No Bonus")\
#                              .alias("bonus_perc"))

bonus_df.show()

sum_bonus_df=bonus_df.groupBy(col("department")).agg(sum(col("bonus_perc")).alias("total_bonus"))
sum_bonus_df.show()

