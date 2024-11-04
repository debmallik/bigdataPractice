from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import when, col, avg, max, count
import os

os.environ["PYSPARK_PYTHON"]="C:/Users/hp/Desktop/bigdata/Python/Python/Python37/python.exe"

conf=SparkConf()
conf.set("spark.app.name","elec-consumption")
conf.set("spark.master","local[4]")

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

classification_df=electricity_usage_df.withColumn("classification",when(((col("kwh_usage")>500) & (col("total_bill")>200)),"High usage")\
                                                  .when(((col("kwh_usage").between(200,500)) & (col("total_bill").between(100,200))),"Medium Usage")\
                                                  .otherwise("Low Usage"))
classification_df.show()

house_usage_cat_df=classification_df.groupBy("classification").agg(count(col("household")).alias("usg_cate_num"))
house_usage_cat_df.show()

#max_bill_df=classification_df.filter(col("classification")=="High usage").agg(max(col("total_bill")).alias("max_bill_high_usage"))
max_bill_df=classification_df.groupBy(col("classification")).agg(max(col("total_bill")).alias("max_bill_high_usage")).filter(col("classification")=="High usage")
max_bill_df.show()

avg_usage_df=classification_df.groupBy(col("classification")).agg(avg(col("kwh_usage")).alias("avg_power_usage")).filter(col("classification")=="Medium Usage")
avg_usage_df.show()

low_kwh_usage=classification_df.filter((col("kwh_usage")>300) & (col("classification")=="Low Usage")).agg(count(col("household")).alias("count"))
low_kwh_usage.show()