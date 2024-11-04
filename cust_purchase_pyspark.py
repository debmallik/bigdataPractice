from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import when, col, avg, min
import os

os.environ["PYSPARK_PYTHON"]="C:/Users/hp/Desktop/bigdata/Python/Python/Python37/python.exe"

conf=SparkConf()
conf.set("spark.app.name","cust_purchase")
conf.set("spark.master","local[4]")

spark=SparkSession.Builder()\
    .config(conf=conf)\
    .getOrCreate()

customer_purchases = [
("karthik", "Premium", 50, 5000),
("neha", "Standard", 10, 2000),
("priya", "Premium", 65, 8000),
("mohan", "Basic", 90, 1200),
("ajay", "Standard", 25, 3500),
("vijay", "Premium", 15, 7000),
("veer", "Basic", 75, 1500),
("aatish", "Standard", 45, 3000),
("animesh", "Premium", 20, 9000),
("nishad", "Basic", 80, 1100)
]
customer_purchases_df = spark.createDataFrame(customer_purchases, ["name", "membership","days_since_last_purchase", "total_purchase_amount"])
customer_purchases_df.show()

frequency_df=customer_purchases_df.withColumn("frequency",when((col("days_since_last_purchase")<30),"Frequent")\
                                              .when(((col("days_since_last_purchase")>=30) & (col("days_since_last_purchase")<=60)),"Occasional")\
                                              .otherwise("Rare")\
                                              )
frequency_df.show()

#filter_df=frequency_df.filter((col("membership")=="Premium") & (col("frequency")=="Frequent"))
#filter_df.show()

#filter_df=frequency_df.filter((col("membership")=="Premium") & (col("frequency")=="Frequent")).agg(avg(col("total_purchase_amount")).alias("avg_pur_amount"))
#filter_df.show()

#avg_tot_purchase_df=filter_df.groupBy(col("frequency"),col("membership")).agg(avg(col("total_purchase_amount")).alias("avg_pur_amount"))
#avg_tot_purchase_df.show()

avg_tot_purchase_df=frequency_df.groupBy(col("membership"),col("frequency")).agg(avg(col("total_purchase_amount")).alias("avg_pur_amount"))\
    .filter((col("membership")=="Premium") & (col("frequency")=="Frequent"))
avg_tot_purchase_df.show()

min_purc_amn_df=frequency_df.groupBy(col("frequency"),col("membership")).agg(min(col("total_purchase_amount")).alias("min_purc_amn")).filter(col("frequency")=="Rare")
min_purc_amn_df.show()
