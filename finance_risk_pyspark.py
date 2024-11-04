from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import when, col, avg
import os

os.environ["PYSPARK_PYTHON"]="C:/Users/hp/Desktop/bigdata/Python/Python/Python37/python.exe"

conf=SparkConf()
conf.set("spark.app.name","financial_risk")
conf.set("spark.master","local[4]")

spark=SparkSession.Builder()\
    .config(conf=conf)\
    .getOrCreate()

loan_applicants = [
("karthik", 60000, 120000, 590),
("neha", 90000, 180000, 610),
("priya", 50000, 75000, 680),
("mohan", 120000, 240000, 560),
("ajay", 45000, 60000, 620),
("vijay", 100000, 100000, 700),
("veer", 30000, 90000, 580),
("aatish", 85000, 85000, 710),
("animesh", 50000, 100000, 650),
("nishad", 75000, 200000, 540)
]

loan_applicants_df = spark.createDataFrame(loan_applicants, ["name", "income", "loan_amount","credit_score"])
loan_applicants_df.show()

risk_df=loan_applicants_df.withColumn("risk",when(((col("loan_amount")>=(2*col("income"))) & (col("credit_score")<600)),"High Risk")\
                                      .when(((col("loan_amount")>col("income")) & (col("loan_amount")<=2*col("income"))) & col("credit_score").between(600,700),"Moderate Risk")\
                                      .otherwise("Low Risk"))

income_range_df=risk_df.withColumn("income-range",when((col("income")<=50000),"less_50k")\
    .when((col("income")>50000) & (col("income")<=100000),"btwn_50_100k")\
    .otherwise("more_than_100k")
   )
income_range_df.show()

avg_loan_df=income_range_df.filter(col("risk")=="High Risk").groupBy(col("income-range")).agg(avg(col("loan_amount")).alias("avg_loan_amount"))
avg_loan_df.show()

#avg_cred_score_df=income_range_df.groupBy(col("income-range"),col("risk")).agg(avg(col("credit_score")).alias("avg_cred_score"))
avg_cred_score_df=income_range_df.groupBy(col("income-range"),col("risk")).agg(avg(col("credit_score")).alias("avg_cred_score")).filter(col("avg_cred_score")<650)
avg_cred_score_df.show()