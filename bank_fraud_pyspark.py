from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import when, col, min, max, count,avg,sum
import os

os.environ["PYSPARK_PYTHON"]="C:/Users/hp/Desktop/bigdata/Python/Python/Python37/python.exe"

conf=SparkConf()
conf.set("spark.app.name","bank_fraud")
conf.set("spark.master","local[4]")

spark=SparkSession.Builder()\
    .config(conf=conf)\
    .getOrCreate()

transactions = [
("Account1", "2024-11-01", 12000, 6, "Savings"),
("Account2", "2024-11-01", 8000, 3, "Current"),
("Account3", "2024-11-02", 2000, 1, "Savings"),
("Account4", "2024-11-02", 15000, 7, "Savings"),
("Account5", "2024-11-03", 9000, 4, "Current"),
("Account6", "2024-11-03", 3000, 1, "Current"),
("Account7", "2024-11-04", 13000, 5, "Savings"),
("Account8", "2024-11-04", 6000, 2, "Current"),
("Account9", "2024-11-05", 20000, 8, "Savings"),
("Account10", "2024-11-05", 7000, 3, "Savings")
]
transactions_df = spark.createDataFrame(transactions, ["account_id", "transaction_date", "amount","frequency", "account_type"])
transactions_df.show()

classify_df=transactions_df.withColumn("classification",when(((col("amount")>10000) & (col("frequency")>5)),"High Risk")\
                                       .when(col("amount").between(5000,10000) & (col("frequency").between(2,5)),"Moderate Risk")\
                                       .otherwise("Low Risk"))
classify_df.show()

trans_df=classify_df.groupBy(col("classification")).agg(sum(col("frequency")).alias("trans_count"))
trans_df.show()

tot_tran_df=classify_df.filter(col("classification")=="High Risk").agg(sum(col("amount")).alias("total_tran_amn"))
tot_tran_df.show()

mod_risk_df=classify_df.filter((col("classification")=="Moderate Risk") & (col("account_type")=="Savings") & (col("amount")>7500).alias(",mod_risk_transaction"))
mod_risk_df.show()