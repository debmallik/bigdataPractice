from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import when, col, min, max, count,avg
import os

os.environ["PYSPARK_PYTHON"]="C:/Users/hp/Desktop/bigdata/Python/Python/Python37/python.exe"

conf=SparkConf()
conf.set("spark.app.name","prod_sales")
conf.set("spark.master","local[4]")

spark=SparkSession.Builder()\
    .config(conf=conf)\
    .getOrCreate()

customer_loyalty = [
("Customer1", 25, 700),
("Customer2", 15, 400),
("Customer3", 5, 50),
("Customer4", 18, 450),
("Customer5", 22, 600),
("Customer6", 2, 80),
("Customer7", 12, 300),
("Customer8", 6, 150),
("Customer9", 10, 200),
("Customer10", 1, 90)
]
customer_loyalty_df = spark.createDataFrame(customer_loyalty, ["customer_name","purchase_frequency", "average_spending"])
customer_loyalty_df.show()

classify_df=customer_loyalty_df.withColumn("classification",when((col("purchase_frequency")>20) & (col("average_spending")>500),"Highly Loyal")\
                                           .when(col("purchase_frequency").between(10,20),"Moderately Loyal")\
                                           .otherwise("Low Loyalty"))
classify_df.show()

cust_count_df=classify_df.groupBy(col("classification")).agg(count(col("customer_name")).alias("cust_count"))
cust_count_df.show()

avg_spend_df=classify_df.groupBy(col("classification")).agg(avg(col("average_spending")).alias("avg_spending"))\
    .filter(col("classification")=="Highly Loyal")
avg_spend_df.show()

min_spend_df=classify_df.groupBy(col("classification")).agg(min(col("average_spending")).alias("min_spending"))\
    .filter(col("classification")=="Moderately Loyal")
min_spend_df.show()

spending_range_df=classify_df.filter(col("classification")=="Low Loyalty").filter((col("average_spending")<100) & (col("purchase_frequency")<5))
spending_range_df.show()