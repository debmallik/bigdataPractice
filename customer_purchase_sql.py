from pyspark.sql import SparkSession
from pyspark import SparkConf
import os

os.environ["PYSPARK_PYTHON"]="C:/Users/hp/Desktop/bigdata/Python/Python/Python37/python.exe"

conf=SparkConf()
conf.set("spark.app.name","cust_purchase")
conf.set("spark.master","local[4]")
conf.set("spark.local.dir","C:/temp")

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

customer_purchases_df.createTempView("purchase_table")
purchase_df=spark.sql("""
select *,
case
    when (days_since_last_purchase<30) then "Frequent"
    when (days_since_last_purchase between 30 and 60) then "Occasional"
    else "Rare"
end as purchase_frequency
from purchase_table
""")
print("===purchase_df=====")
purchase_df.show()

purchase_df.createTempView("category")
category_df=spark.sql("""
select
membership,
count(*) as category_count
from category
group by membership
""")
print("======category_df=====")
category_df.show()

avg_purchase_df=spark.sql("""
select
    membership,
    purchase_frequency,
    avg(total_purchase_amount) as avg_purchase
    from category
    group by membership,purchase_frequency
""")
avg_purchase_df.show()

min_purchase_amount_df=spark.sql("""
select
    membership,
    min(total_purchase_amount) as min_purchase_amount
    from category
    where (purchase_frequency='Rare')
    group by membership
""")
min_purchase_amount_df.show()