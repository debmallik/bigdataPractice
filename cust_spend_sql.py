from pyspark.sql import SparkSession
from pyspark import SparkConf
import os

os.environ["PYSPARK_PYTHON"]="C:/Users/hp/Desktop/bigdata/Python/Python/Python37/python.exe"

conf=SparkConf()
conf.set("spark.app.name","product_return")
conf.set("spark.master","local[4]")
conf.set("spark.local.dir","C:/temp")

spark=SparkSession.Builder()\
    .config(conf=conf)\
    .getOrCreate()

customers = [
("karthik", "Premium", 1050, 32),
("neha", "Standard", 800, 28),
("priya", "Premium", 1200, 40),
("mohan", "Basic", 300, 35),
("ajay", "Standard", 700, 25),
("vijay", "Premium", 500, 45),
("veer", "Basic", 450, 33),
("aatish", "Standard", 600, 29),
("animesh", "Premium", 1500, 60),
("nishad", "Basic", 200, 21)
]
customers_df = spark.createDataFrame(customers, ["name", "membership", "spending", "age"])
customers_df.show()

customers_df.createTempView("main_table")
spend_category_df=spark.sql("""
select *,
case
    when spending>1000 and membership=='Premium' then "High Spender"
    when spending between 500 and 1000 and membership='Standard' then 'Average Spender'
    else "Low Spender"
end as spending_category
from main_table
""")
print("=== spend_category_df ===")
spend_category_df.show()

spend_category_df.createTempView("category_table")

###Group by membership and calculate average spending
avg_spending_df=spark.sql("""
select
membership,
avg(spending) as avg_spend
from category_table
group by membership
""")
print("=== avg_spending_df ===")
avg_spending_df.show()
