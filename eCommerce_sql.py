from pyspark.sql import SparkSession
from pyspark import SparkConf
import os

os.environ["PYSPARK_PYTHON"]="C:/Users/hp/Desktop/bigdata/Python/Python/Python37/python.exe"

conf=SparkConf()
conf.set("spark.app.name","eCommerce")
conf.set("spark.master","local[4]")
conf.set("spark.local.dir","C:/temp")

spark=SparkSession.Builder()\
    .config(conf=conf)\
    .getOrCreate()

orders = [
("Order1", "Laptop", "Domestic", 2),
("Order2", "Shoes", "International", 8),
("Order3", "Smartphone", "Domestic", 3),
("Order4", "Tablet", "International", 5),
("Order5", "Watch", "Domestic", 7),
("Order6", "Headphones", "International", 10),
("Order7", "Camera", "Domestic", 1),
("Order8", "Shoes", "International", 9),
("Order9", "Laptop", "Domestic", 6),
("Order10", "Tablet", "International", 4)
]
orders_df = spark.createDataFrame(orders, ["order_id", "product_type", "origin", "delivery_days"])
orders_df.show()
orders_df.createTempView("main_table")

order_category_df=spark.sql("""
select
*,
case
    when delivery_days>7 and origin=='International' then 'Delayed'
    when delivery_days between 3 and 7 then 'On-Time'
    else 'Fast'
end as order_category
from main_table
""")
print("=== order_category_df ===")
order_category_df.show()

order_category_df.createTempView("order_category_table")

### Group by product type to see the count of each delivery speed category
count_df=spark.sql("""
select 
product_type,
order_category,
count(order_category) as count_del_speed
from order_category_table
group by product_type,order_category
""")
count_df.show()