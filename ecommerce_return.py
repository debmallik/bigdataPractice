from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, min, max, count,avg
import os

os.environ["PYSPARK_PYTHON"]="C:/Users/hp/Desktop/bigdata/Python/Python/Python37/python.exe"

conf=SparkConf()
conf.set("spark.app.name","ecom_return")
conf.set("spark.master","local[4]")
conf.set("spark.local.dir","C:/temp")

spark=SparkSession.Builder()\
    .config(conf=conf)\
    .getOrCreate()

ecommerce_return = [
("Product1", 75, 25),
("Product2", 40, 15),
("Product3", 30, 5),
("Product4", 60, 18),
("Product5", 100, 30),
("Product6", 45, 10),
("Product7", 80, 22),
("Product8", 35, 8),
("Product9", 25, 3),
("Product10", 90, 12)
]
ecommerce_return_df = spark.createDataFrame(ecommerce_return, ["product_name", "sale_price","return_rate"])
ecommerce_return_df.show()

ecommerce_return_df.createTempView("main_table")
return_category_df=spark.sql("""
select *,
case
    when return_rate>20 then "High Return"
    when return_rate between 10 and 20 then "Medium Return"
    else "Low Return"
end as return_category
from main_table
""")
print("=== return_category_df ===")
return_category_df.show()

return_category_df.createTempView("ecommerce_return_table")
avg_sale_price_df=spark.sql("""
select 
return_category,
avg(sale_price) as avg_sale_price
from ecommerce_return_table
group by return_category
having return_category="High Return"
""")
print("=== avg_sale_price_df ===")
avg_sale_price_df.show()

max_return_rate_df=spark.sql("""
select 
return_category,
max(return_rate) as max_return_rate
from ecommerce_return_table
group by return_category
having return_category="Medium Return"
""")
print("=== max_return_rate_df ===")
max_return_rate_df.show()

low_return_df=spark.sql("""
select * from ecommerce_return_table
    where sale_price<50 and return_rate<5
""")
print("=== low_return_df ===")
low_return_df.show()

