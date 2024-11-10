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

products = [
("Laptop", "Electronics", 120, 45),
("Smartphone", "Electronics", 80, 60),
("Tablet", "Electronics", 50, 72),
("Headphones", "Accessories", 110, 47),
("Shoes", "Clothing", 90, 55),
("Jacket", "Clothing", 30, 80),
("TV", "Electronics", 150, 40),
("Watch", "Accessories", 60, 65),
("Pants", "Clothing", 25, 75),
("Camera", "Electronics", 95, 58)
]
products_df = spark.createDataFrame(products, ["product_name", "category", "return_count","satisfaction_score"])
products_df.show()
products_df.createTempView("main_table")

return_rate_category_df=spark.sql("""
select *,
case
    when return_count>100 and satisfaction_score<50 then "High Return Rate"
    when (return_count between 50 and 100) and (satisfaction_score between 50 and 70) then "Moderate Return Rate"
    else "Low return Rate"
end as return_category
from main_table
""")
print("=== return_rate_category_df ===")
return_rate_category_df.show()

return_rate_category_df.createTempView("category_table")

###Group by category to count product return rates.
prod_ret_rate_count_df=spark.sql("""
select
return_category,
count(return_count) as prod_ret_rate_count
from category_table
group by return_category
""")
print("=== prod_ret_rate_count_df ===")
prod_ret_rate_count_df.show()
