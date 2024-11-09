from pyspark.sql import SparkSession
from pyspark import SparkConf
import os

os.environ["PYSPARK_PYTHON"]="C:/Users/hp/Desktop/bigdata/Python/Python/Python37/python.exe"

conf=SparkConf()
conf.set("spark.app.name","prod_sales")
conf.set("spark.master","local[4]")
conf.set("spark.local.dir","C:/temp")

spark=SparkSession.Builder()\
    .config(conf=conf)\
    .getOrCreate()

product_sales = [
("Product1", 250000, 5),
("Product2", 150000, 8),
("Product3", 50000, 20),
("Product4", 120000, 10),
("Product5", 300000, 7),
("Product6", 60000, 18),
("Product7", 180000, 9),
("Product8", 45000, 25),
("Product9", 70000, 15),
("Product10", 10000, 30)
]
product_sales_df = spark.createDataFrame(product_sales, ["product_name", "total_sales","discount"])
product_sales_df.show()

product_sales_df.createTempView("main_table")
category_df=spark.sql("""
select *,
case
    when total_sales>200000 and discount<10 then "Top Seller"
    when total_sales between 100000 and 200000 then "Moderate Seller"
    else "Low Seller"
end as category
from main_table
""")
print("=== category_df ===")
category_df.show()

category_df.createTempView("category_table")
count_prod_df=spark.sql("""
select category,
count(*) as prod_count
from category_table
group by category
""")
print("=== count_prod_df ===")
count_prod_df.show()

max_sales_value_df=spark.sql("""
select category,
max(total_sales) as max_sales_val
from category_table
where category='Top Seller'
group by category
""")
print("=== max_sales_value_df ===")
max_sales_value_df.show()

min_dis_df=spark.sql("""
select category,
min(discount) as min_dis_value
from category_table
where category='Moderate Seller'
group by category
""")
print("=== min_dis_df ===")
min_dis_df.show()

low_seller_df=spark.sql("""
select * from category_table
    where category='Low Seller' and total_sales<50000 and discount>15
""")
print("=== low_seller_df ===")
low_seller_df.show()