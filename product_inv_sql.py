from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import when, col, count
import os

os.environ["PYSPARK_PYTHON"]="C:/Users/hp/Desktop/bigdata/Python/Python/Python37/python.exe"

conf=SparkConf()
conf.set("spark.app.name","product_inv")
conf.set("spark.master","local[4]")
conf.set("spark.local.dir","C:/temp")

spark=SparkSession.Builder()\
    .config(conf=conf)\
    .getOrCreate()

inventory = [
("ProductA", 120),
("ProductB", 95),
("ProductC", 45),
("ProductD", 200),
("ProductE", 75),
("ProductF", 30),
("ProductG", 85),
("ProductH", 100),
("ProductI", 60),
("ProductJ", 20)
]
inventory_df = spark.createDataFrame(inventory, ["product_name", "stock_quantity"])
inventory_df.show()

inventory_df.createTempView("main_table")
prod_category_df=spark.sql("""
select *,
case
    when stock_quantity>1000 then "Overstocked"
    when stock_quantity between 50 and 100 then "Normal"
    else "Low stock"
end as category
from main_table
""")
print("=== prod_category_df ===")
prod_category_df.show()

prod_category_df.createTempView("category_table")
agg_stock_df=spark.sql("""
select
category,
sum(stock_quantity) as total_prod_sum
from category_table
group by category
""")
print("=== agg_stock_df ===")
agg_stock_df.show()