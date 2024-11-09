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

customer_loyalty_df.createTempView("main_table")
category_df=spark.sql("""
select *,
case
    when purchase_frequency>20 and average_spending>500 then "Highly Loyal"
    when purchase_frequency between 10 and 20 then "Moderately Loyal"
    else "Low Loyalty"
end as loyal_category
from main_table
""")
print("===category_df====")
category_df.show()

category_df.createTempView("loyal_category_table")
count_cust_df=spark.sql("""
select loyal_category,
count(*) as cust_count
from loyal_category_table
group by loyal_category
""")
print("===count_cust_df===")
count_cust_df.show()

avg_spend_df=spark.sql("""
select loyal_category,average_spending
    from loyal_category_table
        where loyal_category='Highly Loyal'
""")
print("===avg_spend_df===")
avg_spend_df.show()

min_spend_df=spark.sql("""
select 
    loyal_category,
    min(average_spending) as min_spend
    from loyal_category_table
group by loyal_category
having loyal_category='Moderately Loyal'
""")
print("== min_spend_df ==")
min_spend_df.show()

low_loyal_cust_df=spark.sql("""
select * from loyal_category_table
    where loyal_category='Low Loyalty' and average_spending<100 and purchase_frequency<5
""")
print("=== low_loyal_cust_df ===")
low_loyal_cust_df.show()