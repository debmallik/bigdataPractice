from mileage_pyspark import mileage_df
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import when, initcap, col, count
import os

os.environ["PYSPARK_PYTHON"]="C:/Users/hp/Desktop/bigdata/Python/Python/Python37/python.exe"

conf=SparkConf()
conf.set("spark.app.name","overtime")
conf.set("spark.master","local[4]")
conf.set("spark.local.dir","C:/temp")

spark=SparkSession.Builder()\
    .config(conf=conf)\
    .getOrCreate()

vehicles = [
("CarA", 30),
("CarB", 22),
("CarC", 18),
("CarD", 15),
("CarE", 10),
("CarF", 28),
("CarG", 12),
("CarH", 35),
("CarI", 25),
("CarJ", 16)
]
vehicles_df = spark.createDataFrame(vehicles, ["vehicle_name", "mileage"])
vehicles_df.show()

vehicles_df.createTempView("mileage_table")
mileage_df=spark.sql("""
select
vehicle_name,
mileage,
case
    when mileage>25 then "High Efficiency"
    when mileage between 15 and 25 then "Moderate Efficiency"
    else "Low Efficiency"
end as mileage_category
from mileage_table
""")
mileage_df.show()