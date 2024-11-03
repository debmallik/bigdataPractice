from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import when, initcap, col, count
import os

os.environ["PYSPARK_PYTHON"]="C:/Users/hp/Desktop/bigdata/Python/Python/Python37/python.exe"

conf=SparkConf()
conf.set("spark.app.name","overtime")
conf.set("spark.master","local[4]")

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
mileage_df=vehicles_df.withColumn("Efficiency",when(col("mileage")>25,"High Efficiency").when((col("mileage")>15) & (col("mileage")<25),"Moderate Efficiency").otherwise("Low Efficiency"))

mileage_df.show()