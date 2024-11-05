from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import when, col, min, max, count,avg,sum
import os

os.environ["PYSPARK_PYTHON"]="C:/Users/hp/Desktop/bigdata/Python/Python/Python37/python.exe"

conf=SparkConf()
conf.set("spark.app.name","hospital")
conf.set("spark.master","local[4]")

spark=SparkSession.Builder()\
    .config(conf=conf)\
    .getOrCreate()

patients = [
("Patient1", 62, 10, 3, "ICU"),
("Patient2", 45, 25, 1, "General"),
("Patient3", 70, 8, 2, "ICU"),
("Patient4", 55, 18, 3, "ICU"),
("Patient5", 65, 30, 1, "General"),
("Patient6", 80, 12, 4, "ICU"),
("Patient7", 50, 40, 1, "General"),
("Patient8", 78, 15, 2, "ICU"),
("Patient9", 40, 35, 1, "General"),
("Patient10", 73, 14, 3, "ICU")
]
patients_df = spark.createDataFrame(patients, ["patient_id", "age", "readmission_interval","icu_admissions", "admission_type"])
patients_df.show()

classify_df=patients_df.withColumn("classification",when(((col("readmission_interval")<15) & (col("age")>60)),"High Readmission Risk")\
    .when(col("readmission_interval").between(15,30),"Moderate Risk")\
    .otherwise("Low Risk"))
classify_df.show()

count_df=classify_df.groupBy(col("classification")).agg(count(col("patient_id")).alias("patient_count"))
count_df.show()

avg_readmission_df=classify_df.filter(col("classification")=="High Readmission Risk").agg(avg(col("readmission_interval")).alias("avg_readmin"))
avg_readmission_df.show()

mod_risk_df=classify_df.filter((col("classification")=="Moderate Risk") & (col("admission_type")=="ICU") & (col("icu_admissions")>2).alias("mod_risk_patient"))
mod_risk_df.show()