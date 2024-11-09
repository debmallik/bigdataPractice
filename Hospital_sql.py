from pyspark.sql import SparkSession
from pyspark import SparkConf
import os

os.environ["PYSPARK_PYTHON"]="C:/Users/hp/Desktop/bigdata/Python/Python/Python37/python.exe"

conf=SparkConf()
conf.set("spark.app.name","hospital")
conf.set("spark.master","local[4]")
conf.set("spark.local.dir","C:/temp")

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

patients_df.createTempView("main_table")
risk_category_df=spark.sql("""
select *,
case
    when readmission_interval<15 and age>60 then "High Readmission Risk"
    when readmission_interval between 15 and 30 then "Moderate Risk"
    else "Low Risk"
end as risk_status
from main_table
""")
print("=== risk_category_df ===")
risk_category_df.show()

###Count patients in each category.
risk_category_df.createTempView("risk_category_table")
patient_count_df=spark.sql("""
select 
    risk_status,
    count(patient_id) as patient_count
    from risk_category_table
    group by risk_status
""")
print("=== patient_count_df ===")
patient_count_df.show()

###Find the average readmission interval for "High Readmission Risk" patients
avg_readmin_interval_df=spark.sql("""
select 
risk_status,
avg(readmission_interval) as readmin_interval
from risk_category_table
group by risk_status
""")
print("=== avg_readmin_interval_df ===")
avg_readmin_interval_df.show()

###Identify "Moderate Risk" patients who were admitted to the "ICU" more than twice in the past year
icu_admin_count_df=spark.sql("""
select * from risk_category_table
    where risk_status='Moderate Risk' and icu_admissions>2
""")
print("=== icu_admin_count_df ===")
icu_admin_count_df.show()



