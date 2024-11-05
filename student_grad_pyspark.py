from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import when, col, min, max, count,avg,sum,expr
import os

os.environ["PYSPARK_PYTHON"]="C:/Users/hp/Desktop/bigdata/Python/Python/Python37/python.exe"

conf=SparkConf()
conf.set("spark.app.name","student_grad")
conf.set("spark.master","local[4]")

spark=SparkSession.Builder()\
    .config(conf=conf)\
    .getOrCreate()

students = [
("Student1", 70, 45, 60, 65, 75),
("Student2", 80, 55, 58, 62, 67),
("Student3", 65, 30, 45, 70, 55),
("Student4", 90, 85, 80, 78, 76),
("Student5", 72, 40, 50, 48, 52),
("Student6", 88, 60, 72, 70, 68),
("Student7", 74, 48, 62, 66, 70),
("Student8", 82, 56, 64, 60, 66),
("Student9", 78, 50, 48, 58, 55),
("Student10", 68, 35, 42, 52, 45)
]
students_df = spark.createDataFrame(students, ["student_id", "attendance_percentage","math_score", "science_score", "english_score", "history_score"])
students_df.show()

avg_score_df=students_df.withColumn("avg_score",(col("math_score")+col("science_score")+col("english_score")+col("history_score"))/4)
avg_score_df.show()

category_df=avg_score_df.withColumn("category",when(((col("attendance_percentage")<75) & (col("avg_score")<50)),"At-Risk")\
                                   .when(col("attendance_percentage").between(75,85),"Moderate Risk")\
                                   .otherwise("Low Risk"))
category_df.show()

avg_at_risk_df=category_df.select(col("student_id"),col("avg_score"),col("category")).filter(col("category")=="At-Risk")
avg_at_risk_df.show()

# above_seventy_df=category_df.withColumn("above_seventy",expr("int(math_score > 70) + int(science_score > 70) + int(english_score > 70) + int(history_score > 70)")
# ).filter(col("category")=="Moderate Risk")

above_seventy_df=category_df.withColumn("above_seventy",(when(col("math_score")>70,1).otherwise(0))\
                                        +(when(col("science_score")>70,1).otherwise(0))\
                                        +(when(col("english_score")>70,1).otherwise(0))\
                                        +(when(col("history_score")>70,1).otherwise(0))
                                        ).filter((col("category")=="Moderate Risk") & (col("above_seventy")>=3))
above_seventy_df.show()