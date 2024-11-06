from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import when, col, avg
import os

os.environ["PYSPARK_PYTHON"]="C:/Users/hp/Desktop/bigdata/Python/Python/Python37/python.exe"

conf=SparkConf()
conf.set("spark.app.name","financial_risk")
conf.set("spark.master","local[4]")
conf.set("spark.local.dir","C:/temp")

spark=SparkSession.Builder()\
    .config(conf=conf)\
    .getOrCreate()

loan_applicants = [
("karthik", 60000, 120000, 590),
("neha", 90000, 180000, 610),
("priya", 50000, 75000, 680),
("mohan", 120000, 240000, 560),
("ajay", 45000, 60000, 620),
("vijay", 100000, 100000, 700),
("veer", 30000, 90000, 580),
("aatish", 85000, 85000, 710),
("animesh", 50000, 100000, 650),
("nishad", 75000, 200000, 540)
]

loan_applicants_df = spark.createDataFrame(loan_applicants, ["name", "income", "loan_amount","credit_score"])
loan_applicants_df.show()

loan_applicants_df.createTempView("finance_risk_table")
classification_df=spark.sql("""
select
    name,
    income,
    loan_amount,
    credit_score,
    case
        when loan_amount > 2*income and credit_score < 600 then "High Risk"
        when loan_amount > income and loan_amount < 2*income and credit_score between 600 and 700 then "Moderate Risk"
        else "Low Risk"
    end as classification
    from finance_risk_table
""")
classification_df.show()

classification_df.createTempView("classification_table")
total_count_df=spark.sql("""
select
    classification,
    count(*) as total_count
    from classification_table
    group by classification
""")
total_count_df.show()

range_df=spark.sql("""
select *,
    case
        when income<50000 then "less_50k"
        when income between 50000 and 100000 then "btwn_50k_100K"
        else "more_100k"
    end as income_range
from classification_table
""")
range_df.show()

range_df.createTempView("average_loan_table")
avg_load_df=spark.sql("""
select 
    income_range,
    avg(loan_amount) as avg_loan_amt
    from average_loan_table
    group by(income_range)
""")
avg_load_df.show()

avg_cred_df=spark.sql("""
select 
    income_range,
    classification,
    avg(credit_score) as avg_cred_score
    from average_loan_table
    group by income_range,classification
    having avg(credit_score)<650
""")
print("Average Credit Score")
avg_cred_df.show()


# avg_loan_df=spark.sql("""
# select
#     avg(loan_amount) as avg_loan
#     from classification_table
#     where classification=="High Risk"
#     group by classification
# """)
# avg_loan_df.show()