from pyspark.sql import SparkSession
from pyspark import SparkConf
import os

os.environ["PYSPARK_PYTHON"]="C:/Users/hp/Desktop/bigdata/Python/Python/Python37/python.exe"

conf=SparkConf()
conf.set("spark.app.name","bank_fraud")
conf.set("spark.master","local[4]")
conf.set("spark.local.dir","C:/temp")

spark=SparkSession.Builder()\
    .config(conf=conf)\
    .getOrCreate()

transactions = [
("Account1", "2024-11-01", 12000, 6, "Savings"),
("Account2", "2024-11-01", 8000, 3, "Current"),
("Account3", "2024-11-02", 2000, 1, "Savings"),
("Account4", "2024-11-02", 15000, 7, "Savings"),
("Account5", "2024-11-03", 9000, 4, "Current"),
("Account6", "2024-11-03", 3000, 1, "Current"),
("Account7", "2024-11-04", 13000, 5, "Savings"),
("Account8", "2024-11-04", 6000, 2, "Current"),
("Account9", "2024-11-05", 20000, 8, "Savings"),
("Account10", "2024-11-05", 7000, 3, "Savings")
]
transactions_df = spark.createDataFrame(transactions, ["account_id", "transaction_date", "amount","frequency", "account_type"])
transactions_df.show()

transactions_df.createTempView("main_table")
risk_category_df=spark.sql("""
select *,
case
    when amount>10000 and frequency>5 then "High Risk"
    when (amount between 5000 and 10000) and (frequency between 2 and 5) then "Moderate Risk"
    else "Low Risk"
end as risk_category
from main_table
""")
print("=== risk_category_df ===")
risk_category_df.show()

risk_category_df.createTempView("risk_category_table")
tras_count_df=spark.sql("""
select risk_category,
sum(frequency) as total_trans
from risk_category_table
group by risk_category
""")
print("===tras_count_df===")
tras_count_df.show()

#Identify accounts with at least one "High Risk" transaction
acc_hr_trans_df=spark.sql("""
select
account_id
from risk_category_table
where risk_category='High Risk'
""")
print("=== acc_hr_trans_df ===")
acc_hr_trans_df.show()

#the total amount transacted by those accounts
total_tran_df=spark.sql("""
select 
    account_id,
    sum(amount) as total_sum
from risk_category_table
group by account_id
having account_id in (
    select
        account_id
    from risk_category_table
    where risk_category='High Risk'
    )
""")
print("=== total_tran_df ===")
total_tran_df.show()

#Find all "Moderate Risk" transactions where the account type is "Savings" and the amount is above 7,500
mod_risk_trans_df=spark.sql("""
select * from risk_category_table
    where risk_category='Moderate Risk' and account_type='Savings' and amount>7500
""")
print("=== mod_risk_trans_df ===")
mod_risk_trans_df.show()