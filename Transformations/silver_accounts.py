import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

#######################  DATA TRANSFORMATION  ##########################

@dlt.table
def accounts_silver():
    df = spark.readStream.table("accounts_bronze")

    df = df.filter(col("txn_channel").isin("mobile", "online", "branch", "atm", "app"))
    df = df.filter(col("account_type").isin("current", "savings", "loan"))
    df = df.filter(col("txn_type").isin("debit", "credit"))

    df = df.withColumn("channel_type",when((col("txn_channel") == "atm") | (col("txn_channel") == "branch"), lit("physical")).otherwise(lit("digital")))
    df = df.withColumn("txn_year", year(col("txn_date"))).withColumn("txn_month", month(col("txn_date"))).withColumn("txn_day", dayofmonth(col("txn_date")))
    df = df.withColumn("acct_transformation_date", current_timestamp())

    return df
