import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

###############################  DATA INGESTION   #################################

### Schema
accounts_schema = StructType([
    StructField("account_id", LongType(), True),
    StructField("customer_id", LongType(), True),
    StructField("account_type", StringType(), True),
    StructField("balance", DoubleType(), True),
    StructField("txn_id", LongType(), True),
    StructField("txn_date", DateType(), True),
    StructField("txn_type", StringType(), True),
    StructField("txn_amount", DoubleType(), True),
    StructField("txn_channel", StringType(), True)
])

### Delta Live Streaming Table

@dlt.expect_or_fail("valid_account_id", "account_id IS NOT NULL")
@dlt.expect_or_fail("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_fail("valid_txn_id", "txn_id IS NOT NULL")
@dlt.expect_or_drop("account_type", "account_type IS NOT NULL")
@dlt.expect_or_drop("valid_balance", "balance IS NOT NULL")
@dlt.expect_or_drop("valid_txn_date", "txn_date IS NOT NULL")
@dlt.expect_or_drop("valid_txn_amount", "txn_amount IS NOT NULL")
@dlt.expect_or_drop("valid_txn_type", "txn_type IS NOT NULL")
@dlt.expect_or_drop("valid_txn_channel", "txn_channel IS NOT NULL")
@dlt.table
def accounts_bronze():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("header", "true")
        .schema(accounts_schema)
        .load("/Volumes/project/dlt_bank/vol/accounts/")
    )
    
    string_cols = [ "account_type", "txn_type", "txn_channel" ]
    # trim string columns
    for column in string_cols:
        df = df.withColumn(column, trim(col(column)))

    # lowercase string columns
    for column in string_cols:
        df = df.withColumn(column, lower(col(column)))
    
    df = df.dropDuplicates()

    return df





