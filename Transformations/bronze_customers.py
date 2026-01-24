import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

###############################  DATA INGESTION   #################################

### Schema

customers_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("gender", StringType(), True),
    StructField("city", StringType(), True),
    StructField("join_date", DateType(), True),
    StructField("status", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("preferred_channel", StringType(), True),
    StructField("occupation", StringType(), True),
    StructField("income_range", StringType(), True),
    StructField("risk_segment", StringType(), True)
])

### Delta Live Streaming table

@dlt.expect_or_fail("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_customer_name", "name IS NOT NULL")
@dlt.expect_or_drop("valid_dob", "dob IS NOT NULL")
@dlt.expect("valid_city", "city IS NOT NULL")
@dlt.expect_or_drop("valid_join_date", "join_date IS NOT NULL")
@dlt.expect_or_drop("valid_email", "email IS NOT NULL AND email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'")
@dlt.expect_or_drop("valid_phone", "phone_number IS NOT NULL")
@dlt.expect_or_drop("valid_channel", "preferred_channel IS NOT NULL")
@dlt.expect("valid_occupation", "occupation IS NOT NULL")
@dlt.expect("valid_income", "income_range IS NOT NULL")
@dlt.expect("valid_risk_segment", "risk_segment IS NOT NULL")
@dlt.expect("valid_gender", "gender IS NOT NULL")
@dlt.expect_or_drop("valid_status", "status IS NOT NULL")
@dlt.table
def customers_bronze():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("header", "true")
        .schema(customers_schema)
        .load("/Volumes/project/dlt_bank/vol/customers/")
    )
    
    #trim
    string_columns = [ "name", "gender", "city", "status", "email", "phone_number", "preferred_channel", "occupation", "income_range", "risk_segment" ]
    for column in string_columns:
        df = df.withColumn(column, trim(col(column)))

    #lower
    for column in string_columns:
        df = df.withColumn(column, lower(col(column)))

    df = df.dropDuplicates()
    
    return df













