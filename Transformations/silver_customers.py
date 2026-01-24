import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

#######################  DATA  TRANSFORMATION  ##########################

@dlt.table
def customers_silver():
    df = spark.readStream.table("customers_bronze")

    df = df.withColumn("gender", when(col("gender") == "m", lit("male")).when(col("gender") == "f", lit("female")).otherwise("unknown"))

    df = df.withColumn("city", when(col("city").isNull(), lit("unknown")).otherwise(col("city")))
    df = df.withColumn("occupation", when(col("occupation").isNull(), lit("unknown")).otherwise(col("occupation")))

    df = df.withColumn("income_range",when(col("income_range").isin(["low", "medium", "high", "very high"]),col("income_range")).otherwise(lit("unknown")))
    df = df.withColumn("risk_segment",when(col("risk_segment").isin(["low", "medium", "high", "very high"]),col("risk_segment")).otherwise(lit("unknown")))
  

    df = df.withColumn("phone_number", trim(col("phone_number")))
    df = df.withColumn("phone_number", regexp_replace(col("phone_number"), r"[^0-9\+]", ""))
    df = df.filter(col("phone_number").rlike(r"^\+44\d{10}$"))


    df = df.filter(col("preferred_channel").isin("mobile", "online", "branch", "atm"))
    df = df.filter(col("status").isin("active", "inactive", "pending"))
    df = df.filter( (col("dob") > lit("1900-01-01")) & (col("dob") < current_date()) )
    
    
    df = df.withColumn("customer_age", when(col("dob").isNotNull(), floor(months_between(current_date(), col("dob"))/12)).otherwise(lit(None)))
    df = df.withColumn("tenure_days", when(col("join_date").isNotNull(), datediff(current_date(), col("join_date"))).otherwise(lit(None)))
    df = df.withColumn("transformation_date", current_timestamp())

    return df



