import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

###############################  SCD - Type 1   #################################

dlt.create_streaming_table("customers_gold")

dlt.apply_changes(
    target = "customers_gold",
    source = "customers_silver",
    keys = ["customer_id"],
    sequence_by = col("transformation_date"),
    stored_as_scd_type=1,
    except_column_list=["transformation_date"]
)
