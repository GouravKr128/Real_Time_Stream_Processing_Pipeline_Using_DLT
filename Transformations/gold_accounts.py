import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

###############################  SCD - Type 2   #################################

dlt.create_streaming_table("accounts_gold")

dlt.apply_changes(
    target = "accounts_gold",
    source = "accounts_silver",
    keys = ["txn_id"],
    sequence_by = col("acct_transformation_date"),
    stored_as_scd_type=2,
    except_column_list=["acct_transformation_date"]
)

