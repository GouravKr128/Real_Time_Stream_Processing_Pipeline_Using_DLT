import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

#################### INNER JOIN ####################

@dlt.table
def materialized_tbl_gold():
    customers = dlt.read("customers_gold")
    txn = dlt.read("accounts_gold")

    df = customers.join(txn, on="customer_id", how="inner")

    return df
