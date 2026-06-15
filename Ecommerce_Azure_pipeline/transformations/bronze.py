from pyspark import pipelines as dp
from pyspark.sql.functions import *

from pyspark import pipelines as dp

@dp.table(
    name="customers_bronze"
)
def customers_bronze():
    return (
        spark.read
        .format("csv")
        .option("header","true")
        .load(
            "abfss://bronze@sdpprostorage.dfs.core.windows.net/customers.csv"
        )
    )


@dp.table(name = "orders_bronze")
def orders_bronze():
    return ( 
            spark.read.format("csv")
            .option("header","true")
            .load(
                "abfss://bronze@sdpprostorage.dfs.core.windows.net/orders.csv"
            )

    )


@dp.table(name = "payments_bronze")
def payments_bronze():
    return(
        spark.read
        .format("csv")
        .option("header","true")
        .load(
            "abfss://bronze@sdpprostorage.dfs.core.windows.net/payments.csv"
        )
    )
    

@dp.table(name = "products_bronze")
def products_bronze():
    return(
        spark.read
        .format("csv")
        .option("header","true")
        .load(
            "abfss://bronze@sdpprostorage.dfs.core.windows.net/products.csv"
        )
    )

