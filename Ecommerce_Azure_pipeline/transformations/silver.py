from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql.window import Window


@dp.materialized_view(name="customers_silver")
@dp.expect_or_drop(
    "customer_id_not_null",
    "customer_id IS NOT NULL"
)
def customers_silver():
    return (
        spark.read.table("customers_bronze")
        .dropDuplicates(["customer_id"])
        .filter(col("customer_id").isNotNull())
        .withColumn("customer_city", initcap(trim(col("customer_city"))))
    )


@dp.materialized_view(name="orders_silver")
@dp.expect_or_drop(
    "order_id_not_null",
    "order_id IS NOT NULL"
)
def orders_silver():
    window = Window.partitionBy("order_id").orderBy(col("order_purchase_timestamp").desc())

    return (
        spark.read.table("orders_bronze")
        .withColumn("rank", row_number().over(window))
        .filter(col("rank") == 1)
        .drop("rank")
        .dropna(
            subset=[
                "order_approved_at",
                "order_delivered_carrier_date",
                "order_delivered_customer_date"
            ]
        )
        .withColumn("order_purchase_timestamp",
                    to_timestamp(col("order_purchase_timestamp")))
        .withColumn("order_approved_at",
                    to_timestamp(col("order_approved_at")))
        .withColumn("order_delivered_carrier_date",
                    to_timestamp(col("order_delivered_carrier_date")))
        .withColumn("order_delivered_customer_date",
                    to_timestamp(col("order_delivered_customer_date")))
        .withColumn("order_estimated_delivery_date",
                    to_timestamp(col("order_estimated_delivery_date")))
        .withColumn("delivery_days",
                    datediff(col("order_delivered_customer_date"),
                             col("order_purchase_timestamp")))
    )


@dp.materialized_view(name="payments_silver")
@dp.expect_or_drop(
    "payment_positive",
    "payment_value >= 0"
)
def payments_silver():
    return (
        spark.read.table("payments_bronze")
        .dropDuplicates(["order_id"])
        .dropna(subset=["order_id"])
        .withColumn("payment_type",
                    lower(trim(col("payment_type"))))
        .withColumn("payment_installments",
                    col("payment_installments").cast(IntegerType()))
        .withColumn("payment_sequential",
                    col("payment_sequential").cast(IntegerType()))
        .withColumn("payment_value",
                    col("payment_value").cast(DoubleType()))
    )


@dp.materialized_view(name="products_silver")
@dp.expect_or_drop(
    "product_id_not_null",
    "product_id IS NOT NULL"
)
def products_silver():
    return (
        spark.read.table("products_bronze")
        .dropDuplicates(["product_id"])
        .dropna()
        .withColumn("product_category_name",
                    lower(trim(col("product_category_name"))))
        .withColumn("product_name_lenght",
                    col("product_name_lenght").cast(IntegerType()))
        .withColumn("product_description_lenght",
                    col("product_description_lenght").cast(IntegerType()))
        .withColumn("product_photos_qty",
                    col("product_photos_qty").cast(IntegerType()))
        .withColumn("product_weight_g",
                    col("product_weight_g").cast(IntegerType()))
        .withColumn("product_length_cm",
                    col("product_length_cm").cast(IntegerType()))
        .withColumn("product_height_cm",
                    col("product_height_cm").cast(IntegerType()))
        .withColumn("product_width_cm",
                    col("product_width_cm").cast(IntegerType()))
    )