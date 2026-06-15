from pyspark import pipelines as dp
from pyspark.sql.functions import when, col, count, sum, avg, max, datediff, current_date, round


## Sales & Revenue
@dp.materialized_view(name="revenue_by_city_gold")
def revenue_by_city_gold():
    orders   = spark.read.table("orders_silver")
    payments = spark.read.table("payments_silver")
    customers = spark.read.table("customers_silver")

    return (
        orders
        .join(payments, on="order_id")
        .join(customers, on="customer_id")
        .groupBy("customer_city", "customer_state")
        .agg(
            round(sum("payment_value"), 2).alias("total_revenue"),
            count("order_id").alias("total_orders"),
            round(avg("payment_value"), 2).alias("avg_order_value")
        )
        .orderBy(col("total_revenue").desc())
    )


## Delivery Performance
@dp.materialized_view(name="delivery_performance_gold")
def delivery_performance_gold():
    orders    = spark.read.table("orders_silver")
    customers = spark.read.table("customers_silver")

    return (
        orders
        .join(customers, on="customer_id")
        .groupBy("customer_city", "customer_state")
        .agg(
            round(avg("delivery_days"), 1).alias("avg_delivery_days"),
            count("order_id").alias("total_orders"),
            count(when(
                col("order_delivered_customer_date") > col("order_estimated_delivery_date"), 1
            )).alias("late_deliveries"),
            round(
                count(when(
                    col("order_delivered_customer_date") > col("order_estimated_delivery_date"), 1
                )) * 100.0 / count("order_id"), 1
            ).alias("late_delivery_pct")
        )
        .orderBy(col("avg_delivery_days").desc())
    )


## Product Analysis (Standalone — no order_items)
@dp.materialized_view(name="product_analysis_gold")
def product_analysis_gold():
    products = spark.read.table("products_silver")

    return (
        products
        .groupBy("product_category_name")
        .agg(
            count("product_id").alias("total_products"),
            round(avg("product_weight_g"), 1).alias("avg_weight_g"),
            round(avg("product_length_cm"), 1).alias("avg_length_cm"),
            round(avg("product_height_cm"), 1).alias("avg_height_cm"),
            round(avg("product_width_cm"), 1).alias("avg_width_cm"),
            round(avg("product_photos_qty"), 1).alias("avg_photos")
        )
        .orderBy(col("total_products").desc())
    )


## Payment Analysis
@dp.materialized_view(name="payment_analysis_gold")
def payment_analysis_gold():
    payments = spark.read.table("payments_silver")
    orders   = spark.read.table("orders_silver")

    return (
        payments
        .join(orders, on="order_id")
        .groupBy("payment_type")
        .agg(
            count("order_id").alias("total_orders"),
            round(sum("payment_value"), 2).alias("total_revenue"),
            round(avg("payment_installments"), 1).alias("avg_installments"),
            round(avg("payment_value"), 2).alias("avg_order_value")
        )
        .orderBy(col("total_revenue").desc())
    )


## Customer Segmentation (RFM)
@dp.materialized_view(name="customer_segments_gold")
def customer_segments_gold():
    orders    = spark.read.table("orders_silver")
    payments  = spark.read.table("payments_silver")
    customers = spark.read.table("customers_silver")

    rfm = (
        orders
        .join(payments, on="order_id")
        .join(customers, on="customer_id")
        .groupBy("customer_id", "customer_city")
        .agg(
            count("order_id").alias("frequency"),
            round(sum("payment_value"), 2).alias("monetary"),
            max("order_purchase_timestamp").alias("last_purchase")
        )
        .withColumn("recency_days", datediff(current_date(), col("last_purchase")))
    )

    return (
        rfm.withColumn("segment",
            when((col("frequency") >= 3) & (col("monetary") >= 500), "VIP")
            .when((col("frequency") >= 2) & (col("monetary") >= 200), "Loyal")
            .when(col("recency_days") > 180, "At Risk")
            .otherwise("New Customer")
        )
    )