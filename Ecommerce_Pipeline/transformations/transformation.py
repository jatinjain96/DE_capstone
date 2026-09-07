from pyspark import pipelines as dp
from pyspark.sql.functions import *
 
@dp.table(name="bronze_orders", comment="Raw Orders Data")
def load_orders():
    orders_df = spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "CSV") \
        .option("cloudFiles.inferColumnTypes", "true") \
        .option("header", "true") \
        .load(path="/Volumes/shopsphere/ecomm_schema/capstone_data/sql_server/initial/orders/")
    
    # Metadata columns
    metadata_df = orders_df.withColumn("_ingest_timestamp", expr("current_timestamp()")).withColumn("_source_file", col("_metadata.file_path"))
    return metadata_df

@dp.table(name="bronze_order_items", comment="Raw Order Items Data")
def load_order_items():
    order_items_df = spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "CSV") \
        .option("cloudFiles.inferColumnTypes", "true") \
        .option("header", "true") \
        .load(path="/Volumes/shopsphere/ecomm_schema/capstone_data/sql_server/initial/order_items/")
    
    # Metadata columns
    metadata_df = order_items_df.withColumn("_ingest_timestamp", expr("current_timestamp()")).withColumn("_source_file", col("_metadata.file_path"))
    return metadata_df

@dp.table(name="silver_orders", comment="Cleaned and Validated Orders Data")
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect_or_drop("valid_order_date", "order_date IS NOT NULL")
@dp.expect_or_drop("valid_total_amount", "total_amount >= 0")
def load_silver_orders():
    df = spark.readStream.table("bronze_orders")

    df_dedup = df.dropDuplicates()
    df_standardized = df_dedup.withColumn("order_status", initcap(trim(col("order_status")))) \
        .withColumn("shipping_address", trim(col("shipping_address")))
    
    return df_standardized

@dp.table(name="silver_order_items", comment="Cleaned and Validated Order Items Data")
@dp.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dp.expect_or_drop("valid_quantity", "quantity > 0")
@dp.expect_or_drop("valid_unit_price", "unit_price >= 0")
def load_silver_order_items():
    df = spark.readStream.table("bronze_order_items")
    
    df_dedup = df.dropDuplicates()
    df_calculated = df_dedup.withColumn("gross_amount", col("unit_price") * col("quantity")) \
        .withColumn("discount_amount", col("gross_amount") * col("discount")) \
        .withColumn("net_amount", col("gross_amount") - col("discount_amount"))
    
    return df_calculated

@dp.table(name="gold_daily_sales", comment="Daily Sales Aggregation")
def load_gold_daily_sales():
    orders = spark.read.table("silver_orders")
    order_items = spark.read.table("silver_order_items")
    
    joined = orders.join(order_items, "order_id")
    
    result = joined.groupBy(col("order_date").cast("date").alias("sales_date")) \
        .agg(
            countDistinct("order_id").alias("total_orders"),
            count(when(col("order_status") == "Delivered", 1)).alias("completed_orders"),
            count(when(col("order_status") == "Cancelled", 1)).alias("cancelled_orders"),
            count(when(col("order_status") == "Returned", 1)).alias("returned_orders"),
            sum("gross_amount").alias("gross_sales"),
            sum("discount_amount").alias("discount_amount"),
            sum(when(~col("order_status").isin("Cancelled", "Returned"), col("net_amount")).otherwise(lit(0))).alias("net_sales")
        ) \
        .withColumn("average_order_value",
            when(col("total_orders") > 0, round(col("net_sales") / col("total_orders"), 2)).otherwise(lit(0))) \
        .withColumn("cancellation_rate",
            when(col("total_orders") > 0, round((col("cancelled_orders") * 100.0) / col("total_orders"), 2)).otherwise(lit(0)))
    
    return result
