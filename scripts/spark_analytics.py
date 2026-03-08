"""
Spark Batch Processing for Burkina Faso E-commerce Data
Part 2 of project - 30% of grade
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os

# Initialize Spark
spark = SparkSession.builder \
    .appName("Burkina Faso E-commerce Analytics") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

print("="*60)
print("🔥 SPARK BATCH PROCESSING - Burkina Faso E-COMMERCE")
print("="*60)

# Read the JSON files
print("\n📂 Reading data files...")
users_df = spark.read.option("multiline", "true").json("data/users.json")
products_df = spark.read.option("multiline", "true").json("data/products.json")
sessions_df = spark.read.option("multiline", "true").json("data/sessions.json")

print(f"✅ Users: {users_df.count():,}")
print(f"✅ Products: {products_df.count():,}") 
print(f"✅ Sessions: {sessions_df.count():,}")

# ============================================
# ANALYSIS 1: Users who bought X also bought Y
# ============================================
print("\n🔍 ANALYSIS 1: Product Affinity (Users who bought X also bought Y)")
print("-"*40)

# cart_contents in JSON is an object { "prod_xxx": { "quantity", "price" }, ... }
# Spark reads it as a STRUCT, not an array — convert struct to array of (product_id, quantity, price) then explode
cart_struct = sessions_df.schema["cart_contents"].dataType
cart_product_ids = [f.name for f in cart_struct.fields] if hasattr(cart_struct, "fields") else []

cart_items_array = array([
    struct(
        lit(pid).alias("product_id"),
        col(f"cart_contents.{pid}.quantity").alias("quantity"),
        col(f"cart_contents.{pid}.price").alias("price_rwf")
    )
    for pid in cart_product_ids
]) if cart_product_ids else array()

# Extract transactions from sessions (converted only)
purchases = sessions_df \
    .filter(col("conversion_status") == "converted") \
    .withColumn("_cart_items", cart_items_array) \
    .select("session_id", "user_id", explode("_cart_items").alias("cart_item")) \
    .filter(col("cart_item.quantity").isNotNull()) \
    .select(
        "user_id",
        col("cart_item.product_id").alias("product_id"),
        col("cart_item.quantity").alias("quantity"),
        col("cart_item.price_rwf").alias("price_rwf")
    )

print(f"📊 Transaction items: {purchases .count()}")

# Find pairs of products bought together
product_pairs = purchases .alias("a") \
    .join(purchases .alias("b"), 
          (col("a.user_id") == col("b.user_id")) & 
          (col("a.product_id") < col("b.product_id"))) \
    .groupBy(col("a.product_id").alias("product_x"), 
             col("b.product_id").alias("product_y")) \
    .agg(count("*").alias("times_bought_together")) \
    .orderBy(col("times_bought_together").desc()) \
    .limit(10)

print("\n🏆 Top 10 Product Pairs Bought Together:")
product_pairs.show(truncate=False)

# ============================================
# ANALYSIS 2: Customer Lifetime Value by Location
# ============================================
print("\n🔍 ANALYSIS 2: Customer Lifetime Value by City")
print("-"*40)

# Join transactions with users to get location (city/state from geo_data)
customer_value = purchases  \
    .join(users_df, "user_id") \
    .groupBy(col("geo_data.city").alias("city"), col("geo_data.state").alias("province")) \
    .agg(
        count_distinct("user_id").alias("customer_count"),
        sum("price_rwf").alias("total_revenue_rwf"),
        avg("price_rwf").alias("avg_order_value_rwf"),
        count("*").alias("total_orders")
    ) \
    .orderBy(col("total_revenue_rwf").desc()) \
    .limit(10)

print("\n💰 Top Cities by Revenue:")
customer_value.show(truncate=False)

# Add USD conversion
customer_value_with_usd = customer_value \
    .withColumn("total_revenue_usd", round(col("total_revenue_rwf") * 0.0009, 2)) \
    .withColumn("avg_order_usd", round(col("avg_order_value_rwf") * 0.0009, 2))

print("\n💵 With USD conversion:")
customer_value_with_usd.show(truncate=False)

# ============================================
# ANALYSIS 3: Device Performance Analysis
# ============================================
print("\n🔍 ANALYSIS 3: Conversion Rate by Device Type")
print("-"*40)

device_performance = sessions_df \
    .groupBy("device_profile") \
    .agg(
        count("*").alias("total_sessions"),
        sum(when(col("conversion_status") == "converted", 1).otherwise(0)).alias("converted_sessions"),
        avg(when(col("conversion_status") == "converted", 
         col("duration_seconds")).otherwise(0)).alias("avg_session_duration")
    ) \
    .withColumn("conversion_rate", 
                round(col("converted_sessions") / col("total_sessions") * 100, 2)) \
    .orderBy(col("conversion_rate").desc())

print("\n📱 Device Performance:")
device_performance.show(truncate=False)

# Flatten device_profile for CSV write (CSV doesn't support STRUCT)
device_performance_flat = device_performance \
    .withColumn("device_type", col("device_profile.type")) \
    .withColumn("device_os", col("device_profile.os")) \
    .withColumn("device_browser", col("device_profile.browser")) \
    .drop("device_profile")

# ============================================
# ANALYSIS 4: Time-based Cohort Analysis
# ============================================
print("\n🔍 ANALYSIS 4: User Registration Cohort Analysis")
print("-"*40)

# Extract registration month from users
users_with_month = users_df \
    .withColumn("reg_month", 
                date_format(to_date("registration_date"), "yyyy-MM"))

# Join with transactions
cohort_analysis = users_with_month \
    .join(purchases , "user_id") \
    .groupBy("reg_month") \
    .agg(
        count_distinct("user_id").alias("cohort_size"),
        sum("price_rwf").alias("total_spent"),
        count("*").alias("total_purchases"),
        avg("price_rwf").alias("avg_purchase")
    ) \
    .orderBy("reg_month")

print("\n📊 Cohort Performance by Registration Month:")
cohort_analysis.show(truncate=False)

# ============================================
# Save Results for Report
# ============================================
print("\n💾 Saving results for report...")
os.makedirs("output", exist_ok=True)

# Save as CSV for easy copy-paste to report
product_pairs.coalesce(1).write.mode("overwrite").csv("output/product_pairs", header=True)
customer_value_with_usd.coalesce(1).write.mode("overwrite").csv("output/customer_value", header=True)
device_performance_flat.coalesce(1).write.mode("overwrite").csv("output/device_performance", header=True)
cohort_analysis.coalesce(1).write.mode("overwrite").csv("output/cohort_analysis", header=True)

print("✅ Results saved to 'output/' folder")

print("\n" + "="*60)
print("✅ SPARK ANALYTICS COMPLETE!")
print("="*60)

spark.stop()