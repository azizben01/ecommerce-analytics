"""
Integrated Analytics Query
Customer Lifetime Value using multiple data sources
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, sum, count, avg, array, struct, lit

print("="*60)
print("🔗 INTEGRATED ANALYTICS QUERY")
print("Customer Lifetime Value (CLV)")
print("="*60)

spark = SparkSession.builder \
    .appName("IntegratedAnalytics") \
    .getOrCreate()

# ----------------------------
# Load data sources
# ----------------------------

print("\n📂 Loading datasets...")

users = spark.read.option("multiline", "true").json("data/users.json")
sessions = spark.read.option("multiline", "true").json("data/sessions.json")
products = spark.read.option("multiline", "true").json("data/products.json")

print("Users:", users.count())
print("Sessions:", sessions.count())
print("Products:", products.count())

# ----------------------------
# Extract transaction items
# cart_contents is a STRUCT {prod_xxx: {quantity, price}}, not an array
# Convert struct to array of (product_id, quantity, price_rwf) then explode
# ----------------------------

cart_struct = sessions.schema["cart_contents"].dataType
cart_product_ids = [f.name for f in cart_struct.fields] if hasattr(cart_struct, "fields") else []
cart_items_array = array([
    struct(
        lit(pid).alias("product_id"),
        col(f"cart_contents.{pid}.quantity").alias("quantity"),
        col(f"cart_contents.{pid}.price").alias("price_rwf")
    )
    for pid in cart_product_ids
]) if cart_product_ids else array()

transactions = sessions \
    .filter(col("conversion_status") == "converted") \
    .withColumn("_cart_items", cart_items_array) \
    .select("user_id", explode("_cart_items").alias("item")) \
    .filter(col("item.quantity").isNotNull()) \
    .select(
        "user_id",
        col("item.product_id").alias("product_id"),
        col("item.quantity").alias("quantity"),
        col("item.price_rwf").alias("price_rwf")
    )

print("\n📊 Transaction records:", transactions.count())

# ----------------------------
# Calculate revenue per user
# ----------------------------

transactions = transactions.withColumn(
    "revenue",
    col("quantity") * col("price_rwf")
)

user_revenue = transactions.groupBy("user_id").agg(
    sum("revenue").alias("total_revenue_rwf"),
    count("product_id").alias("total_purchases")
)

# ----------------------------
# Engagement metrics
# ----------------------------

engagement = sessions.groupBy("user_id").agg(
    count("session_id").alias("session_count"),
    avg("duration_seconds").alias("avg_session_duration")
)

# ----------------------------
# Integrated analytics
# ----------------------------

clv = users.join(user_revenue, "user_id", "left") \
           .join(engagement, "user_id", "left")

print("\n🏆 Top Customers by Lifetime Value:")

clv.orderBy(col("total_revenue_rwf").desc()) \
   .select(
       "user_id",
       col("geo_data.city").alias("city"),
       "total_revenue_rwf",
       "total_purchases",
       "session_count"
   ).show(10)

# Save results (flatten geo_data for CSV - CSV doesn't support STRUCT)
clv_flat = clv.select(
    "user_id",
    col("geo_data.city").alias("city"),
    col("geo_data.state").alias("province"),
    "total_revenue_rwf",
    "total_purchases",
    "session_count",
    "avg_session_duration"
)
clv_flat.write.mode("overwrite").csv("output/clv_analysis", header=True)

print("\n✅ Integrated analytics saved to output/clv_analysis")

spark.stop()