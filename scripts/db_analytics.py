"""
MongoDB Aggregation Queries for Burkina Faso E-commerce Data
Part 1 of the project - 3 analytical queries
"""

from pymongo import MongoClient
from datetime import datetime, timedelta
import pprint

# Connect to MongoDB
client = MongoClient('localhost', 27017)
db = client['ecommerce_analytics']

print("=" * 60)
print("📊 MONGODB AGGREGATION ANALYTICS - Burkina Faso E-COMMERCE")
print("=" * 60)

# ==========================================================
# QUERY 1: Product Popularity Analysis
# ==========================================================
print("\n🔍 QUERY 1: Top 10 Best-Selling Products by Revenue")
print("-" * 40)

pipeline_top_products = [
    {"$unwind": "$items"},

    {
        "$group": {
            "_id": "$items.product_id",
            "total_quantity": {"$sum": "$items.quantity"},
            "total_revenue": {"$sum": "$items.subtotal"},
            "order_count": {"$sum": 1}
        }
    },

    {"$sort": {"total_revenue": -1}},

    {"$limit": 10}
]

# Execute the pipeline
results = list(db.transactions.aggregate(pipeline_top_products))
for i, product in enumerate(results, 1):
    print(f"{i}. Product ID: {product['_id']}")
    print(f"   📦 Quantity Sold: {product['total_quantity']}")
    print(f"   💰 Revenue: {product['total_revenue']}")
    print(f"   🛒 Orders: {product['order_count']}")

# ==========================================================
# QUERY 2: Revenue Analytics by Category
# ==========================================================
print("\n\n🔍 QUERY 2: Revenue by Product Category")
print("-" * 40)

pipeline_revenue_category = [

    {"$unwind": "$items"},

    {
        "$lookup": {
            "from": "products",
            "localField": "items.product_id",
            "foreignField": "product_id",
            "as": "product_info"
        }
    },

    {"$unwind": "$product_info"},

    {
        "$group": {
            "_id": "$product_info.category_id",
            "total_revenue": {"$sum": "$items.subtotal"},
            "products_sold": {"$sum": "$items.quantity"}
        }
    },

    {"$sort": {"total_revenue": -1}}
]

# Execute (using transactions collection which has total_rwf)
results2 = list(db.transactions.aggregate(pipeline_revenue_category))
for cat in results2:
    print(f"📁 Category: {cat['_id']}")
    print(f"   💰 Revenue: {cat['total_revenue']}")
    print(f"   📦 Products Sold: {cat['products_sold']}")

# ==========================================================
# QUERY 3: Customer Segmentation (by purchase frequency)
# ==========================================================
print("\n\n🔍 QUERY 3: Customer Segmentation by Purchase Frequency")
print("-" * 40)

pipeline_customer_segment = [

    {
        "$group": {
            "_id": "$user_id",
            "order_count": {"$sum": 1},
            "total_spent": {"$sum": "$total"}
        }
    },

    {
        "$addFields": {
            "segment": {
                "$switch": {
                    "branches": [
                        {"case": {"$gte": ["$order_count", 10]}, "then": "VIP"},
                        {"case": {"$gte": ["$order_count", 5]}, "then": "Regular"},
                        {"case": {"$gte": ["$order_count", 2]}, "then": "Occasional"},
                        {"case": {"$eq": ["$order_count", 1]}, "then": "New"}
                    ],
                    "default": "Inactive"
                }
            }
        }
    },

    {
        "$group": {
            "_id": "$segment",
            "customer_count": {"$sum": 1},
            "avg_spent": {"$avg": "$total_spent"}
        }
    }
]

results3 = list(db.transactions.aggregate(pipeline_customer_segment))
for segment in results3:
    print(f"👥 Segment: {segment['_id']}")
    print(f"   Customers: {segment['customer_count']}")
    print(f"   💰 Avg Spent: {segment['avg_spent']}")