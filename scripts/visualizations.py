"""
Create visualizations for report
Part 4 of project
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import json
from datetime import datetime
import os

print("="*60)
print("📊 CREATING VISUALIZATIONS FOR REPORT")
print("="*60)

# Create output directory
os.makedirs("output/figures", exist_ok=True)

# Load data
with open('data/sessions.json', 'r') as f:
    sessions = json.load(f)

with open('data/products.json', 'r') as f:
    products = json.load(f)

# Convert to DataFrames
sessions_df = pd.DataFrame(sessions)
products_df = pd.DataFrame(products)

# Normalize sessions fields to match the real JSON schema
# sessions.json fields:
# - conversion_status: "converted" | "not_converted"
# - cart_contents: { "prod_xxx": { "quantity": int, "price": float }, ... }
# - device_profile: { "type", "os", "browser" }
# - geo_data: { "city", "state", "country" }

def _safe_get(d, *path, default=None):
    cur = d
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


sessions_df["is_converted"] = sessions_df.get("conversion_status", pd.Series([], dtype="object")).eq("converted")
sessions_df["device_type"] = sessions_df.get("device_profile", pd.Series([], dtype="object")).apply(
    lambda x: x.get("type") if isinstance(x, dict) else "unknown"
)
sessions_df["city"] = sessions_df.get("geo_data", pd.Series([], dtype="object")).apply(
    lambda x: x.get("city") if isinstance(x, dict) else "Unknown"
)

# Extract transaction line-items from cart_contents for converted sessions
transactions = []
for s in sessions:
    if s.get("conversion_status") != "converted":
        continue
    cart = s.get("cart_contents") or {}
    if not isinstance(cart, dict) or len(cart) == 0:
        continue

    for prod_id, item in cart.items():
        if not isinstance(item, dict):
            continue
        qty = item.get("quantity")
        price = item.get("price")
        if qty is None or price is None:
            continue
        transactions.append(
            {
                "user_id": s.get("user_id"),
                "product_id": prod_id,
                "quantity": qty,
                "price_rwf": price,
                "revenue_rwf": float(qty) * float(price),
                "device_type": _safe_get(s, "device_profile", "type", default="unknown"),
                "city": _safe_get(s, "geo_data", "city", default="Unknown"),
                "session_id": s.get("session_id"),
            }
        )

transactions_df = pd.DataFrame(transactions)

# ============================================
# Visualization 1: Revenue by Category
# ============================================
print("\n📈 Creating Revenue by Category chart...")

# Join transactions with products to get category_id, then aggregate
if not transactions_df.empty:
    tx_with_cat = transactions_df.merge(
        products_df[["product_id", "category_id"]],
        on="product_id",
        how="left",
    )
    cat_revenue_series = (
        tx_with_cat.groupby("category_id")["revenue_rwf"].sum().sort_values(ascending=False)
    )
    categories = [str(c) for c in cat_revenue_series.index.tolist()]
    revenues = cat_revenue_series.values.tolist()
else:
    categories = []
    revenues = []

plt.figure(figsize=(10, 6))
plt.bar(categories, revenues, color='green', alpha=0.7)
plt.title('Revenue by Product Category (RWF)', fontsize=16, fontweight='bold')
plt.xlabel('Category')
plt.ylabel('Revenue (RWF)')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('output/figures/revenue_by_category.png', dpi=150)
plt.close()
print("✅ Saved: revenue_by_category.png")

# ============================================
# Visualization 2: Conversion Funnel
# ============================================
print("\n📈 Creating Conversion Funnel chart...")

total_sessions = len(sessions)
sessions_with_cart = len([s for s in sessions if s.get('cart_contents')])
converted = len([s for s in sessions if s.get('conversion_status') == "converted"])

funnel_data = [total_sessions, sessions_with_cart, converted]
funnel_labels = ['Total Sessions', 'Added to Cart', 'Completed Purchase']

plt.figure(figsize=(8, 6))
plt.barh(funnel_labels, funnel_data, color=['blue', 'orange', 'green'])
plt.title('E-commerce Conversion Funnel', fontsize=16, fontweight='bold')
plt.xlabel('Number of Sessions')

# Add percentage labels
for i, v in enumerate(funnel_data):
    percentage = (v / total_sessions) * 100
    plt.text(v + 5, i, f'{v} ({percentage:.1f}%)', va='center')

plt.tight_layout()
plt.savefig('output/figures/conversion_funnel.png', dpi=150)
plt.close()
print("✅ Saved: conversion_funnel.png")

# ============================================
# Visualization 3: Device Performance
# ============================================
print("\n📈 Creating Device Performance chart...")

device_stats = sessions_df.groupby("device_type").agg(
    sessions=("session_id", "count"),
    conversions=("is_converted", "sum"),
).reset_index()
device_stats["conversion_rate"] = (device_stats["conversions"] / device_stats["sessions"] * 100).round(1)

plt.figure(figsize=(10, 6))
x = range(len(device_stats["device_type"]))
width = 0.35

plt.bar([i - width/2 for i in x], device_stats["sessions"], width, label='Sessions', color='blue', alpha=0.7)
plt.bar([i + width/2 for i in x], device_stats["conversions"], width, label='Conversions', color='green', alpha=0.7)
plt.xlabel('Device Type')
plt.ylabel('Count')
plt.title('Sessions and Conversions by Device', fontsize=16, fontweight='bold')
plt.xticks(x, device_stats["device_type"])
plt.legend()

# Add conversion rate as text
for i, row in enumerate(device_stats.itertuples()):
    plt.text(i, max(row.sessions, row.conversions) + 5, 
             f"{row.conversion_rate}%", ha='center', fontweight='bold')

plt.tight_layout()
plt.savefig('output/figures/device_performance.png', dpi=150)
plt.close()
print("✅ Saved: device_performance.png")

# ============================================
# Visualization 4: Top Products
# ============================================
print("\n📈 Creating Top Products chart...")

# Get product popularity from cart_contents
product_counts = {}
for s in sessions:
    if s.get('cart_contents'):
        for prod_id in s['cart_contents']:
            product_counts[prod_id] = product_counts.get(prod_id, 0) + 1

# Get top 5
top_products = sorted(product_counts.items(), key=lambda x: x[1], reverse=True)[:5]
top_prod_names = []
top_prod_counts = []

for prod_id, count in top_products:
    # Find product name
    product = next((p for p in products if p['product_id'] == prod_id), None)
    name = product['name'] if product else prod_id[:10]
    top_prod_names.append(name)
    top_prod_counts.append(count)

plt.figure(figsize=(10, 6))
plt.barh(top_prod_names, top_prod_counts, color='purple', alpha=0.7)
plt.xlabel('Number of Times Added to Cart')
plt.title('Top 5 Most Popular Products', fontsize=16, fontweight='bold')
plt.tight_layout()
plt.savefig('output/figures/top_products.png', dpi=150)
plt.close()
print("✅ Saved: top_products.png")

print("\n✅ All visualizations saved to output/figures/")
print("📁 Copy these images to your report!")