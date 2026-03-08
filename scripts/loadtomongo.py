"""
Load the generated dataset into MongoDB
Creates collections: users, products, sessions, transactions
"""

from pymongo import MongoClient, errors
import json
import os
from datetime import datetime

print("🚀 Loading data into MongoDB...")

# Connect to MongoDB
try:
    client = MongoClient('localhost', 27017, serverSelectionTimeoutMS=5000)
    client.admin.command('ping')
    print("✅ Connected to MongoDB")
except Exception as e:
    print(f"❌ Failed to connect: {e}")
    exit(1)

# Create/use database
db = client['ecommerce_analytics']
print(f"📊 Using database: ecommerce_analytics")

# Clear existing collections (optional - comment out if you want to keep data)
print("\n🧹 Clearing existing collections...")
for collection in ['users', 'products', 'categories', 'sessions', 'transactions']:
    result = db[collection].delete_many({})
    print(f"   - {collection}: cleared {result.deleted_count} documents")

# Load users
print("\n👤 Loading users...")
with open('data/users.json', 'r') as f:
    users = json.load(f)
    
# Add metadata
for user in users:
    user['_load_timestamp'] = datetime.now()
    
result = db.users.insert_many(users)
print(f"✅ Inserted {len(result.inserted_ids)} users")

# Load products
print("\n📦 Loading products...")
with open('data/products.json', 'r') as f:
    products = json.load(f)
    
for product in products:
    product['_load_timestamp'] = datetime.now()
    
result = db.products.insert_many(products)
print(f"✅ Inserted {len(result.inserted_ids)} products")

# Load categories
print("\n📂 Loading categories...")
with open('data/categories.json', 'r') as f:
    categories = json.load(f)

for c in categories:
    c['_load_timestamp'] = datetime.now()

result = db.categories.insert_many(categories)
print(f"✅ Inserted {len(result.inserted_ids)} categories")

# Load sessions and extract transactions
print("\n🖥️ Loading sessions and extracting transactions...")
with open('data/sessions.json', 'r') as f:
    sessions = json.load(f)

transactions = []
for session in sessions:
    # Add load timestamp
    session['_load_timestamp'] = datetime.now()
    
    # Extract transaction if exists
    if session.get('converted') and session.get('transaction'):
        transaction = session['transaction']
        transaction['session_id'] = session['session_id']
        transaction['user_id'] = session['user_id']
        transaction['device'] = session['device']
        transaction['_load_timestamp'] = datetime.now()
        transactions.append(transaction)
        
        # Remove transaction from session to avoid duplication
        # (we'll keep it in both places for different query patterns)
        # Comment this out if you want it embedded only
        # del session['transaction']

# Insert sessions
result = db.sessions.insert_many(sessions)
print(f"✅ Inserted {len(result.inserted_ids)} sessions")


# Loading 
print("\n💳 Loading transactions...")
with open('data/transactions.json', 'r') as f:
    transactions = json.load(f)

for t in transactions:
    t['_load_timestamp'] = datetime.now()

if transactions:
    result = db.transactions.insert_many(transactions)
    print(f"✅ Inserted {len(result.inserted_ids)} transactions")
else:
    print("⚠️ No transactions found")

# Create indexes for better query performance
print("\n📑 Creating indexes...")
db.users.create_index("user_id", unique=True)
db.products.create_index("product_id", unique=True)
db.products.create_index("category")
db.sessions.create_index("session_id", unique=True)
db.sessions.create_index("user_id")
db.sessions.create_index("start_time")
db.transactions.create_index("transaction_id", unique=True)
db.transactions.create_index("user_id")
db.transactions.create_index("timestamp")
print("✅ Indexes created")

# Summary
print("\n" + "="*50)
print("📊 DATABASE SUMMARY")
print("="*50)
print(f"Database: ecommerce_analytics")
print(f"Users: {db.users.count_documents({})}")
print(f"Products: {db.products.count_documents({})}")
print(f"Categories: {db.categories.count_documents({})}")
print(f"Sessions: {db.sessions.count_documents({})}")
print(f"Transactions: {db.transactions.count_documents({})}")
print("="*50)