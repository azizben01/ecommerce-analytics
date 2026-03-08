"""
HBase Implementation for Time-Series Data
Part 1 (HBase section)
"""

import happybase
import json
from datetime import datetime

print("="*60)
print("📊 HBASE IMPLEMENTATION - USER SESSION DATA")
print("="*60)

# Connect to HBase
try:
    connection = happybase.Connection('localhost', 9090, timeout=10000)
    print("✅ Connected to HBase")
except Exception as e:
    print(f"❌ HBase not running. For project, we'll simulate.")
    print("   In report, explain how it would work.")
    exit()

# Create tables if they don't exist
tables = list(connection.tables())

# Table 1: User Sessions (time-series data)
if b'user_sessions' not in tables:
    connection.create_table(
        'user_sessions',
        {
            'cf': dict()  # column family for session data
        }
    )
    print("✅ Created table: user_sessions")

# Table 2: Product Daily Metrics
if b'product_metrics' not in tables:
    connection.create_table(
        'product_metrics',
        {
            'daily': dict(),  # daily metrics
            'weekly': dict()  # weekly aggregates
        }
    )
    print("✅ Created table: product_metrics")

# Load some sample data
print("\n📝 Loading sample session data...")

# Read sessions from JSON
with open('data/sessions.json', 'r') as f:
    sessions = json.load(f)

# Take first 50 sessions for HBase
sample_sessions = sessions[:50]

user_sessions = connection.table('user_sessions')

# Design Row Key: user_id|reverse_timestamp (for latest first)
for session in sample_sessions:
    # Reverse timestamp for time-series (newest first)
    # Convert timestamp to reverse format: (MAX_TIME - timestamp)
    # Simplified: just use user_id|timestamp
    row_key = f"{session['user_id']}|{session['start_time']}"
    
    data = {
        b'cf:session_id': session['session_id'].encode(),
        b'cf:device': session['device_profile']['type'].encode(),
        b'cf:page_views': str(len(session['page_views'])).encode(),
        b'cf:converted': session['conversion_status'].encode(),
        b'cf:referrer': session['referrer'].encode(),
    }
    
    # Add transaction info if exists
    if session.get('transaction'):
        data[b'cf:has_transaction'] = b'true'
        data[b'cf:transaction_amount'] = str(session['transaction']['total_rwf']).encode()
    
    user_sessions.put(row_key.encode(), data)

print(f"✅ Loaded {len(sample_sessions)} sessions into HBase")

# Query examples (for report)
print("\n🔍 QUERY EXAMPLES (for report):")
print("-"*40)

print("\n1. Get all sessions for a specific user:")
print("   hbase(main)> get 'user_sessions', 'user_000001|2025-03-05T10:30:00'")
print("   - Row key design: user_id|timestamp")
print("   - This allows fast retrieval of user's browsing history")

print("\n2. Scan sessions within a time range:")
print("   hbase(main)> scan 'user_sessions', {")
print("     STARTROW => 'user_000001|2025-03-01',")
print("     ENDROW => 'user_000001|2025-04-01'")
print("   }")
print("   - Enables time-series analysis per user")

print("\n3. Get daily product metrics:")
print("   Row key: product_id|YYYY-MM-DD")
print("   Column family: daily:views, daily:add_to_cart, daily:purchases")

# Show sample data
print("\n📋 Sample data from HBase:")
sample = user_sessions.scan(limit=3)
for key, data in sample:
    print(f"   Row key: {key.decode()}")
    for col, val in data.items():
        print(f"      {col.decode()}: {val.decode()}")
    print()

connection.close()
print("\n✅ HBase operations complete!")