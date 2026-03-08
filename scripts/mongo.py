"""
Test MongoDB Connection
Run this to verify MongoDB is accessible
"""

from pymongo import MongoClient
from datetime import datetime

print("=" * 50)
print("MongoDB Connection Test")
print("=" * 50)

try:
    # Try to connect
    print("\n1. Connecting to MongoDB...")
    client = MongoClient('localhost', 27017, serverSelectionTimeoutMS=5000)
    
    # Force a connection to test
    client.admin.command('ping')
    print("✅ SUCCESS: Connected to MongoDB!")
    
    # Show server info
    server_info = client.server_info()
    print(f"   MongoDB version: {server_info.get('version', 'unknown')}")
    
    # List databases
    dbs = client.list_database_names()
    print(f"   Available databases: {dbs}")
    
    # Test insert
    print("\n2. Testing insert operation...")
    db = client['test_db']
    result = db.test_collection.insert_one({
        'test': 'Hello from Python!',
        'timestamp': datetime.now()
    })
    print(f"✅ Inserted document with ID: {result.inserted_id}")
    
    # Test find
    print("\n3. Testing find operation...")
    doc = db.test_collection.find_one({'_id': result.inserted_id})
    print(f"✅ Found: {doc}")
    
    # Clean up
    print("\n4. Cleaning up...")
    db.test_collection.delete_one({'_id': result.inserted_id})
    print("✅ Test document removed")
    
    print("\n🎉 ALL TESTS PASSED! MongoDB is working correctly!")
    
except Exception as e:
    print(f"\n❌ ERROR: {e}")
    print("\n🔍 Troubleshooting:")
    print("   1. Is MongoDB running? Run: docker ps | grep mongo")
    print("   2. Check port: Is it using 27017?")
    print("   3. Try: docker restart mongodb")