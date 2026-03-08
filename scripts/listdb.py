from pymongo import MongoClient

client = MongoClient('localhost', 27017)
print("Databases in THIS MongoDB instance:")
for db in client.list_database_names():
    print(f"  - {db}")
    
# Also check if we can connect to specific ones
print("\nTrying to connect to your databases:")
for db_name in ['your_db_name', 'test', 'admin']:  # Add names you might have used
    try:
        db = client[db_name]
        collections = db.list_collection_names()
        if collections:
            print(f"  ✅ {db_name}: collections = {collections}")
        else:
            print(f"  ⚠️  {db_name}: exists but empty")
    except:
        print(f"  ❌ {db_name}: cannot access")