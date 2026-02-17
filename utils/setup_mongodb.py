"""
Setup MongoDB collections and indexes
Run this once after creating your MongoDB Atlas cluster
"""

import os
import sys
from pymongo import MongoClient, ASCENDING, DESCENDING
from dotenv import load_dotenv

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv()

def setup_mongodb():
    mongo_url = os.getenv('MONGO_URL')
    if not mongo_url:
        print("❌ MONGO_URL not found in environment")
        print("Please create .env file with MONGO_URL")
        return False
    
    try:
        client = MongoClient(mongo_url)
        db = client['rewardbot']
        
        print("🔧 Setting up MongoDB collections...")
        
        # Create collections
        collections = ['users', 'daily_stats', 'rewards', 'penalties', 'config', 'system_stats', 'action_logs']
        
        for coll_name in collections:
            if coll_name not in db.list_collection_names():
                db.create_collection(coll_name)
                print(f"✅ Created: {coll_name}")
            else:
                print(f"ℹ️  Exists: {coll_name}")
        
        print("\n📊 Creating indexes...")
        
        # Users indexes
        db.users.create_index([("_id", ASCENDING)])
        db.users.create_index([("stats.total_msgs", DESCENDING)])
        db.users.create_index([("stats.total_stocks", DESCENDING)])
        db.users.create_index([("status.is_banned", ASCENDING)])
        db.users.create_index([("status.is_whitelisted", ASCENDING)])
        print("✅ Users indexes created")
        
        # Daily stats indexes
        db.daily_stats.create_index([("date", ASCENDING), ("user_id", ASCENDING)], unique=True)
        db.daily_stats.create_index([("date", ASCENDING), ("messages", DESCENDING)])
        db.daily_stats.create_index([("created_at", ASCENDING)], expireAfterSeconds=2592000)  # 30 days
        print("✅ Daily stats indexes created")
        
        # Rewards indexes
        db.rewards.create_index([("user_id", ASCENDING), ("timestamp", DESCENDING)])
        db.rewards.create_index([("timestamp", DESCENDING)])
        db.rewards.create_index([("calculation.jackpot", ASCENDING)])
        print("✅ Rewards indexes created")
        
        # Penalties indexes
        db.penalties.create_index([("user_id", ASCENDING), ("expires_at", DESCENDING)])
        db.penalties.create_index([("expires_at", ASCENDING)])
        print("✅ Penalties indexes created")
        
        # Action logs indexes
        db.action_logs.create_index([("timestamp", DESCENDING)])
        db.action_logs.create_index([("timestamp", ASCENDING)], expireAfterSeconds=7776000)  # 90 days
        print("✅ Action logs indexes created")
        
        print("\n✅ MongoDB setup complete!")
        
        # Show summary
        print("\n📋 Current collections:")
        for coll in db.list_collection_names():
            count = db[coll].count_documents({})
            print(f"  - {coll}: {count} documents")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    success = setup_mongodb()
    sys.exit(0 if success else 1)
