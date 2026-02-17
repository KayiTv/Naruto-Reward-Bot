import os
import sys
import io
from pymongo import MongoClient
from dotenv import load_dotenv

# Force UTF-8 output
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

load_dotenv()

def verify():
    # Explicitly use 'rewardbot' database
    client = MongoClient(os.getenv('MONGO_URL'))
    db = client['rewardbot']
    
    print("🔍 VERIFICATION REPORT")
    print("=" * 50)
    
    # Check collections
    print("\n📊 Collections:")
    for coll in ['users', 'daily_stats', 'rewards', 'penalties', 'config', 'system_stats', 'action_logs']:
        count = db[coll].count_documents({})
        status = "✅" if count > 0 or coll in ['rewards', 'penalties', 'action_logs'] else "⚠️"
        print(f"  {status} {coll}: {count} documents")
    
    # Check config
    print("\n⚙️ Config:")
    config = db.config.find_one({"_id": "bot_config"})
    if config:
        print(f"  ✅ Config exists")
        # print(f"     - Owner ID: {config.get('owner_id', 'Missing')}")
        # print(f"     - Bot Token: {'Present' if config.get('bot_token') else 'Missing'}")
    else:
        print(f"  ❌ Config missing!")
    
    # Check system stats
    print("\n📈 System Stats:")
    stats = db.system_stats.find_one({"_id": "global_stats"})
    if stats:
        print(f"  ✅ System stats exist")
        print(f"     - Total users: {stats.get('total_users', 0)}")
        print(f"     - Total stocks: {stats.get('total_stocks_distributed', 0)}")
    else:
        print(f"  ❌ System stats missing!")
    
    # Sample user
    print("\n👤 Sample User:")
    sample = db.users.find_one()
    if sample:
        print(f"  ✅ User ID: {sample['_id']}")
        print(f"     - Messages: {sample.get('stats', {}).get('total_msgs', 0)}")
        print(f"     - Stocks: {sample.get('stats', {}).get('total_stocks', 0)}")
    else:
        print(f"  ❌ No users found!")
    
    # Check for old structure
    print("\n🔍 Old Structure Check:")
    old_found = False
    for coll_name in db.list_collection_names():
        if db[coll_name].find_one({"_id": "global_store"}):
            print(f"  ⚠️  Old 'global_store' still exists in {coll_name}")
            old_found = True
    
    if not old_found:
        print(f"  ✅ No old structure found")
    
    print("\n" + "=" * 50)
    print("✅ Verification complete!")

if __name__ == "__main__":
    verify()
