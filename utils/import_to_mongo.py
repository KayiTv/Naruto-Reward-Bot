import os
import json
import time
import io
import sys
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta

# Force UTF-8 output
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

load_dotenv()

IST_OFFSET = timedelta(hours=5, minutes=30)
IST_TZ = timezone(IST_OFFSET)

def import_data():
    # Connect to MongoDB
    client = MongoClient(os.getenv('MONGO_URL'))
    
    # Explicitly use 'rewardbot' database
    try:
        db = client.get_database("rewardbot")
    except:
        db = client.get_default_database()
        
    print(f"📂 Connected to DB: {db.name}")
    print("📂 Loading data from user_data.json...")
    
    # Load local JSON file
    if not os.path.exists('user_data.json'):
        print("❌ 'user_data.json' not found!")
        sys.exit(1)

    with open('user_data.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    print(f"✅ Loaded {len(data.get('users', {}))} users from JSON\n")
    
    # === IMPORT USERS ===
    print("👥 Importing users...")
    users_to_insert = []
    
    for user_id, user_data in data.get('users', {}).items():
        # Handle case where user_data might be just an int (migration artifact?)
        # logic in provided script implies user_data is dict
        if not isinstance(user_data, dict):
            # If structure is different, skip or adapt
            continue

        user_doc = {
            "_id": user_id,
            "first_name": "Unknown",  # Will be fetched by bot later
            "username": None,
            
            "stats": {
                "total_msgs": user_data.get('total_msgs', 0),
                "total_stocks": user_data.get('total_stocks', 0),
                "last_win": user_data.get('last_win', 0)
            },
            
            "status": {
                "is_banned": user_id in data.get('banned_users', {}),
                "ban_reason": data.get('banned_users', {}).get(user_id, {}).get('reason'),
                "banned_by": data.get('banned_users', {}).get(user_id, {}).get('by'),
                "banned_at": data.get('banned_users', {}).get(user_id, {}).get('time'),
                "is_whitelisted": user_id in data.get('whitelisted_users', []),
                "whitelisted_at": None,
                "is_penalized": False,
                "penalty_expires": None,
                "penalty_reason": None,
                "penalty_level": 0
            },
            
            "violations": {
                "count": data.get('violations', {}).get(user_id, {}).get('count', 0),
                "last_violation": data.get('violations', {}).get(user_id, {}).get('last_violation', None),
                "history": []
            },
            
            "created_at": int(time.time()),
            "updated_at": int(time.time())
        }
        
        users_to_insert.append(user_doc)
    
    if users_to_insert:
        # Clear existing users first
        db.users.delete_many({})
        result = db.users.insert_many(users_to_insert)
        print(f"✅ Imported {len(result.inserted_ids)} users")
    
    # === IMPORT DAILY STATS ===
    print("\n📅 Importing daily stats...")
    daily_to_insert = []
    
    for date, stats in data.get('daily_stats', {}).items():
        for user_id, user_stats in stats.items():
            # Handle both old format (integer) and new format (dict)
            if isinstance(user_stats, dict):
                msgs = user_stats.get('msgs', 0)
                stocks = user_stats.get('stocks', 0)
            else:
                msgs = user_stats if isinstance(user_stats, int) else 0
                stocks = 0
            
            daily_doc = {
                "date": date,
                "user_id": user_id,
                "messages": msgs,
                "stocks_won": stocks,
                "wins_count": 1 if stocks > 0 else 0,
                "tier": "Unknown",
                "tier_multiplier": 1.0,
                "created_at": int(time.time()),
                "updated_at": int(time.time())
            }
            
            daily_to_insert.append(daily_doc)
    
    if daily_to_insert:
        # Clear existing daily stats
        db.daily_stats.delete_many({})
        result = db.daily_stats.insert_many(daily_to_insert)
        print(f"✅ Imported {len(result.inserted_ids)} daily stat entries")
    
    # === CREATE CONFIG ===
    print("\n⚙️ Creating config document...")
    
    # Load config from config.json
    config_dict = {}
    if os.path.exists('config.json'):
        with open('config.json', 'r', encoding='utf-8') as f:
            config_dict = json.load(f)
    
    config_doc = config_dict.copy()
    config_doc['_id'] = 'bot_config'
    config_doc['updated_at'] = int(time.time())
    
    # Remove old config if exists
    db.config.delete_many({})
    db.config.insert_one(config_doc)
    print("✅ Config document created")
    
    # === CREATE SYSTEM STATS ===
    print("\n📊 Creating system stats...")
    
    system_stats = {
        "_id": "global_stats",
        "total_users": len(data.get('users', {})),
        "total_messages_processed": sum(u.get('total_msgs', 0) for u in data.get('users', {}).values() if isinstance(u, dict)),
        "total_rewards_given": data.get('total_selections', 0),
        "total_stocks_distributed": data.get('distributed_stocks', 0),
        "uptime_start": int(time.time()),
        "last_restart": int(time.time()),
        "restarts_count": 0,
        "today": {
            "date": datetime.now(IST_TZ).strftime("%Y-%m-%d"),
            "messages": 0,
            "rewards": 0,
            "stocks": 0,
            "active_users": 0
        },
        "updated_at": int(time.time())
    }
    
    db.system_stats.delete_many({})
    db.system_stats.insert_one(system_stats)
    print("✅ System stats created")
    
    # === SUMMARY ===
    print("\n" + "="*50)
    print("✅ MIGRATION COMPLETE!")
    print("="*50)
    print(f"\n📊 Summary:")
    print(f"  Users: {db.users.count_documents({})}")
    print(f"  Daily stats: {db.daily_stats.count_documents({})}")
    print(f"  Config: {db.config.count_documents({})}")
    print(f"  System stats: {db.system_stats.count_documents({})}")
    
    print("\n🧹 Next step: Clean old 'global_store' document")
    print("   Run: python clean_old_data.py")

if __name__ == "__main__":
    import_data()
