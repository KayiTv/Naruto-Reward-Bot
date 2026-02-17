import os
import time
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

mongo_url = os.getenv("MONGO_URL")
client = MongoClient(mongo_url)
db = client['rewardbot']

# 1. Fetch from config
config_doc = db['config'].find_one({"_id": "bot_config"})
if not config_doc:
    print("❌ No config found in 'config' collection.")
    exit(1)

# Combined reward settings
reward_settings = config_doc.get('reward_settings', {})

# Explicitly pick up top-level keys if they exist separately
if 'base' in config_doc:
    reward_settings['base'] = config_doc['base']
if 'jackpot' in config_doc:
    reward_settings['jackpot'] = config_doc['jackpot']

# Ensure defaults for required keys
if 'base' not in reward_settings:
    reward_settings['base'] = {'mode': 'fixed', 'amount': 5}
if 'jackpot' not in reward_settings:
    reward_settings['jackpot'] = {'enabled': False, 'chance': 1, 'amount': 100}
if 'tiers' not in reward_settings:
    reward_settings['tiers'] = {
        'enabled': True,
        'bronze': {'multiplier': 1.0, 'range': [1, 100]},
        'silver': {'multiplier': 1.5, 'range': [101, 250]},
        'gold': {'multiplier': 2.0, 'range': [251, 500]},
        'platinum': {'multiplier': 2.5, 'range': [501, 9999]}
    }

# 2. Save Reward settings to rewards collection
db['rewards'].replace_one(
    {"_id": "settings"},
    {"_id": "settings", "settings": reward_settings, "updated_at": int(time.time())},
    upsert=True
)

# 3. Handle Anti-Spam settings
spam_settings = config_doc.get('spam_settings')
if not spam_settings:
    print("[WARN] No 'spam_settings' found in config document. Using defaults.")
    spam_settings = {
        'threshold_seconds': 5,
        'ignore_duration_minutes': 30,
        'burst_limit': 5,
        'burst_window_seconds': 10,
        'global_flood_limit': 20,
        'global_flood_window': 3,
        'raid_limit': 5
    }

db['anti_spam'].replace_one(
    {"_id": "settings"},
    {"_id": "settings", "settings": spam_settings, "updated_at": int(time.time())},
    upsert=True
)

print("[SUCCESS] Consolidated settings into 'rewards' and 'anti_spam' collections!")
