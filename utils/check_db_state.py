import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

mongo_url = os.getenv("MONGO_URL")
client = MongoClient(mongo_url)
db = client['rewardbot']

print("--- COLLECTIONS ---")
print(db.list_collection_names())

print("\n--- CONFIG COLLECTION ---")
print(list(db['config'].find()))

print("\n--- REWARDS COLLECTION (SETTINGS) ---")
print(list(db['rewards'].find({"_id": "settings"})))

print("\n--- ANTI-SPAM COLLECTION (SETTINGS) ---")
print(list(db['anti_spam'].find({"_id": "settings"})))

print("\n--- ACTION LOGS (LAST 3) ---")
print(list(db['action_logs'].find().sort("timestamp", -1).limit(3)))

print("\n--- PENALTIES (COUNT) ---")
print(db['penalties'].count_documents({}))
