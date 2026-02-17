
import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

def migrate():
    mongo_url = os.getenv("MONGO_URL")
    if not mongo_url:
        print("MONGO_URL not found in .env")
        return

    client = MongoClient(mongo_url)
    db = client['rewardbot']
    daily_stats = db.daily_stats

    # Drop the old unique index that conflicts with nested structure
    try:
        daily_stats.drop_index("date_1_user_id_1")
        print("Dropped old unique index.")
    except Exception as e:
        print(f"Index drop skipped or failed: {e}")
    # Old structure had a 'user_id' field
    old_docs = list(daily_stats.find({"user_id": {"$exists": True}}))
    
    if not old_docs:
        print("No old documents found.")
        return

    print(f"Found {len(old_docs)} old documents. Migrating...")

    for doc in old_docs:
        date = doc.get("date")
        user_id = doc.get("user_id")
        
        if not date or not user_id:
            continue

        # Prepare nested data
        user_data = {
            "messages": doc.get("messages", 0),
            "stocks_won": doc.get("stocks_won", 0),
            "wins_count": doc.get("wins_count", 0),
            "tier": doc.get("tier", "Bronze"),
            "updated_at": doc.get("updated_at"),
            "created_at": doc.get("created_at")
        }

        # Update into the new day-based document
        daily_stats.update_one(
            {"_id": date},
            {
                "$set": {f"stats.{user_id}": user_data},
                "$setOnInsert": {"created_at": doc.get("created_at")}
            },
            upsert=True
        )

        # Delete the old flat document
        daily_stats.delete_one({"_id": doc["_id"]})
        print(f"Migrated user {user_id} for date {date}")

    print("Migration complete!")

if __name__ == "__main__":
    migrate()
