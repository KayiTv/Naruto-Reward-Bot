
import asyncio
import os
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

async def create_all_indexes():
    mongo_url = os.getenv("MONGO_URL")
    if not mongo_url:
        print("[ERROR] MONGO_URL not found in environment")
        return

    print("[INFO] Connecting to MongoDB for Index Creation...")
    client = AsyncIOMotorClient(mongo_url)
    db = client['rewardbot']

    print("[INFO] Creating Indexes...")

    # === users collection ===
    try:
        await db.users.create_index(
            [("user_id", 1), ("group_id", 1)],
            unique=True, name="idx_user_group", background=True
        )
        await db.users.create_index([("tier", 1)], background=True)
        await db.users.create_index([("is_banned", 1)], background=True)
        print("[SUCCESS] Users indexes created")
    except Exception as e:
        print(f"[ERROR] Users index error: {e}")

    # === daily_stats collection ===
    try:
        await db.daily_stats.create_index(
            [("user_id", 1), ("group_id", 1), ("date", -1)],
            name="idx_daily_user_group_date", background=True
        )
        await db.daily_stats.create_index([("date", -1)], background=True)
        print("[SUCCESS] Daily Stats indexes created")
    except Exception as e:
        print(f"[ERROR] Daily Stats index error: {e}")

    # === rewards collection ===
    try:
        await db.rewards.create_index(
            [("user_id", 1), ("group_id", 1), ("timestamp", -1)],
            name="idx_rewards_user_group_ts", background=True
        )
        await db.rewards.create_index([("timestamp", -1)], background=True)
        print("[SUCCESS] Rewards indexes created")
    except Exception as e:
        print(f"[ERROR] Rewards index error: {e}")

    # === penalties collection ===
    try:
        await db.penalties.create_index(
            [("user_id", 1), ("group_id", 1), ("timestamp", -1)],
            name="idx_penalties_user_ts", background=True
        )
        print("[SUCCESS] Penalties indexes created")
    except Exception as e:
        print(f"[ERROR] Penalties index error: {e}")

    # === action_logs collection ===
    try:
        await db.action_logs.create_index([("timestamp", -1)], background=True)
        await db.action_logs.create_index([("admin_id", 1), ("timestamp", -1)], background=True)
        print("[SUCCESS] Action Logs indexes created")
    except Exception as e:
        print(f"[ERROR] Action Logs index error: {e}")

    # === system_stats & config ===
    try:
        await db.config.create_index([("key", 1)], unique=True, background=True)
        print("[SUCCESS] Config indexes created")
    except Exception as e:
        print(f"[ERROR] Config index error: {e}")

    print("[INFO] All indexes processed.")
    client.close()

if __name__ == "__main__":
    asyncio.run(create_all_indexes())
