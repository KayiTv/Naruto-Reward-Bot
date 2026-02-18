import os
import time
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne, DESCENDING
from core.cache import (
    user_cache, config_cache, stats_cache, elgbl_cache, top_cache,
    user_key, stats_key, top_key, config_key,
    invalidate_user, invalidate_stats, invalidate_config
)

IST_OFFSET = timedelta(hours=5, minutes=30)
IST_TZ = timezone(IST_OFFSET)

from core.write_queue import WriteQueue

class MongoStorage:
    def __init__(self, db_file: str = None):
        # db_file arg kept for compatibility with main.py instantiation
        mongo_url = os.getenv("MONGO_URL")
        if not mongo_url:
            raise ValueError("MONGO_URL not found in environment")
        
        # Phase 5: Motor Connection Pool Configuration
        self.client = AsyncIOMotorClient(
            mongo_url,
            maxPoolSize=50,
            minPoolSize=10,
            maxIdleTimeMS=30000,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000,
            socketTimeoutMS=10000,
            retryWrites=True,
            w='majority'
        )
        self.db = self.client['rewardbot']
        
        # Collections
        self.users = self.db['users']
        self.daily_stats = self.db['daily_stats']
        self.rewards = self.db['rewards']
        self.penalties = self.db['penalties']
        self.config = self.db['config']
        self.system_stats = self.db['system_stats']
        self.action_logs = self.db['action_logs']
        self.anti_spam = self.db['anti_spam']
        self.bot_storage = self.db['bot_storage']
        
        # Phase 4: Write-Behind Queue
        self.write_queue = WriteQueue(self.db, flush_interval=5)
        
        print("✅ Connected to MongoDB (Async/Motor + WriteQueue)")
    
    # --- CONFIGURATION (ASYNC + CACHE) ---
    
    async def get_config(self) -> Dict:
        """Get bot configuration (Cached)"""
        k = config_key("bot_config")
        if k in config_cache:
            return config_cache[k]
            
        doc = await self.config.find_one({"_id": "bot_config"})
        if doc:
            config_cache[k] = doc
        return doc or {}

    async def save_config(self, new_config: Dict):
        """Save bot configuration (Invalidates Cache)"""
        new_config['_id'] = "bot_config"
        new_config['updated_at'] = int(time.time())
        await self.config.replace_one({"_id": "bot_config"}, new_config, upsert=True)
        invalidate_config("bot_config")

    async def get_reward_settings(self) -> Dict:
        """Get reward settings (Cached)"""
        k = config_key("reward_settings")
        if k in config_cache:
            return config_cache[k]
            
        doc = await self.rewards.find_one({"_id": "settings"})
        if not doc:
            # Migration check
            main_conf = await self.get_config()
            reward_settings = main_conf.get('reward_settings', {})
            if reward_settings:
                await self.save_reward_settings(reward_settings)
                return reward_settings
            return {}
        
        settings = doc.get('settings', {})
        config_cache[k] = settings
        return settings

    async def save_reward_settings(self, settings: Dict):
        """Save reward settings (Invalidates Cache)"""
        await self.rewards.replace_one(
            {"_id": "settings"},
            {"_id": "settings", "settings": settings, "updated_at": int(time.time())},
            upsert=True
        )
        invalidate_config("reward_settings")

    async def get_anti_spam_settings(self) -> Dict:
        """Get anti-spam settings (Cached)"""
        k = config_key("anti_spam_settings")
        if k in config_cache:
            return config_cache[k]
            
        doc = await self.anti_spam.find_one({"_id": "settings"})
        if not doc:
            main_conf = await self.get_config()
            spam_settings = main_conf.get('spam_settings', {})
            if spam_settings:
                await self.save_anti_spam_settings(spam_settings)
                return spam_settings
            return {}
            
        settings = doc.get('settings', {})
        config_cache[k] = settings
        return settings

    async def save_anti_spam_settings(self, settings: Dict):
        """Save anti-spam settings (Invalidates Cache)"""
        await self.anti_spam.replace_one(
            {"_id": "settings"},
            {"_id": "settings", "settings": settings, "updated_at": int(time.time())},
            upsert=True
        )
        invalidate_config("anti_spam_settings")

    async def get_event_state(self) -> Dict:
        """Get event manager state (No Cache - critical)"""
        return await self.rewards.find_one({"_id": "event_state"}) or {}

    async def save_event_state(self, state: Dict):
        """Save event manager state (Direct)"""
        state['_id'] = "event_state"
        state['updated_at'] = int(time.time())
        await self.rewards.replace_one({"_id": "event_state"}, state, upsert=True)

    # --- AUDIT & STORAGE (ASYNC) ---

    async def add_penalty_log(self, user_id: int, group_id: int, p_type: str, duration_sec: int, reason: str):
        """Record a user penalty log (Async)"""
        expiry = int(time.time()) + duration_sec if duration_sec > 0 else 0
        await self.penalties.insert_one({
            "user_id": user_id,
            "group_id": group_id,
            "type": p_type,
            "duration": duration_sec,
            "expiry": expiry,
            "reason": reason,
            "timestamp": int(time.time())
        })

    async def set_data(self, key: str, value: Any):
        """Set generic bot data (Async)"""
        await self.bot_storage.replace_one(
            {"_id": key},
            {"_id": key, "value": value, "updated_at": int(time.time())},
            upsert=True
        )

    async def get_data(self, key: str, default: Any = None) -> Any:
        """Get generic bot data (Async)"""
        doc = await self.bot_storage.find_one({"_id": key})
        return doc['value'] if doc else default

    # --- USER OPERATIONS (ASYNC + CACHE + PROJECTION) ---
    
    async def get_user(self, user_id: int, group_id: int = 0) -> Optional[Dict]:
        """Get user profile (Phase 2: Cached + Phase 5: Projected)"""
        k = user_key(user_id, group_id)
        if k in user_cache:
            return user_cache[k]
            
        doc = await self.users.find_one(
            {"user_id": user_id, "group_id": group_id},
            {
                "_id": 0, "user_id": 1, "group_id": 1, 
                "first_name": 1, "username": 1,
                "stats": 1, "status": 1, "violations": 1,
                "updated_at": 1
            }
        )
        if not doc:
            # Fallback for old records without user_id/group_id fields (using _id)
            doc = await self.users.find_one({"_id": str(user_id)})
            if doc:
                # Fill missing fields for consistency
                doc['user_id'] = user_id
                doc['group_id'] = group_id
        
        if doc:
            user_cache[k] = doc
        return doc

    async def add_user_message(self, user_id: int, amount: int, group_id: int = 0):
        """Add message count (Phase 4: Bulk Write for atomicity)"""
        now = int(time.time())
        today = self.get_today_date().isoformat()
        
        operations = [
            UpdateOne(
                {"user_id": user_id, "group_id": group_id},
                {"$inc": {"stats.total_msgs": amount}, "$set": {"updated_at": now}},
                upsert=True
            ),
            UpdateOne(
                {"user_id": user_id, "group_id": group_id, "date": today},
                {"$inc": {"messages": amount}, "$set": {"updated_at": now}},
                upsert=True
            )
        ]
        
        await asyncio.gather(
            self.users.bulk_write([operations[0]], ordered=False),
            self.daily_stats.bulk_write([operations[1]], ordered=False)
        )
        invalidate_user(user_id, group_id)
        invalidate_stats(user_id, group_id, today)

    async def update_user_msg(self, user_id: int, group_id: int = 0):
        """Increment user message count (Legacy compatibility)"""
        await self.add_user_message(user_id, 1, group_id)

    async def get_user_rank(self, user_id: int, group_id: int = 0) -> tuple[int, int]:
        """Get user's rank today (Cached)"""
        today = self.get_today_date()
        k = top_key(group_id, today.isoformat())
        
        # This is a bit complex to cache perfectly, but we can cache the whole day doc
        day_doc = await self.daily_stats.find_one({"_id": today})
        if not day_doc or "stats" not in day_doc:
            return 0, 0
            
        stats = day_doc.get("stats", {})
        user_id_str = str(user_id)
        user_stats = stats.get(user_id_str)
        if not user_stats:
            return 0, len(stats)
            
        msgs = user_stats.get('messages', 0)
        rank = 1
        for uid, s in stats.items():
            if s.get('messages', 0) > msgs:
                rank += 1
        return rank, len(stats)

    async def get_all_daily_stats(self) -> Dict:
        """Get all daily stats (Async)"""
        today = self.get_today_date()
        day_doc = await self.daily_stats.find_one({"_id": today})
        if not day_doc:
            return {}
        return day_doc.get("stats", {})
    
    async def add_user_stock(self, user_id: int, group_id: int, amount: int):
        """Add stocks (Phase 4: Bulk Write for atomicity)"""
        now = int(time.time())
        today = self.get_today_date().isoformat()
        
        # Phase 6.1: 1 round trip for all stat increments
        operations = [
            # 1. Update User Stats
            UpdateOne(
                {"user_id": user_id, "group_id": group_id},
                {
                    "$inc": {"stats.total_stocks": amount},
                    "$set": {"stats.last_win": now, "updated_at": now}
                },
                upsert=True
            ),
            # 2. Update Daily Stats (Flat Structure as per Phase 1 Indexes)
            UpdateOne(
                {"user_id": user_id, "group_id": group_id, "date": today},
                {
                    "$inc": {"stocks_won": amount, "wins_count": 1},
                    "$set": {"updated_at": now}
                },
                upsert=True
            ),
            # 3. Update Global Stats
            UpdateOne(
                {"_id": "global_stats"},
                {
                    "$inc": {"total_rewards_given": 1, "total_stocks_distributed": amount},
                    "$set": {"updated_at": now}
                },
                upsert=True
            )
        ]
        
        # Execute across collections (bulk_write is collection-specific in Motor)
        await asyncio.gather(
            self.users.bulk_write([operations[0]], ordered=False),
            self.daily_stats.bulk_write([operations[1]], ordered=False),
            self.system_stats.bulk_write([operations[2]], ordered=False)
        )
        # Invalidate cache
        invalidate_user(user_id, group_id)
        invalidate_stats(user_id, group_id, today)

    async def add_user_message_bulk(self, user_id: int, group_id: int):
        """Phase 6.2: Write-Behind Pattern for Message Counts"""
        today = self.get_today_date().isoformat()
        
        # Buffer increments instead of direct write
        await asyncio.gather(
            self.write_queue.increment_stat('users', {"user_id": user_id, "group_id": group_id}, 'stats.total_msgs'),
            self.write_queue.increment_stat('daily_stats', {"user_id": user_id, "group_id": group_id, "date": today}, 'messages')
        )

    async def get_user_stats(self, user_id: int, group_id: int = 0) -> Dict:
        """Get user stats (Cached)"""
        user = await self.get_user(user_id, group_id)
        return user.get('stats', {}) if user else {"total_msgs": 0, "total_stocks": 0, "last_win": 0}

    async def get_daily_stats(self, user_id: int, group_id: int = 0) -> Dict:
        """Get daily stats (Cached)"""
        today = self.get_today_date().isoformat()
        k = stats_key(user_id, group_id, today)
        
        if k in stats_cache:
            return stats_cache[k]
            
        doc = await self.daily_stats.find_one(
            {"user_id": user_id, "group_id": group_id, "date": today},
            {"_id": 0, "messages": 1, "stocks_won": 1}
        )
        res = {"msgs": doc.get('messages', 0), "stocks": doc.get('stocks_won', 0)} if doc else {"msgs": 0, "stocks": 0}
        stats_cache[k] = res
        return res

    async def get_top_daily(self, group_id: int, limit: int = 10) -> List[tuple]:
        """Get top users today (Phase 1: Indexed, Phase 5: Projected)"""
        today = self.get_today_date().isoformat()
        cursor = self.daily_stats.find(
            {"group_id": group_id, "date": today},
            {"_id": 0, "user_id": 1, "messages": 1, "stocks_won": 1}
        ).sort("messages", DESCENDING).limit(limit)
        
        results = []
        async for doc in cursor:
            results.append((
                doc.get('user_id'),
                doc.get('messages', 0),
                doc.get('stocks_won', 0)
            ))
        return results
    
    
    # --- BAN/WHITELIST OPERATIONS ---
    
    # --- BAN/WHITELIST OPERATIONS (ASYNC + CACHE) ---
    
    async def is_banned(self, user_id: int, group_id: int = 0) -> bool:
        """Check if user is banned (Cached)"""
        user = await self.get_user(user_id, group_id)
        return user.get('status', {}).get('is_banned', False) if user else False
    
    async def ban_user(self, user_id: int, group_id: int = 0, reason: str = "Manual", by_admin: str = "Admin"):
        """Ban a user (Invalidates Cache)"""
        now = int(time.time())
        await self.users.update_one(
            {"user_id": user_id, "group_id": group_id},
            {
                "$set": {
                    "status.is_banned": True,
                    "status.ban_reason": reason,
                    "status.banned_by": by_admin,
                    "status.banned_at": now,
                    "updated_at": now
                }
            },
            upsert=True
        )
        invalidate_user(user_id, group_id)
    
    async def unban_user(self, user_id: int, group_id: int = 0):
        """Unban a user (Invalidates Cache)"""
        await self.users.update_one(
            {"user_id": user_id, "group_id": group_id},
            {
                "$set": {
                    "status.is_banned": False,
                    "status.ban_reason": None,
                    "status.banned_by": None,
                    "status.banned_at": None,
                    "updated_at": int(time.time())
                }
            }
        )
        invalidate_user(user_id, group_id)
    
    async def is_whitelisted(self, user_id: int, group_id: int = 0) -> bool:
        """Check if user is whitelisted (Cached)"""
        user = await self.get_user(user_id, group_id)
        return user.get('status', {}).get('is_whitelisted', False) if user else False
    
    async def whitelist_user(self, user_id: int, group_id: int = 0):
        """Whitelist a user (Invalidates Cache)"""
        now = int(time.time())
        await self.users.update_one(
            {"user_id": user_id, "group_id": group_id},
            {
                "$set": {
                    "status.is_whitelisted": True,
                    "status.whitelisted_at": now,
                    "updated_at": now
                }
            },
            upsert=True
        )
        invalidate_user(user_id, group_id)
    
    async def unwhitelist_user(self, user_id: int, group_id: int = 0):
        """Remove whitelist (Invalidates Cache)"""
        await self.users.update_one(
            {"user_id": user_id, "group_id": group_id},
            {
                "$set": {
                    "status.is_whitelisted": False,
                    "status.whitelisted_at": None,
                    "updated_at": int(time.time())
                }
            }
        )
        invalidate_user(user_id, group_id)
    
    # --- PENALTY OPERATIONS ---
    
    # --- PENALTY OPERATIONS (ASYNC) ---
    
    async def add_penalty_v2(self, user_id: int, group_id: int, duration_minutes: int, reason: str = "Spam"):
        """Add penalty to user (Async + Cache Invalidation)"""
        now = int(time.time())
        expires = now + (duration_minutes * 60)
        
        # Phase 5: Projection check
        user = await self.get_user(user_id, group_id)
        level = user.get('violations', {}).get('count', 0) if user else 1
        
        await asyncio.gather(
            self.users.update_one(
                {"user_id": user_id, "group_id": group_id},
                {
                    "$set": {
                        "status.is_penalized": True,
                        "status.penalty_expires": expires,
                        "status.penalty_reason": reason,
                        "status.penalty_level": level,
                        "updated_at": now
                    }
                },
                upsert=True
            ),
            self.penalties.insert_one({
                "user_id": user_id,
                "group_id": group_id,
                "reason": reason,
                "duration_minutes": duration_minutes,
                "level": level,
                "issued_at": now,
                "expires_at": expires
            })
        )
        invalidate_user(user_id, group_id)
        return level, duration_minutes
    
    async def get_penalty_v2(self, user_id: int, group_id: int = 0) -> Optional[Dict]:
        """Get active penalty (Async + Cached via get_user)"""
        user = await self.get_user(user_id, group_id)
        if not user: return None
        
        status = user.get('status', {})
        if not status.get('is_penalized'): return None
        
        expires = status.get('penalty_expires', 0)
        if expires < time.time():
            # Auto-clear expired penalty
            await self.remove_penalty_v2(user_id, group_id)
            return None
        
        return {
            "expiry": expires,
            "reason": status.get('penalty_reason'),
            "level": status.get('penalty_level', 1)
        }
    
    async def remove_penalty_v2(self, user_id: int, group_id: int = 0):
        """Remove penalty (Invalidates Cache)"""
        await self.users.update_one(
            {"user_id": user_id, "group_id": group_id},
            {
                "$set": {
                    "status.is_penalized": False,
                    "status.penalty_expires": None,
                    "status.penalty_reason": None,
                    "status.penalty_level": 0,
                    "updated_at": int(time.time())
                }
            }
        )
        invalidate_user(user_id, group_id)

    async def get_all_active_penalties(self, group_id: int) -> Dict:
        """Get all active penalties for a group (Async)"""
        now = int(time.time())
        cursor = self.penalties.find({
            "group_id": group_id,
            "expires_at": {"$gt": now}
        }, {"_id": 0, "user_id": 1, "reason": 1, "expires_at": 1, "level": 1})
        
        result = {}
        async for p in cursor:
            result[p['user_id']] = {
                "expiry": p['expires_at'],
                "reason": p['reason'],
                "level": p['level']
            }
        return result
    
    async def get_whitelisted_users(self) -> List[int]:
        """Get whitelisted user IDs (Async)"""
        doc = await self.config.find_one({"_id": "bot_config"})
        return doc.get('whitelisted_ids', []) if doc else []

    async def get_banned_users(self) -> List[int]:
        """Get banned user IDs (Async)"""
        doc = await self.config.find_one({"_id": "bot_config"})
        return doc.get('banned_ids', []) if doc else []

    async def get_recent_winners(self) -> List[Dict]:
        """Get recent winners (Async)"""
        stats = await self.system_stats.find_one({"_id": "global_stats"})
        return stats.get('recent_winners', []) if stats else []

    async def get_all_penalties(self) -> List[Dict]:
        """Get all active penalties for all users (Async)"""
        now = int(time.time())
        cursor = self.penalties.find({"expires_at": {"$gt": now}})
        return await cursor.to_list(length=100)
    
    async def reset_violations_v2(self, user_id: int, group_id: int = 0) -> bool:
        """Reset user violations (Async)"""
        result = await self.users.update_one(
            {"user_id": user_id, "group_id": group_id},
            {
                "$set": {
                    "violations.count": 0,
                    "violations.last_violation": None,
                    "violations.history": [],
                    "updated_at": int(time.time())
                }
            }
        )
        invalidate_user(user_id, group_id)
        return result.modified_count > 0
    
    async def get_violation_level_v2(self, user_id: int, group_id: int = 0) -> int:
        """Get user's current violation level (Cached)"""
        user = await self.get_user(user_id, group_id)
        if not user:
            return 0
        return user.get('violations', {}).get('count', 0)
    
    # --- ADMIN OPERATIONS ---
    
    async def is_admin(self, user_id: int) -> bool:
        """Check if user is admin (Cached)"""
        conf = await self.get_config()
        if not conf:
            return False
            
        admin_ids = conf.get('admin_ids', [])
        return user_id in admin_ids or str(user_id) in [str(a) for a in admin_ids]
    
    def add_admin(self, user_id: int):
        """Add admin"""
    # --- VIOLATION TRACKING (ASYNC) ---
    
    async def add_violation(self, user_id: int, group_id: int = 0) -> int:
        """Add violation (Async + Cache Invalidation)"""
        now = int(time.time())
        user = await self.get_user(user_id, group_id)
        
        violations = user.get('violations', {}) if user else {}
        count = violations.get('count', 0)
        last_time = violations.get('last_violation', 0)
        
        # Reset if 7 days passed
        if now - last_time > (7 * 24 * 3600):
            count = 0
        count += 1
        
        await self.users.update_one(
            {"user_id": user_id, "group_id": group_id},
            {
                "$set": {
                    "violations.count": count,
                    "violations.last_violation": now,
                    "updated_at": now
                },
                "$push": {
                    "violations.history": {
                        "$each": [now],
                        "$slice": -10
                    }
                }
            },
            upsert=True
        )
        invalidate_user(user_id, group_id)
        return count
    
    async def reset_violations(self, user_id: int, group_id: int = 0) -> bool:
        """Reset violations (Async + Cache Invalidation)"""
        result = await self.users.update_one(
            {"user_id": user_id, "group_id": group_id},
            {
                "$set": {
                    "violations.count": 0,
                    "violations.last_violation": None,
                    "violations.history": [],
                    "updated_at": int(time.time())
                }
            }
        )
        invalidate_user(user_id, group_id)
        return result.modified_count > 0
    
    async def get_violation_level(self, user_id: int, group_id: int = 0) -> int:
        """Get violation level (Cached)"""
        user = await self.get_user(user_id, group_id)
        return user.get('violations', {}).get('count', 0) if user else 0
    
    # --- ADMIN OPERATIONS (ASYNC + CACHED CONFIG) ---
    
    async def is_admin(self, user_id: int) -> bool:
        """Check if user is admin (Cached)"""
        config = await self.get_config()
        admin_ids = config.get('admin_ids', [])
        return user_id in admin_ids or str(user_id) in [str(a) for a in admin_ids]
    
    async def add_admin(self, user_id: int):
        """Add admin (Invalidates Config Cache)"""
        await self.config.update_one(
            {"_id": "bot_config"},
            {
                "$addToSet": {"admin_ids": user_id},
                "$set": {"updated_at": int(time.time())}
            }
        )
        invalidate_config("bot_config")
    
    async def remove_admin(self, user_id: int):
        """Remove admin (Invalidates Config Cache)"""
        await self.config.update_one(
            {"_id": "bot_config"},
            {
                "$pull": {"admin_ids": user_id},
                "$set": {"updated_at": int(time.time())}
            }
        )
        invalidate_config("bot_config")
    
    async def get_admins(self) -> List[int]:
        """Get admins (Cached)"""
        config = await self.get_config()
        return config.get('admin_ids', [])
    
    # --- AUDIT & GLOBAL STATS (ASYNC) ---
    
    async def log_action(self, user_id: int, action: str, details: Any = None):
        """Log action (Async)"""
        try:
            log_type = action.split(":")[0].upper() if ":" in action else action.split()[0].upper()
        except:
            log_type = "UNKNOWN"
            
        await self.action_logs.insert_one({
            "user_id": user_id,
            "type": log_type,
            "action": action,
            "details": details or {},
            "timestamp": int(time.time()),
            "date": self.get_today_date().isoformat()
        })
    
    async def get_logs(self, limit: int = 20) -> List[Dict]:
        """Get logs (Async)"""
        cursor = self.action_logs.find().sort("timestamp", DESCENDING).limit(limit)
        return await cursor.to_list(length=limit)
    
    async def increment_total_selections(self):
        """Increment total selections (Async)"""
        await self.system_stats.update_one(
            {"_id": "global_stats"},
            {
                "$inc": {"total_selections": 1},
                "$set": {"updated_at": int(time.time())}
            },
            upsert=True
        )
    
    async def get_global_stats(self) -> Dict:
        """Get global stats (Async)"""
        stats = await self.system_stats.find_one({"_id": "global_stats"})
        if not stats:
            return {"total_selections": 0, "distributed_stocks": 0, "total_tracked_users": 0}
        return {
            "total_selections": stats.get('total_selections', 0),
            "distributed_stocks": stats.get('total_stocks_distributed', 0),
            "total_tracked_users": stats.get('total_users', 0)
        }
    
    async def add_recent_winner(self, user_id: int, name: str, stocks: int):
        """Add recent winner (Async)"""
        await self.system_stats.update_one(
            {"_id": "global_stats"},
            {
                "$push": {
                    "recent_winners": {
                        "$each": [{
                            "user_id": user_id,
                            "name": name,
                            "stocks": stocks,
                            "time": int(time.time())
                        }],
                        "$slice": -10 # Keep last 10
                    }
                }
            },
            upsert=True
        )

    # --- HELPERS ---
    
    def get_ist_now(self) -> datetime:
        return datetime.now(IST_TZ)

    def get_today_date(self) -> datetime:
        now = self.get_ist_now()
        return datetime(now.year, now.month, now.day)
    
    def get_ist_str(self) -> str:
        return self.get_ist_now().strftime("%H:%M IST")

    async def close(self):
        """Close client (Async)"""
        self.client.close()
