import os
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional
from pymongo import MongoClient, DESCENDING

IST_OFFSET = timedelta(hours=5, minutes=30)
IST_TZ = timezone(IST_OFFSET)

class MongoStorage:
    def __init__(self, db_file: str = None):
        # db_file arg kept for compatibility with main.py instantiation if not updated yet
        mongo_url = os.getenv("MONGO_URL")
        if not mongo_url:
            raise ValueError("MONGO_URL not found in environment")
        
        self.client = MongoClient(mongo_url)
        self.db = self.client['rewardbot']
        
        # Collections
        self.users = self.db['users']
        self.daily_stats = self.db['daily_stats']
        self.rewards = self.db['rewards']
        self.penalties = self.db['penalties']
        self.config = self.db['config']
        
        # Ensure config document exists
        if self.config.count_documents({"_id": "bot_config"}) == 0:
            self.config.insert_one({"_id": "bot_config", "updated_at": int(time.time())})
            
        self.system_stats = self.db['system_stats']
        self.action_logs = self.db['action_logs']
        self.anti_spam = self.db['anti_spam']
        self.bot_storage = self.db['bot_storage']
        
        print("✅ Connected to MongoDB")
    
    # --- CONFIGURATION (NEW) ---
    
    def get_config(self) -> Dict:
        """Get bot configuration"""
        return self.config.find_one({"_id": "bot_config"}) or {}

    def save_config(self, new_config: Dict):
        """Save bot configuration"""
        # Ensure _id is preserved or set
        new_config['_id'] = "bot_config"
        new_config['updated_at'] = int(time.time())
        # Replace entire config
        self.config.replace_one({"_id": "bot_config"}, new_config, upsert=True)

    def get_reward_settings(self) -> Dict:
        """Get reward settings from rewards collection"""
        doc = self.rewards.find_one({"_id": "settings"})
        if not doc:
            # Migration: Check if they exist in main config
            main_conf = self.get_config()
            reward_settings = main_conf.get('reward_settings', {})
            
            # Also pick up base/jackpot if at top level
            if 'base' in main_conf: reward_settings['base'] = main_conf['base']
            if 'jackpot' in main_conf: reward_settings['jackpot'] = main_conf['jackpot']
            
            if reward_settings:
                # Initialize new location
                self.save_reward_settings(reward_settings)
                return reward_settings
            return {}
        return doc.get('settings', {})

    def save_reward_settings(self, settings: Dict):
        """Save reward settings to rewards collection"""
        self.rewards.replace_one(
            {"_id": "settings"},
            {"_id": "settings", "settings": settings, "updated_at": int(time.time())},
            upsert=True
        )

    # --- ANTI-SPAM (NEW) ---

    def get_anti_spam_settings(self) -> Dict:
        """Get anti-spam settings from anti_spam collection"""
        doc = self.anti_spam.find_one({"_id": "settings"})
        if not doc:
            # Migration: Check if they exist in main config
            main_conf = self.get_config()
            spam_settings = main_conf.get('spam_settings', {})
            if spam_settings:
                self.save_anti_spam_settings(spam_settings)
                return spam_settings
            return {}
        return doc.get('settings', {})

    def save_anti_spam_settings(self, settings: Dict):
        """Save anti-spam settings to anti_spam collection"""
        self.anti_spam.replace_one(
            {"_id": "settings"},
            {"_id": "settings", "settings": settings, "updated_at": int(time.time())},
            upsert=True
        )

    # --- EVENT STATE (NEW) ---

    def get_event_state(self) -> Dict:
        """Get event manager state from rewards collection"""
        return self.rewards.find_one({"_id": "event_state"}) or {}

    def save_event_state(self, state: Dict):
        """Save event manager state to rewards collection"""
        state['_id'] = "event_state"
        state['updated_at'] = int(time.time())
        self.rewards.replace_one({"_id": "event_state"}, state, upsert=True)

    # --- AUDIT & STORAGE (NEW) ---

    def add_penalty(self, user_id: int, p_type: str, duration_sec: int, reason: str):
        """Record a user penalty"""
        expiry = int(time.time()) + duration_sec if duration_sec > 0 else 0
        self.penalties.insert_one({
            "user_id": user_id,
            "type": p_type,
            "duration": duration_sec,
            "expiry": expiry,
            "reason": reason,
            "timestamp": int(time.time())
        })

    def set_data(self, key: str, value: Any):
        """Set generic bot data"""
        self.bot_storage.replace_one(
            {"_id": key},
            {"_id": key, "value": value, "updated_at": int(time.time())},
            upsert=True
        )

    def get_data(self, key: str, default: Any = None) -> Any:
        """Get generic bot data"""
        doc = self.bot_storage.find_one({"_id": key})
        return doc['value'] if doc else default

    # --- USER OPERATIONS ---
    
    def update_user_msg(self, user_id: str):
        """Increment user message count (global + daily)"""
        user_id = str(user_id)
        now = int(time.time())
        today = self.get_today_date()
        
        # Update user stats
        self.users.update_one(
            {"_id": user_id},
            {
                "$inc": {"stats.total_msgs": 1},
                "$set": {"updated_at": now},
                "$setOnInsert": {
                    "first_name": "Unknown",
                    "username": None,
                    "stats.total_stocks": 0,
                    "stats.last_win": 0,
                    "status": {
                        "is_banned": False,
                        "is_whitelisted": False,
                        "is_penalized": False
                    },
                    "violations": {"count": 0, "last_violation": None, "history": []},
                    "created_at": now
                }
            },
            upsert=True
        )
        
        # Update daily stats (Nested Structure)
        self.daily_stats.update_one(
            {"_id": today},
            {
                "$inc": {f"stats.{user_id}.messages": 1},
                "$set": {
                    f"stats.{user_id}.updated_at": now,
                    "updated_at": now
                },
                "$setOnInsert": {
                    "created_at": now,
                    f"stats.{user_id}.stocks_won": 0,
                    f"stats.{user_id}.wins_count": 0,
                    f"stats.{user_id}.tier": "Bronze",
                    f"stats.{user_id}.created_at": now
                }
            },
            upsert=True
        )
    
    def get_user_rank(self, user_id: int) -> tuple[int, int]:
        """Get user's rank and total users today"""
        today = self.get_today_date()
        user_id = str(user_id)
        
        day_doc = self.daily_stats.find_one({"_id": today})
        if not day_doc or "stats" not in day_doc:
            return 0, 0
            
        stats = day_doc.get("stats", {})
        user_stats = stats.get(user_id)
        if not user_stats:
            return 0, len(stats)
            
        msgs = user_stats.get('messages', 0)
        
        # Calculate rank in-memory
        rank = 1
        for uid, s in stats.items():
            if s.get('messages', 0) > msgs:
                rank += 1
                
        return rank, len(stats)

    def get_all_daily_stats(self) -> Dict:
        """Get all daily stats for today"""
        today = self.get_today_date()
        day_doc = self.daily_stats.find_one({"_id": today})
        if not day_doc:
            return {}
            
        stats = day_doc.get("stats", {})
        result = {}
        for uid, s in stats.items():
            result[uid] = {
                "msgs": s.get('messages', 0),
                "stocks": s.get('stocks_won', 0),
                "tier": s.get('tier', 'Bronze')
            }
        return result
    
    def add_user_stock(self, user_id: str, amount: int):
        """Add stocks to user (global + daily)"""
        user_id = str(user_id)
        now = int(time.time())
        today = self.get_today_date()
        
        # Update user
        self.users.update_one(
            {"_id": user_id},
            {
                "$inc": {"stats.total_stocks": amount},
                "$set": {
                    "stats.last_win": now,
                    "updated_at": now
                }
            }
        )
        
        # Update daily stats (Nested)
        self.daily_stats.update_one(
            {"_id": today},
            {
                "$inc": {
                    f"stats.{user_id}.stocks_won": amount,
                    f"stats.{user_id}.wins_count": 1
                },
                "$set": {
                    f"stats.{user_id}.updated_at": now,
                    "updated_at": now
                }
            }
        )
        
        # Update system stats
        self.system_stats.update_one(
            {"_id": "global_stats"},
            {
                "$inc": {
                    "total_rewards_given": 1,
                    "total_stocks_distributed": amount
                },
                "$set": {"updated_at": now}
            }
        )
    
    def add_user_message(self, user_id: str, amount: int):
        """Add messages to user (global + daily)"""
        user_id = str(user_id)
        now = int(time.time())
        today = self.get_today_date()
        
        # Update user
        self.users.update_one(
            {"_id": user_id},
            {
                "$inc": {"stats.total_msgs": amount},
                "$set": {"updated_at": now}
            }
        )
        
        # Update daily stats (Nested)
        self.daily_stats.update_one(
            {"_id": today},
            {
                "$inc": {f"stats.{user_id}.messages": amount},
                "$set": {
                    f"stats.{user_id}.updated_at": now,
                    "updated_at": now
                },
                "$setOnInsert": {
                    "created_at": now,
                    f"stats.{user_id}.stocks_won": 0,
                    f"stats.{user_id}.wins_count": 0,
                    f"stats.{user_id}.tier": "Bronze",
                    f"stats.{user_id}.created_at": now
                }
            },
            upsert=True
        )

    def get_user_stats(self, user_id: str) -> Dict:
        """Get user statistics"""
        user = self.users.find_one({"_id": str(user_id)})
        if user:
            return user.get('stats', {})
        return {"total_msgs": 0, "total_stocks": 0, "last_win": 0}
    
    def get_daily_stats(self, user_id: str) -> Dict:
        """Get today's stats for user"""
        today = self.get_today_date()
        user_id = str(user_id)
        day_doc = self.daily_stats.find_one({"_id": today})
        
        if day_doc and "stats" in day_doc:
            user_stats = day_doc["stats"].get(user_id)
            if user_stats:
                return {
                    "msgs": user_stats.get('messages', 0),
                    "stocks": user_stats.get('stocks_won', 0)
                }
        return {"msgs": 0, "stocks": 0}
    
    def get_top_daily(self, limit: int = 10) -> List[tuple]:
        """Get top users by messages today"""
        today = self.get_today_date()
        day_doc = self.daily_stats.find_one({"_id": today})
        
        if not day_doc or "stats" not in day_doc:
            return []
            
        stats = day_doc.get("stats", {})
        
        # Sort in-memory
        sorted_users = sorted(
            stats.items(), 
            key=lambda x: x[1].get('messages', 0), 
            reverse=True
        )
        
        results = []
        for uid, s in sorted_users[:limit]:
            results.append((
                uid,
                s.get('messages', 0),
                s.get('stocks_won', 0)
            ))
        
        return results
    
    
    # --- BAN/WHITELIST OPERATIONS ---
    
    def is_banned(self, user_id: str) -> bool:
        """Check if user is banned"""
        user = self.users.find_one({"_id": str(user_id)})
        return user.get('status', {}).get('is_banned', False) if user else False
    
    def ban_user(self, user_id: str, reason: str = "Manual", by_admin: str = "Admin"):
        """Ban a user"""
        self.users.update_one(
            {"_id": str(user_id)},
            {
                "$set": {
                    "status.is_banned": True,
                    "status.ban_reason": reason,
                    "status.banned_by": by_admin,
                    "status.banned_at": int(time.time()),
                    "updated_at": int(time.time())
                }
            },
            upsert=True
        )
    
    def unban_user(self, user_id: str):
        """Unban a user"""
        self.users.update_one(
            {"_id": str(user_id)},
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
    
    def is_whitelisted(self, user_id: str) -> bool:
        """Check if user is whitelisted"""
        user = self.users.find_one({"_id": str(user_id)})
        return user.get('status', {}).get('is_whitelisted', False) if user else False
    
    def whitelist_user(self, user_id: str):
        """Whitelist a user"""
        self.users.update_one(
            {"_id": str(user_id)},
            {
                "$set": {
                    "status.is_whitelisted": True,
                    "status.whitelisted_at": int(time.time()),
                    "updated_at": int(time.time())
                }
            },
            upsert=True
        )
    
    def unwhitelist_user(self, user_id: str):
        """Remove whitelist"""
        self.users.update_one(
            {"_id": str(user_id)},
            {
                "$set": {
                    "status.is_whitelisted": False,
                    "status.whitelisted_at": None,
                    "updated_at": int(time.time())
                }
            }
        )
    
    # --- PENALTY OPERATIONS ---
    
    def add_penalty(self, user_id: str, duration_minutes: int, reason: str = "Spam"):
        """Add penalty to user"""
        now = int(time.time())
        expires = now + (duration_minutes * 60)
        
        # Get violation level
        user = self.users.find_one({"_id": str(user_id)})
        level = user.get('violations', {}).get('count', 0) if user else 1
        
        # Update user status
        self.users.update_one(
            {"_id": str(user_id)},
            {
                "$set": {
                    "status.is_penalized": True,
                    "status.penalty_expires": expires,
                    "status.penalty_reason": reason,
                    "status.penalty_level": level,
                    "updated_at": now
                }
            }
        )
        
        # Create penalty record
        self.penalties.insert_one({
            "user_id": str(user_id),
            "user_name": "Unknown",
            "type": reason.split()[0].lower() if ' ' in reason else reason.lower(),
            "reason": reason,
            "duration_minutes": duration_minutes,
            "level": level,
            "issued_at": now,
            "expires_at": expires,
            "auto_cleared": False,
            "cleared_by": None,
            "cleared_at": None
        })
        
        return level, duration_minutes
    
    def get_penalty(self, user_id: str) -> Optional[Dict]:
        """Get active penalty for user"""
        user = self.users.find_one({"_id": str(user_id)})
        if not user:
            return None
        
        status = user.get('status', {})
        if not status.get('is_penalized'):
            return None
        
        expires = status.get('penalty_expires', 0)
        if expires < time.time():
            # Penalty expired, clear it
            self.remove_penalty(user_id)
            return None
        
        return {
            "expiry": expires,
            "reason": status.get('penalty_reason'),
            "level": status.get('penalty_level', 1)
        }
    
    def remove_penalty(self, user_id: str):
        """Remove penalty from user"""
        self.users.update_one(
            {"_id": str(user_id)},
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
    
    def get_all_penalties(self) -> Dict:
        """Get all active penalties"""
        now = int(time.time())
        active_penalties = self.penalties.find({
            "expires_at": {"$gt": now},
            "auto_cleared": False
        })
        
        result = {}
        for penalty in active_penalties:
            result[penalty['user_id']] = {
                "expiry": penalty['expires_at'],
                "reason": penalty['reason'],
                "level": penalty['level']
            }
        
        return result
    
    def clear_all_penalties(self) -> int:
        """Clear all active penalties"""
        # Get count before clearing
        count = self.penalties.count_documents({"auto_cleared": False})
        
        # Mark all as cleared
        self.penalties.update_many(
            {"auto_cleared": False},
            {
                "$set": {
                    "auto_cleared": True,
                    "cleared_at": int(time.time())
                }
            }
        )
        
        # Clear user statuses
        self.users.update_many(
            {"status.is_penalized": True},
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
        
        return count
    
    # --- VIOLATION TRACKING ---
    
    def add_violation(self, user_id: str) -> int:
        """Add violation to user and return new count"""
        now = int(time.time())
        
        # Get current violations
        user = self.users.find_one({"_id": str(user_id)})
        if not user:
            # New user
            self.users.update_one(
                {"_id": str(user_id)},
                {
                    "$set": {
                        "violations.count": 1,
                        "violations.last_violation": now,
                        "violations.history": [now],
                        "updated_at": now
                    }
                },
                upsert=True
            )
            return 1
        
        violations = user.get('violations', {})
        count = violations.get('count', 0)
        last_time = violations.get('last_violation', 0)
        
        # Reset if 7 days passed
        if now - last_time > (7 * 24 * 3600):
            count = 0
        
        count += 1
        
        # Update
        self.users.update_one(
            {"_id": str(user_id)},
            {
                "$set": {
                    "violations.count": count,
                    "violations.last_violation": now,
                    "updated_at": now
                },
                "$push": {
                    "violations.history": {
                        "$each": [now],
                        "$slice": -10  # Keep last 10
                    }
                }
            }
        )
        
        return count
    
    def reset_violations(self, user_id: str) -> bool:
        """Reset user violations"""
        result = self.users.update_one(
            {"_id": str(user_id)},
            {
                "$set": {
                    "violations.count": 0,
                    "violations.last_violation": None,
                    "violations.history": [],
                    "updated_at": int(time.time())
                }
            }
        )
        return result.modified_count > 0
    
    def get_violation_level(self, user_id: str) -> int:
        """Get user's current violation level"""
        user = self.users.find_one({"_id": str(user_id)})
        if not user:
            return 0
        return user.get('violations', {}).get('count', 0)
    
    # --- ADMIN OPERATIONS ---
    
    def is_admin(self, user_id: int) -> bool:
        """Check if user is admin"""
        config = self.config.find_one({"_id": "bot_config"})
        if not config:
            return False
            
        admin_ids = config.get('admin_ids', [])
        
        # Check ID match
        str_id = str(user_id)
        for a in admin_ids:
            if str(a) == str_id:
                return True
        return False
    
    def add_admin(self, user_id: int):
        """Add admin"""
        self.config.update_one(
            {"_id": "bot_config"},
            {
                "$addToSet": {"admin_ids": user_id},
                # Remove deprecated 'admins' field if present to avoid confusion
                "$unset": {"admins": ""},
                "$set": {"updated_at": int(time.time())}
            }
        )
    
    def remove_admin(self, user_id: int):
        """Remove admin"""
        self.config.update_one(
            {"_id": "bot_config"},
            {
                "$pull": {"admin_ids": user_id},
                "$unset": {"admins": ""}, # Cleanup
                "$set": {"updated_at": int(time.time())}
            }
        )
    
    def get_admins(self) -> List[int]:
        """Get all admins"""
        config = self.config.find_one({"_id": "bot_config"})
        if not config:
            return []
        return config.get('admin_ids', [])
    
    # --- UTILITY METHODS ---
    
    def get_ist_now(self) -> datetime:
        """Get current IST time"""
        return datetime.now(IST_TZ)

    def get_today_date(self) -> datetime:
        """Get today's date (midnight) as datetime object"""
        now = self.get_ist_now()
        # Return datetime at 00:00:00
        return datetime(now.year, now.month, now.day)
    
    def get_ist_str(self) -> str:
        """Get IST time as string"""
        return self.get_ist_now().strftime("%H:%M IST")
    
    def log_action(self, user_id: int, action: str, details: Any = None):
        """Log an action (Audit Trail)"""
        try:
            log_type = action.split(":")[0].upper() if ":" in action else action.split()[0].upper()
        except:
            log_type = "UNKNOWN"
            
        self.action_logs.insert_one({
            "user_id": user_id,
            "type": log_type,
            "action": action,
            "details": details or {},
            "timestamp": int(time.time()),
            "date_str": datetime.now(IST_TZ).strftime("%Y-%m-%d %H:%M:%S"),
            "date": self.get_today_date() # Keep date object for queries
        })
    
    def get_logs(self, limit: int = 10) -> List[Dict]:
        """Get recent logs"""
        logs = self.action_logs.find().sort("timestamp", DESCENDING).limit(limit)
        return list(logs)
    
    def increment_total_selections(self):
        """Increment total selections counter"""
        self.system_stats.update_one(
            {"_id": "global_stats"},
            {
                "$inc": {"total_selections": 1},
                "$set": {"updated_at": int(time.time())}
            },
            upsert=True
        )
    
    def get_global_stats(self) -> Dict:
        """Get global statistics"""
        stats = self.system_stats.find_one({"_id": "global_stats"})
        if not stats:
            return {
                "total_selections": 0,
                "distributed_stocks": 0,
                "total_tracked_users": 0
            }
        return {
            "total_selections": stats.get('total_selections', 0),
            "distributed_stocks": stats.get('total_stocks_distributed', 0),
            "total_tracked_users": stats.get('total_users', 0)
        }
    
    def add_recent_winner(self, user_id: str, name: str, stocks: int):
        """Add to recent winners (stored in system_stats)"""
        self.system_stats.update_one(
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
                        "$slice": -10  # Keep last 10
                    }
                }
            },
            upsert=True
        )
    
    def get_all_daily_stats(self) -> Dict:
        """Get all daily stats for today (for admin view/stats)"""
        today = self.get_today_date()
        cursor = self.daily_stats.find({"date": today})
        result = {}
        for doc in cursor:
            result[doc['user_id']] = {
                'msgs': doc.get('messages', 0),
                'stocks': doc.get('stocks_won', 0)
            }
        return result

    def get_banned_users(self) -> List[str]:
        """Get list of banned user IDs"""
        cursor = self.users.find({"status.is_banned": True})
        return [doc['_id'] for doc in cursor]

    def get_whitelisted_users(self) -> List[str]:
        """Get list of whitelisted user IDs"""
        cursor = self.users.find({"status.is_whitelisted": True})
        return [doc['_id'] for doc in cursor]

    def reset_daily_stats(self):
        """Reset all daily stats for today"""
        today = self.get_today_date()
        self.daily_stats.delete_many({"date": today})
        # Note: This removes records. logic in main.py seems to imply full reset. 
        # If persistent history is needed, we should just set values to 0, but deletion is cleaner for "reset".

    def get_backup_data(self) -> Dict:
        """Export all data for backup (JSON serializable)"""
        # Helper to serialize
        def serialize(doc):
            if not doc: return doc
            d = doc.copy()
            if '_id' in d: d['_id'] = str(d['_id'])
            for k, v in d.items():
                if isinstance(v, (datetime, )): # Handle datetime if any
                    d[k] = str(v)
            return d

        return {
            "users": [serialize(d) for d in self.users.find()],
            "daily_stats": [serialize(d) for d in self.daily_stats.find()],
            "rewards": [serialize(d) for d in self.rewards.find()],
            "penalties": [serialize(d) for d in self.penalties.find()],
            "config": [serialize(d) for d in self.config.find()],
            "system_stats": [serialize(d) for d in self.system_stats.find()],
            "action_logs": [serialize(d) for d in self.action_logs.find()]
        }

    def clear_all_penalties(self) -> int:
        """Clear all active penalties"""
        result = self.penalties.delete_many({})
        return result.deleted_count

    def restore_data(self, data: Dict):
        """Restore database from backup"""
        # data is expected to be the dict returned by get_backup_data
        # We will clear collections and insert valid data
        
        # Helper to parse (handle strings to ObjectId/datetime if needed, but we used strings)
        # PyMongo handles strings for _id? No, usually needs ObjectId.
        # But if we stored meaningful IDs (like user_id as string), it's fine.
        # However, _id field in backup was converted to str.
        # Checking imports.. we need ObjectId?
        # For simplicity, we can remove _id and let Mongo generate new ones, OR try to convert.
        # Let's remove _id to avoid dup errors or format issues, unless it's critical.
        # For users/config, _id is auto. user_id field is what matters.
        
        def clean(doc_list):
            cleaned = []
            for d in doc_list:
                if '_id' in d: del d['_id']
                cleaned.append(d)
            return cleaned

        if "users" in data:
            self.users.delete_many({})
            if data["users"]: self.users.insert_many(clean(data["users"]))
            
        if "daily_stats" in data:
            self.daily_stats.delete_many({})
            if data["daily_stats"]: self.daily_stats.insert_many(clean(data["daily_stats"]))
            
        if "rewards" in data:
            self.rewards.delete_many({})
            if data["rewards"]: self.rewards.insert_many(clean(data["rewards"]))
            
        if "penalties" in data:
            self.penalties.delete_many({})
            if data["penalties"]: self.penalties.insert_many(clean(data["penalties"]))
            
        if "config" in data:
            self.config.delete_many({})
            if data["config"]: self.config.insert_many(clean(data["config"]))
            
        if "system_stats" in data:
            self.system_stats.delete_many({})
            if data["system_stats"]: self.system_stats.insert_many(clean(data["system_stats"]))

    def get_recent_winners(self, limit=10) -> List[Dict]:
        """Get recent reward winners"""
        # Query rewards collection, exclude settings document
        cursor = self.rewards.find({"user_id": {"$exists": True}}).sort("timestamp", -1).limit(limit)
        results = []
        for doc in cursor:
            # Fetch name
            uid = doc.get('user_id')
            if not uid: continue
            
            user = self.users.find_one({"_id": uid})
            name = user.get('first_name', f"User {uid}") if user else f"User {uid}"
            
            results.append({
                'name': name,
                'stocks': doc.get('amount', 0),
                'time': doc.get('timestamp', int(time.time()))
            })
        return results

    def close(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
