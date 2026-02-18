
# core/cache.py
from cachetools import TTLCache

# -- Cache Stores --------------------------------------
# TTLCache(maxsize, ttl_seconds)
user_cache   = TTLCache(maxsize=2000, ttl=120)   # 2 min: user profiles, tier, ban status
config_cache = TTLCache(maxsize=50,   ttl=300)   # 5 min: bot config (rarely changes)
stats_cache  = TTLCache(maxsize=1000, ttl=30)    # 30 sec: daily stats (write-through)
elgbl_cache  = TTLCache(maxsize=2000, ttl=10)    # 10 sec: eligibility (changes fast)
top_cache    = TTLCache(maxsize=20,   ttl=60)    # 1 min: leaderboard queries

# -- Cache Key Builders --------------------------------
def user_key(user_id, group_id):            return f'u:{user_id}:{group_id}'
def stats_key(user_id, group_id, date):    return f's:{user_id}:{group_id}:{date}'
def top_key(group_id, date):               return f't:{group_id}:{date}'
def config_key(k):                         return f'cfg:{k}'

# -- Invalidation Helpers ------------------------------
def invalidate_user(user_id, group_id):
    k = user_key(user_id, group_id)
    user_cache.pop(k, None)
    elgbl_cache.pop(k, None)

def invalidate_stats(user_id, group_id, date):
    stats_cache.pop(stats_key(user_id, group_id, date), None)
    top_cache.pop(top_key(group_id, date), None)

def invalidate_config(key):
    config_cache.pop(config_key(key), None)
