import time
import re
from collections import defaultdict, deque
from difflib import SequenceMatcher

class SpamDetector:
    def __init__(self, threshold_seconds=5, ignore_duration=1800, burst_limit=5, burst_window=10, 
                 global_flood_limit=20, global_flood_window=3, raid_limit=5):
        self.enabled = True
        self.threshold = threshold_seconds
        self.ignore_duration = ignore_duration
        self.burst_limit = burst_limit
        self.burst_window = burst_window
        self.global_flood_limit = global_flood_limit
        self.global_flood_window = global_flood_window
        self.raid_limit = raid_limit
        
        # Configurable thresholds
        self.duplicate_threshold = 0.85
        self.media_limit = 3
        self.media_window = 5
        
        # Toggles
        self.types = {
            'burst': True,
            'flood': True,
            'duplicate': True,
            'lowquality': True,
            'stickers': True
        }

        self.user_history = defaultdict(deque) # user_id -> deque of (timestamp, text)
        self.media_history = defaultdict(deque) # user_id -> deque of timestamp
        self.global_history = deque() # deque of timestamp
        self.ignored_users = {} # user_id -> ignore_until_timestamp

    def toggle(self, enabled: bool):
        self.enabled = enabled

    def set_type_state(self, spam_type: str, state: bool):
        if spam_type in self.types:
            self.types[spam_type] = state
            return True
        return False

    async def is_spam(self, user_id, text, is_media=False, is_whitelisted=False):
        if not self.enabled:
            return False
            
        if is_whitelisted:
            return False

        now = time.time()
        
        # Check if currently ignored
        if self.is_ignored(user_id):
            return True

        # --- Global Flood Check ---
        if self.types['flood']:
            self.global_history.append(now)
            while self.global_history and self.global_history[0] < now - self.global_flood_window:
                self.global_history.popleft()
                
            if len(self.global_history) > self.global_flood_limit:
                return "global_flood", f"{len(self.global_history)} msgs in {self.global_flood_window}s"

        # --- Media / Sticker Check ---
        if is_media and self.types['stickers']:
            m_hist = self.media_history[user_id]
            m_hist.append(now)
            while m_hist and m_hist[0] < now - self.media_window:
                m_hist.popleft()
            
            if len(m_hist) > self.media_limit:
                self.ignored_users[user_id] = now + self.ignore_duration
                return "stickers", f"{len(m_hist)} stickers in {self.media_window}s"

        # --- Burst Check (User History) ---
        history = self.user_history[user_id]
        
        # Clean up old history
        while history and history[0][0] < now - self.burst_window:
            history.popleft()
            
        if self.types['burst']:
            if len(history) >= self.burst_limit:
                self.ignored_users[user_id] = now + self.ignore_duration
                time_span = int(now - history[0][0]) if history else 0
                return "burst", f"{len(history)} msgs in {time_span}s"
            
        # --- Text Analysis (Only if not media and has text) ---
        if not is_media and text:
            # 1. Duplicate Check
            if self.types['duplicate']:
                duplicates = 0
                for _, old_text in history:
                    # Quick equality check first
                    if old_text == text:
                        duplicates += 1
                        continue
                    # Similarity check
                    if SequenceMatcher(None, text, old_text).ratio() >= self.duplicate_threshold:
                        duplicates += 1
                
                if duplicates >= 2: # 2 previous similar messages + current = 3
                    self.ignored_users[user_id] = now + self.ignore_duration
                    return "duplicate", f"{duplicates + 1} similar messages"

            # 2. Low Quality Check
            if self.types['lowquality']:
                # Repeated characters (e.g. "aaaaaaaaaa")
                if re.search(r'(.)\1{9,}', text):
                    self.ignored_users[user_id] = now + self.ignore_duration
                    return "lowquality", "Repeated characters"
                
                # Single word keyboard smash (long word, no spaces, high variety? or just random)
                # "asdfghjkl" -> hard to catch perfectly without dict.
                # Let's stick to simple repetitive or pattern based.
                # All Emojis (if length > 5)
                # Simple regex for "only symbols/emojis" is hard without external lib.
                # We can check if ratio of alphanumeric is low?
                if len(text) > 5 and len(re.findall(r'\w', text)) < 2:
                     # Mostly symbols/emojis?
                     self.ignored_users[user_id] = now + self.ignore_duration
                     return "lowquality", "Symbol/Emoji spam"

        # Add to history
        history.append((now, text))
            
        return False

    def is_ignored(self, user_id):
        now = time.time()
        if user_id in self.ignored_users:
             if now < self.ignored_users[user_id]:
                 return True
             else:
                 self.ignored_users.pop(user_id, None)
        return False

    def get_ignored_users(self):
        now = time.time()
        return [(uid, expiry - now) for uid, expiry in self.ignored_users.items() if expiry > now]
        
    def update_config(self, config):
        """Dynamic update of limits from config dict"""
        self.enabled = config.get('antispam_enabled', True)
        spam_settings = config.get('spam_settings', {})
        self.threshold = spam_settings.get('threshold_seconds', 5)
        self.ignore_duration = spam_settings.get('ignore_duration_minutes', 30) * 60
        self.burst_limit = spam_settings.get('burst_limit', 5)
        self.burst_window = spam_settings.get('burst_window_seconds', 10)
        self.global_flood_limit = spam_settings.get('global_flood_limit', 20)
        self.global_flood_window = spam_settings.get('global_flood_window', 3)
        self.raid_limit = spam_settings.get('raid_limit', 5)
        
        # New configs
        self.duplicate_threshold = spam_settings.get('duplicate_threshold', 0.85)
        self.media_limit = spam_settings.get('media_limit', 3)
        self.media_window = spam_settings.get('media_window', 5)

    def reset_history(self, user_id):
        if user_id in self.user_history:
            del self.user_history[user_id]
        if user_id in self.media_history:
             del self.media_history[user_id]
        if user_id in self.ignored_users:
            del self.ignored_users[user_id]

    def reset_global(self):
        self.global_history.clear()
