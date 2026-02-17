import time
import random

class EventManager:
    def __init__(self, storage):
        self.storage = storage
        self.mode = "random" # random, fixed
        self.min_target = 100
        self.max_target = 250
        self.target_count = 0
        self.current_count = 0
        self.active = True 
        self.loop = True   
        self.paused_until = 0
        
        # Load from DB
        self.reload()
    
    def reload(self):
        """Load configuration and state from MongoDB"""
        try:
            # 1. Load Config from reward settings
            reward_settings = self.storage.get_reward_settings()
            interval_config = reward_settings.get('interval', {})
            
            if interval_config:
                self.mode = interval_config.get('mode', 'random')
                self.min_target = interval_config.get('min', 100)
                self.max_target = interval_config.get('max', 250)
                self.loop = interval_config.get('loop', True)
                self.active = interval_config.get('active', True)
            
            # 2. Load Runtime State
            state = self.storage.get_event_state()
            if state:
                self.current_count = state.get('current_count', 0)
                self.target_count = state.get('target_count', 0)
            
            # If no target set, generate one
            if self.target_count == 0:
                self.target_count = self._generate_random_target() if self.mode == "random" else self.min_target
                
        except Exception as e:
            print(f"⚠️ EventManager Load Error: {e}")

    def _save_config(self):
        """Save configuration to reward settings"""
        try:
            reward_settings = self.storage.get_reward_settings()
            reward_settings['interval'] = {
                'mode': self.mode,
                'min': self.min_target,
                'max': self.max_target,
                'loop': self.loop,
                'active': self.active
            }
            self.storage.save_reward_settings(reward_settings)
        except Exception as e:
            print(f"⚠️ EventManager Save Config Error: {e}")

    def _save_state(self):
        """Save runtime state to rewards collection"""
        try:
            state = {
                'current_count': self.current_count,
                'target_count': self.target_count
            }
            self.storage.save_event_state(state)
        except Exception as e:
            print(f"⚠️ EventManager Save State Error: {e}")

    def _generate_random_target(self):
        return random.randint(self.min_target, self.max_target)

    def start_event(self, min_val=50, max_val=200, loop=False):
        self.active = True
        self.mode = "random"
        self.loop = loop
        self.min_target = int(min_val)
        self.max_target = int(max_val)
        self.target_count = self._generate_random_target()
        self.current_count = 0
        self._save_config()
        self._save_state()
    
    def stop_event(self):
        self.active = False
        self.loop = False
        self._save_config()
    
    def set_fixed(self, count, loop=False):
        self.active = True
        self.mode = "fixed"
        self.loop = loop
        self.target_count = int(count)
        self.current_count = 0
        self._save_config()
        self._save_state()

    def get_remaining(self):
        if not self.active: return None
        return max(0, self.target_count - self.current_count)

    def pause_processing(self, duration_seconds):
        self.paused_until = time.time() + duration_seconds

    def is_paused(self):
        return time.time() < self.paused_until

    def unpause(self):
        self.paused_until = 0

    def process_message(self):
        if not self.active:
            return False
        
        if self.is_paused():
            return False
            
        self.current_count += 1
        
        # Save progress every message (required for Render survival)
        self._save_state()
        
        if self.current_count >= self.target_count:
            # Trigger Event!
            
            # Reset logic
            self.current_count = 0 
            
            if not self.loop:
                self.active = False # Stop if not looping
                self._save_config()
            else:
                # If looping, set next target
                if self.mode == "random":
                    self.target_count = self._generate_random_target()
                # If fixed, target_count stays same
            
            self._save_state()
            return True 
            
        return False
