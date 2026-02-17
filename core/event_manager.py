import time
import random

class EventManager:
    def __init__(self, storage):
        self.storage = storage
        self.mode = "random" # random, fixed
        self.loop = False
        self.min_target = 100
        self.max_target = 250
        self.target_count = self._generate_random_target()
        self.current_count = 0
        self.active = True # Active by default
        self.loop = True   # Loop by default
        self.paused_until = 0
    
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
    
    def stop_event(self):
        self.active = False
        self.loop = False
    
    def set_fixed(self, count, loop=False):
        self.active = True
        self.mode = "fixed"
        self.loop = loop
        self.target_count = int(count)
        self.current_count = 0

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
        
        if self.current_count >= self.target_count:
            # Trigger Event!
            
            # Reset logic
            self.current_count = 0 
            
            if not self.loop:
                self.active = False # Stop if not looping
            else:
                # If looping, set next target
                if self.mode == "random":
                    self.target_count = self._generate_random_target()
                # If fixed, target_count stays same
            
            return True 
            
        return False
