import time

class MilestoneManager:
    def __init__(self, config, save_callback):
        self.config = config
        self.save_callback = save_callback
        # Ensure milestones section exists
        if 'milestones' not in self.config:
            self.config['milestones'] = {
                "enabled": True,
                "target_group": "@NarutoMainGroup",
                "pinned_message_id": None,
                "update_interval_posts": 50,
                "events": {},
                "active_event": {"active": False},
                "last_triggered": 0
            }

    def _get_conf(self):
        return self.config.get('milestones', {})

    def _save(self):
        self.config['milestones'] = self._get_conf()
        self.save_callback(self.config)

    def get_progress_data(self, current_count):
        conf = self._get_conf()
        events = conf.get('events', {})
        
        # Find next milestone
        sorted_milestones = sorted([int(k) for k in events.keys()])
        next_target = None
        for m in sorted_milestones:
            if m > current_count:
                next_target = m
                break
        
        if not next_target:
            return None # No more milestones

        details = events[str(next_target)]
        percent = int((current_count / next_target) * 100)
        remaining = next_target - current_count
        
        return {
            "current": current_count,
            "next_target": next_target,
            "percent": percent,
            "remaining": remaining,
            "next_duration": details['duration_hours'],
            "next_multiplier": details['multiplier']
        }

    def check_milestone(self, current_count):
        """
        Checks if a milestone triggers event.
        Returns (triggered_bool, event_details_or_none)
        """
        conf = self._get_conf()
        
        # If event active, check expiry only
        if conf.get('active_event', {}).get('active'):
            return False, None

        events = conf.get('events', {})
        last_triggered = conf.get('last_triggered', 0)
        
        sorted_m = sorted([int(k) for k in events.keys()])
        for m in sorted_m:
            # TRIGGER CONDITION:
            # 1. We crossed the milestone (current >= m)
            # 2. We haven't triggered this one yet (m > last_triggered)
            # 3. We are reasonably close to it (e.g. within 50 msgs) to avoid triggering old ones on restart 
            #    (Though user might WANT old ones? Let's stick to safe crossing)
            
            if current_count >= m and m > last_triggered:
                # Milestone crossed!
                return True, m
        
        return False, None

    def activate_event(self, milestone):
        conf = self._get_conf()
        details = conf['events'][str(milestone)]
        
        end_time = time.time() + (details['duration_hours'] * 3600)
        
        conf['active_event'] = {
            "active": True,
            "end_time": end_time,
            "milestone": milestone,
            "multiplier": details['multiplier'],
            "jackpot_chance": details['jackpot_chance']
        }
        
        conf['last_triggered'] = milestone
        self._save()
        
        return details

    def check_expiry(self):
        conf = self._get_conf()
        active = conf.get('active_event', {})
        
        if not active.get('active'):
            return False

        if time.time() > active.get('end_time', 0):
            # Expire
            conf['active_event'] = {
                "active": False,
                "end_time": 0,
                "milestone": 0,
                "multiplier": 1.0,
                "jackpot_chance": 5 # Default? Or 0?
            }
            self._save()
            return True
            
        return False

    def get_active_bonus(self):
        conf = self._get_conf()
        active = conf.get('active_event', {})
        if active.get('active'):
            return {
                "active": True,
                "multiplier": active.get('multiplier', 1.0),
                "jackpot_chance": active.get('jackpot_chance', 0)
            }
        return {"active": False, "multiplier": 1.0, "jackpot_chance": 0}

    def get_pinned_text(self, current_count):
        conf = self._get_conf()
        active = conf.get('active_event', {})
        
        if active.get('active'):
            # EVENT ACTIVE TEXT
            time_left_sec = max(0, active['end_time'] - time.time())
            hrs = int(time_left_sec // 3600)
            mins = int((time_left_sec % 3600) // 60)
            
            # Get next milestone for "Next:" field
            # ... logic to get next ...
            sorted_m = sorted([int(k) for k in conf['events'].keys()])
            next_m = "Unknown"
            for m in sorted_m:
                if m > active['milestone']:
                    next_m = f"{m:,} members"
                    break
            
            return f"""🎊 **EVENT ACTIVE**

Time left: {hrs}h {mins}m

**Bonuses:**
✅ Rewards: {active['multiplier']}x
✅ Jackpot: {active['jackpot_chance']}%

Next: {next_m}"""

        else:
            # PROGRESS TRACKER TEXT
            data = self.get_progress_data(current_count)
            if not data:
                return f"📊 **Current Members:** {current_count}\n\nNo active milestones."
                
            return f"""📊 **NEXT MILESTONE: {data['next_target']:,}**

Progress: {data['current']:,} / {data['next_target']:,} ({data['percent']}%)
{data['remaining']:,} to go!

Reward: {data['next_multiplier']}x multiplier for {data['next_duration']}h"""

