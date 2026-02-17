from telethon.tl.functions.channels import GetParticipantRequest
from telethon.tl.types import ChannelParticipant, ChannelParticipantAdmin, ChannelParticipantCreator
from telethon.errors import UserNotParticipantError
import asyncio

class EligibilityChecker:
    def __init__(self, client, config):
        self.client = client
        self.config = config
        self.eligibility_conf = config.get('eligibility', {})
        self.required_bio_string = self.eligibility_conf.get('required_bio_string', "@Naruto_X_Boruto_Bot")
        self.required_groups = self.eligibility_conf.get('required_groups_map', {"group_main": "@NarutoMainGroup"})

    async def check_user(self, user_id, status_msg=None):
        results = {"bio": False}
        for key in self.required_groups:
            results[key] = False
        missing_requirements = []

        # 1. Check Bio
        if status_msg:
             await status_msg.edit("Checking Eligibility...\n\nFetching User Profile...")
        
        from telethon.tl.functions.users import GetFullUserRequest
        
        try:
            full_user_data = await self.client(GetFullUserRequest(user_id))
            about = getattr(full_user_data.full_user, 'about', '') or ''
            print(f"DEBUG: Checking Bio for {user_id}. Fetched: '{about}'. Required: '{self.required_bio_string}'")
            
            # Check for required bio (flexible)
            req = self.required_bio_string.lower().strip('@')
            if req not in about.lower():
                missing_requirements.append(f"Bio missing `{self.required_bio_string}`")
            else:
                results["bio"] = True
            
            # Check for OTHER bot usernames (Strict Mode)
            # Regex to find @username ending in bot
            import re
            bot_mentions = re.findall(r'@\w+bot', about, re.IGNORECASE)
            for mention in bot_mentions:
                if mention.lower() != self.required_bio_string.lower():
                    missing_requirements.append(f"Bio contains other bot: `{mention}`. Only `{self.required_bio_string}` allowed.")
                    break # One error is enough

        except Exception as e:
            print(f"Error checking bio: {e}")
            missing_requirements.append("Could not fetch bio")

        # Update Status
        await self._update_status(status_msg, results, step=1)

        # 2. Check Groups
        groups_to_check = self.required_groups
        step_val = 2
        
        for key, group_username in groups_to_check.items():
            try:
                # Resolve entity first
                entity = await self.client.get_entity(group_username)
                await self.client(GetParticipantRequest(entity, user_id))
                results[key] = True
            except UserNotParticipantError:
                results[key] = False
                missing_requirements.append(f"Not in group {group_username}")
            except Exception as e:
                print(f"Error checking group {group_username}: {e}")
                results[key] = False
                missing_requirements.append(f"Error checking group {group_username}")
            
            # Update Status incrementally
            await self._update_status(status_msg, results, step=step_val)
            step_val += 1
            await asyncio.sleep(0.5) 

        return missing_requirements if missing_requirements else True

    async def _update_status(self, msg, results, step):
        if not msg:
            return
            
        text = "Checking Eligibility...\n\n"
        
        # Bio
        if step >= 1:
            icon = "[OK]" if results["bio"] else "[X]"
            text += f"{icon} Bio contains `{self.required_bio_string}`\n"
        else:
             text += f"[..] Bio contains `{self.required_bio_string}`\n"

        # Dynamic Groups
        checked_count = 0
        for group_key, group_name in self.required_groups.items():
            checked_count += 1
            # Current step (group check) implies previous groups are done/attempted
            # Step 1 was Bio. Step 2 starts groups.
            # If step >= 1 + checked_count, we know the result for this group.
            
            if step >= (1 + checked_count):
                icon = "[OK]" if results.get(group_key) else "[X]"
                text += f"{icon} Member of `{group_name}`\n"
            else:
                text += f"[..] Member of `{group_name}`\n"

        await msg.edit(text)
