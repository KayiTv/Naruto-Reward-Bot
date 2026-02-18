from telethon import TelegramClient
import asyncio
from datetime import datetime, timedelta, timezone

# IST Timezone
IST_OFFSET = timedelta(hours=5, minutes=30)
IST_TZ = timezone(IST_OFFSET)

class Logger:
    def __init__(self, client: TelegramClient, channel_id: int, db=None):
        self.client = client
        self.channel_id = channel_id
        self.db = db

    async def log_config(self, event: str, details: str, admin_name: str, admin_id: int):
        timestamp = datetime.now(IST_TZ).strftime("%H:%M IST")
        
        message = f"⚙️ **CONFIG**\n\n"
        message += f"**Changed:** {event}\n"
        message += f"{details}\n"
        message += f"**By:** {admin_name} ({admin_id})\n"
        message += f"**Time:** `{timestamp}`"
        
        await self._send(message)
        
        # Log to DB
        if self.db:
            await self.db.log_action(admin_id, f"CONFIG: {event}", details)

    async def log(self, event_type: str, user_name: str, user_id: int, details: str = "", extra: str = ""):
        timestamp = datetime.now(IST_TZ).strftime("%H:%M IST")
        
        # Emoji mapping
        emojis = {
            "REWARD": "✅",
            "INELIGIBLE": "⚠️",
            "ERROR": "❌",
            "SPAM": "🚫",
            "BAN": "🔨",
            "ADMIN": "👑",
            "CONFIG": "⚙️",
            "SYSTEM": "🔄",
            "UNBAN": "🔓"
        }
        icon = emojis.get(event_type, "📝")
        
        message = f"**{icon} {event_type}**\n\n"
        message += f"**User:** [{user_name}](tg://user?id={user_id})\n"
        message += f"**ID:** `{user_id}`\n"
        
        if details:
            message += f"{details}\n"
            
        message += f"\n**Time:** `{timestamp}`"
        
        if extra:
            message += f"\n{extra}"

        await self._send(message)
        
        # Log to DB
        if self.db:
            await self.db.log_action(user_id, f"EVENT: {event_type}", f"{details} | {extra}")

    async def _send(self, message: str):
        try:
            # Try sending directly
            await self.client.send_message(self.channel_id, message)
        except Exception as e:
            # If failed, try to get entity first (fixes "Could not find input entity" if not cached)
            try:
                # Remove -100 prefix if present for get_entity/ PeerChannel sometimes needs it or simple ID
                # But get_entity usually handles it.
                # If "Could not find input entity", it usually means we need to fetch it.
                entity = await self.client.get_entity(self.channel_id)
                await self.client.send_message(entity, message)
            except Exception as e2:
                print(f"Failed to log to channel {self.channel_id}: {e} | Retry error: {e2}")
                print(f"Make sure bot is added to the channel {self.channel_id} as admin.")
