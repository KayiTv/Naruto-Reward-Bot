
import asyncio
import logging
import os
import sys 
import json # Added json
import time
import subprocess
from datetime import datetime, timedelta

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# --- EVENT LOOP FIX FOR CLOUD DEPLOYMENT ---
# Telethon requires an active loop during initialization in some environments
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

from telethon import TelegramClient, events, functions, types, Button, errors
from telethon.sessions import StringSession
from telethon.errors import MessageNotModifiedError
from telethon.tl.functions.users import GetFullUserRequest
import random

# Custom Modules - Import from core package
from core.storage_mongodb import MongoStorage
from core.spam_check import SpamDetector
from core.event_manager import EventManager
from core.eligibility import EligibilityChecker
from core.logger import Logger
from core.milestones import MilestoneManager
from core.web_server import start_server

# Phase 2 & 6 Monitoring/Utility Imports
from core.cache import user_cache, config_cache
from utils.db_monitor import get_performance_report, check_performance

# --- CONFIGURATION & DATABASE ---

try:
    db = MongoStorage()
except Exception as e:
    print(f"[ERROR] Database Connection Error: {e}")
    sys.exit(1)

# Load config from DB (Async-to-Sync Bridge for startup)
config = loop.run_until_complete(db.get_config())

# If empty, try to migrate or set defaults
if not config:
    print("[WARN] No config found in DB. Checking local config.json...")
    if os.path.exists('config.json'):
         with open('config.json', 'r') as f:
             local_config = json.load(f)
             db_sync = MongoStorage() # Re-init if needed
             loop.run_until_complete(db_sync.save_config(local_config))
             config = local_config
             print("[INFO] Migrated local config.json to MongoDB.")
    else:
        print("[ERROR] No config found. Exiting.")
        sys.exit(1)


# Hydrate config from environment variables if missing
if config:
    for key in ['api_id', 'owner_id', 'log_channel_id']:
        val = config.get(key) or os.getenv(key.upper())
        if val:
            try:
                config[key] = int(val)
            except:
                config[key] = val

    for key in ['api_hash', 'bot_token', 'phone_number']:
        val = config.get(key) or os.getenv(key.upper())
        if val:
            config[key] = val

OWNER_ID = config.get('owner_id')
LOG_CHANNEL_ID = config.get('log_channel_id')

# CONSTANTS & HELPER FUNCTIONS
OWNER_FIRST_NAME = "Fetched from Telegram"
REQUIRED_BOT = config.get('eligibility', {}).get('required_bio_string', "@Naruto_X_Boruto_Bot")
REQUIRED_GROUP = config.get('eligibility', {}).get('required_groups_map', {}).get('group_main', "@NarutoMainGroup")

DEFAULT_REWARD_AMOUNT = 5
DEFAULT_INTERVAL = [100, 250]

async def update_owner_info(client):
    global OWNER_FIRST_NAME
    owner_id = config.get('owner_id')
    if not owner_id: return
    try:
        user = await client.get_entity(owner_id)
        OWNER_FIRST_NAME = user.first_name
    except Exception as e:
        print(f"[ERROR] Failed to fetch owner info: {e}")

async def edit_with_delay(msg, new_text, delay=0.5):
    await asyncio.sleep(delay)
    await msg.edit(new_text)

async def save_config(new_config):
    """Save config to all collections async"""
    await db.save_config(new_config)
    if 'reward_settings' in new_config:
        await db.save_reward_settings(new_config['reward_settings'])
    if 'spam_settings' in new_config:
        await db.save_anti_spam_settings(new_config['spam_settings'])

def validate_config():
    required = ['api_id', 'api_hash', 'bot_token', 'phone_number']
    # Check both config and env (env loaded later/implicitly via load_dotenv calls in main, but better to check config object if it's populated from env)
    # Actually, main.py loads .dotenv at top. but load_config loads from json.
    # config.json might have "LOADED_FROM_ENV" or be empty.
    # Let's check if the values are present in os.environ OR config.
    
    missing = []
    for k in required:
        val = config.get(k)
        if not val or val == "LOADED_FROM_ENV":
             # Check env
             if not os.getenv(k.upper()):
                 missing.append(k)
    
    if missing:
        print(f"[ERROR] Missing required config (in .env or config.json): {missing}")
        sys.exit(1)

validate_config()
command_cooldowns = {}

    # --- INITIALIZATION ---
try:
    # db initialized above
    
    # Load separate reward settings from rewards collection
    reward_settings = db.get_reward_settings()
    
    # Ensure tiers enabled by default if not set
    if 'tiers' not in reward_settings:
        reward_settings['tiers'] = {'enabled': True}
    elif not reward_settings['tiers'].get('enabled'):
        reward_settings['tiers']['enabled'] = True
    
    # Cache in global config for easy access
    config['reward_settings'] = reward_settings
    
    # Load separate anti-spam settings from anti_spam collection
    spam_settings = db.get_anti_spam_settings()
    
    spam_detector = SpamDetector(
        threshold_seconds=spam_settings.get('threshold_seconds', 5),
        ignore_duration=spam_settings.get('ignore_duration_minutes', 30) * 60,
        burst_limit=spam_settings.get('burst_limit', 5),
        burst_window=spam_settings.get('burst_window_seconds', 10),
        global_flood_limit=spam_settings.get('global_flood_limit', 20),
        global_flood_window=spam_settings.get('global_flood_window', 3),
        raid_limit=spam_settings.get('raid_limit', 5)
    )
    
    event_manager = EventManager(db)
    
    # Clients
    bot_session = os.getenv('BOT_STRING_SESSION') or 'bot'
    userbot_session = os.getenv('STRING_SESSION') or config.get('session_name', 'userbot')

    bot = TelegramClient(
        StringSession(bot_session) if os.getenv('BOT_STRING_SESSION') else bot_session, 
        config['api_id'], 
        config['api_hash']
    )
    userbot = TelegramClient(
        StringSession(userbot_session) if os.getenv('STRING_SESSION') else userbot_session, 
        config['api_id'], 
        config['api_hash']
    )
    
    eligibility_checker = EligibilityChecker(bot, config)
    logger = Logger(bot, config.get('log_channel_id', 0), db=db)

    # Milestone Manager
    async def save_config_callback(new_conf):
        global config
        config = new_conf
        await db.save_config(config)
    milestone_manager = MilestoneManager(config, save_config_callback)

except Exception as e:
    print(f"[ERROR] Initialization Error: {e}")
    exit(1)

# Start background task to fetch owner info
# Moved to main()

# --- HELPERS ---

# db_buffer_flusher removed in favor of Phase 4 WriteQueue

async def check_admin(user_id):
    """Check if user is admin (Cached)"""
    # 1. Check DB/Cache first (Phase 2/5)
    if await db.is_admin(user_id):
        return True
        
    # 2. Check config
    admin_ids = config.get('admin_ids', [])
    owner_id = config.get('owner_id')
    return user_id == owner_id or user_id in admin_ids or str(user_id) in [str(a) for a in admin_ids]

@bot.on(events.NewMessage(pattern=r'^/reload$'))
async def reload_cmd(event):
    if not await check_admin(event.sender_id): return
    
    msg = await event.reply("🔄 Reloading config from DB...")
    
    try:
        global config
        new_config = await db.get_config()
        if not new_config:
            await msg.edit("❌ Failed to fetch config from DB.")
            return
            
        # Update Global Config
        config = new_config
        
        # Reload modules from separated locations
        config['reward_settings'] = await db.get_reward_settings()
        config['spam_settings'] = await db.get_anti_spam_settings()
        
        # Update Components
        spam_detector.update_config(config)
        milestone_manager.config = config
        event_manager.reload()
        
        # Update Helper Constants (if any depend on config directly)
        global REQUIRED_BOT, REQUIRED_GROUP
        REQUIRED_BOT = config.get('eligibility', {}).get('required_bio_string', "@Naruto_X_Boruto_Bot")
        REQUIRED_GROUP = config.get('eligibility', {}).get('required_groups_map', {}).get('group_main', "@NarutoMainGroup")
        
        await msg.edit("✅ **Config Reloaded Successfully!**\n\n• Spam Settings updated\n• Milestones updated\n• Reward Interval synced\n• Admin list updated (in-memory)")
        await logger.log("ADMIN", event.sender.first_name, event.sender_id, "Reloaded Configuration")
        
    except Exception as e:
        await msg.edit(f"❌ Error reloading: {e}")

@bot.on(events.NewMessage(pattern=r'^/restart$'))
async def restart_cmd(event):
    if event.sender_id != config.get('owner_id', OWNER_ID): return
    
    msg = await event.reply("🔄 **Restarting Bot...**")
    
    try:
        # 1. Pull latest code from GitHub
        await msg.edit("🔄 **Restarting Bot...**\n\n• Checking for updates (git pull)...")
        try:
            pull_output = subprocess.check_output(["git", "pull"], stderr=subprocess.STDOUT).decode()
            update_status = f"✅ Success: `{pull_output[:50]}...`" if "Already up to date" not in pull_output else "✅ Already up to date"
        except Exception as e:
            update_status = f"⚠️ Skip: {str(e)[:50]}"

        # 2. Sync DB (reload config)
        await msg.edit(f"🔄 **Restarting Bot...**\n\n• Updates: {update_status}\n• Syncing Database...")
        await db.get_config()
        
        # 3. Final log
        await logger.log("SYSTEM", "Bot", 0, "Bot Restarting", "Initiated by Owner via /restart")
        await msg.edit(f"✅ **Restarting now!**\n\nUpdates: {update_status}\nStatus: **Reloading Process...**")
        
        # 4. Restart Process
        os.execl(sys.executable, sys.executable, *sys.argv)
        
    except Exception as e:
        await msg.edit(f"❌ **Restart Failed:** {e}")


def encode_data(action, user_id=0):
    return f"{action}:{user_id}"

def get_mention(user):
    return f"[{user.first_name}](tg://user?id={user.id})"

async def get_tier_details(user_id):
    """Determine user tier (Async + Cached)"""
    # Phase 2: db.get_daily_stats is already cached
    daily_stats = await db.get_daily_stats(user_id)
    daily_msgs = daily_stats['msgs']
    
    reward_conf = config.get('reward_settings', {})
    tiers = reward_conf.get('tiers', {})
    
    if not tiers.get('enabled', False):
        return None, 1.0, daily_msgs, None, 0
        
    tier_list = []
    for name, data in tiers.items():
        if name == 'enabled' or not isinstance(data, dict): continue
        if 'range' in data:
            tier_list.append({
                'name': name.title(),
                'min': data['range'][0],
                'max': data['range'][1],
                'multiplier': data.get('multiplier', 1.0)
            })
    
    tier_list.sort(key=lambda x: x['min'])
    
    current_tier = "Bronze"
    multiplier = 1.0
    next_tier = None
    msgs_needed = 0
    
    for i, t in enumerate(tier_list):
        if t['min'] <= daily_msgs <= t['max']:
            current_tier = t['name']
            multiplier = t['multiplier']
            if i + 1 < len(tier_list):
                next_t = tier_list[i+1]
                next_tier = next_t['name']
                msgs_needed = max(0, next_t['min'] - daily_msgs)
            break
            
    if tier_list and daily_msgs > tier_list[-1]['max']:
        current_tier = tier_list[-1]['name']
        multiplier = tier_list[-1]['multiplier']
        next_tier = None
        msgs_needed = 0

    return current_tier, multiplier, daily_msgs, next_tier, msgs_needed

async def calculate_reward(user_id):
    """Calculate reward amount (Async)"""
    reward_conf = config.get('reward_settings', {})
    ms_bonus = milestone_manager.get_active_bonus()
    tier_name, multiplier, daily_msgs, _, _ = await get_tier_details(user_id)

    # Jackpot Check
    jackpot = reward_conf.get('jackpot', {})
    if jackpot.get('enabled', False):
        chance = ms_bonus['jackpot_chance'] if ms_bonus['active'] else jackpot.get('chance', 0)
        if random.randint(1, 100) <= chance:
            return {
                'type': 'jackpot', 'amount': jackpot['amount'], 'base': 0,
                'multiplier': 1.0, 'tier': tier_name, 'tier_multiplier': multiplier,
                'heading_extra': " (EVENT ACTIVE!)" if ms_bonus['active'] else "",
                'msg': f"🎰 **JACKPOT HIT!** 🎰"
            }
            
    # Base Amount
    base = reward_conf.get('base', {})
    amount = random.randint(base.get('min', 5), base.get('max', 10)) if base.get('mode') == 'random' else base.get('amount', 5)
    final_amount = int(amount * multiplier * ms_bonus['multiplier'])
    
    return {
        'type': 'normal', 'amount': final_amount, 'base': amount,
        'multiplier': multiplier, 'ms_multiplier': ms_bonus['multiplier'],
        'tier': tier_name, 'msg': f"Reward{' (Event Bonus!)' if ms_bonus['active'] else ''}"
    }

def get_settings_menu(user_id):
    s = config.get('spam_settings', {})
    antispam_status = "✅ ON" if config.get('antispam_enabled') else "❌ OFF"
    
    # Safely get reward settings
    r = config.get('reward_settings', {})
    base = r.get('base', {})
    base_val = base.get('amount', 5)
    tiers_on = "✅" if r.get('tiers', {}).get('enabled') else "❌"
    jackpot_on = "✅" if r.get('jackpot', {}).get('enabled') else "❌"
    
    text = f"""⚙️ **Bot Settings**

🛡️ **Anti-Spam:** {antispam_status}

**Limits:**
• Burst: {s.get('burst_limit')}/{s.get('burst_window_seconds')}s
• Flood: {s.get('global_flood_limit')}/{s.get('global_flood_window')}s
• Penalty: {s.get('ignore_duration_minutes')}m

💰 **Rewards:**
• Base: {base_val}
• Tiers: {tiers_on}
• Jackpot: {jackpot_on}"""

    buttons = [
        [Button.inline("🛡️ Toggle Anti-Spam", encode_data("toggle_spam", user_id))],
        [Button.inline("💰 Configure Rewards", b"reward:menu")], # Use callback for menu if possible, or just command hint
        [Button.inline("⬅️ Back", encode_data("admin_menu", user_id))]
    ]
    return text, buttons

async def get_menu_layout(user_id):
    is_admin = await check_admin(user_id)
    
    # Buttons
    buttons = [
        [Button.inline("🔍 Check Eligibility", encode_data("eligible", user_id))],
        [Button.inline("📊 Stats", encode_data("stats", user_id)), Button.inline("🏆 Top", encode_data("top", user_id))]
    ]
    
    # Row 3
    row3 = [Button.inline("📜 Rules", encode_data("rules", user_id))]
    if is_admin:
        row3.append(Button.inline("🛡️ Admin", encode_data("admin_menu", user_id)))
    buttons.append(row3)
    
    # Text
    owner = config.get('owner_id', OWNER_ID)
    
    text = f"""👋 **Welcome to Naruto X Boruto Reward Bot!**

🎁 Win stocks by being active in the group!

**Commands:**
/eligible - Check if you qualify
/rules - Requirements & spam rules
/stats - Bot statistics
/top - Today's top active users

Managed by [{OWNER_FIRST_NAME}](tg://user?id={owner})"""
    
    return text, buttons

# Security Encoding
def encode_data(action, user_id):
    return f"{action}:{user_id}".encode('utf-8')

# --- CONFIGURATION HANDLERS ---



@bot.on(events.NewMessage(pattern=r'^/setlimit (burst|flood) (\d+) (\d+)$'))
async def setlimit_cmd(event):
    if not await check_admin(event.sender_id): return
    
    limit_type = event.pattern_match.group(1).lower()
    count = int(event.pattern_match.group(2))
    seconds = int(event.pattern_match.group(3))
    
    # Frame 1
    msg = await event.reply(f"⚙️ Updating {limit_type} limit...")
    
    s = config.get('spam_settings', {})
    old_count = s.get(f'{limit_type}_limit')
    old_window = s.get(f'{limit_type}_window_seconds') # Note: variable name flood vs global_flood in config?
    # Config keys in main.py init:
    # burst_limit, burst_window_seconds
    # global_flood_limit, global_flood_window
    
    conf_key_limit = f"{limit_type}_limit"
    if limit_type == 'flood': conf_key_limit = "global_flood_limit"
    
    conf_key_window = f"{limit_type}_window_seconds"
    if limit_type == 'flood': conf_key_window = "global_flood_window"
    
    old_count = s.get(conf_key_limit)
    old_window = s.get(conf_key_window)
    
    # Update Config
    s[conf_key_limit] = count
    s[conf_key_window] = seconds
    config['spam_settings'] = s
    await save_config(config)
    spam_detector.update_config(config)
    
    # Frame 2
    if limit_type == 'burst':
        text = f"""✅ **Burst limit updated!**

Old: {old_count} messages in {old_window} seconds
New: {count} messages in {seconds} seconds

Per-user rate limit changed.
Users can send more messages now."""
    else:
        text = f"""✅ **Flood limit updated!**

Old: {old_count} messages in {old_window} seconds
New: {count} messages in {seconds} seconds

Global rate limit changed.
Applies to ALL users combined."""

    await edit_with_delay(msg, text)
    
    await logger.log_config(
        f"{limit_type.title()} limit",
        f"Old: {old_count} msgs / {old_window}s\nNew: {count} msgs / {seconds}s",
        event.sender.first_name,
        event.sender_id
    )

def decode_data(data_bytes):
    try:
        text = data_bytes.decode('utf-8')
        if ':' in text:
            action, owner_id = text.rsplit(':', 1)
            return action, int(owner_id)
        return text, None
    except:
        return None, None

# --- COMMAND HANDLERS ---

@bot.on(events.NewMessage(pattern=r'^/start$'))
async def start_cmd(event):
    sender = await event.get_sender()
    uid = sender.id
    
    text, buttons = await get_menu_layout(uid)
    await event.reply(text, buttons=buttons)

@bot.on(events.NewMessage(pattern=r'^/eligible$'))
async def eligible_cmd(event):
    uid = event.sender_id
    
    # Admin checking another user
    if event.is_reply and await check_admin(uid):
        reply = await event.get_reply_message()
        if not reply: return
        target_uid = reply.sender_id
        user_entity = reply.sender
        name = user_entity.first_name if user_entity else str(target_uid)
        
        msg = await event.reply(f"🔍 Checking `{name}`'s eligibility...")
        await asyncio.sleep(1.5)
        
        res = await eligibility_checker.check_user(target_uid)
        stats = await db.get_user_stats(target_uid)
        is_eligible = (res is True)
        
        # Report
        report = f"""📋 **Eligibility Report**

User: [{name}](tg://user?id={target_uid}) (`{target_uid}`)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📝 **BIO CHECK**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
        # We need more granular results from eligibility_checker ideally, 
        # but for now we parse the result list if it's not True
        if is_eligible:
            report += f"✅ {REQUIRED_BOT}: Found\n✅ Other bots: None detected\n"
        else:
            # res is a list of failures
            if any("Bio missing" in r for r in res):
                report += f"❌ {REQUIRED_BOT}: Not Found\n"
            else:
                report += f"✅ {REQUIRED_BOT}: Found\n"
                
            if any("Other bot" in r for r in res):
                 report += "❌ Other bots: Detected\n"
            else:
                 report += "✅ Other bots: None detected\n"

        report += f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
👥 **GROUPS**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
        if is_eligible:
            report += f"✅ {REQUIRED_GROUP}\n"
        else:
            if any("Member of" in r for r in res):
                report += f"❌ Not in {REQUIRED_GROUP}\n"
            else:
                 report += f"✅ {REQUIRED_GROUP}\n"

        report += f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📊 **USER STATS**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Messages today: {(await db.get_daily_stats(target_uid))['msgs']}
Total messages: {stats['total_msgs']}
Stocks won today: {(await db.get_daily_stats(target_uid))['stocks']}
Total stocks won: {stats['total_stocks']}
"""
        if stats['last_win'] > 0:
            import datetime
            # Calculate time ago
            diff = time.time() - stats['last_win']
            mins = int(diff / 60)
            report += f"Last win: {mins} minutes ago\n"
        else:
            report += "Last win: Never\n"

        report += f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FINAL: {"✅ ELIGIBLE" if is_eligible else "❌ NOT ELIGIBLE"}
"""
        if not is_eligible:
            report += "Reason: " + ", ".join(res)
            
        await msg.edit(report)
        return

    # Regular User Check
    msg = await event.reply("🔍 Checking eligibility...")
    # Animation handled inside check_user if msg is passed
    res = await eligibility_checker.check_user(uid, msg)
    
    if res is True:
        text = f"""✅ **You are ELIGIBLE!**

Bio Check: ✅ {REQUIRED_BOT} found
Other Bots: ✅ None detected
Group: ✅ Member of {REQUIRED_GROUP}

Keep chatting to win rewards!
**No cooldown - win anytime!** 🎉"""
    else:
        text = f"""❌ **You are NOT eligible**

Bio Check: {"❌ Not found" if any("Bio missing" in r for r in res) else "✅ Found"}
Other Bots: {"⚠️ Detected" if any("Other bot" in r for r in res) else "✅ None"}
Group: {"❌ Not joined" if any("Member of" in r for r in res) else "✅ Joined"}

Fix these issues to qualify!"""
        
    await msg.edit(text)

@bot.on(events.NewMessage(pattern=r'^/mytier$'))
async def mytier_cmd(event):
    uid = event.sender_id
    msg = await event.reply("📊 Checking your tier...")
    await asyncio.sleep(0.5)
    
    tier_name, multiplier, msgs, next_tier, needed = await get_tier_details(uid)
    
    if not tier_name:
        await msg.edit("⚠️ Tier system is currently disabled.")
        return
        
    # Calculate potential reward for display
    r_conf = config.get('reward_settings', {})
    base_data = r_conf.get('base', {})
    if base_data.get('mode') == 'random':
        base_disp = f"{base_data.get('min')}-{base_data.get('max')}"
        # Show example max
        example_base = base_data.get('max', 10)
    else:
        base_inv = int(base_data.get('amount', 5))
        base_disp = str(base_inv)
        example_base = base_inv
        
    calc_disp = int(example_base * multiplier)

    # Emoji config
    icons = {
        'Bronze': '🥉', 'Silver': '🥈', 'Gold': '🥇', 'Platinum': '💎'
    }
    icon = icons.get(tier_name, '🎖️')
    
    status_text = f"""{icon} **{tier_name} Tier**

Messages today: {msgs}
Multiplier: {multiplier}x
Potential Reward: {calc_disp} stocks ({base_disp} × {multiplier}x)
"""

    if next_tier:
        status_text += f"\n🚀 **Next Tier:** {next_tier}\nNeeds {needed} more messages!"
    else:
        status_text += "\n🏆 **Max Tier Reached!**"
        
    status_text += "\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n📅 Resets daily at 00:00 IST"
    
    await msg.edit(status_text)

@bot.on(events.NewMessage(pattern=r'^/top(?: (.+))?$'))
async def top_cmd(event):
    args = event.pattern_match.group(1)
    admin_view = False
    
    if args and (args.lower() == 'details' or args.lower() == 'admin'):
        if not await check_admin(event.sender_id):
            await event.reply("❌ Admin only.")
            return
        admin_view = True

    # Frame 1
    msg = await event.reply("🏆 Loading leaderboard...")
    # Delay implied by fetching
    
    top_users = await db.get_top_daily(event.chat_id, limit=10)
    now_str = db.get_ist_now().strftime("%H:%M IST")
    
    if not top_users:
        text = f"""🏆 **TOP ACTIVE USERS (Today)**

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
No activity yet today.

Be the first to chat and win rewards!
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📅 Resets daily at 00:00 IST
🕐 Current time: {now_str}"""
        await msg.edit(text)
        return

    title = "🏆 **TOP 10 ACTIVE (Today - Admin View)**" if admin_view else "🏆 **TOP 10 ACTIVE (Today)**"
    text = f"{title}\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    
    user_rank_in_top = False
    
    for i, user_tuple in enumerate(top_users, 1):
        uid, msgs, stocks = user_tuple
        d = {'msgs': msgs, 'stocks': stocks}
        icon = "🥇" if i==1 else "🥈" if i==2 else "🥉" if i==3 else f"{i}️⃣"
        
        try:
            u_obj = await bot.get_entity(int(uid))
            name = u_obj.first_name
            name_link = f"[{name}](tg://user?id={uid})"
        except:
            name_link = f"User {uid}"
            
        if admin_view:
            # Admin View: ID, Tier, Wins
            tier_name, _, _, _, _ = await get_tier_details(int(uid))
            tier_icon = {'Bronze': '🥉', 'Silver': '🥈', 'Gold': '🥇', 'Platinum': '💎'}.get(tier_name, '')
            stats = await db.get_user_stats(uid)
            last_win = "Never"
            if stats['last_win']:
                # Calculate time ago roughly
                ago = int((time.time() - stats['last_win']) / 3600)
                last_win = f"{ago}h ago" if ago < 24 else "1d+ ago"
                
            text += f"{icon} {name_link} (`{uid}`)\n"
            text += f"   {d['msgs']} msgs | {d['stocks']} stocks | {tier_name} {tier_icon}\n"
            text += f"   Total Wins: {int(stats['total_stocks']/5)}? | Last: {last_win}\n\n"
            
        else:
            # User View
            if int(uid) == event.sender_id:
                user_rank_in_top = True
                text += f"{icon} **You** - {d['msgs']} msgs | {d['stocks']} stocks ⭐\n"
            else:
                text += f"{icon} {name_link} - {d['msgs']} msgs | {d['stocks']} stocks\n"

    text += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    
    if not admin_view and not user_rank_in_top:
        rank, total = await db.get_user_rank(event.sender_id, event.chat_id)
        if rank:
            daily = await db.get_daily_stats(event.sender_id, event.chat_id)
            text += f"Your rank: #{rank}\nYour messages: {daily['msgs']} msgs | {daily['stocks']} stocks\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"

    if admin_view:
        # Extra stats footer
        all_stats = await db.get_all_daily_stats(event.chat_id)
        total_active_users = len(all_stats)
        # Calculate sums (all_stats is a list of dicts)
        daily_stocks_sum = sum(u.get('stocks', 0) for u in all_stats)
        
        text += f"Total active: {total_active_users} users\nTotal stocks won: {daily_stocks_sum} stocks\n"

    text += f"📅 Resets daily at 00:00 IST\n🕐 Current time: {now_str}"

    # Buttons for User View
    buttons = None
    if not admin_view:
        buttons = [
            [Button.inline("🔄 Refresh", encode_data("top", event.sender_id)), Button.inline("📊 My Stats", encode_data("stats", event.sender_id))]
        ]

    await msg.edit(text, buttons=buttons)
        


@bot.on(events.NewMessage(pattern=r'^/rules$'))
async def rules_cmd(event):
    # Dynamic values from config
    s = config.get('spam_settings', {})
    
    text = f"""📜 **REWARD BOT RULES**

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🎁 **HOW TO WIN**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- Be active in the group
- Winner selected every {event_manager.min_target}-{event_manager.max_target} messages
- Eligibility checked automatically
- Win {config.get('reward_cmd_template', '5').split()[-1] if 'stocks' in config.get('reward_cmd_template', '') else 'rewards'} per reward
- **No cooldown - win unlimited times!** 🔥

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✅ **REQUIREMENTS**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1️⃣ Bio must contain: `{REQUIRED_BOT}`
2️⃣ Bio must NOT contain other bot usernames
3️⃣ Must be member of: `{REQUIRED_GROUP}`
4️⃣ Not banned

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🚫 **SPAM RULES**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- Max {s.get('burst_limit')} messages in {s.get('burst_window_seconds')} seconds
- Max {s.get('global_flood_limit')} messages in {s.get('global_flood_window')} seconds (global)
- No duplicate/similar messages
- No random spam (asdfgh, only emojis)

⚠️ **Penalty:** Ignored for {s.get('ignore_duration_minutes')} minutes

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Use /eligible to check your status
Managed by [{OWNER_FIRST_NAME}](tg://user?id={OWNER_ID})"""
    await event.reply(text)

@bot.on(events.NewMessage(pattern=r'^/botstats$'))
async def botstats_cmd(event):
    # Restricted to Admin
    if not await check_admin(event.sender_id):
        await event.reply("❌ **Access Denied.** This command is for admins only.")
        return
        
    msg = await event.reply("📊 Loading statistics...")
    await asyncio.sleep(1.0)
    
    global_stats = await db.get_global_stats()
    
    # Calculate daily (sum of all daily stats for today)
    daily_stats = await db.get_all_daily_stats() # Returns List[Dict]
    day_msgs = sum(u.get('msgs', 0) for u in daily_stats)
    day_stocks = sum(u.get('stocks', 0) for u in daily_stats)
    active_users = len(daily_stats)
    
    # Get active penalties
    ap = await db.get_all_penalties()
    
    text = f"""📊 **Bot Statistics**

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⏱️ **UPTIME**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Running: (Simulated)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📈 **TODAY'S ACTIVITY**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Messages: {day_msgs}
Rewards given: ? (Not tracked daily separately)
Stocks distributed: {day_stocks}
Active users: {active_users}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🎯 **REWARD SYSTEM**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Interval: {event_manager.min_target}-{event_manager.max_target} messages
Stock amount: {config.get('reward_cmd_template', '5').split()[-1] if 'stocks' in config.get('reward_cmd_template', '') else '5'} per reward
Next reward: ~{event_manager.target_count - event_manager.current_count} messages
Cooldown: **None** ✨

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🚫 **SPAM DETECTION**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Status: {"✅ Active" if config.get('antispam_enabled', True) else "❌ Disabled"}
Blocked today: ?
Active penalties: {len(ap)}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📊 **ALL TIME**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Total rewards: {global_stats['total_selections']}
Total stocks: {global_stats['distributed_stocks']}
Total users: {global_stats['total_tracked_users']}

Managed by [{OWNER_FIRST_NAME}](tg://user?id={config.get('owner_id')})"""

    await msg.edit(text)



# --- ADMIN COMMANDS ---

@bot.on(events.NewMessage(pattern=r'^/admins$'))
async def admins_cmd(event):
    if not await check_admin(event.sender_id): return
    
    admin_ids = config.get('admin_ids', [])
    owner_id = config.get('owner_id', OWNER_ID)
    
    text = f"""👥 **BOT ADMINS ({len(admin_ids) + 1})**

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
👑 [{OWNER_FIRST_NAME}](tg://user?id={owner_id}) - Owner
"""
    for uid in admin_ids:
        # In real bot, fetch name. For now use ID or cache
        try:
             u = await bot.get_entity(int(uid))
             name = u.first_name
        except: name = f"Admin{uid}"
        text += f"\n🛡️ [{name}](tg://user?id={uid}) (`{uid}`)"
        
    text += "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\nUse /addadmin or /rmadmin to manage."
    await event.reply(text)

@bot.on(events.NewMessage(pattern=r'^/addadmin(?: (\d+|@\w+))?$'))
async def add_admin_cmd(event):
    if not await check_admin(event.sender_id): return
    
    # Check if user is owner (only owner can add admins?) 
    # Request doesn't specify, but usually only owner. 
    # Let's allow existing admins to add admins for now based on previous logic.
    
    user = None
    if event.is_reply:
         user = (await event.get_reply_message()).sender
    elif event.pattern_match.group(1):
        try:
            user = await bot.get_entity(event.pattern_match.group(1))
        except: pass
        
    if not user:
        await event.reply("❌ Reply to user or provide ID/Username.")
        return
        
    uid = user.id
    if uid not in config.get('admin_ids', []):
        config.setdefault('admin_ids', []).append(uid)
        await save_config(config)
        
        msg = await event.reply("⏳ Adding admin...")
        await asyncio.sleep(0.5)
        
        text = f"""✅ **Admin added!**

User: [{user.first_name}](tg://user?id={uid}) (`{uid}`)
Added by: [{event.sender.first_name}](tg://user?id={event.sender_id})
Total admins: {len(config['admin_ids'])}

Use /admins to view all."""
        await msg.edit(text)
        await logger.log("ADMIN", user.first_name, uid, "Added as admin", f"By: {event.sender_id}")
    else:
        await event.reply("⚠️ Already an admin.")

@bot.on(events.NewMessage(pattern=r'^/thappad(?: (\d+|@\w+))?$'))
async def thappad_cmd(event):
    if not await check_admin(event.sender_id): return
    
    user = None
    if event.is_reply:
         user = (await event.get_reply_message()).sender
    elif event.pattern_match.group(1):
        try:
            user = await bot.get_entity(event.pattern_match.group(1))
        except: pass
        
    if not user:
        await event.reply("❌ Reply to user or provide ID/Username.")
        return
        
    db.ban_user(user.id)
    await event.reply(f"👋 **THAPPAD!**\n\nUser: [{user.first_name}](tg://user?id={user.id})\nStatus: **BANNED** 🚫\n\n_User has been slapped out of the bot._")
    await logger.log("BAN", user.first_name, user.id, "Banned User", f"By: {event.sender_id}")

@bot.on(events.NewMessage(pattern=r'^/maafi(?: (\d+|@\w+))?$'))
async def maafi_cmd(event):
    if not await check_admin(event.sender_id): return
    
    user = None
    if event.is_reply:
         user = (await event.get_reply_message()).sender
    elif event.pattern_match.group(1):
        try:
            user = await bot.get_entity(event.pattern_match.group(1))
        except: pass
        
    if not user:
        await event.reply("❌ Reply to user or provide ID/Username.")
        return
        
    db.unban_user(user.id)
    await event.reply(f"🕊️ **MAAFI GRANTED!**\n\nUser: [{user.first_name}](tg://user?id={user.id})\nStatus: **UNBANNED** ✅\n\n_Play nice now!_")
    await logger.log("UNBAN", user.first_name, user.id, "Unbanned User", f"By: {event.sender_id}")

@bot.on(events.NewMessage(pattern=r'^/banned$'))
async def banned_cmd(event):
    if not await check_admin(event.sender_id): return
    
    banned = await db.get_banned_users()
    if not banned:
        await event.reply("📜 **No one is banned.** (Yet...)")
        return
        
    text = "🚫 **BANNED USERS**\n\n"
    for uid in banned:
        text += f"• `{uid}`\n"
    await event.reply(text)

@bot.on(events.NewMessage(pattern=r'^/rmadmin(?: (\d+|@\w+))?$'))
async def rmadmin_cmd(event):
    if not await check_admin(event.sender_id): return
    
    user = None
    if event.is_reply:
         user = (await event.get_reply_message()).sender
    elif event.pattern_match.group(1):
        try:
            user = await bot.get_entity(event.pattern_match.group(1))
        except: pass
        
    if not user:
        await event.reply("❌ Reply to user or provide ID/Username.")
        return
        
    uid = user.id
    if uid in config.get('admin_ids', []):
        config['admin_ids'].remove(uid)
        await save_config(config)
        
        msg = await event.reply("⏳ Removing admin...")
        await asyncio.sleep(0.5)
        
        text = f"""✅ **Admin removed!**

User: [{user.first_name}](tg://user?id={uid}) (`{uid}`)
Removed by: [{event.sender.first_name}](tg://user?id={event.sender_id})

Remaining admins: {len(config['admin_ids'])}"""
        await msg.edit(text)
        await logger.log("ADMIN", user.first_name, uid, "Removed admin", f"By: {event.sender_id}")
    else:
        await event.reply("⚠️ User is not an admin.")

@bot.on(events.NewMessage(pattern=r'^/setinterval (\d+)(?:[ -](\d+))?$'))
async def set_interval_cmd(event):
    if not await check_admin(event.sender_id): return
    
    min_val = int(event.pattern_match.group(1))
    max_val = event.pattern_match.group(2)
    msg = await event.reply("⚙️ Updating interval...")
    
    if max_val:
        # Random
        event_manager.start_event(min_val, max_val, loop=True)
        text = f"""✅ **Random Interval set!**

Range: {min_val} - {max_val} messages
Next reward in: 🤫 messages (Randomized)"""
        
    else:
        # Fixed
        event_manager.set_fixed(min_val, loop=True)
        text = f"""✅ **Fixed Interval set!**

Count: {min_val} messages"""
        
    await msg.edit(text)
    await logger.log("CONFIG", event.sender.first_name, event.sender_id, f"Set interval: {min_val}-{max_val if max_val else ''}")

@bot.on(events.NewMessage(pattern=r'^/next$'))
async def next_cmd(event):
    if not await check_admin(event.sender_id): return
    
    remaining = event_manager.get_remaining()
    if remaining is None:
        await event.reply("❌ **Event is not running.**")
        return
        
    try:
        # Try sending in DM first
        await bot.send_message(event.sender_id, f"🕵️ **Admin Intel**\n\nNext reward in: `{remaining}` messages")
        if not event.is_private:
            await event.reply("✅ Sent to DM.")
    except:
        # Fallback to group if DM fails
        await event.reply(f"🕵️ **Next Reward:** `{remaining}` messages")

# --- TIER SYSTEM ADMIN COMMANDS ---



@bot.on(events.NewMessage(pattern=r'^/tierinfo$'))
async def tierinfo_cmd(event):
    if not await check_admin(event.sender_id): return
    
    conf = config.get('reward_settings', {})
    tiers = conf.get('tiers', {})
    
    if not tiers.get('enabled', False):
        await event.reply("⚠️ Tier system is currently **DISABLED**.")
        return
        
    text = f"""🏆 **TIER SYSTEM INFO**
    
**Configuration:**
"""
    for t in ['bronze', 'silver', 'gold', 'platinum']:
        data = tiers.get(t, {})
        rng = data.get('range', [0,0])
        r_str = f"{rng[0]}-{rng[1]}" if rng[1] < 999999 else f"{rng[0]}+"
        mult = data.get('multiplier', 1.0)
        text += f"• **{t.title()}**: {r_str} msgs → {mult}x\n"
        
    text += "\n**Admin Options:**"
    
    buttons = [
        [Button.inline("✏️ Edit Tiers", b"rw_tiers:menu"), Button.inline("📊 View Stats", b"tier_stats:view")],
        [Button.inline("❌ Close", b"close:menu")]
    ]
    await event.reply(text, buttons=buttons)


@bot.on(events.NewMessage(pattern=r'^/resettiers$'))
async def resettiers_cmd(event):
    # Owner only
    if event.sender_id != config.get('owner_id', OWNER_ID): return
    
    msg = await event.reply("⚠️ **Resetting ALL Daily Stats...**")
    await asyncio.sleep(1.0)
    
    db.reset_daily_stats()
    
    await msg.edit("""✅ **TIERS RESET**
    
All daily message counts have been reset to 0.
All users are now back to 0 messages.
Rewards calculated from base again.""")
    
    await logger.log("ADMIN", event.sender.first_name, event.sender_id, "RESET ALL DAILY STATS")

@bot.on(events.NewMessage(pattern=r'^/setreward(?: (.+))?$'))
async def set_reward_cmd(event):
    if not await check_admin(event.sender_id): return
    
    args = event.pattern_match.group(1)
    conf = config.get('reward_settings', {})
    
    if not args or args == "show":
        # Interactive Menu
        text, buttons = get_reward_settings_menu(conf, event.sender_id)
        msg = await event.reply("📊 Loading reward settings...")
        await edit_with_delay(msg, text)
        await msg.edit(text, buttons=buttons)
        return

    # Helper to save and log
    async def save():
        config['reward_settings'] = conf
        await save_config(config)

    # Sub-commands
    parts = args.split()
    args_list = parts # Fix for NameError
    cmd = parts[0].lower()
    
    # 1. Base (Fixed/Range) - /setreward 5 or /setreward 3-8
    if cmd.replace('-', '').isdigit():
        msg = await event.reply("⚙️ Updating base reward...")
        
        base = conf.get('base', {})
        old_val = f"{base.get('min')}-{base.get('max')}" if base.get('mode') == 'random' else f"{base.get('amount')}"
        old_mode = "Random Range" if base.get('mode') == 'random' else "Fixed Amount"

        if '-' in cmd:
            mn, mx = map(int, cmd.split('-'))
            conf['base'] = {'mode': 'random', 'min': mn, 'max': mx, 'amount': f"{mn}-{mx}"}
            mode_str = "Random Range"
            val_str = f"{mn}-{mx}"
            details = f"Example Rewards (if rolled {mn+2}):\n• Bronze user: {mn+2} stocks\n• Silver user: {int((mn+2)*1.5)} stocks (1.5x)"
        else:
            val = int(cmd)
            conf['base'] = {'mode': 'fixed', 'amount': val}
            mode_str = "Fixed Amount"
            val_str = f"{val}"
            details = f"Example Rewards:\n• Bronze user: {val} stocks\n• Silver user: {int(val*1.5)} stocks (1.5x)"
            
        await save()
        
        # Frame 2
        tiers_enabled = conf.get('tiers', {}).get('enabled', False)
        jackpot_enabled = conf.get('jackpot', {}).get('enabled', False)
        jackpot_chance = conf.get('jackpot', {}).get('chance', 0)
        jackpot_amount = conf.get('jackpot', {}).get('amount', 0)
        
        text = f"""✅ **Base reward updated!**

Mode: {mode_str}
Base: {val_str} stocks

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
**Active Modifiers:**
✅ Tiers: {"Enabled" if tiers_enabled else "Disabled"}
✅ Jackpot: {"Enabled" if jackpot_enabled else "Disabled"} {f"({jackpot_chance}% chance for {jackpot_amount} stocks)" if jackpot_enabled else ""}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━

{details}

Previous: {old_val} ({old_mode})"""

        await edit_with_delay(msg, text)
        await logger.log_config("Base reward", f"Mode: {mode_str}\nOld: {old_val}\nNew: {val_str} stocks", event.sender.first_name, event.sender_id)
        return

    # 2. Tiered System - /setreward tiered on/off
    if cmd == 'tiered':
        if len(parts) < 2: return
        state = (parts[1].lower() == 'on')
        
        msg = await event.reply("⚙️ Toggling tier system...")
        
        conf.setdefault('tiers', {})['enabled'] = state
        await save()
        
        if state:
            text = f"""✅ **Tier system enabled!**

🥉 Bronze: 1-50 msgs → 1.0x multiplier
🥈 Silver: 51-100 msgs → 1.5x multiplier
🥇 Gold: 101-200 msgs → 2.0x multiplier
💎 Platinum: 201+ msgs → 2.5x multiplier

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Current Base: {conf.get('base', {}).get('amount', 5)} stocks"""
        else:
            text = """⚠️ **Tier system disabled!**

All users receive same base reward.
No activity-based multipliers."""
            
        await edit_with_delay(msg, text)
        await logger.log_config("Tier system", f"Status: {'Enabled' if state else 'Disabled'}", event.sender.first_name, event.sender_id)
        return

    # 3. Configure Tier - /setreward tier <name> <val> OR /setreward tier <name> range <min>-<max>
    if cmd == 'tier':
        if len(parts) < 3: return
        
        tier = parts[1].lower()
        if tier not in ['bronze', 'silver', 'gold', 'platinum']: return
        
        msg = await event.reply(f"⚙️ Updating {tier} tier...")
        
        if parts[2].lower() == 'range':
            # Range update
            try:
                mn, mx = parts[3].split('-')
                if mx == '+': mx = 999999
                else: mx = int(mx)
                mn = int(mn)
                
                old_range = conf['tiers'].get(tier, {}).get('range', [0,0])
                old_range_str = f"{old_range[0]}-{old_range[1]}"
                
                conf.setdefault('tiers', {}).setdefault(tier, {})['range'] = [mn, mx]
                await save()
                
                text = f"""✅ **Tier range updated!**

{tier.title()} Tier
Messages: {old_range_str} → {mn}-{parts[3]}

More users can now reach {tier.title()} tier!"""
                await edit_with_delay(msg, text)
                await logger.log_config(f"{tier.title()} tier range", f"Old: {old_range_str}\nNew: {mn}-{parts[3]}", event.sender.first_name, event.sender_id)
            except: pass
        else:
            # Multiplier update
            try:
                mult = float(parts[2])
                old_mult = conf['tiers'].get(tier, {}).get('multiplier', 1.0)
                
                conf.setdefault('tiers', {}).setdefault(tier, {})['multiplier'] = mult
                config['reward_settings'] = conf # Keep global config and sub-conf synced
                await save_config(config)
                
                text = f"""✅ **{tier.title()} Multiplier Updated**

Multiplier: {old_mult}x → {mult}x"""
                await edit_with_delay(msg, text)
                await logger.log_config(f"{tier.title()} tier multiplier", f"Old: {old_mult}x\nNew: {mult}x", event.sender.first_name, event.sender_id)
            except: pass

    # Check for 'msgs'
    if 'msgs' in args_list:
        try:
            val_idx = args_list.index('msgs') + 1
            if val_idx < len(args_list):
                val_str = args_list[val_idx]
                parts = val_str.split('-')
                min_v = int(parts[0])
                # If only one value provided, treat as min
                if len(parts) > 1:
                    max_v = int(parts[1])
                else:
                    # Provide huge max for last tier if needed, or previous max? 
                    # Let's assume user provides "min-max" always.
                    max_v = 99999
                
                old_range = conf.get('tiers', {}).get(tier, {}).get('range', [0, 0])
                
                conf.setdefault('tiers', {}).setdefault(tier, {})['range'] = [min_v, max_v]
                config['reward_settings'] = conf # Keep global config and sub-conf synced
                await save_config(config)
                
                text = f"""✅ **{tier.title()} Tier Updated**

Range: {old_range} → [{min_v}, {max_v}]"""
                await edit_with_delay(msg, text)
                await logger.log_config(f"{tier.title()} tier range", f"Old: {old_range}\nNew: [{min_v}, {max_v}]", event.sender.first_name, event.sender_id)
        except Exception as e:
            await msg.edit(f"❌ Error setting range: {e}")
            
    return

@bot.on(events.NewMessage(pattern=r'^/settier(?: (.+))?$'))
async def settier_cmd(event):
    if not await check_admin(event.sender_id): return
    
    args = event.pattern_match.group(1)
    if not args:
        await event.reply("Usage: `/settier <tier> [multiplier <float>] [msgs <min>-<max>]`")
        return
        
    args_list = args.split()
    tier = args_list[0].lower()
    
    if tier not in ['bronze', 'silver', 'gold', 'platinum']:
        await event.reply("❌ Invalid tier. (bronze, silver, gold, platinum)")
        return

    msg = await event.reply(f"⚙️ Updating **{tier.title()}**...")
    
    # Use global config directly
    global config
    r_settings = config.get('reward_settings', {})
    tiers_conf = r_settings.get('tiers', {})
    
    items_updated = []
    
    # Check for 'multiplier'
    if 'multiplier' in args_list:
        try:
            val_idx = args_list.index('multiplier') + 1
            if val_idx < len(args_list):
                mult = float(args_list[val_idx])
                old_mult = tiers_conf.get(tier, {}).get('multiplier', 1.0)
                
                tiers_conf.setdefault(tier, {})['multiplier'] = mult
                items_updated.append(f"Multiplier: `{old_mult}x` → `{mult}x`")
            else:
                await msg.edit("❌ Missing value for multiplier.")
                return
        except Exception as e:
            await msg.edit(f"❌ Error setting multiplier: {e}")
            return

    # Check for 'msgs'
    if 'msgs' in args_list:
        try:
            val_idx = args_list.index('msgs') + 1
            if val_idx < len(args_list):
                val_str = args_list[val_idx]
                parts = val_str.split('-')
                min_v = int(parts[0])
                max_v = int(parts[1]) if len(parts) > 1 else 99999
                
                old_range = tiers_conf.get(tier, {}).get('range', [0, 0])
                
                tiers_conf.setdefault(tier, {})['range'] = [min_v, max_v]
                items_updated.append(f"Msgs: `{old_range[0]}-{old_range[1]}` → `{min_v}-{max_v}`")
            else:
                await msg.edit("❌ Missing value for msgs.")
                return
        except Exception as e:
            await msg.edit(f"❌ Error setting range: {e}")
            return

    if items_updated:
        # Re-assign to ensure hierarchy is preserved in some dictionary implementations
        r_settings['tiers'] = tiers_conf
        config['reward_settings'] = r_settings
        
        await save_config(config)
        text = f"✅ **{tier.title()} Updated**\n\n" + "\n".join(items_updated)
        await msg.edit(text)
        await logger.log_config(f"{tier.title()} Tier Update", "\n".join(items_updated), event.sender.first_name, event.sender_id)
    else:
        await msg.edit("⚠️ No changes made. Check syntax.\nUsage: `/settier <tier> [multiplier <float>] [msgs <min>-<max>]`")



@bot.on(events.NewMessage(pattern=r'^/setspam(?: (.+))?$'))
async def setspam_cmd(event):
    if not await check_admin(event.sender_id): return
    
    args = event.pattern_match.group(1)
    
    # Load settings
    spam_settings = await db.get_anti_spam_settings()
    
    if not args or args.lower() == 'show':
        text = get_spam_settings_menu(spam_settings)
        await event.reply(text)
        return
        
    parts = args.split()
    if len(parts) < 2:
        await event.reply("❌ Usage: `/setspam <key> <value>`\nKeys: `threshold_seconds`, `ignore_duration_minutes`, `burst_limit`, `burst_window_seconds`, `global_flood_limit`, `global_flood_window`, `raid_limit`")
        return
        
    key = parts[0].lower()
    val_str = parts[1]
    
    # Valid keys and their types
    valid_keys = {
        'threshold_seconds': int,
        'ignore_duration_minutes': int,
        'burst_limit': int,
        'burst_window_seconds': int,
        'global_flood_limit': int,
        'global_flood_window': int,
        'raid_limit': int
    }
    
    if key not in valid_keys:
        await event.reply(f"❌ Invalid key. Valid keys: {', '.join(valid_keys.keys())}")
        return
        
    try:
        val = valid_keys[key](val_str)
        old_val = spam_settings.get(key, "Not set")
        spam_settings[key] = val
        
        # Save to DB
        await db.save_anti_spam_settings(spam_settings)
        
        # Update in-memory for immediate effect
        config['spam_settings'] = spam_settings
        spam_detector.update_config(config)
        
        await event.reply(f"✅ **Spam Setting Updated!**\n\n**Key:** `{key}`\n**Old:** `{old_val}`\n**New:** `{val}`")
        await logger.log_config(f"Spam Setting: {key}", f"Old: {old_val}\nNew: {val}", event.sender.first_name, event.sender_id)
        
    except ValueError:
        await event.reply(f"❌ Invalid value for `{key}`. Expected `{valid_keys[key].__name__}`.")
    except Exception as e:
        await event.reply(f"❌ Error updating setting: {e}")

def get_spam_settings_menu(settings):
    text = f"""🛡️ **ANTI-SPAM SETTINGS**

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📌 **THRESHOLD & DURATIONS**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
• `threshold_seconds`: {settings.get('threshold_seconds', 5)}s (Min delay between msgs)
• `ignore_duration_minutes`: {settings.get('ignore_duration_minutes', 30)}m (Mute duration)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🚀 **BURST LIMITS**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
• `burst_limit`: {settings.get('burst_limit', 5)} msgs
• `burst_window_seconds`: {settings.get('burst_window_seconds', 10)}s

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🌊 **FLOOD PROTECTION**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
• `global_flood_limit`: {settings.get('global_flood_limit', 20)} msgs
• `global_flood_window`: {settings.get('global_flood_window', 3)}s

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🚨 **RAID PROTECTION**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
• `raid_limit`: {settings.get('raid_limit', 5)} joins/window

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Usage: `/setspam <key> <value>`
Example: `/setspam burst_limit 10`
"""
    return text

def get_reward_settings_menu(r_settings, user_id):
    # r_settings is passed directly from set_reward_cmd
    base = r_settings.get('base', {})
    tiers = r_settings.get('tiers', {})
    jackpot = r_settings.get('jackpot', {})
    
    base_amount = base.get('amount', 5)
    base_mode = "Random Range" if base.get('mode') == 'random' else "Fixed Amount"
    
    tier_status = "✅ Enabled" if tiers.get('enabled') else "❌ Disabled"
    jackpot_status = "✅ Enabled" if jackpot.get('enabled') else "❌ Disabled"
    
    template = config.get('reward_cmd_template', '/add stocks {amount}')
    
    text = f"""💰 **REWARD SETTINGS**

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📌 **BASE REWARD**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Mode: {base_mode}
Amount: {base_amount} stocks

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🏆 **TIER SYSTEM**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Status: {tier_status}

🥉 Bronze: {tiers.get('bronze',{}).get('range',[0,0])[0]}-{tiers.get('bronze',{}).get('range',[0,0])[1]} msgs → {tiers.get('bronze',{}).get('multiplier',1.0)}x
🥈 Silver: {tiers.get('silver',{}).get('range',[0,0])[0]}-{tiers.get('silver',{}).get('range',[0,0])[1]} msgs → {tiers.get('silver',{}).get('multiplier',1.5)}x
🥇 Gold: {tiers.get('gold',{}).get('range',[0,0])[0]}-{tiers.get('gold',{}).get('range',[0,0])[1]} msgs → {tiers.get('gold',{}).get('multiplier',2.0)}x
💎 Platinum: {tiers.get('platinum',{}).get('range',[0,0])[0]}+ msgs → {tiers.get('platinum',{}).get('multiplier',2.5)}x

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🎰 **JACKPOT SYSTEM**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Status: {jackpot_status}
Chance: {jackpot.get('chance')}%
Amount: {jackpot.get('amount')} stocks

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⚙️ **COMMAND FORMAT**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Template: `{template}`

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Last changed: Today
Changed by: {OWNER_FIRST_NAME}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━"""

    buttons = [
        [Button.inline("✏️ Edit Base", encode_data("rw_base", user_id)), Button.inline("🏆 Edit Tiers", encode_data("rw_tiers", user_id))],
        [Button.inline("🎰 Edit Jackpot", encode_data("rw_jackpot", user_id))],
        [Button.inline("❌ Close", encode_data("close", user_id))]
    ]
    return text, buttons

@bot.on(events.CallbackQuery(pattern=b'rw_base:(.*)'))
async def rw_base_cb(event):
    action, uid = decode_data(event.data)
    if event.sender_id != uid: return
    
    text = """💰 **BASE REWARD SETTINGS**

Current: See main menu

**Commands:**
`/setreward 5` - Fixed amount
`/setreward 3-8` - Random range"""
    
    await event.edit(text, buttons=[[Button.inline("⬅️ Back", encode_data("rw_menu", uid))]])

@bot.on(events.CallbackQuery(pattern=b'rw_tiers:(.*)'))
async def rw_tiers_cb(event):
    action, uid = decode_data(event.data)
    if event.sender_id != uid: return
    
    text = """🏆 **TIER SYSTEM SETTINGS**

**Commands:**
`/settier bronze multiplier 1.5`
`/settier bronze msgs 1-50`
`/settier silver multiplier 2.0`
`/settier silver msgs 51-100`

**Tiers:** Bronze, Silver, Gold, Platinum"""
    
    await event.edit(text, buttons=[[Button.inline("⬅️ Back", encode_data("rw_menu", uid))]])

@bot.on(events.CallbackQuery(pattern=b'rw_jackpot:(.*)'))
async def rw_jackpot_cb(event):
    action, uid = decode_data(event.data)
    if event.sender_id != uid: return
    
    text = """🎰 **JACKPOT SYSTEM SETTINGS**

**Commands:**
`/setreward jackpot on/off` - Toggle
`/setreward jackpot set 10% 50` - Configure"""
    
    await event.edit(text, buttons=[[Button.inline("⬅️ Back", encode_data("rw_menu", uid))]])

@bot.on(events.CallbackQuery(pattern=b'rw_menu:(.*)'))
async def rw_menu_cb(event):
    action, uid = decode_data(event.data)
    if event.sender_id != uid: return
    
    conf = config.get('reward_settings', {})
    text, buttons = get_reward_settings_menu(conf, uid)
    await event.edit(text, buttons=buttons)

@bot.on(events.CallbackQuery(pattern=b'close:(.*)'))
async def close_cb(event):
    await event.delete()


async def save_reward_config(new_conf):
    config['reward_settings'] = new_conf
    await save_config(config)

def get_reward_settings_text(conf):
    base = conf['base']
    base_val = base['amount'] if base['mode'] == 'fixed' else f"{base['min']}-{base['max']}"
    
    tiers = conf['tiers']
    jackpot = conf['jackpot']
    
    # Dynamic Tier Display
    tier_text = ""
    # Collect and sort
    tier_display = []
    for name, data in tiers.items():
        if name == 'enabled' or not isinstance(data, dict): continue
        if 'range' in data:
            tier_display.append({
                'name': name.title(),
                'min': data['range'][0],
                'max': data['range'][1],
                'multiplier': data.get('multiplier', 1.0)
            })
    tier_display.sort(key=lambda x: x['min'])
    
    icons = {'Bronze': '🥉', 'Silver': '🥈', 'Gold': '🥇', 'Platinum': '💎', 'Diamond': '💠', 'Master': '👑', 'Legend': '🦄'}
    
    for t in tier_display:
        icon = icons.get(t['name'], '▪️')
        r_max = f"{t['max']}" if t['max'] < 999999 else "+"
        tier_text += f"{icon} {t['name']}: {t['min']}-{r_max} → {t['multiplier']}x\n"

    return f"""💰 **REWARD SETTINGS**

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📌 **BASE REWARD**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Mode: {base['mode'].title()}
Amount: {base_val} stocks

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🏆 **TIER SYSTEM**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Status: {"✅ Enabled" if tiers['enabled'] else "❌ Disabled"}

{tier_text}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🎰 **JACKPOT SYSTEM**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Status: {"✅ Enabled" if jackpot['enabled'] else "❌ Disabled"}
Chance: {jackpot['chance']}%
Amount: {jackpot['amount']} stocks

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⚙️ **COMMAND FORMAT**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Template: `{conf['command_template']}`"""

@bot.on(events.NewMessage(pattern=r'^/whitelist (\d+|@\w+)$'))
async def whitelist_cmd(event):
    if not await check_admin(event.sender_id): return
    
    try:
        user = await bot.get_entity(event.pattern_match.group(1))
        db.whitelist_user(user.id)
        await event.reply(f"✅ **Bypassing Spam Checks!**\n\nUser: [{user.first_name}](tg://user?id={user.id})")
        await logger.log("ADMIN", event.sender.first_name, event.sender_id, f"Whitelisted {user.id}")
    except:
        await event.reply("❌ User not found.")

@bot.on(events.NewMessage(pattern=r'^/rmwhitelist (\d+|@\w+)$'))
async def rmwhitelist_cmd(event):
    if not await check_admin(event.sender_id): return
    
    try:
        user = await bot.get_entity(event.pattern_match.group(1))
        db.unwhitelist_user(user.id)
        await event.reply(f"🚫 **Removed from Whitelist!**\n\nUser: [{user.first_name}](tg://user?id={user.id})")
        await logger.log("ADMIN", event.sender.first_name, event.sender_id, f"Un-whitelisted {user.id}")
    except:
        await event.reply("❌ User not found.")

@bot.on(events.NewMessage(pattern=r'^/wlist$'))
async def wlist_cmd(event):
    if not await check_admin(event.sender_id): return
    
    wlist = await db.get_whitelisted_users()
    if not wlist:
        await event.reply("📜 **Whitelist is empty.**")
        return
        
    text = "📜 **Whitelisted Users**\n\n"
    for uid in wlist:
        text += f"• `{uid}`\n"
    await event.reply(text)

@bot.on(events.NewMessage(pattern=r'^/antispam (on|off)$'))
async def antispam_cmd(event):
    if not await check_admin(event.sender_id): return
    
    action = event.pattern_match.group(1).lower()
    enabled = (action == "on")
    
    config['antispam_enabled'] = enabled
    await save_config(config)
    spam_detector.toggle(enabled)
    
    # Send Frame 1
    msg = await event.reply("⚙️ Toggling anti-spam...")
    await asyncio.sleep(0.5)
    
    s = config.get('spam_settings', {})
    
    # Frame 2
    if enabled:
        text = f"""🚫 **Anti-Spam: ON** ✅

Protections active:
✅ Burst limit ({s.get('burst_limit', 5)}/{s.get('burst_window_seconds', 10)}s)
✅ Flood limit ({s.get('global_flood_limit', 20)}/{s.get('global_flood_window', 3)}s)
✅ Duplicate detection
✅ Low-quality detection

Penalty: {s.get('ignore_duration_minutes', 30)} minutes
Public warnings: Enabled"""
        
        log_text = f"Changed: Anti-spam status\nOld: OFF\nNew: ON"
    else:
        text = """⚠️ **Anti-Spam: OFF**

All spam checks disabled.
Users can send unlimited messages.

⚠️ **Not recommended for public groups!**"""
        
        log_text = f"Changed: Anti-spam status\nOld: ON\nNew: OFF"

    await msg.edit(text)
    await logger.log("CONFIG", event.sender.first_name, event.sender_id, log_text)

# --- SPAM COMMANDS ---

@bot.on(events.NewMessage(pattern=r'^/spamtypes (enable|disable|list) ?(\w+)?$'))
async def spamtypes_cmd(event):
    if not await check_admin(event.sender_id): return
    
    action = event.pattern_match.group(1).lower()
    stype = event.pattern_match.group(2)
    
    if action == "list":
        text = "⚙️ **Spam Detection Types**\n\n"
        for t, enabled in spam_detector.types.items():
            status = "✅ Enabled" if enabled else "❌ Disabled"
            text += f"• **{t.title()}**: {status}\n"
        await event.reply(text)
        return

    if not stype or stype not in spam_detector.types:
        await event.reply(f"❌ Invalid type. Valid: {', '.join(spam_detector.types.keys())}")
        return
        
    new_state = (action == "enable")
    spam_detector.set_type_state(stype, new_state)
    
    s = config.get('spam_settings', {})
    if 'types' not in s: s['types'] = {}
    s['types'][stype] = new_state
    config['spam_settings'] = s
    await save_config(config)
    
    msg = await event.reply("⚙️ Updating spam detection...")
    await asyncio.sleep(0.5)
    
    text = f"""✅ **Spam type updated!**

Type: {stype.title()} detection
Status: {"Enabled" if new_state else "Disabled"}

{get_type_desc(stype)}"""

    await msg.edit(text)
    await logger.log("CONFIG", event.sender.first_name, event.sender_id, f"Changed: Spam type {stype} -> {new_state}")

def get_type_desc(t):
    descs = {
        'burst': "Per-user message rate limit.",
        'flood': "Global group traffic limit.",
        'duplicate': "Detects similar messages (85% match).",
        'lowquality': "Detects keysmash/repeated chars.",
        'stickers': "Prevents sticker/GIF spam."
    }
    return descs.get(t, "")

@bot.on(events.NewMessage(pattern=r'^/spamconfig (\w+) (\w+) (.+)$'))
async def spamconfig_cmd(event):
    if not await check_admin(event.sender_id): return
    
    stype = event.pattern_match.group(1).lower()
    setting = event.pattern_match.group(2).lower()
    value = event.pattern_match.group(3)
    
    key_map = {
        'duplicate': {'threshold': 'duplicate_threshold'},
        'stickers': {'limit': 'media_limit', 'window': 'media_window'},
        'burst': {'limit': 'burst_limit', 'window': 'burst_window_seconds'},
        'flood': {'limit': 'global_flood_limit', 'window': 'global_flood_window'},
        'penalty': {'duration': 'ignore_duration_minutes', 'pause': 'global_flood_pause_seconds'}
    }
    
    config_key = key_map.get(stype, {}).get(setting)
    
    if not config_key:
        valid_opts = []
        for t, m in key_map.items():
            for k in m: valid_opts.append(f"{t} {k}")
        await event.reply(f"❌ Invalid setting. Valid options:\n" + "\n".join(valid_opts))
        return
        
    s = config.get('spam_settings', {})
    old_val = s.get(config_key, "Default")
    
    try:
        if '.' in value: new_val = float(value)
        else: new_val = int(value)
    except:
        await event.reply("❌ Value must be a number.")
        return
        
    s[config_key] = new_val
    config['spam_settings'] = s
    await save_config(config)
    spam_detector.update_config(config)
    
    msg = await event.reply("⚙️ Updating configure...")
    await asyncio.sleep(0.5)
    
    text = f"""✅ **Spam config updated!**

Setting: {stype} {setting}
Old: {old_val}
New: {new_val}"""

    await msg.edit(text)
    await logger.log("CONFIG", event.sender.first_name, event.sender_id, f"Config: {config_key} {old_val} -> {new_val}")

@bot.on(events.NewMessage(pattern=r'^/setlimit (burst|flood) (\d+) (\d+)$'))
async def setlimit_cmd(event):
    if not await check_admin(event.sender_id): return
    
    limit_type = event.pattern_match.group(1).lower()
    count = int(event.pattern_match.group(2))
    seconds = int(event.pattern_match.group(3))
    
    s = config.get('spam_settings', {})
    
    msg = await event.reply(f"⚙️ Updating {limit_type} limit...")
    await asyncio.sleep(0.5)
    
    if limit_type == "burst":
        old_c = s.get('burst_limit', 5)
        old_s = s.get('burst_window_seconds', 10)
        
        s['burst_limit'] = count
        s['burst_window_seconds'] = seconds
        
        text = f"""✅ **Burst limit updated!**

Old: {old_c} messages in {old_s} seconds
New: {count} messages in {seconds} seconds

Per-user rate limit changed.
Users can send more messages now."""
        
        log_text = f"Changed: Burst limit\nOld: {old_c} msgs / {old_s}s\nNew: {count} msgs / {seconds}s"
        
    else: # flood
        old_c = s.get('global_flood_limit', 20)
        old_s = s.get('global_flood_window', 3)
        
        s['global_flood_limit'] = count
        s['global_flood_window'] = seconds
        
        text = f"""✅ **Flood limit updated!**

Old: {old_c} messages in {old_s} seconds
New: {count} messages in {seconds} seconds

Global rate limit changed.
Applies to ALL users combined."""
        
        log_text = f"Changed: Flood limit\nOld: {old_c} msgs / {old_s}s\nNew: {count} msgs / {seconds}s"
    
    config['spam_settings'] = s
    await save_config(config)
    spam_detector.update_config(config)
    
    await msg.edit(text)
    await logger.log("CONFIG", event.sender.first_name, event.sender_id, log_text)

@bot.on(events.NewMessage(pattern=r'^/setpenalty (global )?(\d+)$'))
async def setpenalty_cmd(event):
    if not await check_admin(event.sender_id): return
    
    is_global = event.pattern_match.group(1) == "global "
    value = int(event.pattern_match.group(2))
    s = config.get('spam_settings', {})
    
    msg = await event.reply(f"⚙️ Updating penalty duration...")
    await asyncio.sleep(0.5)

    if is_global:
        old = s.get('global_flood_pause_seconds', 60)
        s['global_flood_pause_seconds'] = value
        
        text = f"""✅ **Global Pause Duration updated!**

Old: {old} seconds
New: {value} seconds

Bot will pause for {value}s during raid."""
        
        log_text = f"Changed: Global Pause\nOld: {old}s\nNew: {value}s"
        
    else:
        old = s.get('ignore_duration_minutes', 30)
        s['ignore_duration_minutes'] = value
        
        text = f"""✅ **Penalty duration updated!**

Old: {old} minutes
New: {value} minutes

Spam violators will be ignored for {value} minutes."""
        
        log_text = f"Changed: Penalty duration\nOld: {old} minutes\nNew: {value} minutes"
    
    config['spam_settings'] = s
    await save_config(config)
    spam_detector.update_config(config)
    
    await msg.edit(text)
    await logger.log("CONFIG", event.sender.first_name, event.sender_id, log_text)

@bot.on(events.NewMessage(pattern=r'^/resetviolations (?:@?(\w+)|(\d+))?$'))
async def resetviolations_cmd(event):
    if not await check_admin(event.sender_id): return
    
    # Handle reply
    input_arg = event.pattern_match.group(1) or event.pattern_match.group(2)
    user_id = None
    user = None
    
    if event.is_reply:
        reply = await event.get_reply_message()
        user_id = reply.sender_id
        user = reply.sender
    elif input_arg:
        try:
            user = await bot.get_entity(input_arg)
            user_id = user.id
        except: pass
        
    if not user_id:
        await event.reply("❌ Reply to a user or provide username/ID.")
        return

    msg = await event.reply("⏳ Resetting violations...")
    await asyncio.sleep(0.5)
    
    prev_violations = await db.get_violation_level_v2(user_id)
    await db.reset_violations_v2(user_id)
    
    # Use user object for name if available, else ID
    name = get_mention(user) if user else f"`{user_id}`"
    
    text = f"""✅ **Violations reset!**

User: {name}
Previous: {prev_violations}
Reset to: 0

Next penalty will be 1st offense.
Current penalty (if active) unchecked."""

    await msg.edit(text)
    await logger.log("ADMIN", event.sender.first_name, event.sender_id, f"Violations reset for {user_id}")

@bot.on(events.NewMessage(pattern=r'^/clear (?:@?(\w+)|(\d+))?$'))
async def clear_cmd(event):
    if not await check_admin(event.sender_id): return

    input_arg = event.pattern_match.group(1) or event.pattern_match.group(2)
    user_id = None
    user = None
    
    if event.is_reply:
        reply = await event.get_reply_message()
        user_id = reply.sender_id
        user = reply.sender
    elif input_arg:
        try:
            user = await bot.get_entity(input_arg)
            user_id = user.id
        except: pass
        
    if not user_id:
        await event.reply("❌ Reply to a user or provide username/ID.")
        return

    await db.remove_penalty_v2(user_id)
    spam_detector.reset_history(str(user_id)) # Also clear memory
    
    name = get_mention(user) if user else f"`{user_id}`"
    await event.reply(f"✅ **Penalty Removed**\n\nUser: {name}\nActive penalty lifted.")
    await logger.log("ADMIN", event.sender.first_name, event.sender_id, f"Cleared penalty for {user_id}")

@bot.on(events.NewMessage(pattern=r'^/(addadmin|rmadmin)(?: (?:@?(\w+)|(\d+)))?$'))
async def admin_mgmt_cmd(event):
    if not await check_admin(event.sender_id): return
    
    cmd = event.pattern_match.group(1).lower()
    input_arg = event.pattern_match.group(2) or event.pattern_match.group(3)
    

    # Owner only for add/remove
    if event.sender_id != config.get('owner_id', OWNER_ID):
        await event.reply("❌ Owner only command.")
        return
        
    user_id = None
    if event.is_reply:
        reply = await event.get_reply_message()
        user_id = reply.sender_id
    elif input_arg:
        # Try to resolve user
        try:
            if input_arg.isdigit():
                user_id = int(input_arg)
                # Try to get entity for logging, but accept ID even if lookup fails
                try:
                    user = await bot.get_entity(user_id)
                except:
                    user = None
            else:
                user = await bot.get_entity(input_arg)
                user_id = user.id
        except Exception as e:
            print(f"Error resolving user: {e}")
            pass
            
    if not user_id:
        await event.reply("❌ User not found.")
        return
        
    if cmd == "addadmin":
        if db.is_admin(user_id):
            await event.reply("⚠️ Already an admin.")
            return
        await db.add_admin(user_id)
        await event.reply(f"✅ **Admin Added:** `{user_id}`")
        await logger.log("ADMIN", event.sender.first_name, event.sender_id, f"Added admin {user_id}")
        
    elif cmd == "rmadmin":
        if not db.is_admin(user_id):
            await event.reply("⚠️ Not an admin.")
            return
        await db.remove_admin(user_id)
        await event.reply(f"🗑️ **Admin Removed:** `{user_id}`")
        await logger.log("ADMIN", event.sender.first_name, event.sender_id, f"Removed admin {user_id}")

@bot.on(events.NewMessage(pattern=r'^/penalty (?:@?(\w+)|(\d+))(?:\s+(\d+))?$'))
async def penalty_cmd(event):
    if not await check_admin(event.sender_id): return
    
    args = event.pattern_match.group(1) or event.pattern_match.group(2)
    duration = event.pattern_match.group(3)
    
    if not args:
        await event.reply("Usage: `/penalty <user> [minutes]`")
        return

    try:
        user = await bot.get_entity(args)
    except:
        await event.reply("❌ User not found.")
        return
        
    minutes = int(duration) if duration else config.get('spam_settings', {}).get('ignore_duration_minutes', 30)
    
    # Apply penalty
    spam_detector.ignored_users[user.id] = time.time() + (minutes * 60)
    
    await event.reply(f"🚫 **PENALTY APPLIED**\n\nUser: [{user.first_name}](tg://user?id={user.id})\nDuration: {minutes} minutes")
    await logger.log("ADMIN", event.sender.first_name, event.sender_id, f"Penalized {user.id} for {minutes}m")

@bot.on(events.NewMessage(pattern=r'^/unpenalty (?:@?(\w+)|(\d+))$'))
async def unpenalty_cmd(event):
    if not await check_admin(event.sender_id): return
    
    args = event.pattern_match.group(1) or event.pattern_match.group(2)
    try:
        user = await bot.get_entity(args)
    except:
        await event.reply("❌ User not found.")
        return
        
    spam_detector.reset_history(user.id)
    
    await event.reply(f"✅ **PENALTY REMOVED**\n\nUser: [{user.first_name}](tg://user?id={user.id})")
    await logger.log("ADMIN", event.sender.first_name, event.sender_id, f"Un-penalized {user.id}")

@bot.on(events.NewMessage(pattern=r'^/unpause$'))
async def unpause_cmd(event):
    if not await check_admin(event.sender_id): return
    
    event_manager.unpause()
    spam_detector.reset_global()
    
    await event.reply("✅ **Global Paused LIFTED**\n\nResuming reward processing immediately.")
    await logger.log("ADMIN", event.sender.first_name, event.sender_id, "Manually UNPAUSED global flood")

@bot.on(events.NewMessage(pattern=r'^/adminhelp$'))
async def admin_help_cmd(event):
    if not await check_admin(event.sender_id): return
    
    text = """🛡️ **Admin Commands**

**Spam Control**
`/antispam on|off` - Toggle system
`/spamtypes list` - List detection types
`/spamtypes enable|disable <type>`
`/spamconfig <type> <setting> <value>`
`/setlimit burst|flood <count> <seconds>`
`/setpenalty <minutes>` or `global <seconds>`

**User Management**
`/resetviolations <user>` - Reset count
`/clear <user>` - Remove active penalty
`/clearall` - Remove for ALL (Owner)
`/cooldowns` - List active penalties
`/whitelist <user>` - Exempt from spam checks

**Bot Data**
`/admins`, `/addadmin`, `/rmadmin`
`/logs [count]` - View recent actions
`/backup`, `/restore` - Data management
`/announce <msg>` - Broadcast to group"""

    await event.reply(text)

@bot.on(events.NewMessage(pattern=r'^/logs(?: (\d+))?$'))
async def logs_cmd(event):
    if not await check_admin(event.sender_id): return
    count = int(event.pattern_match.group(1) or 10)
    if count > 50: count = 50
    
    logs = await db.get_logs(count)
    if not logs:
        await event.reply("📜 No logs available.")
        return
        
    text = f"📜 **Recent Logs ({count})**\n\n"
    for l in logs:
        text += f"`{l['time']}` - {l['action']}\n"
        
    # Send in DM to avoid spam
    try:
        await bot.send_message(event.sender_id, text)
        await event.reply("✅ Logs sent to DM.")
    except:
        await event.reply("❌ usage: allow headers or start bot in DM.")

@bot.on(events.NewMessage(pattern=r'^/announce (.+)$'))
async def announce_cmd(event):
    if not await check_admin(event.sender_id): return
    msg = event.pattern_match.group(1)
    
    group_id = config.get('target_group_id')
    if not group_id:
        await event.reply("❌ Target group not configured.")
        return
        
    try:
        await bot.send_message(group_id, f"📢 **ANNOUNCEMENT**\n\n{msg}")
        await event.reply("✅ Announcement sent.")
    except Exception as e:
        await event.reply(f"❌ Failed: {e}")

@bot.on(events.NewMessage(pattern=r'^/backup$'))
async def backup_cmd(event):
    if event.sender_id != config.get('owner_id', OWNER_ID): return
    
    data = db.get_backup_data()
    # Save to temp file
    fname = f"backup_{int(time.time())}.json"
    with open(fname, 'w') as f:
        json.dump(data, f, indent=4)
        
    await bot.send_file(event.sender_id, fname, caption="📦 **Bot Backup**")
    import os
    os.remove(fname)

@bot.on(events.NewMessage(pattern=r'^/restore$'))
async def restore_cmd(event):
    if event.sender_id != config.get('owner_id', OWNER_ID): return
    
    if not event.is_reply:
        await event.reply("❌ Reply to a backup JSON file.")
        return
        
    reply = await event.get_reply_message()
    if not reply.document or not reply.file.name.endswith('.json'):
        await event.reply("❌ Invalid file.")
        return
        
    fname = await reply.download_media()
    try:
        with open(fname, 'r') as f:
            data = json.load(f)
            
        if "full_db" in data:
            db.restore_data(data["full_db"])
            await event.reply("✅ **Restored full database!**")
        else:
             # Try partial
             # Just fail if not standard format
             await event.reply("⚠️ Unknown backup format.")
    except Exception as e:
        await event.reply(f"❌ Restore failed: {e}")
        
    import os
    os.remove(fname)

@bot.on(events.NewMessage(pattern=r'^/cooldowns$'))
async def cooldowns_cmd(event):
    if not await check_admin(event.sender_id): return
    
    penalties = await db.get_all_penalties()
    if not penalties:
        await event.reply("✅ **No active penalties.**")
        return
        
    text = f"🛑 **Active Penalties ({len(penalties)})**\n\n"
    now = time.time()
    for data in penalties:
        uid = data['user_id']
        remaining = int((data['expiry'] - now) / 60)
        level = data.get('level', 1)
        text += f"• [{uid}](tg://user?id={uid}) (L{level}): {remaining}m remaining\n"
             
    await event.reply(text)

@bot.on(events.NewMessage(pattern=r'^/recent$'))
async def recent_cmd(event):
    if not await check_admin(event.sender_id): return
    
    recent = await db.get_recent_winners()
    if not recent:
        await event.reply("📜 **No recent winners.**")
        return
        
    text = "🏆 **Recent Winners**\n\n"
    for entry in recent:
        name = entry.get('name', 'Unknown')
        stocks = entry.get('stocks', 0)
        
        raw_time = entry.get('time', 0)
        if isinstance(raw_time, (int, float)):
             # IST is UTC+5:30 = 19800 seconds
             # gmtime is UTC. We want IST.
             # Add offset manually or use datetime
             ts = raw_time + 19800
             when = time.strftime('%H:%M', time.gmtime(ts))
        else:
             # Legacy string "HH:MM IST" -> just use first part or whole
             when = str(raw_time).split(' ')[0] # "13:30" from "13:30 IST"
             
        text += f"• {when} - {name} ({stocks} stocks)\n"
        
    await event.reply(text)

@bot.on(events.NewMessage(pattern=r'^/usage$'))
async def usage_cmd(event):
    if not await check_admin(event.sender_id): return
    
    # Simple stats for now
    stats = await db.get_global_stats()
    total = stats['total_selections']
    distributed = stats['distributed_stocks']
    
    text = f"""📊 **Usage Statistics**
    
Rewards Distributed: {total}
Stocks Given: {distributed}
Current Session: {event_manager.current_msg_count} messages processed
    """
    await event.reply(text)

    await event.reply(text)

    await event.reply(text)

async def resolve_amount_and_user(event):
    """Helper to parse amount and user from command args"""
    args = event.pattern_match.group(1)
    if not args:
        return None, None, "❌ Usage: `<amount> [user]` or reply."
        
    parts = args.strip().split()
    amount = None
    target_arg = None
    
    # helper checks
    def is_int(s): return s.lstrip('-').isdigit()
    
    if len(parts) == 1:
        if is_int(parts[0]):
            amount = int(parts[0])
            if event.is_reply:
                reply = await event.get_reply_message()
                target_arg = reply.sender_id
            else:
                return None, None, "❌ Reply to a user or specify valid identifier."
        else:
            return None, None, "❌ Specify amount (integer)."
            
    else:
        # 2+ parts. Try to find amount.
        if is_int(parts[0]):
            amount = int(parts[0])
            target_arg = parts[1]
        elif is_int(parts[-1]):
            amount = int(parts[-1])
            target_arg = parts[0]
        else:
            return None, None, "❌ Amount not found."
            
    user = None
    if target_arg:
        try:
             # handle int ID passed as arg
             if isinstance(target_arg, int):
                 user = await bot.get_entity(target_arg)
             elif str(target_arg).isdigit():
                 user = await bot.get_entity(int(target_arg))
             else:
                 user = await bot.get_entity(target_arg)
        except:
             return None, None, "❌ User not found."
             
    return amount, user, None

@bot.on(events.NewMessage(pattern=r'^/add(?:\s+(.+))?$'))
async def add_stock_cmd(event):
    if not await check_admin(event.sender_id): return
    
    amount, user, err = await resolve_amount_and_user(event)
    if err:
        await event.reply(err)
        return
        
    await db.add_user_stock(user.id, event.chat_id, amount)
    stats = await db.get_user_stats(user.id, event.chat_id)
    name = user.first_name if hasattr(user, 'first_name') and user.first_name else 'User'
    
    await event.reply(f"✅ Added **{amount}** stocks to [{name}](tg://user?id={user.id}).\nNew Balance: {stats['total_stocks']}")
    await logger.log("ADMIN", event.sender.first_name, event.sender_id, f"Added {amount} stocks to {user.id}")

@bot.on(events.NewMessage(pattern=r'^/remove(?:\s+(.+))?$'))
async def remove_stock_cmd(event):
    if not await check_admin(event.sender_id): return
    
    amount, user, err = await resolve_amount_and_user(event)
    if err:
        await event.reply(err)
        return
        
    await db.add_user_stock(user.id, event.chat_id, -amount)
    stats = await db.get_user_stats(user.id, event.chat_id)
    name = user.first_name if hasattr(user, 'first_name') and user.first_name else 'User'
    
    await event.reply(f"✅ Removed **{amount}** stocks from [{name}](tg://user?id={user.id}).\nNew Balance: {stats['total_stocks']}")
    await logger.log("ADMIN", event.sender.first_name, event.sender_id, f"Removed {amount} stocks from {user.id}")

@bot.on(events.NewMessage(pattern=r'^/addmsg(?:\s+(.+))?$'))
async def add_msg_cmd(event):
    if not await check_admin(event.sender_id): return
    
    amount, user, err = await resolve_amount_and_user(event)
    if err:
        await event.reply(err)
        return

    await db.add_user_message(user.id, amount, event.chat_id)
    stats = await db.get_user_stats(user.id, event.chat_id)
    name = user.first_name if hasattr(user, 'first_name') and user.first_name else 'User'
    
    await event.reply(f"✅ Added **{amount}** messages to [{name}](tg://user?id={user.id}).\nNew Total: {stats['total_msgs']}")
    await logger.log("ADMIN", event.sender.first_name, event.sender_id, f"Added {amount} msgs to {user.id}")

@bot.on(events.NewMessage(pattern=r'^/removemsg(?:\s+(.+))?$'))
async def remove_msg_cmd(event):
    if not await check_admin(event.sender_id): return
    
    amount, user, err = await resolve_amount_and_user(event)
    if err:
        await event.reply(err)
        return

    await db.add_user_message(user.id, -amount, event.chat_id)
    stats = await db.get_user_stats(user.id, event.chat_id)
    name = user.first_name if hasattr(user, 'first_name') and user.first_name else 'User'
    
    await event.reply(f"✅ Removed **{amount}** messages from [{name}](tg://user?id={user.id}).\nNew Total: {stats['total_msgs']}")
    await logger.log("ADMIN", event.sender.first_name, event.sender_id, f"Removed {amount} msgs from {user.id}")

@bot.on(events.NewMessage(pattern=r'^/botstats$'))
async def botstats_cmd(event):
    if not await check_admin(event.sender_id): return
    
    # 1. Global Stats
    global_stats = await db.get_global_stats()
    
    # 2. Daily Stats
    daily_stats_list = await db.get_all_daily_stats(0) # 0 for all
    total_today = sum(d['msgs'] for d in daily_stats_list)
    
    text = f"""📊 **Bot Performance Stats**
    
**Global Activity:**
• Total Selections: `{global_stats['total_selections']}`
• Cumulative Reward: `{global_stats['distributed_stocks']}`
• Tracked Users: `{global_stats['total_tracked_users']}`

**Today's Activity:**
• Total Messages: `{total_today}`
• Active Winners: `{len(daily_stats_list)}`

**System Status:**
• DB Engine: `Motor/MongoDB (Async)`
• Queue Status: `Active (Phase 4)`
"""
    await event.reply(text)

@bot.on(events.NewMessage(pattern=r'^/stats$'))
async def stats_cmd(event):
    user_id = event.sender_id
    
    # Check if reply to see OTHER user's stats
    if event.is_reply:
         reply = await event.get_reply_message()
         target_id = reply.sender_id
         target_name = reply.sender.first_name if reply.sender else "User"
    else:
         target_id = user_id
         target_name = event.sender.first_name
         
    stats = await db.get_user_stats(target_id, event.chat_id)
    daily = await db.get_daily_stats(target_id, event.chat_id)
    
    text = f"""📊 **User Statistics**

👤 **{target_name}**

📅 **Today**
Messages: `{daily['msgs']}`
Stocks Won: `{daily['stocks']}`

📈 **All Time**
Total Messages: `{stats['total_msgs']}`
Total Stocks: `{stats['total_stocks']}`
Last Win: {time.strftime("%Y-%m-%d %H:%M", time.localtime(stats['last_win'])) if stats['last_win'] > 0 else "Never"}

Keep going! 🚀"""
    
    await event.reply(text)

@bot.on(events.NewMessage(pattern=r'^/clearall$'))
async def clearall_cmd(event):
    if not await check_admin(event.sender_id): return
    
    count = await db.clear_all_penalties()
    spam_detector.ignored_users.clear() # Clear memory cache too
    
    await event.reply(f"✅ **All Restrictions Lifted**\n\nCleared {count} penalties.")
    await logger.log("ADMIN", event.sender.first_name, event.sender_id, f"Cleared ALL ({count} users)")

@bot.on(events.NewMessage(pattern=r'^/perf$'))
async def perf_cmd(event):
    if not await check_admin(event.sender_id): return
    msg = await event.reply("📡 **Checking Database Performance...**")
    report = await get_performance_report(db)
    await msg.edit(report)

# --- CALLBACK HANDLER ---

# Wrapper to catch MessageNotModifiedError
@bot.on(events.CallbackQuery)
async def callback_handler(event):
    try:
        await _handle_callback_logic(event)
    except MessageNotModifiedError:
        pass # Ignore redundant edits
    except Exception as e:
        print(f"[ERROR] Callback Error: {e}")

async def _handle_callback_logic(event):
    user_id = event.sender_id
    data_bytes = event.data
    
    try:
        data = data_bytes.decode('utf-8')
    except:
        return
        
    # Format: "action:user_id"
    # Format: "action:user_id" or "action:sub_action:user_id"?
    # Some buttons might send "menu:user_id" -> action="menu", target_uid=user_id
    # But error says "invalid literal for int() with base 10: 'menu'".
    # This means parts[1] is 'menu'. So data was "something:menu".
    # Ensure we handle different lengths and types.
    
    parts = data.split(':')
    action = parts[0]
    
    target_uid = 0
    if len(parts) > 1:
        # Try to find the user_id, usually the last part or second part.
        # If parts[1] is not int, maybe it's "top:menu:123"?
        # Let's try to grab the last part as ID if it looks like an int.
        try:
            target_uid = int(parts[-1])
        except ValueError:
            try:
                # If not last, maybe second?
                target_uid = int(parts[1])
            except ValueError:
                # Data might be "page:next" where no ID is involved or implicit.
                # However, we check `if target_uid != user_id`.
                # If target_uid is 0, we skip the check? or fail?
                # If it's a public menu, target_uid should be 0 or current user.
                # Let's assume if we can't parse ID, we don't enforce "not for you".
                target_uid = user_id # Bypass check if no ID found
    
    if target_uid != 0 and target_uid != user_id:
        await event.answer("⚠️ This menu is not for you!", alert=True)
        return
        
    if action == "eligible":
        # Run check
        await event.answer("Checking...")
        res = await eligibility_checker.check_user(user_id)
        if res is True:
            text = f"""✅ **You are ELIGIBLE!**

Bio Check: ✅ {REQUIRED_BOT} found
Other Bots: ✅ None detected
Group: ✅ Member of {REQUIRED_GROUP}

Keep chatting to win rewards!"""
        else:
            text = f"""❌ **You are NOT eligible**

Bio Check: {"❌ Not found" if any("Bio missing" in r for r in res) else "✅ Found"}
Other Bots: {"⚠️ Detected" if any("Other bot" in r for r in res) else "✅ None"}
Group: {"❌ Not joined" if any("Member of" in r for r in res) else "✅ Joined"}

Fix these issues to qualify!"""
        
        user = await event.get_sender() # Get sender object for first_name
        is_eligible = await eligibility_checker.check_user(user.id)
    
        status = "✅ Eligible" if is_eligible is True else "❌ Ineligible"
        if is_eligible is not True:
            status += f" ({is_eligible})"
            
        # Check for penalty/cooldown
        penalty = await db.get_penalty_v2(user.id, event.chat_id)
        if penalty:
            remaining = int((penalty['expiry'] - time.time()) / 60)
            status = f"🚫 **Restricted** ({remaining}m remaining)"
            
        # Get stats
        stats = await db.get_user_stats(user.id, event.chat_id)
        
        text = f"""🔍 **Eligibility Report**
        
👤 **User:** [{user.first_name}](tg://user?id={user.id}) (`{user.id}`)
📜 **Status:** {status}

📊 **Stats:**
• Messages: {stats['total_msgs']}
• Stocks Won: {stats['total_stocks']}
• Last Win: {datetime.fromtimestamp(stats['last_win']).strftime('%Y-%m-%d %H:%M') if stats['last_win'] else 'Never'}"""
        
        buttons = [[Button.inline("⬅️ Back", encode_data("menu", user_id))]]
        await event.edit(text, buttons=buttons)
        
    elif action == "stats":
        stats = await db.get_user_stats(user_id, event.chat_id)
        daily = await db.get_daily_stats(user_id, event.chat_id)
        text = f"""📊 **Your Statistics**

👤 **{event.sender.first_name}**

📅 **Today**
Messages: `{daily['msgs']}`
Stocks Won: `{daily['stocks']}`

📈 **All Time**
Total Messages: `{stats['total_msgs']}`
Total Stocks: `{stats['total_stocks']}`
Last Win: {time.strftime("%Y-%m-%d %H:%M", time.localtime(stats['last_win'])) if stats['last_win'] > 0 else "Never"}

Keep going! 🚀"""
        
        buttons = [[Button.inline("⬅️ Back", encode_data("menu", user_id))]]
        await event.edit(text, buttons=buttons)
        
    elif action == "top":
        # Frame 1: Loading
        await event.answer("🏆 Loading leaderboard...")
        
        target_uid = user_id # View my own top context or just top. 
        # Action 'top' is just refreshing the top list.
        
        top_users = await db.get_top_daily(event.chat_id, limit=10)
        now_str = db.get_ist_now().strftime("%H:%M IST")
        
        if not top_users:
            text = f"""🏆 **TOP ACTIVE USERS (Today)**

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
No activity yet today.

Be the first to chat and win rewards!
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📅 Resets daily at 00:00 IST
🕐 Current time: {now_str}"""
        else:
            text = "🏆 **TOP 10 ACTIVE (Today)**\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            
            user_rank_in_top = False
            for i, (uid, msgs, stocks) in enumerate(top_users, 1):
                icon = "🥇" if i==1 else "🥈" if i==2 else "🥉" if i==3 else f"{i}️⃣"
                
                try:
                    u_obj = await bot.get_entity(int(uid))
                    name = u_obj.first_name
                    name_link = f"[{name}](tg://user?id={uid})"
                except:
                    name_link = f"User {uid}"
                    
                if int(uid) == user_id:
                    user_rank_in_top = True
                    text += f"{icon} **You** - {msgs} msgs | {stocks} stocks ⭐\n"
                else:
                    text += f"{icon} {name_link} - {msgs} msgs | {stocks} stocks\n"
            
            text += "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            
            # Check user rank if not in top 10
            if not user_rank_in_top:
                rank, total = await db.get_user_rank(user_id, event.chat_id)
                daily = await db.get_daily_stats(user_id, event.chat_id)
                if rank:
                    text += f"Your rank: #{rank}\nYour messages: {daily['msgs']} msgs | {daily['stocks']} stocks\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            
            text += f"📅 Resets daily at 00:00 IST\n🕐 Current time: {now_str}"
                
        buttons = [
            [Button.inline("🔄 Refresh", encode_data("top", user_id)), Button.inline("📊 My Stats", encode_data("stats", user_id))],
            [Button.inline("⬅️ Back", encode_data("menu", user_id))]
        ]
        await event.edit(text, buttons=buttons)

    elif action == "stats":
        # Personal stats view
        await event.answer("📊 Loading stats...")
        
        daily = await db.get_daily_stats(user_id, event.chat_id)
        current_stats = await db.get_user_stats(user_id, event.chat_id) # Global stats
        
        tier, multiplier, _, next_tier, msgs_needed = await get_tier_details(user_id)
        
        text = f"""📊 **YOUR STATISTICS**
        
👤 **User:** [{event.sender.first_name if event.sender else 'User'}](tg://user?id={user_id})
🏅 **Tier:** {tier} ({multiplier}x)

**📅 Today's Activity:**
💬 Messages: {daily['msgs']}
📦 Stocks Won: {daily['stocks']}

**📈 All-Time Stats:**
🏆 Total Stocks: {current_stats['total_stocks']}
⚠️ Violations: {current_stats['violations']}

"""
        if next_tier:
             text += f"🚀 **Next Tier:** {next_tier} (Needs {msgs_needed} msgs)"
        else:
             text += "🏆 **Max Tier Reached!**"

        buttons = [[Button.inline("⬅️ Back", encode_data("top", user_id))]]
        await event.edit(text, buttons=buttons)
        
    elif action == "rules":
        s = config.get('spam_settings', {})
        text = f"""📜 **Rules & Limits**

✅ Bio: `{REQUIRED_BOT}`
✅ Group: `{REQUIRED_GROUP}`

🚫 **Spam Limits**
• Burst: {s.get('burst_limit')}/{s.get('burst_window_seconds')}s
• Flood: {s.get('global_flood_limit')}/{s.get('global_flood_window')}s
• Penalty: {s.get('ignore_duration_minutes')}m

Don't spam, behave!"""
        buttons = [[Button.inline("⬅️ Back", encode_data("menu", user_id))]]
        await event.edit(text, buttons=buttons)
        
    elif action == "menu":
        text, buttons = await get_menu_layout(user_id)
        await event.edit(text, buttons=buttons)
        
    elif action == "admin_menu":
        if not await check_admin(user_id):
            await event.answer("❌ Admin only", alert=True)
            return
            
        text = "🛡️ **Admin Panel**\n\nSelect an option:"
        buttons = [
            [Button.inline("⚙️ Settings", encode_data("settings", user_id))],
            [Button.inline("📊 Bot Stats", encode_data("botstats", user_id))],
            [Button.inline("⬅️ Exit", encode_data("menu", user_id))]
        ]
        await event.edit(text, buttons=buttons)
        
    elif action == "settings":
        if not await check_admin(user_id): return
        text, buttons = get_settings_menu(user_id)
        await event.edit(text, buttons=buttons)
        
    elif action == "toggle_spam":
        if not check_admin(user_id): return
        
        curr = config.get('antispam_enabled', True)
        new_state = not curr
        config['antispam_enabled'] = new_state
        await save_config(config)
        spam_detector.toggle(new_state)
        
        await event.answer(f"Anti-Spam {'Enabled' if new_state else 'Disabled'}")
        text, buttons = get_settings_menu(user_id)
        await event.edit(text, buttons=buttons)
        
    elif action == "botstats":
         if not await check_admin(user_id): return
         g = await db.get_global_stats()
         ap = await db.get_all_penalties()
         text = f"""📊 **Quick Stats**
         
Users: {g['total_tracked_users']}
Rewards: {g['total_selections']}
Stocks: {g['distributed_stocks']}
Penalties: {len(ap)}"""
         buttons = [[Button.inline("⬅️ Back", encode_data("admin_menu", user_id))]]
         await event.edit(text, buttons=buttons)

# --- MILESTONE COMMANDS ---
@bot.on(events.NewMessage(pattern=r'^/milestone(?: (check|status))?$'))
async def milestone_cmd(event):
    if not await check_admin(event.sender_id): return
    
    msg = await event.reply("🔄 Checking milestones...")
    
    try:
        target_group = config['milestones'].get('target_group')
        if not target_group: target_group = config.get('target_group_id')
        
        # Fetch count
        participants = await bot.get_participants(target_group, limit=0)
        count = participants.total
        
        # Check Expiry & Trigger
        expired = milestone_manager.check_expiry()
        
        # Force check without changing state first to see if we WOULD trigger
        # Actually check_milestone updates nothing, it just checks.
        
        triggered, milestone = milestone_manager.check_milestone(count)
        
        data = milestone_manager.get_progress_data(count)
        active = milestone_manager.get_active_bonus()
        
        report = f"""📊 **MILESTONE STATUS**
        
Members: `{count}`
Active Event: {"✅ YES" if active['active'] else "❌ NO"}
"""
        if active['active']:
            report += f"Multiplier: {active['multiplier']}x\n"
        
        if data:
            report += f"\nNext: **{data['next_target']:,}** ({data['remaining']:,} to go)"
            
        if triggered:
            # Manually activate if prompted? Or just report?
            # User said "come when cmd is used". I assume they want the EVENT to start too.
            details = milestone_manager.activate_event(milestone)
            end_ts = time.time() + details['duration_hours']*3600
            end_str = time.strftime('%H:%M', time.localtime(end_ts))
            
            report += f"\n\n🎉 **NEW MILESTONE REACHED!**\nTarget: {milestone}\nMultiplier: {details['multiplier']}x until {end_str}\n"
            
        await msg.edit(report)
        
    except Exception as e:
        await msg.edit(f"❌ Error: {e}")

@bot.on(events.NewMessage(pattern=r'^/tierstats$'))
async def tierstats_cmd(event):
    if not await check_admin(event.sender_id): return
    
    msg = await event.reply("📊 Calculating tier stats...")
    
    # Calculate stats
    # Iterate all users in DB
    stats = {} # "Gold": 10
    total_users = 0
    
    today_active = await db.get_all_daily_stats()
    
    if not today_active:
        await msg.edit("📊 **Tier Stats (Today)**\n\nNo active users today yet.")
        return

    for uid, data in today_active.items():
        # define minimal check
        msgs = data.get('msgs', 0)
        
        # We need to map msgs -> tier using same logic
        # Re-use logic? 
        # Ideally we refactor tier logic to helper `get_tier_from_msgs(msgs)`
        # For now, duplicate or quick lookup
        current = "Bronze"
        
        reward_conf = config.get('reward_settings', {})
        tiers = reward_conf.get('tiers', {})
        
        # Quick Sort
        tier_list = []
        for name, t_data in tiers.items():
            if name == 'enabled' or not isinstance(t_data, dict): continue
            if 'range' in t_data:
                tier_list.append({'name': name.title(), 'min': t_data['range'][0], 'max': t_data['range'][1]})
        tier_list.sort(key=lambda x: x['min'])
        
        for t in tier_list:
            if t['min'] <= msgs <= t['max']:
                current = t['name']
                break
        if msgs > tier_list[-1]['max']:
            current = tier_list[-1]['name']
            
        stats[current] = stats.get(current, 0) + 1
        total_users += 1
        
    text = f"📊 **TIER STATISTICS (Today)**\n\n"
    text += f"👥 Active Users: {total_users}\n\n"
    
    # Sort for display (Bronze -> Legend)
    # Use tier_list order
    for t in tier_list:
        name = t['name']
        count = stats.get(name, 0)
        if count > 0:
            text += f"• **{name}:** {count}\n"
            
    await msg.edit(text)

@bot.on(events.NewMessage(pattern=r'^/tier (.*)$'))
async def tier_check_cmd(event):
    if not await check_admin(event.sender_id): return
    
    input_str = event.pattern_match.group(1).strip()
    user = None
    
    try:
        if input_str.isdigit():
            user = await bot.get_entity(int(input_str))
        elif input_str.startswith('@'):
            user = await bot.get_entity(input_str)
        elif event.is_reply:
             r = await event.get_reply_message()
             user = await bot.get_entity(r.sender_id)
        else:
             # Try reply
             if event.is_reply:
                 r = await event.get_reply_message()
                 user = await bot.get_entity(r.sender_id)
    except:
        await event.reply("❌ User not found.")
        return
        
    if not user:
        if event.is_reply:
             r = await event.get_reply_message()
             user = await bot.get_entity(r.sender_id)
        else:
             await event.reply("❌ Usage: `/tier <@user/id>` or reply to user.")
             return

    # Get details
    tier, multiplier, msgs, next_tier, needed = await get_tier_details(user.id)
    
    text = f"""👤 **User:** [{user.first_name}](tg://user?id={user.id})
📊 **Messages Today:** {msgs}
🏅 **Current Tier:** {tier}
⚡ **Multiplier:** {multiplier}x

"""
    if next_tier:
        text += f"🚀 **Next Tier:** {next_tier} (Needs {needed} msgs)"
    else:
        text += "🏆 **Max Tier Reached!**"
        
    await event.reply(text)

@bot.on(events.NewMessage(pattern=r'^/setmilestone (\d+)$'))
async def setmilestone_cmd(event):
    if not await check_admin(event.sender_id): return
    
    # Debug command to simulate member count
    sim_count = int(event.pattern_match.group(1))
    
    msg = await event.reply(f"🔧 Simulating {sim_count:,} members...")
    
    # Process
    expired = milestone_manager.check_expiry()
    triggered, milestone = milestone_manager.check_milestone(sim_count)
    
    status = "✅ No event triggered."
    if triggered:
        details = milestone_manager.activate_event(milestone)
        status = f"🎉 **TRIGGERED {milestone:,} Event!**"
        
        # Announce
        ann = f"🎉 **{milestone:,} MEMBERS!** (Simulation)"
        await event.respond(ann)
    
    # Update Pin
    new_text = milestone_manager.get_pinned_text(sim_count)
    # Just show preview
    await event.respond(f"📌 **Pin Preview:**\n\n{new_text}")
    await msg.edit(f"🔧 Simulation Complete.\n{status}")

# --- ADMIN HELP SYSTEM ---

HELP_CATEGORIES = {
    "info": config.get('messages', {}).get('help', {}).get('info', "Help Info Unavailable"),

    "settings": config.get('messages', {}).get('help', {}).get('settings', "Settings Help Unavailable"),

    "mod": config.get('messages', {}).get('help', {}).get('mod', "Mod Help Unavailable"),

    "admin": config.get('messages', {}).get('help', {}).get('admin', "Admin Help Unavailable"),

    "utils": config.get('messages', {}).get('help', {}).get('utils', "Utils Help Unavailable")
}

@bot.on(events.NewMessage(pattern=r'^/(?:help|adminhelp)(?: (.+))?$'))
async def admin_help_cmd(event):
    if not await check_admin(event.sender_id): return
    
    args = event.pattern_match.group(1)
    
    if args:
        # Detailed help for specific commands
        cmd = args.lower()
        if cmd == 'ban':
            text = """📚 **COMMAND: /ban**

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📝 **DESCRIPTION**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Ban user from winning rewards.
They can still chat normally.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
💡 **USAGE**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
/ban <user_id>
/ban @username
/ban (reply to user)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📋 **EXAMPLES**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
/ban 123456789
→ Ban by user ID

/ban @spammer
→ Ban by username

Reply to message + /ban
→ Ban replied user

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⚠️ **EFFECTS**
• Cannot win rewards
• Messages still counted
• Can be unbanned with /unban

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🔐 Access: Admins only"""

        elif cmd == 'tier':
             text = """📚 **COMMAND: /tier**

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📝 **DESCRIPTION**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Check any user's tier status (admin).

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
💡 **USAGE**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
/tier <user_id>
/tier @username
/tier (reply to user)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📋 **EXAMPLES**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
/tier 123456789
→ Check user by ID

/tier @username
→ Check user by username

Reply to message + /tier
→ Check replied user

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ℹ️ **NOTE**
Users can check their own with /mytier

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🔐 Access: Admins only"""

        elif cmd == 'setreward':
             text = """📚 **COMMAND: /setreward**

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📝 **DESCRIPTION**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Configure reward amounts and systems.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
💡 **USAGE**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
/setreward <amount>
• Fixed amount

/setreward <min>-<max>
• Random range

/setreward tiered on/off
• Toggle tier system

/setreward jackpot on/off
• Toggle jackpot

/setreward jackpot set <chance>% <amount>
• Configure jackpot

/setreward
• View current settings

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📋 **EXAMPLES**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
/setreward 10
→ Set fixed 10 stocks

/setreward 5-15
→ Random 5-15 stocks

/setreward jackpot set 15% 100
→ 15% chance for 100 stocks

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🔐 Access: Admins only"""

        elif cmd == 'spamconfig':
             text = """📚 **COMMAND: /spamconfig**

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📝 **DESCRIPTION**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Configure spam settings dynamically.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
💡 **USAGE**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
/spamconfig <type> <setting> <value>

**Types:** burst, flood, raid
**Settings:** limit, window, pause

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📋 **EXAMPLES**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
/spamconfig burst limit 5
→ Set burst limit to 5

/spamconfig flood window 10
→ Set flood window to 10s

/unpause
→ Lift global flood pause manually"""
        elif cmd == 'penalty':
             text = """📚 **COMMAND: /penalty**

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📝 **DESCRIPTION**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Apply a spam penalty manually.
User will be ignored by bot.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
💡 **USAGE**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
/penalty <user> [minutes]
• Default: 30 mins

/unpenalty <user>
• Remove penalty

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📋 **EXAMPLES**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
/penalty @spammer 60
→ Ignore for 1 hour

/unpenalty @spammer
→ Restore access

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🔐 Access: Admins only"""

        elif cmd == 'milestone':
             text = """📚 **COMMAND: /milestone**

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📝 **DESCRIPTION**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Check group member milestones.
Triggers event if crossed.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
💡 **USAGE**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
/milestone
• Check status & trigger

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🔐 Access: Admins only"""

        else:
            text = f"❌ Help not found for `{cmd}` yet. Check category menu."
            
        await event.reply(text)
        return

    # Frame 1: Loading
    msg = await event.reply("📚 Loading admin commands...")
    await asyncio.sleep(0.7) # Using delay for UI effect

    # Frame 2 / Interactive Menu
    text = """🛡️ **ADMIN COMMANDS**

Choose a category:"""
    
    buttons = [
        [Button.inline("📊 Info", b"help:info"), Button.inline("⚙️ Settings", b"help:settings")],
        [Button.inline("🚫 Moderation", b"help:mod"), Button.inline("👥 Admin", b"help:admin")],
        [Button.inline("📢 Utilities", b"help:utils"), Button.inline("❌ Close", b"help:close")]
    ]
    
    await msg.edit(text, buttons=buttons)

@bot.on(events.CallbackQuery(pattern=b'help:(.*)'))
async def help_cb(event):
    cat = event.pattern_match.group(1).decode('utf-8')
    
    if cat == "close":
        await event.delete()
        return
        
    if cat == "main":
        text = "🛡️ **ADMIN COMMANDS**\n\nChoose a category:"
        buttons = [
            [Button.inline("📊 Info", b"help:info"), Button.inline("⚙️ Settings", b"help:settings")],
            [Button.inline("🚫 Moderation", b"help:mod"), Button.inline("👥 Admin", b"help:admin")],
            [Button.inline("📢 Utilities", b"help:utils"), Button.inline("❌ Close", b"help:close")]
        ]
        await event.edit(text, buttons=buttons)
        return

    text = HELP_CATEGORIES.get(cat, "Invalid Category")
    buttons = [[Button.inline("⬅️ Back", b"help:main")]]
    
    await event.edit(text, buttons=buttons)

# --- REWARD CALCULATION ---

# Duplicate calculate_reward removed. Using the one defined in HELPERS section.

# --- GROUP MESSAGE MONITOR ---

@bot.on(events.NewMessage(chats=config.get('target_group_id')))

async def group_handler(event):
    # Strict Filtering: Exclude bots and userbots
    if event.is_private or not event.sender or event.sender.bot:
        return
        
    # Exclude service messages (Join, Leave, Pin, etc.)
    if event.action:
        return
        
    user_id = event.sender_id
    group_id = event.chat_id
    
    # Pre-calculate media status
    is_media = bool(event.media or event.sticker or event.gif or event.photo or event.video)

    # --- Phase 3 & 5: Parallel Async Parallelization ---
    # We fetch user data (cached) and run spam detection (async) concurrently.
    user_data, spam_result = await asyncio.gather(
        db.get_user(user_id, group_id),
        spam_detector.is_spam(user_id, event.text, is_media=is_media)
    )
    
    # Extract status from user_data (Zero DB overhead if cached)
    status = user_data.get('status', {}) if user_data else {}
    is_whitelisted = status.get('is_whitelisted', False)
    is_banned = status.get('is_banned', False)
    is_penalized = status.get('is_penalized', False)
    penalty_expires = status.get('penalty_expires', 0)

    # 0. Global Pause Check
    if event_manager.is_paused():
        return

    # 1. Banned Check
    if is_banned:
        return
        
    # 2. Penalty Check
    if is_penalized:
        if penalty_expires < time.time():
             # Auto-clear expired penalty (Phase 5)
             await db.remove_penalty_v2(user_id, group_id)
        else:
             return # User is penalized
    
    # 3. Spam Check Processing (Already computed above)
    
    spam_type = spam_result
    spam_details = "Spam detected"
    
    if isinstance(spam_result, tuple):
        spam_type, spam_details = spam_result
    
    if spam_type == "global_flood":
        # Global Flood Detected!
        duration = config.get('spam_settings', {}).get('global_flood_pause_seconds', 60)
        event_manager.pause_processing(duration)
        
        await logger.log("SPAM", "GLOBAL", "ALL", "Global Flood", f"Details: {spam_details}\nPaused: {duration}s")
        
        try:
            msg = f"⛔ **GLOBAL OVERLOAD DETECTED** ⛔\n\n"
            msg += f"⚠️ **Traffic Level:** Critical\n"
            msg += f"⏳ **System Paused:** {duration} seconds\n\n"
            msg += f"__All messages are being ignored. Stop spamming or face permanent bans.__"
            await bot.send_message(event.chat_id, msg)
        except: pass
        return

    elif spam_type:
        # Check if already ignored (Phase 2: Cached)
        if spam_type is True or await spam_detector.is_ignored(user_id):
             return

        # 1. Increment Violation (Async)
        level = await db.add_violation(user_id, group_id)
        
        # 2. Determine Duration (Stacking)
        if level == 1:
            duration = 30
        elif level == 2:
            duration = 90
        elif level == 3:
            duration = 180
        level, duration = await db.add_penalty_v2(user_id, group_id, duration, reason=f"{spam_type} spam")
        
        # Log Spam
        status = "ESCALATED" if level > 1 else "PENALIZED"
        await logger.log("SPAM", event.sender.first_name, user_id, f"Type: {spam_type}", f"Details: {spam_details}\nStatus: {status} (L{level}) | {duration}m")
        
        # 1. Group Warning (Public)
        try:
            msg_text = f"⚠️ **Spam detected!**\n\n"
            msg_text += f"User: {get_mention(event.sender)}\n"
            msg_text += f"Penalty: {duration} minutes\n\n"
            msg_text += f"Reason: {spam_type.title()} spam\n"
            msg_text += f"Please slow down."
            
            await event.reply(msg_text)
        except: pass
        
        # 2. DM Warning (Private)
        try:
            expiry_time = time.strftime('%H:%M UTC', time.gmtime(time.time() + duration * 60))
            
            dm_text = f"⏸️ **Slow down!**\n\n"
            dm_text += f"Penalty: {duration} minutes\n"
            dm_text += f"Expires: {expiry_time}\n\n"
            dm_text += f"Reason: {spam_details}\n"
            
            # Add Limit context if possible
            s = config.get('spam_settings', {})
            if spam_type == 'burst':
                dm_text += f"Limit: {s.get('burst_limit', 5)} msgs in {s.get('burst_window_seconds', 10)} seconds\n"
            elif spam_type == 'global_flood':
                dm_text += f"Limit: {s.get('global_flood_limit', 20)} msgs in {s.get('global_flood_window', 3)} seconds\n"
                
            dm_text += "\nCalm down and try again later!"
            
            await bot.send_message(user_id, dm_text)
        except: pass
        
        return

    # 3. Final Restriction Check (Redundant but safe)
    if is_banned or is_penalized:
        return

    # 4. Command Cooldown Check (Commands Only)
    if event.text.startswith('/'):
        # Check command cooldown
        cd_time = config.get('spam_settings', {}).get('command_cooldown_seconds', 2)
        last_cmd = command_cooldowns.get(user_id, 0)
        now = time.time()
        
        if now - last_cmd < cd_time:
            # On cooldown, ignore logic but maybe warn if persistent?
            # Just return to suppress spammy commands
            return
            
        command_cooldowns[user_id] = now
        # Allow command to be processed by other handlers (Telethon handles this via other @bot.on)
        # BUT we must RETURN here so it doesn't count as a rewardable message
        return

    # 5. Reward Eligibility Check (TEXT Only)
    # Exclude media, stickers, etc. for rewards
    if is_media:
        return

    # 6. Valid Message for Reward (Phase 4: Bulk Write-Behind)
    await db.add_user_message_bulk(user_id, group_id)
    
    # 7. Event Loop (Trigger Reward)
    if await event_manager.process_message():
        # Winner Selected!
        sender = await event.get_sender()
        
        # Eligibility Check (No Cooldown)
        is_eligible = await eligibility_checker.check_user(user_id)
        
        if is_eligible is True:
            # EXECUTE REWARD
            user_msg = await event.reply(f"🎉 **WINNER** {get_mention(sender)}!\n\nSending reward...")
            
            # 1. Calculate Reward (Async)
            reward_data = await calculate_reward(user_id)
            amount = reward_data['amount']
            
            # 2. Update DB (Phase 4: Bulk Operation)
            await asyncio.gather(
                db.add_user_stock(user_id, group_id, amount),
                db.increment_total_selections(),
                db.add_recent_winner(user_id, sender.first_name, amount)
            )
            
            # 3. Userbot Action
            # Default to old template if not set in new structure
            template = config.get('reward_settings', {}).get('command_template', '/add stocks {amount}')
            # Fallback legacy check
            if 'reward_cmd_template' in config and 'reward_settings' not in config:
                 template = config['reward_cmd_template']
            
            # Format command
            user_name = f"@{sender.username}" if sender.username else sender.first_name
            try:
                cmd = template.format(amount=amount, user=user_name, userid=user_id)
            except:
                cmd = f"/add stocks {amount}" # Safe fallback
            
            try:
                await userbot.send_message(event.chat_id, cmd, reply_to=event.id)
                
                # 4. Notifications
                log_detail = f"Amt: {amount} | Type: {reward_data['type']} | Tier: {reward_data['tier']}"
                await logger.log("REWARD", sender.first_name, user_id, "Reward Sent", log_detail)
                
                # DM User
                dm_text = f"🎉 **CONGRATULATIONS!**\n\n"
                if reward_data['type'] == 'jackpot':
                    dm_text += f"{reward_data['msg']}\nYou won **{amount} stocks**!"
                else:
                    dm_text += f"You won **{amount} stocks**!\n\n"
                    dm_text += f"📊 **Breakdown:**\n"
                    dm_text += f"• Base: {reward_data['base']}\n"
                    if reward_data['multiplier'] > 1.0:
                         dm_text += f"• Tier Multiplier: {reward_data['multiplier']}x ({reward_data['tier'].title()})\n"
                
                dm_text += "\nNo cooldown - keep chatting to win again!"
                try:
                    await bot.send_message(user_id, dm_text)
                except: pass
                
                # Edit Group Msg
                # Add jackpot tag if applicable
                extra_tag = f"\n{reward_data['msg']}" if reward_data['type'] == 'jackpot' else ""
                await user_msg.edit(f"🎉 **WINNER** {get_mention(sender)}!\n\n✅ **Reward Sent!** ({amount} stocks){extra_tag}")

            except Exception as e:
                await logger.log("ERROR", sender.first_name, user_id, "Reward Failed", f"Error: {e}")
                await user_msg.edit(f"⚠️ **Error sending reward.** Admin notified.")

        else:
            # Not Eligible -> RE-ROLL
            # We want the VERY NEXT message to trigger the reward again.
            # Set count to target - 1, so next msg (+1) equals target.
            event_manager.current_count = event_manager.target_count - 1
            
            reason = is_eligible[0] if isinstance(is_eligible, list) else "Unknown"
            await logger.log("INELIGIBLE", sender.first_name, user_id, "Winner skipped (Re-rolling)", f"Reason: {reason}")
            
            # Notify user why via DM (Optional but good)
            try:
                await bot.send_message(user_id, f"⚠️ **You were selected but ineligible!**\n\nReason: {reason}\n\nFix this to win next time!")
            except: pass

# --- MILESTONE CONTROLS ---

@bot.on(events.NewMessage(pattern=r'^/stopevent$'))
async def stopevent_cmd(event):
    if not await check_admin(event.sender_id): return
    
    if not event_manager.active:
        await event.reply("✅ Reward events are already **STOPPED**.")
        return

    event_manager.stop_event()
    await event.reply("🛑 **Reward events STOPPED.**\nNo new rewards will be triggered.")

@bot.on(events.NewMessage(pattern=r'^/startevent$'))
async def startevent_cmd(event):
    if not await check_admin(event.sender_id): return
    
    if event_manager.active:
        await event.reply("✅ Reward events are already **RUNNING**.")
        return

    # Start with default random loop
    # We use the defaults from __init__ or config if we had them passed? 
    # For now, duplicate default logic or just set active=True if it was just paused?
    # start_event resets counters, so it's a fresh start.
    
    # Use defaults: min=50, max=200 (or whatever is in set_interval)
    # Ideally we should remember the last settings.
    # But user said "usig the default interval (unkess manually set)"
    # So we'll use the hardcoded defaults in start_event or config if available.
    
    event_manager.start_event(min_val=100, max_val=250, loop=True)
    await event.reply("🟢 **Reward events STARTED.**\nRandom interval: 100-250 msgs (Default)")

# --- ENTRY POINT ---


# ============================================
# DATABASE PING & LATENCY
# ============================================

@bot.on(events.NewMessage(pattern=r'^/dbping$'))
async def db_ping(event):
    """Check MongoDB connection and latency"""
    if event.sender_id != OWNER_ID and not db.is_admin(event.sender_id):
        return
    
    status_msg = await event.reply("🔄 Checking database connection...")
    
    try:
        # Measure ping time
        start = time.time()
        db.db.client.server_info()  # Force connection check
        latency = (time.time() - start) * 1000  # Convert to ms
        
        # Get database stats
        stats = db.db.client.admin.command('ping')
        server_status = db.db.client.admin.command('serverStatus')
        
        # Connection info
        connections = server_status.get('connections', {})
        
        message = f"""🔌 **DATABASE CONNECTION**

**Status:** ✅ Connected
**Latency:** {latency:.2f}ms

**Server Info:**
• Version: {server_status.get('version', 'Unknown')}
• Uptime: {server_status.get('uptime', 0) / 3600:.1f} hours
• Active: {connections.get('current', 0)} connections
• Available: {connections.get('available', 0)} connections

**Performance:**
• Network: {'🟢 Excellent' if latency < 50 else '🟡 Good' if latency < 100 else '🔴 Slow'}
• Response: {latency:.0f}ms
"""
        
        await status_msg.edit(message)
        
    except Exception as e:
        await status_msg.edit(f"[ERROR] **Database Error**\n\n{str(e)}")

# ============================================
# DATABASE STATISTICS
# ============================================

@bot.on(events.NewMessage(pattern=r'^/dbstats$'))
async def db_stats(event):
    """View detailed database statistics"""
    if event.sender_id != OWNER_ID and not db.is_admin(event.sender_id):
        return
    
    status_msg = await event.reply("📊 Loading database statistics...")
    
    try:
        # Collection counts
        users_count = db.users.count_documents({})
        daily_stats_count = db.daily_stats.count_documents({})
        rewards_count = db.rewards.count_documents({})
        penalties_count = db.penalties.count_documents({})
        
        # Active penalties
        active_penalties = await db.get_all_penalties()
        
        # Banned users
        banned_count = db.users.count_documents({"status.is_banned": True})
        
        # Whitelisted users
        whitelisted_count = db.users.count_documents({"status.is_whitelisted": True})
        
        # Today's activity
        today = db.get_today_date()
        today_stats = db.daily_stats.count_documents({"date": today})
        
        # Database size
        stats = db.db.command("dbStats")
        db_size = stats.get('dataSize', 0) / (1024 * 1024)  # MB
        storage_size = stats.get('storageSize', 0) / (1024 * 1024)  # MB
        
        message = f"""📊 **DATABASE STATISTICS**

**Collections:**
• Users: {users_count:,}
• Daily Stats: {daily_stats_count:,}
• Rewards: {rewards_count:,}
• Penalties: {penalties_count:,}

**Status:**
• Banned: {banned_count}
• Whitelisted: {whitelisted_count}
• Active Penalties: {len(active_penalties)}

**Today's Activity:**
• Active Users: {today_stats}

**Storage:**
• Data Size: {db_size:.2f} MB
• Storage Size: {storage_size:.2f} MB
• Free Tier Limit: 512 MB
• Usage: {(db_size / 512 * 100):.1f}%

**Actions:**
/dbview - Browse collections
/dbedit - Edit user data
/dbsearch - Search users
"""
        
        await status_msg.edit(message)
        
    except Exception as e:
        await status_msg.edit(f"[ERROR] **Error:**\n\n{str(e)}")

# ============================================
# BROWSE COLLECTIONS
# ============================================

@bot.on(events.NewMessage(pattern=r'^/dbview$'))
async def db_view(event):
    """Browse database collections with buttons"""
    if event.sender_id != OWNER_ID and not db.is_admin(event.sender_id):
        return
    
    buttons = [
        [Button.inline("👥 Users", b"dbview_users")],
        [Button.inline("📅 Daily Stats", b"dbview_daily")],
        [Button.inline("🎁 Rewards", b"dbview_rewards")],
        [Button.inline("🚫 Penalties", b"dbview_penalties")],
        [Button.inline("⚙️ Config", b"dbview_config")],
        [Button.inline("📊 System Stats", b"dbview_system")],
    ]
    
    await event.reply(
        "📂 **DATABASE BROWSER**\n\nSelect a collection to view:",
        buttons=buttons
    )

@bot.on(events.CallbackQuery(pattern=b'^dbview_users$'))
async def dbview_users_callback(event):
    """View users collection"""
    if event.sender_id != OWNER_ID and not db.is_admin(event.sender_id):
        return await event.answer("❌ Admin only", alert=True)
    
    await event.edit("⏳ Loading users...")
    
    # Get top 10 users by messages
    users = list(db.users.find().sort("stats.total_msgs", -1).limit(10))
    
    if not users:
        return await event.edit("📭 No users found")
    
    message = "👥 **TOP 10 USERS**\n\n"
    
    for idx, user in enumerate(users, 1):
        user_id = user['_id']
        stats = user.get('stats', {})
        status = user.get('status', {})
        
        badges = []
        if status.get('is_banned'):
            badges.append("🚫")
        if status.get('is_whitelisted'):
            badges.append("⭐")
        if status.get('is_penalized'):
            badges.append("⏸️")
        
        badge_str = " ".join(badges) if badges else ""
        
        message += f"{idx}. `{user_id}` {badge_str}\n"
        message += f"   📨 {stats.get('total_msgs', 0):,} msgs | "
        message += f"💎 {stats.get('total_stocks', 0)} stocks\n\n"
    
    buttons = [
        [Button.inline("🔍 Search User", b"db_search_prompt")],
        [Button.inline("« Back", b"dbview_back")],
    ]
    
    await event.edit(message, buttons=buttons)

@bot.on(events.CallbackQuery(pattern=b'^dbview_daily$'))
async def dbview_daily_callback(event):
    """View today's stats"""
    if event.sender_id != OWNER_ID and not db.is_admin(event.sender_id):
        return await event.answer("❌ Admin only", alert=True)
    
    await event.edit("⏳ Loading daily stats...")
    
    # Get top today
    today = db.get_today_date()
    stats = list(db.daily_stats.find({"date": today}).sort("messages", -1).limit(10))
    
    if not stats:
        return await event.edit("📭 No activity today")
    
    message = f"📅 **TODAY'S ACTIVITY** ({today})\n\n"
    
    for idx, stat in enumerate(stats, 1):
        message += f"{idx}. `{stat['user_id']}`\n"
        message += f"   📨 {stat.get('messages', 0)} msgs | "
        message += f"💎 {stat.get('stocks_won', 0)} stocks | "
        message += f"🏆 {stat.get('tier', 'Unknown')}\n\n"
    
    buttons = [[Button.inline("« Back", b"dbview_back")]]
    await event.edit(message, buttons=buttons)

@bot.on(events.CallbackQuery(pattern=b'^dbview_rewards$'))
async def dbview_rewards_callback(event):
    """View recent rewards"""
    if event.sender_id != OWNER_ID and not db.is_admin(event.sender_id):
        return await event.answer("❌ Admin only", alert=True)
    
    await event.edit("⏳ Loading rewards...")
    
    # Get last 10 rewards
    rewards = list(db.rewards.find().sort("timestamp", -1).limit(10))
    
    if not rewards:
        return await event.edit("📭 No rewards yet")
    
    message = "🎁 **RECENT REWARDS**\n\n"
    
    for idx, reward in enumerate(rewards, 1):
        from datetime import datetime, timedelta, timezone
        IST_TZ = timezone(timedelta(hours=5, minutes=30))
        ts = datetime.fromtimestamp(reward['timestamp'], IST_TZ)
        time_str = ts.strftime("%H:%M")
        
        calc = reward.get('calculation', {})
        jackpot = "🎰" if calc.get('jackpot') else ""
        
        message += f"{idx}. `{reward['user_id']}` {jackpot}\n"
        message += f"   💎 {reward['amount']} stocks at {time_str}\n"
        message += f"   Tier: {calc.get('tier', 'N/A')} ({calc.get('tier_multi', 1)}x)\n\n"
    
    buttons = [[Button.inline("« Back", b"dbview_back")]]
    await event.edit(message, buttons=buttons)

@bot.on(events.CallbackQuery(pattern=b'^dbview_config$'))
async def dbview_config_callback(event):
    """View bot configuration"""
    if event.sender_id != OWNER_ID:
        return await event.answer("❌ Owner only", alert=True)
    
    await event.edit("⏳ Loading config...")
    
    config = await db.get_config()
    
    reward_settings = config.get('reward_settings', {})
    base = reward_settings.get('base', {})
    jackpot = reward_settings.get('jackpot', {})
    
    message = f"""⚙️ **BOT CONFIGURATION**

**Reward Settings:**
• Mode: {base.get('mode', 'N/A')}
• Base: {base.get('min', 0)}-{base.get('max', 0)}
• Tiers: {'✅' if reward_settings.get('tiers', {}).get('enabled') else '❌'}
• Jackpot: {'✅' if jackpot.get('enabled') else '❌'} ({jackpot.get('chance', 0)}%)

**Spam Protection:**
• Status: {'✅ Enabled' if config.get('antispam_enabled') else '❌ Disabled'}

**Group IDs:**
• Owner: `{config.get('owner_id', 'N/A')}`
• Target Group: `{config.get('target_group_id', 'N/A')}`
• Log Channel: `{config.get('log_channel_id', 'N/A')}`

**Admins:** {len(config.get('admin_ids', []))}
"""
    
    buttons = [
        [Button.inline("✏️ Edit Config", b"dbedit_config")],
        [Button.inline("« Back", b"dbview_back")],
    ]
    
    await event.edit(message, buttons=buttons)

@bot.on(events.CallbackQuery(pattern=b'^dbview_back$'))
async def dbview_back_callback(event):
    """Return to main menu"""
    buttons = [
        [Button.inline("👥 Users", b"dbview_users")],
        [Button.inline("📅 Daily Stats", b"dbview_daily")],
        [Button.inline("🎁 Rewards", b"dbview_rewards")],
        [Button.inline("🚫 Penalties", b"dbview_penalties")],
        [Button.inline("⚙️ Config", b"dbview_config")],
        [Button.inline("📊 System Stats", b"dbview_system")],
    ]
    
    await event.edit(
        "📂 **DATABASE BROWSER**\n\nSelect a collection to view:",
        buttons=buttons
    )

# ============================================
# SEARCH USERS
# ============================================

# Global dict to store search states
user_search_states = {}

@bot.on(events.NewMessage(pattern=r'^/dbsearch(?:\s+(.+))?$'))
async def db_search(event):
    """Search for users by ID or name"""
    if event.sender_id != OWNER_ID and not db.is_admin(event.sender_id):
        return
    
    query = event.pattern_match.group(1)
    
    if not query:
        return await event.reply(
            "🔍 **USER SEARCH**\n\n"
            "Usage:\n"
            "`/dbsearch <user_id>`\n"
            "`/dbsearch <partial_id>`\n\n"
            "Example: `/dbsearch 986380678`"
        )
    
    status_msg = await event.reply("🔍 Searching...")
    
    try:
        # Search by exact ID
        user = db.users.find_one({"_id": query})
        
        if not user:
            # Search by partial ID
            users = list(db.users.find({
                "_id": {"$regex": f"^{query}"}
            }).limit(5))
            
            if not users:
                return await status_msg.edit("❌ No users found")
            
            # Multiple results
            message = f"🔍 **SEARCH RESULTS** ({len(users)} found)\n\n"
            buttons = []
            
            for u in users:
                stats = u.get('stats', {})
                message += f"• `{u['_id']}` - {stats.get('total_msgs', 0)} msgs\n"
                buttons.append([Button.inline(
                    f"View {u['_id']}", 
                    f"dbuser_{u['_id']}".encode()
                )])
            
            return await status_msg.edit(message, buttons=buttons)
        
        # Single result - show details
        await show_user_details(status_msg, user)
        
    except Exception as e:
        await status_msg.edit(f"❌ Error: {str(e)}")

async def show_user_details(msg, user):
    """Display detailed user information"""
    user_id = user['_id']
    stats = user.get('stats', {})
    status = user.get('status', {})
    violations = user.get('violations', {})
    
    # Get daily stats
    today = db.get_today_date()
    daily = await db.daily_stats.find_one({"date": today, "user_id": user_id})
    
    message = f"""👤 **USER DETAILS**

**ID:** `{user_id}`
**Name:** {user.get('first_name', 'Unknown')}
**Username:** @{user.get('username', 'none')}

**Statistics:**
• Total Messages: {stats.get('total_msgs', 0):,}
• Total Stocks: {stats.get('total_stocks', 0)}
• Last Win: {stats.get('last_win', 0)}

**Today:**
• Messages: {daily.get('messages', 0) if daily else 0}
• Stocks: {daily.get('stocks_won', 0) if daily else 0}
• Tier: {daily.get('tier', 'N/A') if daily else 'N/A'}

**Status:**
• Banned: {'✅ Yes' if status.get('is_banned') else '❌ No'}
• Whitelisted: {'✅ Yes' if status.get('is_whitelisted') else '❌ No'}
• Penalized: {'✅ Yes' if status.get('is_penalized') else '❌ No'}

**Violations:**
• Count: {violations.get('count', 0)}
• Last: {violations.get('last_violation', 'Never')}
"""
    
    buttons = [
        [
            Button.inline("✏️ Edit", f"dbedit_user_{user_id}".encode()),
            Button.inline("🚫 Ban", f"dbban_{user_id}".encode())
        ],
        [
            Button.inline("⭐ Whitelist", f"dbwhite_{user_id}".encode()),
            Button.inline("📊 History", f"dbhist_{user_id}".encode())
        ],
        [Button.inline("« Back", b"dbview_users")]
    ]
    
    await msg.edit(message, buttons=buttons)

@bot.on(events.CallbackQuery(pattern=rb'^dbuser_(.+)$'))
async def dbuser_callback(event):
    """Show user details from button"""
    if event.sender_id != OWNER_ID and not await db.is_admin(event.sender_id):
        return await event.answer("❌ Admin only", alert=True)
    
    user_id = event.pattern_match.group(1).decode()
    user = await db.users.find_one({"_id": user_id})
    
    if not user:
        return await event.answer("❌ User not found", alert=True)
    
    await event.edit("⏳ Loading...")
    await show_user_details(event, user)

# ============================================
# EDIT USER DATA
# ============================================

@bot.on(events.CallbackQuery(pattern=rb'^dbedit_user_(.+)$'))
async def dbedit_user_callback(event):
    """Edit user data"""
    if event.sender_id != OWNER_ID:
        return await event.answer("❌ Owner only", alert=True)
    
    user_id = event.pattern_match.group(1).decode()
    
    buttons = [
        [
            Button.inline("📨 Messages", f"dbedit_msgs_{user_id}".encode()),
            Button.inline("💎 Stocks", f"dbedit_stocks_{user_id}".encode())
        ],
        [
            Button.inline("🚫 Toggle Ban", f"dbedit_ban_{user_id}".encode()),
            Button.inline("⭐ Toggle Whitelist", f"dbedit_white_{user_id}".encode())
        ],
        [
            Button.inline("🔄 Reset Violations", f"dbedit_viols_{user_id}".encode())
        ],
        [Button.inline("« Back", f"dbuser_{user_id}".encode())]
    ]
    
    await event.edit(
        f"✏️ **EDIT USER:** `{user_id}`\n\nSelect what to edit:",
        buttons=buttons
    )

# Quick action buttons
@bot.on(events.CallbackQuery(pattern=rb'^dbban_(.+)$'))
async def dbban_callback(event):
    """Quick ban user"""
    if event.sender_id != OWNER_ID:
        return await event.answer("❌ Owner only", alert=True)
    
    user_id = event.pattern_match.group(1).decode()
    user = await db.users.find_one({"_id": user_id})
    
    if not user:
        return await event.answer("❌ User not found", alert=True)
    
    is_banned = user.get('status', {}).get('is_banned', False)
    
    if is_banned:
        await db.unban_user(user_id)
        await event.answer("✅ User unbanned", alert=True)
    else:
        await db.ban_user(user_id, "Manual ban", "Admin")
        await event.answer("✅ User banned", alert=True)
    
    # Refresh display
    await show_user_details(event, await db.users.find_one({"_id": user_id}))

@bot.on(events.CallbackQuery(pattern=rb'^dbwhite_(.+)$'))
async def dbwhite_callback(event):
    """Quick whitelist user"""
    if event.sender_id != OWNER_ID:
        return await event.answer("❌ Owner only", alert=True)
    
    user_id = event.pattern_match.group(1).decode()
    user = await db.users.find_one({"_id": user_id})
    
    if not user:
        return await event.answer("❌ User not found", alert=True)
    
    is_whitelisted = user.get('status', {}).get('is_whitelisted', False)
    
    if is_whitelisted:
        await db.unwhitelist_user(user_id)
        await event.answer("✅ Removed from whitelist", alert=True)
    else:
        await db.whitelist_user(user_id)
        await event.answer("✅ Added to whitelist", alert=True)
    
    # Refresh display
    await show_user_details(event, await db.users.find_one({"_id": user_id}))

# ============================================
# PERFORMANCE OPTIMIZATION
# ============================================

# Add index creation for better performance
async def optimize_database():
    """Create indexes for better query performance"""
    try:
        # These indexes improve query speed significantly
        await db.users.create_index([("stats.total_msgs", -1)])
        await db.users.create_index([("status.is_banned", 1)])
        await db.daily_stats.create_index([("date", 1), ("messages", -1)])
        await db.rewards.create_index([("timestamp", -1)])
        
        print("✅ Database indexes optimized")
    except Exception as e:
        print(f"⚠️ Index optimization warning: {e}")

async def main():
    print("Starting Web Server...")
    start_server()
    print("Bot Starting...")
    
    # Start background database queue (Phase 7 fix)
    db.write_queue.start()
    
    await bot.start(bot_token=config['bot_token'])
    print("Userbot Starting...")
    await userbot.start(phone=config['phone_number'])
    await optimize_database()
    
    # Start background tasks
    bot.loop.create_task(update_owner_info(bot))
    # db_buffer_flusher removed in Phase 5
    
    await logger.log("SYSTEM", "Bot", 0, "Bot Started", "Status: Online")
    print("Online.")
    
    # Check Logger Access (Explicit Startup Check)
    log_cid = config.get('log_channel_id')
    if log_cid:
        try:
            entity = await bot.get_entity(log_cid)
            print(f"Log Channel Connected: {entity.title} (ID: {log_cid})")
        except Exception as e:
            print(f"WARNING: Could not access Log Channel (ID: {log_cid}).")
            print(f"   Error: {e}")
            print(f"   Make sure the bot is an admin in the channel and has sent a message there.")
    
    await asyncio.gather(bot.run_until_disconnected(), userbot.run_until_disconnected())

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
