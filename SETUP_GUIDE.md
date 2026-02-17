# 🚀 Complete Setup Guide

This guide will walk you through setting up the Naruto Reward Bot from scratch.

---

## 📋 Part 1: Prerequisites (15 minutes)

### 1.1 Install Python
- Download Python 3.8+ from python.org
- During installation, check "Add Python to PATH"
- Verify: `python --version` (should show 3.8 or higher)

### 1.2 Create MongoDB Atlas Account
1. Go to mongodb.com/cloud/atlas
2. Sign up (free)
3. Create organization: "MyBots"
4. Create project: "RewardBot"
5. Create cluster:
   - Choose FREE tier (M0)
   - Provider: AWS
   - Region: Closest to you
   - Name: "rewardbot-cluster"
6. Wait 3-5 minutes for cluster creation

### 1.3 Setup MongoDB Security
1. **Database Access:**
   - Security → Database Access
   - Add New Database User
   - Username: `rewardbot_user`
   - Password: Click "Autogenerate Secure Password" → SAVE THIS!
   - Database User Privileges: Read and write to any database
   - Add User

2. **Network Access:**
   - Security → Network Access
   - Add IP Address
   - Allow Access from Anywhere: `0.0.0.0/0`
   - Confirm

3. **Get Connection String:**
   - Database → Connect
   - Choose: Connect your application
   - Driver: Python
   - Copy the connection string
   - Should look like: `mongodb+srv://rewardbot_user:<password>@...`
   - Replace `<password>` with your actual password

### 1.4 Get Telegram API Credentials
1. Go to https://my.telegram.org
2. Login with your phone number
3. API Development Tools
4. Create new application:
   - App title: "Naruto Reward Bot"
   - Short name: "naruto_bot"
   - Platform: Other
5. Save your `api_id` and `api_hash`

### 1.5 Create Telegram Bot
1. Open Telegram
2. Search for @BotFather
3. Send `/newbot`
4. Follow prompts:
   - Bot name: "Naruto Reward Bot"
   - Username: "naruto_reward_bot" (must end with 'bot')
5. Save the bot token (looks like: `123456:ABC-DEF...`)

---

## 📦 Part 2: Project Setup (10 minutes)

### 2.1 Download Project Files
Extract the `naruto-reward-bot-clean` folder to your desired location, for example:
- Windows: `C:\Users\YourName\Desktop\naruto-reward-bot`
- Mac/Linux: `~/projects/naruto-reward-bot`

### 2.2 Open Terminal in Project Folder
- **Windows:** Right-click folder → "Open in Terminal" or use CMD
- **Mac:** Right-click → Services → New Terminal at Folder
- **Linux:** Right-click → Open in Terminal

### 2.3 Install Dependencies
```bash
pip install -r requirements.txt
```

Wait for installation to complete (1-2 minutes).

### 2.4 Create .env File
Copy the template:
```bash
cp .env.example .env
```

Edit `.env` with your text editor and fill in:
```bash
MONGO_URL=mongodb+srv://rewardbot_user:YOUR_PASSWORD@cluster0.xxxxx.mongodb.net/
API_ID=your_api_id_from_telegram
API_HASH=your_api_hash_from_telegram
BOT_TOKEN=your_bot_token_from_botfather
PHONE_NUMBER=+1234567890
```

**Important:**
- Replace YOUR_PASSWORD in MONGO_URL with your actual MongoDB password
- No quotes around values
- PHONE_NUMBER must include country code with +

---

## 🗄️ Part 3: Database Setup (5 minutes)

### 3.1 Create Collections
```bash
python utils/setup_mongodb.py
```

**Expected output:**
```
🔧 Setting up MongoDB collections...
✅ Created: users
✅ Created: daily_stats
✅ Created: rewards
✅ Created: penalties
✅ Created: config
✅ Created: system_stats
✅ Created: action_logs

📊 Creating indexes...
✅ Users indexes created
✅ Daily stats indexes created
✅ Rewards indexes created
✅ Penalties indexes created
✅ Action logs indexes created

✅ MongoDB setup complete!
```

If you see errors, check:
- MONGO_URL in .env is correct
- Password doesn't have special characters (or they're URL-encoded)
- Network Access allows your IP

### 3.2 Create config.json (First Time Only)
Create a file called `config.json` in the project root:

```json
{
  "owner_id": 986380678,
  "log_channel_id": -1003780751228,
  "target_group_id": -1001862866818,
  "admin_ids": [986380678],
  "eligibility": {
    "required_bio_string": "@Naruto_X_Boruto_Bot",
    "required_groups_map": {
      "group_main": "@NarutoMainGroup"
    }
  },
  "reward_settings": {
    "base": {
      "mode": "random",
      "min": 1,
      "max": 3
    },
    "tiers": {
      "enabled": true,
      "bronze": {"range": [0, 150], "multiplier": 1.0},
      "silver": {"range": [151, 450], "multiplier": 1.1},
      "gold": {"range": [451, 900], "multiplier": 1.2},
      "platinum": {"range": [901, 1500], "multiplier": 1.3},
      "diamond": {"range": [1501, 2500], "multiplier": 1.4},
      "master": {"range": [2501, 5000], "multiplier": 1.5},
      "legend": {"range": [5001, 999999], "multiplier": 1.6}
    },
    "jackpot": {
      "enabled": true,
      "chance": 1,
      "amount": 10
    }
  },
  "spam_settings": {
    "threshold_seconds": 5,
    "ignore_duration_minutes": 15,
    "burst_limit": 3,
    "burst_window_seconds": 3,
    "global_flood_limit": 7,
    "global_flood_window": 3
  },
  "antispam_enabled": true,
  "milestones": {
    "enabled": true,
    "target_group": "@NarutoMainGroup",
    "events": {
      "2500": {
        "duration_hours": 24,
        "multiplier": 2.0,
        "jackpot_chance": 20
      },
      "5000": {
        "duration_hours": 48,
        "multiplier": 3.0,
        "jackpot_chance": 30
      }
    }
  }
}
```

**Replace these values:**
- `owner_id`: Your Telegram user ID
- `log_channel_id`: Your log channel ID (create a channel, add bot as admin)
- `target_group_id`: Your group ID where bot will work

**To get IDs:** Use @userinfobot on Telegram

---

## 🎮 Part 4: Running the Bot (5 minutes)

### 4.1 First Run
```bash
python main.py
```

**You'll see:**
```
🔄 Connecting to MongoDB...
✅ Connected to MongoDB
✅ All environment variables loaded
✅ Bot modules initialized
✅ Owner ID: 986380678
✅ Log Channel: -1003780751228
✅ Target Group: -1001862866818

🚀 Starting Telegram clients...
```

**Then you'll be asked:**
1. **Enter OTP:** Check your Telegram for login code
2. **Enter 2FA password:** If you have two-factor authentication

After successful login:
```
✅ Bot started successfully!
   Owner: YourName
   Logged in as bot and userbot

📡 Bot is now running. Press Ctrl+C to stop.
```

### 4.2 Test the Bot
1. Send a message in your group
2. Bot should process it silently
3. Check logs for any errors

### 4.3 Stop the Bot
Press `Ctrl+C` in the terminal

---

## ✅ Part 5: Verification (5 minutes)

### 5.1 Check MongoDB
```bash
python -c "from pymongo import MongoClient; import os; from dotenv import load_dotenv; load_dotenv(); client = MongoClient(os.getenv('MONGO_URL')); db = client['rewardbot']; print('Users:', db.users.count_documents({})); print('Config:', db.config.count_documents({}))"
```

Should show:
```
Users: 0 (or more if you have data)
Config: 1
```

### 5.2 Check Bot Commands
In your group, send:
- `/start` - Should get welcome message
- `/stats` - Should show stats
- `/rules` - Should show rules

### 5.3 Check Admin Commands
As admin, send:
- `/setreward 5` - Should confirm
- `/admins` - Should list you

---

## 🎯 Common Issues & Solutions

### Issue: "ModuleNotFoundError: No module named 'core'"
**Solution:**
- Make sure you're in the project directory
- Check that `core/__init__.py` exists
- Run: `python -c "import core; print('OK')"`

### Issue: "MONGO_URL not found"
**Solution:**
- Check `.env` file exists in project root
- Check no typos in MONGO_URL
- Try: `python -c "import os; from dotenv import load_dotenv; load_dotenv(); print(os.getenv('MONGO_URL'))"`

### Issue: "Could not connect to MongoDB"
**Solution:**
- Check MongoDB Atlas cluster is running
- Verify Network Access allows 0.0.0.0/0
- Test connection string in MongoDB Compass

### Issue: "Phone number already in use"
**Solution:**
- Delete old session files: `rm -rf session/*.session`
- Run bot again

### Issue: "Bot doesn't respond to commands"
**Solution:**
- Check bot is admin in the group
- Check TARGET_GROUP_ID is correct
- Look at console for error messages

---

## 📦 Part 6: Deployment (Optional)

### For Render.com:
1. Push code to GitHub
2. Create new Background Worker on Render
3. Connect repository
4. Add all environment variables from .env
5. Deploy

See full deployment guide in the documentation you received earlier.

---

## 🎉 Success!

Your bot is now running! Here's what you should do next:

1. ✅ Test all commands as admin
2. ✅ Add other admins using `/addadmin`
3. ✅ Configure rewards using `/setreward`
4. ✅ Test with a few messages
5. ✅ Monitor logs for any issues
6. ✅ Deploy to cloud (Render/Heroku) for 24/7 uptime

---

## 📞 Need Help?

Check these resources:
- README.md - General documentation
- GitHub Issues - Report bugs
- MongoDB Atlas docs - Database help
- Telethon docs - Bot API help

---

**Estimated Total Time:** 40 minutes  
**Difficulty:** Medium  
**Success Rate:** 95% if you follow carefully

Happy botting! 🎮
