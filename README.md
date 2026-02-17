# 🎮 Naruto Reward Bot

A Telegram reward bot for the Naruto community with MongoDB integration, spam protection, tier system, and milestone celebrations.

## ✨ Features

- ✅ **Dual-Client System:** Bot (frontend) + Userbot (backend for executing commands)
- ✅ **MongoDB Integration:** Scalable cloud database storage
- ✅ **Reward System:** Fixed or random rewards with tier multipliers
- ✅ **Spam Protection:** Multi-layer spam detection (burst, flood, duplicate, low-quality)
- ✅ **Tier System:** Bronze → Legend tiers with increasing multipliers
- ✅ **Jackpot System:** Random chance for bonus rewards
- ✅ **Milestone Celebrations:** Group growth rewards with multiplier events
- ✅ **Admin Commands:** Full suite of moderation and configuration tools
- ✅ **Daily Stats:** Track activity, leaderboards, and rankings

## 📋 Prerequisites

- Python 3.8 or higher
- MongoDB Atlas account (free tier works)
- Telegram API credentials
- Bot token from @BotFather

## 🚀 Quick Start

### 1. Clone Repository

```bash
git clone https://github.com/yourusername/naruto-reward-bot.git
cd naruto-reward-bot
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Setup Environment Variables

Copy `.env.example` to `.env` and fill in your credentials:

```bash
cp .env.example .env
```

Edit `.env`:
```bash
MONGO_URL=mongodb+srv://username:password@cluster.mongodb.net/
API_ID=your_api_id
API_HASH=your_api_hash
BOT_TOKEN=your_bot_token
PHONE_NUMBER=+1234567890
```

**Where to get credentials:**
- `MONGO_URL`: MongoDB Atlas → Database → Connect → Connection String
- `API_ID` & `API_HASH`: https://my.telegram.org/apps
- `BOT_TOKEN`: @BotFather on Telegram
- `PHONE_NUMBER`: Your phone number (for userbot)

### 4. Setup MongoDB

Create `config.json` with your bot configuration (see config.example.json), then run:

```bash
python utils/setup_mongodb.py
python utils/import_to_mongo.py
```

### 5. Run Bot

```bash
python main.py
```

On first run, you'll be asked for:
- OTP (one-time password sent to your Telegram)
- 2FA password (if enabled)

## 📁 Project Structure

```
naruto-reward-bot/
├── main.py                 # Main bot file
├── requirements.txt        # Python dependencies
├── .env                    # Environment variables (not in git)
├── .env.example            # Template for .env
├── .gitignore              # Git ignore rules
├── README.md               # This file
│
├── core/                   # Core bot modules
│   ├── __init__.py
│   ├── storage_mongodb.py  # Database operations
│   ├── event_manager.py    # Message counting & events
│   ├── eligibility.py      # User eligibility checks
│   ├── spam_check.py       # Spam detection system
│   ├── logger.py           # Logging to channel
│   └── milestones.py       # Milestone tracking
│
├── utils/                  # Utility scripts
│   ├── __init__.py
│   ├── setup_mongodb.py    # Initial MongoDB setup
│   └── import_to_mongo.py  # Data migration script
│
├── session/                # Session files (gitignored)
├── data/                   # Data backups (gitignored)
└── backup/                 # Archived data (gitignored)
```

## 🎯 Key Commands

### User Commands
- `/start` - Welcome message
- `/eligible` - Check eligibility
- `/rules` - View requirements
- `/stats` - Bot statistics
- `/mytier` - Check your tier
- `/top` - Today's leaderboard

### Admin Commands
- `/setreward <amount>` - Set reward amount
- `/setinterval <min> <max>` - Set message interval
- `/ban <user>` - Ban from rewards
- `/unban <user>` - Remove ban
- `/addadmin <user>` - Add admin
- `/antispam on/off` - Toggle spam protection

## 🔒 Security

- ✅ `.env` file excluded from git
- ✅ Session files excluded from git
- ✅ MongoDB connection secured with password
- ✅ Admin-only commands protected
- ✅ Input validation on all commands

## 🐛 Troubleshooting

### "ModuleNotFoundError: No module named 'core'"
- Make sure you're running from the project root directory
- Check that `core/__init__.py` exists

### "MONGO_URL not found"
- Verify `.env` file exists
- Check environment variables are loaded
- Ensure `python-dotenv` is installed

### "Could not connect to MongoDB"
- Verify MongoDB Atlas cluster is running
- Check connection string in `.env`
- Ensure IP whitelist includes your IP (or 0.0.0.0/0)

### "Bot not responding"
- Check bot is running without errors
- Verify bot is in target group
- Check log channel for error messages

## 📊 Database Structure

The bot uses MongoDB with the following collections:

- `users` - User profiles and stats
- `daily_stats` - Daily activity tracking
- `rewards` - Reward history
- `penalties` - Spam penalties
- `config` - Bot configuration
- `system_stats` - Global statistics
- `action_logs` - Admin action logs

## 🔄 Backup & Restore

### Create Backup
```bash
mongodump --uri="your_mongo_url" --out=backup/
```

### Restore Backup
```bash
mongorestore --uri="your_mongo_url" backup/
```

## 🚀 Deployment

### Render.com (Free)
1. Push code to GitHub
2. Create new Web Service on Render
3. Connect repository
4. Add environment variables
5. Deploy!

See full deployment guide in `docs/DEPLOYMENT.md`

## 📝 License

This project is for educational purposes. Use responsibly and comply with Telegram's Terms of Service.

## 🤝 Contributing

Contributions welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## 📞 Support

For issues and questions:
- Open an issue on GitHub
- Check existing issues for solutions
- Read the documentation

## ⚠️ Disclaimer

This bot is for educational purposes. The developers are not responsible for misuse or violations of Telegram's Terms of Service.

---

**Made with ❤️ for the Naruto community**
