import os
import sys
from telethon import TelegramClient
from telethon.sessions import StringSession
from dotenv import load_dotenv

load_dotenv()

def convert_session(session_name):
    session_file = f"{session_name}.session"
    if not os.path.exists(session_file):
        print(f"ERROR: {session_file} not found in current directory.")
        return

    api_id = os.getenv('API_ID')
    api_hash = os.getenv('API_HASH')

    if not api_id or not api_hash:
        print("ERROR: API_ID and API_HASH must be set in .env")
        return

    print(f"INFO: Converting {session_file}...")
    
    try:
        # Load existing file session
        client = TelegramClient(session_name, int(api_id), api_hash)
        
        async def get_string():
            await client.connect()
            # client.session.save() is not needed here, we just want the string
            string = StringSession.save(client.session)
            return string

        import asyncio
        loop = asyncio.get_event_loop()
        string_session = loop.run_until_complete(get_string())
        
        print(f"\nSUCCESS! Here is your {session_name} String Session:\n")
        print("-" * 50)
        print(string_session)
        print("-" * 50)
        print("\nCopy the long string above and use it as your ENV variable on Render.")
        
    except Exception as e:
        print(f"ERROR: Failed to convert: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python utils/convert_session_to_string.py <session_basename>")
        print("Example: python utils/convert_session_to_string.py userbot")
        sys.exit(1)
        
    convert_session(sys.argv[1])
