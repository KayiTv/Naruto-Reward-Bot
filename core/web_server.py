from flask import Flask
import threading
import os
import logging

import time
import requests

app = Flask(__name__)

# Disable Flask's default logging to keep terminal clean
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

@app.route('/')
def home():
    return "Bot is Online", 200

def ping_self():
    """Periodically ping the home route to prevent Render from sleeping"""
    # Wait for server to start
    time.sleep(10)
    
    # Get the URL from environment if set (e.g., https://myapp.onrender.com)
    # If not set, it will just fail gracefully until configured
    url = os.environ.get("RENDER_EXTERNAL_URL")
    if not url:
        print("[INFO] Uptime: RENDER_EXTERNAL_URL not set. Skipping self-ping.")
        return

    print(f"🚀 Uptime: Starting self-pinging loop for {url}")
    while True:
        try:
            requests.get(url)
            # print("✅ Uptime: Ping successful")
        except Exception as e:
            print(f"⚠️ Uptime: Ping failed: {e}")
        
        # Sleep for 14 minutes (Render sleeps after 15 mins)
        time.sleep(14 * 60)

def run():
    port = int(os.environ.get("PORT", 8080))
    # Use 0.0.0.0 to be accessible externally
    app.run(host='0.0.0.0', port=port)

def start_server():
    """Start the health-check server and self-ping thread"""
    # Start web server
    server_thread = threading.Thread(target=run)
    server_thread.daemon = True
    server_thread.start()
    
    # Start self-pinging loop
    ping_thread = threading.Thread(target=ping_self)
    ping_thread.daemon = True
    ping_thread.start()
    
    print(f"✅ Health-check server started on port {os.environ.get('PORT', 8080)}")
