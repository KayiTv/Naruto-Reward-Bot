from flask import Flask
import threading
import os
import logging

app = Flask(__name__)

# Disable Flask's default logging to keep terminal clean
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

@app.route('/')
def home():
    return "Bot is Online", 200

def run():
    port = int(os.environ.get("PORT", 8080))
    # Use 0.0.0.0 to be accessible externally
    app.run(host='0.0.0.0', port=port)

def start_server():
    """Start the health-check server in a background thread"""
    server_thread = threading.Thread(target=run)
    server_thread.daemon = True
    server_thread.start()
    print(f"✅ Health-check server started on port {os.environ.get('PORT', 8080)}")
