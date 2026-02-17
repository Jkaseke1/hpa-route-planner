import hashlib
import pandas as pd
import smtplib
from email.message import EmailMessage
import os
from datetime import datetime

# =========================
# CONFIG
# =========================
DATA_FILE = "data/facilities_raw.csv"
HASH_FILE = "data/last_hash.txt"

SENDER_EMAIL = "mkanganwigrace4@gmail.com"
RECEIVER_EMAIL = "nmkanganwi@pulse-pharmaceuticals.co.zw"

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

# Use Gmail App Password (NOT your normal Gmail password)
EMAIL_PASSWORD = "PUT_YOUR_GMAIL_APP_PASSWORD_HERE"

# =========================
# HELPERS
# =========================
def file_hash(path):
    with open(path, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()

# =========================
# MAIN LOGIC
# =========================
if not os.path.exists(DATA_FILE):
    print("❌ Data file not found.")
    exit()

new_hash = file_hash(DATA_FILE)
old_hash = ""

if os.path.exists(HASH_FILE):
    with open(HASH_FILE, "r") as f:
        old_hash = f.read().strip()

if new_hash != old_hash:
    print("🔔 Change detected — sending email alert")

    msg = EmailMessage()
    msg["Subject"] = "🚨 HPA Facilities Data Change Detected"
    msg["From"] = SENDER_EMAIL
    msg["To"] = RECEIVER_EMAIL

    msg.set_content(f"""
Hello,

A change has been detected in the HPA facilities dataset.

📅 Date: {datetime.now().strftime('%d %b %Y')}
⏰ Time: {datetime.now().strftime('%H:%M:%S')}

The dashboard has been refreshed with the latest data.

Regards,
HPA Intelligence Dashboard
""")

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SENDER_EMAIL, EMAIL_PASSWORD)
            server.send_message(msg)

        with open(HASH_FILE, "w") as f:
            f.write(new_hash)

        print("✅ Email alert sent successfully")

    except Exception as e:
        print("❌ Failed to send email:", e)

else:
    print("ℹ️ No data change detected")
