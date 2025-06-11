import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Google Sheets
GSHEET_ID = os.getenv("GSHEET_ID")
GSHEET_CREDS_PATH = Path(os.getenv("GSHEET_CREDS_PATH", "google-auth/sa_gsheet.json"))

# Proxies
PROXY_FILE = Path(os.getenv("PROXY_FILE", "proxies.txt"))

# Selenium / Playwright
PAGELOAD_TIMEOUT = int(os.getenv("PAGELOAD_TIMEOUT", 120))