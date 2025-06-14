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


# Добавить новые параметры конфигурации
CONFIG = {
    # Существующие настройки
    "area_search_radius": 7,
    "debug_search_radius": 7,
    
    # Новые настройки
    "lot_save_interval": 5,           # Сохранять лоты каждые N обработанных
    "browser_refresh_interval": 20,   # Перезапускать браузер после N операций
    
    # Настройки для CIAN
    "cian": {
        "use_proxies": False,               # Включаем использование прокси
        "rotate_proxies": False,            # Включаем ротацию прокси при блокировке
        "stealth_mode": True,              # Режим скрытности (дополнительная защита)
        "max_retries": 5,                  # Увеличиваем количество попыток
        "cooldown_after_block": 45,        # Увеличиваем паузу после блокировки
        "session_limit": 15,               # Уменьшаем количество запросов на сессию
        "save_cookies": True,              # Сохраняем рабочие куки
        "debug_save_html": True,           # Сохраняем HTML для отладки
        "requests_interval": [5, 12]       # Увеличиваем интервал между запросами
    }
}