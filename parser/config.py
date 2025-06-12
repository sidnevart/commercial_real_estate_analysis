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
    "area_search_radius": 5,
    "debug_search_radius": 100,
    
    # Новые настройки
    "lot_save_interval": 5,           # Сохранять лоты каждые N обработанных
    "browser_refresh_interval": 20,   # Перезапускать браузер после N операций
    
    # Настройки для CIAN
    "cian": {
        "use_proxies": False,              # Использовать прокси (пока False)
        "rotate_proxies": False,           # Ротировать прокси при блокировке
        "stealth_mode": True,              # Режим скрытности (дополнительная защита)
        "max_retries": 3,                  # Количество попыток при ошибке
        "cooldown_after_block": 30,        # Пауза после обнаружения блокировки (сек)
        "session_limit": 20,               # Количество запросов за одну сессию
        "save_cookies": True,              # Сохранять рабочие куки
        "debug_save_html": True,           # Сохранять HTML для отладки
        "requests_interval": [3, 8]        # Интервал между запросами (мин, макс)
    }
}