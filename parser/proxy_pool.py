"""from itertools import cycle
from pathlib import Path
from typing import Optional
from .config import PROXY_FILE
import logging

_raw: list[str] = []
if PROXY_FILE.exists():
    _raw = [ln.strip() for ln in PROXY_FILE.read_text().splitlines() if ln.strip()]

_parsed: list[str] = []
for ln in _raw:
    parts = ln.split(":")
    if len(parts) == 4:
        host, port, user, pwd = parts
        _parsed.append(f"http://{user}:{pwd}@{host}:{port}")
    elif len(parts) == 2:
        host, port = parts
        _parsed.append(f"http://{host}:{port}")

_cycle = cycle(_parsed) if _parsed else None

def get() -> Optional[str]:
    if not _cycle:
        return None
    proxy = next(_cycle)
    logging.getLogger(__name__).info(f"[proxy] {proxy}")
    logging.debug(f"[proxy] {proxy}")
    return proxy

def drop(bad: str):
    global _parsed, _cycle
    _parsed = [p for p in _parsed if p != bad]
    _cycle = cycle(_parsed) if _parsed else None"""

import os
import random
import logging
from pathlib import Path

log = logging.getLogger(__name__)
used_proxies = set()  # Для отслеживания использованных прокси

def get():
    """Получить следующий доступный прокси из списка"""
    global used_proxies
    
    proxy_file = Path(os.path.dirname(os.path.dirname(__file__))) / "proxies.txt"
    
    if not proxy_file.exists() or proxy_file.stat().st_size == 0:
        log.warning(f"Файл прокси {proxy_file} не существует или пуст")
        return None
        
    # Читаем все прокси из файла
    with open(proxy_file, 'r') as f:
        proxies = [line.strip() for line in f if line.strip() and not line.startswith('//') and not line.startswith('#')]
    
    if not proxies:
        log.warning("Нет доступных прокси")
        return None
    
    # Ищем неиспользованные прокси
    available_proxies = [p for p in proxies if p not in used_proxies]
    
    # Если все использованы, сбрасываем
    if not available_proxies:
        used_proxies.clear()
        available_proxies = proxies
    
    # Выбираем случайный прокси
    proxy = random.choice(available_proxies)
    used_proxies.add(proxy)
    
    # Форматируем прокси
    if len(proxy.split(':')) == 2:  # IP:PORT
        #log.info(f"Используется прокси: {proxy}")
        return proxy
    elif len(proxy.split(':')) == 4:  # IP:PORT:USERNAME:PASSWORD
        ip, port, username, password = proxy.split(':')
        formatted = f"http://{username}:{password}@{ip}:{port}"
        #log.info(f"Используется авторизованный прокси: {ip}:{port}")
        return formatted
    else:
        log.warning(f"Неправильный формат прокси: {proxy}")
        return None

def drop(proxy):
    """Отметить прокси как неработающий"""
    if proxy in used_proxies:
        used_proxies.remove(proxy)
    #log.info(f"Прокси {proxy} помечен как неработающий")