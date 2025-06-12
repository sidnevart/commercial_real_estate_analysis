import os
import random
import logging
from typing import Optional, List, Dict

logger = logging.getLogger(__name__)

class ProxyRotator:
    """Управляет ротацией прокси для обхода блокировок"""
    
    def __init__(self, proxy_file="proxies.txt"):
        self.proxy_list = []
        self.current_index = -1
        self.load_proxies(proxy_file)
        self.banned_proxies = set()
        
    def load_proxies(self, proxy_file: str):
        """Загружает список прокси из файла"""
        try:
            if os.path.exists(proxy_file):
                with open(proxy_file, "r") as f:
                    lines = f.readlines()
                    self.proxy_list = [line.strip() for line in lines if line.strip()]
                    logger.info(f"Загружено {len(self.proxy_list)} прокси-адресов")
            else:
                logger.warning(f"Файл прокси {proxy_file} не найден")
        except Exception as e:
            logger.error(f"Ошибка при загрузке прокси: {str(e)}")
    
    def get_next_proxy(self) -> Optional[Dict]:
        """Возвращает следующий прокси в формате для Selenium"""
        if not self.proxy_list:
            logger.warning("Список прокси пуст")
            return None
            
        # Пробуем найти прокси, которого нет в бан-листе
        attempts = 0
        while attempts < len(self.proxy_list):
            self.current_index = (self.current_index + 1) % len(self.proxy_list)
            proxy_str = self.proxy_list[self.current_index]
            
            if proxy_str in self.banned_proxies:
                attempts += 1
                continue
                
            try:
                if "://" in proxy_str:
                    protocol, address = proxy_str.split("://")
                else:
                    protocol, address = "http", proxy_str
                    
                proxy = {
                    "proxy": {
                        "http": f"{protocol}://{address}",
                        "https": f"{protocol}://{address}",
                        "no_proxy": "localhost,127.0.0.1"
                    }
                }
                
                logger.info(f"Используем прокси: {protocol}://{address}")
                return proxy
                
            except Exception as e:
                logger.error(f"Ошибка при обработке прокси {proxy_str}: {str(e)}")
                self.banned_proxies.add(proxy_str)
                
            attempts += 1
                
        logger.warning("Не удалось найти работающий прокси")
        return None
        
    def mark_proxy_banned(self, proxy: Dict):
        """Помечает текущий прокси как забаненный"""
        try:
            proxy_str = proxy["proxy"]["http"].replace("http://", "")
            self.banned_proxies.add(proxy_str)
            logger.info(f"Прокси {proxy_str} добавлен в бан-лист")
        except:
            pass
            
    def get_random_proxy(self) -> Optional[Dict]:
        """Возвращает случайный прокси"""
        if not self.proxy_list:
            return None
            
        available_proxies = [p for p in self.proxy_list if p not in self.banned_proxies]
        if not available_proxies:
            return None
            
        proxy_str = random.choice(available_proxies)
        try:
            if "://" in proxy_str:
                protocol, address = proxy_str.split("://")
            else:
                protocol, address = "http", proxy_str
                
            proxy = {
                "proxy": {
                    "http": f"{protocol}://{address}",
                    "https": f"{protocol}://{address}",
                    "no_proxy": "localhost,127.0.0.1"
                }
            }
            
            return proxy
        except:
            return None