"""
SQLite база данных для дедупликации лотов и объявлений
"""

import sqlite3
import logging
import hashlib
from pathlib import Path
from typing import List, Set, Optional, Dict, Any
from datetime import datetime
from contextlib import contextmanager

logger = logging.getLogger(__name__)

# Путь к базе данных
DB_PATH = Path(__file__).parent.parent / "data" / "dedupe.db"

class Database:
    """Управление SQLite базой данных для дедупликации"""
    
    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        # Создаем директорию если не существует
        self.db_path.parent.mkdir(exist_ok=True)
        self.init_db()
    
    @contextmanager
    def get_connection(self):
        """Контекстный менеджер для работы с БД"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # Доступ к колонкам по имени
        try:
            yield conn
        finally:
            conn.close()
    
    def init_db(self):
        """Инициализация базы данных"""
        with self.get_connection() as conn:
            # Таблица для лотов
            conn.execute("""
                CREATE TABLE IF NOT EXISTS lots (
                    id TEXT PRIMARY KEY,
                    lot_hash TEXT UNIQUE NOT NULL,
                    name TEXT NOT NULL,
                    address TEXT NOT NULL,
                    area REAL,
                    price REAL,
                    auction_url TEXT,
                    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    parse_count INTEGER DEFAULT 1
                )
            """)
            
            # Таблица для объявлений
            conn.execute("""
                CREATE TABLE IF NOT EXISTS offers (
                    id TEXT PRIMARY KEY,
                    offer_hash TEXT UNIQUE NOT NULL,
                    cian_id TEXT,
                    address TEXT NOT NULL,
                    price REAL,
                    area REAL,
                    offer_type TEXT, -- 'sale' or 'rent'
                    url TEXT,
                    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    parse_count INTEGER DEFAULT 1
                )
            """)
            
            # Таблица связей лот-объявления
            conn.execute("""
                CREATE TABLE IF NOT EXISTS lot_offers (
                    lot_id TEXT,
                    offer_id TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (lot_id, offer_id),
                    FOREIGN KEY (lot_id) REFERENCES lots (id),
                    FOREIGN KEY (offer_id) REFERENCES offers (id)
                )
            """)
            
            # Индексы для быстрого поиска
            conn.execute("CREATE INDEX IF NOT EXISTS idx_lots_hash ON lots (lot_hash)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_offers_hash ON offers (offer_hash)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_offers_cian_id ON offers (cian_id)")
            
            conn.commit()
            logger.info("База данных инициализирована")
    
    def _calculate_lot_hash(self, lot_data: Dict[str, Any]) -> str:
        """Вычисляет хеш лота для дедупликации"""
        # Используем комбинацию адреса, площади и цены
        key_data = {
            "address": str(lot_data.get("address", "")).lower().strip(),
            "area": float(lot_data.get("area", 0)),
            "price": float(lot_data.get("price", 0))
        }
        
        # Создаем строку для хеширования
        hash_string = f"{key_data['address']}_{key_data['area']}_{key_data['price']}"
        return hashlib.md5(hash_string.encode()).hexdigest()
    
    def _calculate_offer_hash(self, offer_data: Dict[str, Any]) -> str:
        """Вычисляет хеш объявления для дедупликации"""
        # Используем CIAN ID если есть, иначе адрес + цену + площадь
        if offer_data.get("cian_id"):
            return hashlib.md5(str(offer_data["cian_id"]).encode()).hexdigest()
        
        key_data = {
            "address": str(offer_data.get("address", "")).lower().strip(),
            "price": float(offer_data.get("price", 0)),
            "area": float(offer_data.get("area", 0)),
            "type": str(offer_data.get("type", ""))
        }
        
        hash_string = f"{key_data['address']}_{key_data['price']}_{key_data['area']}_{key_data['type']}"
        return hashlib.md5(hash_string.encode()).hexdigest()
    
    def is_lot_duplicate(self, lot_data: Dict[str, Any]) -> bool:
        """Проверяет, является ли лот дубликатом"""
        lot_hash = self._calculate_lot_hash(lot_data)
        
        with self.get_connection() as conn:
            cursor = conn.execute(
                "SELECT id FROM lots WHERE lot_hash = ?", 
                (lot_hash,)
            )
            return cursor.fetchone() is not None
    
    def is_offer_duplicate(self, offer_data: Dict[str, Any]) -> bool:
        """Проверяет, является ли объявление дубликатом"""
        offer_hash = self._calculate_offer_hash(offer_data)
        
        with self.get_connection() as conn:
            cursor = conn.execute(
                "SELECT id FROM offers WHERE offer_hash = ?", 
                (offer_hash,)
            )
            return cursor.fetchone() is not None
    
    def add_lot(self, lot_data: Dict[str, Any]) -> bool:
        """Добавляет лот в базу, возвращает True если новый"""
        lot_hash = self._calculate_lot_hash(lot_data)
        
        with self.get_connection() as conn:
            # Проверяем существование
            cursor = conn.execute(
                "SELECT id, parse_count FROM lots WHERE lot_hash = ?", 
                (lot_hash,)
            )
            existing = cursor.fetchone()
            
            if existing:
                # Обновляем счетчик и время последнего появления
                conn.execute("""
                    UPDATE lots 
                    SET last_seen = CURRENT_TIMESTAMP, parse_count = parse_count + 1 
                    WHERE lot_hash = ?
                """, (lot_hash,))
                conn.commit()
                logger.debug(f"Лот уже существует: {lot_data.get('name', 'Unknown')}")
                return False
            else:
                # Добавляем новый лот
                conn.execute("""
                    INSERT INTO lots (id, lot_hash, name, address, area, price, auction_url)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    lot_data.get("id"),
                    lot_hash,
                    lot_data.get("name", ""),
                    lot_data.get("address", ""),
                    lot_data.get("area", 0),
                    lot_data.get("price", 0),
                    lot_data.get("auction_url", "")
                ))
                conn.commit()
                logger.info(f"Новый лот добавлен: {lot_data.get('name', 'Unknown')}")
                return True
    
    def add_offer(self, offer_data: Dict[str, Any]) -> bool:
        """Добавляет объявление в базу, возвращает True если новое"""
        offer_hash = self._calculate_offer_hash(offer_data)
        
        with self.get_connection() as conn:
            # Проверяем существование
            cursor = conn.execute(
                "SELECT id, parse_count FROM offers WHERE offer_hash = ?", 
                (offer_hash,)
            )
            existing = cursor.fetchone()
            
            if existing:
                # Обновляем счетчик и время последнего появления
                conn.execute("""
                    UPDATE offers 
                    SET last_seen = CURRENT_TIMESTAMP, parse_count = parse_count + 1 
                    WHERE offer_hash = ?
                """, (offer_hash,))
                conn.commit()
                logger.debug(f"Объявление уже существует: {offer_data.get('cian_id', 'Unknown')}")
                return False
            else:
                # Добавляем новое объявление
                conn.execute("""
                    INSERT INTO offers (id, offer_hash, cian_id, address, price, area, offer_type, url)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    offer_data.get("id"),
                    offer_hash,
                    offer_data.get("cian_id"),
                    offer_data.get("address", ""),
                    offer_data.get("price", 0),
                    offer_data.get("area", 0),
                    offer_data.get("type", ""),
                    offer_data.get("url", "")
                ))
                conn.commit()
                logger.info(f"Новое объявление добавлено: {offer_data.get('cian_id', 'Unknown')}")
                return True
    
    def link_lot_offer(self, lot_id: str, offer_id: str):
        """Связывает лот с объявлением"""
        with self.get_connection() as conn:
            conn.execute("""
                INSERT OR IGNORE INTO lot_offers (lot_id, offer_id)
                VALUES (?, ?)
            """, (lot_id, offer_id))
            conn.commit()
    
    def get_stats(self) -> Dict[str, int]:
        """Возвращает статистику базы данных"""
        with self.get_connection() as conn:
            stats = {}
            
            # Общее количество лотов
            cursor = conn.execute("SELECT COUNT(*) FROM lots")
            stats["total_lots"] = cursor.fetchone()[0]
            
            # Общее количество объявлений
            cursor = conn.execute("SELECT COUNT(*) FROM offers")
            stats["total_offers"] = cursor.fetchone()[0]
            
            # Связи
            cursor = conn.execute("SELECT COUNT(*) FROM lot_offers")
            stats["total_links"] = cursor.fetchone()[0]
            
            # Новые за последние 24 часа
            cursor = conn.execute("""
                SELECT COUNT(*) FROM lots 
                WHERE first_seen > datetime('now', '-1 day')
            """)
            stats["new_lots_24h"] = cursor.fetchone()[0]
            
            cursor = conn.execute("""
                SELECT COUNT(*) FROM offers 
                WHERE first_seen > datetime('now', '-1 day')
            """)
            stats["new_offers_24h"] = cursor.fetchone()[0]
            
            return stats
    
    def cleanup_old_data(self, days: int = 30):
        """Очищает старые данные (старше N дней)"""
        with self.get_connection() as conn:
            # Удаляем связи для старых данных
            conn.execute("""
                DELETE FROM lot_offers 
                WHERE lot_id IN (
                    SELECT id FROM lots 
                    WHERE last_seen < datetime('now', '-{} days')
                )
            """.format(days))
            
            # Удаляем старые лоты
            cursor = conn.execute("""
                DELETE FROM lots 
                WHERE last_seen < datetime('now', '-{} days')
            """.format(days))
            lots_deleted = cursor.rowcount
            
            # Удаляем старые объявления
            cursor = conn.execute("""
                DELETE FROM offers 
                WHERE last_seen < datetime('now', '-{} days')
            """.format(days))
            offers_deleted = cursor.rowcount
            
            conn.commit()
            
            logger.info(f"Очищено старых данных: {lots_deleted} лотов, {offers_deleted} объявлений")
            return {"lots_deleted": lots_deleted, "offers_deleted": offers_deleted}

# Глобальный экземпляр базы данных
db = Database()
