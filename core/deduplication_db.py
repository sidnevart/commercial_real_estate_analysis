import sqlite3
import hashlib
import logging
import os
from typing import Optional, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)

class DeduplicationDB:
    def __init__(self, db_path: str = "data/deduplication.db"):
        self.db_path = db_path
        # Создаем директорию если не существует
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._init_db()
    
    def _init_db(self):
        """Инициализация базы данных"""
        with sqlite3.connect(self.db_path) as conn:
            # ИСПРАВЛЕНО: убираем INDEX из CREATE TABLE
            conn.execute("""
                CREATE TABLE IF NOT EXISTS lots (
                    id TEXT PRIMARY KEY,
                    address_hash TEXT NOT NULL,
                    area REAL NOT NULL,
                    price REAL NOT NULL,
                    notice_number TEXT,
                    first_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
                    last_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
                    times_seen INTEGER DEFAULT 1,
                    last_price REAL,
                    price_changed BOOLEAN DEFAULT FALSE
                )
            """)
            
            # ИСПРАВЛЕНО: создаем индексы отдельно
            conn.execute("CREATE INDEX IF NOT EXISTS idx_address_hash ON lots(address_hash)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_first_seen ON lots(first_seen)")
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS processed_lots (
                    lot_id TEXT PRIMARY KEY,
                    processed_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                    has_analytics BOOLEAN DEFAULT FALSE,
                    sent_to_telegram BOOLEAN DEFAULT FALSE
                )
            """)
    
    def _get_lot_signature(self, lot) -> str:
        """Создает уникальную подпись лота"""
        # Используем адрес + площадь как основу для идентификации
        signature_data = f"{lot.address.strip().lower()}|{lot.area}|{lot.notice_number}"
        return hashlib.md5(signature_data.encode()).hexdigest()
    
    def is_duplicate(self, lot) -> tuple[bool, Optional[Dict]]:
        """
        Проверяет, является ли лот дубликатом
        Returns: (is_duplicate, existing_lot_info)
        """
        signature = self._get_lot_signature(lot)
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT * FROM lots WHERE address_hash = ? AND area = ?",
                (signature, lot.area)
            )
            existing = cursor.fetchone()
            
            if existing:
                # Проверяем, изменилась ли цена
                price_changed = abs(existing[3] - lot.price) > 1000  # Изменение больше 1000 руб
                
                if price_changed:
                    # Обновляем информацию о лоте
                    conn.execute("""
                        UPDATE lots 
                        SET last_seen = CURRENT_TIMESTAMP, 
                            times_seen = times_seen + 1,
                            last_price = price,
                            price = ?,
                            price_changed = 1
                        WHERE address_hash = ? AND area = ?
                    """, (lot.price, signature, lot.area))
                    
                    logger.info(f"📊 Лот {lot.id}: цена изменилась с {existing[3]:,.0f} на {lot.price:,.0f}")
                    return False, {"price_changed": True, "old_price": existing[3]}
                else:
                    # Просто обновляем время последнего появления
                    conn.execute("""
                        UPDATE lots 
                        SET last_seen = CURRENT_TIMESTAMP, 
                            times_seen = times_seen + 1
                        WHERE address_hash = ? AND area = ?
                    """, (signature, lot.area))
                    
                    return True, {"existing": True, "times_seen": existing[7]}
            
            return False, None
    
    def add_lot(self, lot):
        """Добавляет новый лот в базу"""
        signature = self._get_lot_signature(lot)
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO lots (id, address_hash, area, price, notice_number, last_price)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (lot.id, signature, lot.area, lot.price, lot.notice_number, lot.price))
    
    def mark_processed(self, lot_id: str, has_analytics: bool = False, sent_to_telegram: bool = False):
        """Отмечает лот как обработанный"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO processed_lots 
                (lot_id, has_analytics, sent_to_telegram)
                VALUES (?, ?, ?)
            """, (lot_id, has_analytics, sent_to_telegram))
    
    def get_stats(self) -> Dict[str, Any]:
        """Получает статистику базы данных"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM lots")
            total_lots = cursor.fetchone()[0]
            
            cursor = conn.execute("SELECT COUNT(*) FROM lots WHERE price_changed = 1")
            price_changed_lots = cursor.fetchone()[0]
            
            cursor = conn.execute("SELECT COUNT(*) FROM processed_lots")
            processed_lots = cursor.fetchone()[0]
            
            return {
                "total_lots": total_lots,
                "price_changed_lots": price_changed_lots,
                "processed_lots": processed_lots
            }

# Глобальный экземпляр
dedup_db = DeduplicationDB()