"""
База данных для дедупликации лотов и объявлений
"""
# filepath: core/deduplication_db.py

import sqlite3
import logging
import hashlib
from pathlib import Path
from typing import List, Set, Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class DeduplicationDB:
    def __init__(self, db_path: str = "data/deduplication.db"):
        """Инициализация БД для дедупликации"""
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(exist_ok=True)
        self._init_db()
    
    def _init_db(self):
        """Создает таблицы БД"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Таблица обработанных лотов
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS processed_lots (
                        lot_id TEXT PRIMARY KEY,
                        lot_hash TEXT NOT NULL,
                        address TEXT,
                        area REAL,
                        price INTEGER,
                        first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        times_seen INTEGER DEFAULT 1
                    )
                """)
                
                # Таблица обработанных объявлений
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS processed_offers (
                        offer_id TEXT PRIMARY KEY,
                        offer_hash TEXT NOT NULL,
                        url TEXT,
                        address TEXT,
                        area REAL,
                        price INTEGER,
                        offer_type TEXT,
                        first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        times_seen INTEGER DEFAULT 1
                    )
                """)
                
                # Индексы для быстрого поиска
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_lot_hash ON processed_lots(lot_hash)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_offer_hash ON processed_offers(offer_hash)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_last_seen_lots ON processed_lots(last_seen)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_last_seen_offers ON processed_offers(last_seen)")
                
                conn.commit()
                logger.info("✅ База данных дедупликации инициализирована")
                
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации БД: {e}")
            raise
    
    def _get_lot_hash(self, lot) -> str:
        """Создает хэш лота для дедупликации"""
        # Используем адрес + площадь + стартовую цену для уникальности
        hash_string = f"{lot.address}_{lot.area}_{lot.price}"
        return hashlib.md5(hash_string.encode()).hexdigest()
    
    def _get_offer_hash(self, offer) -> str:
        """Создает хэш объявления для дедупликации"""
        # Используем URL + площадь + цену для уникальности
        hash_string = f"{offer.url}_{offer.area}_{offer.price}"
        return hashlib.md5(hash_string.encode()).hexdigest()
    
    def is_lot_processed(self, lot) -> bool:
        """Проверяет, обрабатывался ли уже этот лот"""
        lot_hash = self._get_lot_hash(lot)
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT lot_id FROM processed_lots WHERE lot_hash = ? AND last_seen > ?",
                    (lot_hash, (datetime.now() - timedelta(days=7)).isoformat())
                )
                result = cursor.fetchone()
                return result is not None
                
        except Exception as e:
            logger.error(f"Ошибка проверки лота: {e}")
            return False
    
    def is_offer_processed(self, offer) -> bool:
        """Проверяет, обрабатывалось ли уже это объявление"""
        offer_hash = self._get_offer_hash(offer)
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT offer_id FROM processed_offers WHERE offer_hash = ? AND last_seen > ?",
                    (offer_hash, (datetime.now() - timedelta(days=3)).isoformat())
                )
                result = cursor.fetchone()
                return result is not None
                
        except Exception as e:
            logger.error(f"Ошибка проверки объявления: {e}")
            return False
    
    def mark_lot_processed(self, lot):
        """Отмечает лот как обработанный"""
        lot_hash = self._get_lot_hash(lot)
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT OR REPLACE INTO processed_lots 
                    (lot_id, lot_hash, address, area, price, first_seen, last_seen, times_seen)
                    VALUES (?, ?, ?, ?, ?, 
                           COALESCE((SELECT first_seen FROM processed_lots WHERE lot_hash = ?), CURRENT_TIMESTAMP),
                           CURRENT_TIMESTAMP,
                           COALESCE((SELECT times_seen FROM processed_lots WHERE lot_hash = ?), 0) + 1)
                """, (lot.id, lot_hash, lot.address, lot.area, lot.price, lot_hash, lot_hash))
                conn.commit()
                
        except Exception as e:
            logger.error(f"Ошибка сохранения лота: {e}")
    
    def mark_offer_processed(self, offer):
        """Отмечает объявление как обработанное"""
        offer_hash = self._get_offer_hash(offer)
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT OR REPLACE INTO processed_offers 
                    (offer_id, offer_hash, url, address, area, price, offer_type, first_seen, last_seen, times_seen)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 
                           COALESCE((SELECT first_seen FROM processed_offers WHERE offer_hash = ?), CURRENT_TIMESTAMP),
                           CURRENT_TIMESTAMP,
                           COALESCE((SELECT times_seen FROM processed_offers WHERE offer_hash = ?), 0) + 1)
                """, (offer.id, offer_hash, offer.url, offer.address, offer.area, offer.price, 
                     offer.type, offer_hash, offer_hash))
                conn.commit()
                
        except Exception as e:
            logger.error(f"Ошибка сохранения объявления: {e}")
    
    def cleanup_old_records(self, days_old: int = 30):
        """Удаляет старые записи для экономии места"""
        cutoff_date = (datetime.now() - timedelta(days=days_old)).isoformat()
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Удаляем старые лоты
                cursor.execute("DELETE FROM processed_lots WHERE last_seen < ?", (cutoff_date,))
                lots_deleted = cursor.rowcount
                
                # Удаляем старые объявления
                cursor.execute("DELETE FROM processed_offers WHERE last_seen < ?", (cutoff_date,))
                offers_deleted = cursor.rowcount
                
                conn.commit()
                logger.info(f"🧹 Очистка БД: удалено {lots_deleted} лотов и {offers_deleted} объявлений")
                
        except Exception as e:
            logger.error(f"Ошибка очистки БД: {e}")
    
    def get_stats(self) -> dict:
        """Возвращает статистику БД"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute("SELECT COUNT(*) FROM processed_lots")
                lots_count = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM processed_offers")
                offers_count = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM processed_lots WHERE last_seen > ?", 
                              ((datetime.now() - timedelta(days=7)).isoformat(),))
                recent_lots = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM processed_offers WHERE last_seen > ?", 
                              ((datetime.now() - timedelta(days=3)).isoformat(),))
                recent_offers = cursor.fetchone()[0]
                
                return {
                    "total_lots": lots_count,
                    "total_offers": offers_count,
                    "recent_lots": recent_lots,
                    "recent_offers": recent_offers
                }
                
        except Exception as e:
            logger.error(f"Ошибка получения статистики: {e}")
            return {}

# Глобальный экземпляр
dedup_db = DeduplicationDB()