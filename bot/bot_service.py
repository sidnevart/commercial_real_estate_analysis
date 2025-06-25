"""
Сервис для управления Telegram ботом
"""
import asyncio
import logging
from typing import List, Optional
from .telegram_bot import RealEstateBot
from core.models import Lot
from core.config import CONFIG

logger = logging.getLogger(__name__)

class BotService:
    def __init__(self):
        self.bot: Optional[RealEstateBot] = None
        self._initialized = False
    
    def initialize(self, token: str = None):
        """Инициализация бота"""
        if self._initialized:
            return
        
        bot_token = token or CONFIG.get('telegram_bot_token')
        if not bot_token:
            logger.warning("Telegram bot token not found in config, bot disabled")
            return
        
        try:
            self.bot = RealEstateBot(bot_token)
            self._initialized = True
            logger.info("Telegram bot service initialized")
        except Exception as e:
            logger.error(f"Failed to initialize bot service: {e}")
    
    async def notify_new_lots(self, lots: List[Lot]):
        """Уведомляет подписчиков о новых лотах"""
        if not self._initialized or not self.bot:
            logger.debug("Bot not initialized, skipping notifications")
            return
        
        if not lots:
            logger.debug("No lots to notify about")
            return
        
        try:
            await self.bot.notify_new_lots(lots)
            logger.info(f"Successfully processed notifications for {len(lots)} lots")
        except Exception as e:
            logger.error(f"Error sending lot notifications: {e}")
    
    async def send_daily_summary(self, total_lots: int, recommended_lots: int):
        """Отправляет ежедневную сводку"""
        if not self._initialized or not self.bot:
            return
        
        try:
            from .message_formatter import MessageFormatter
            summary_text = MessageFormatter.format_subscription_stats(total_lots, recommended_lots)
            
            # Отправляем сводку всем подписчикам
            for chat_id in self.bot.subscribers:
                try:
                    await self.bot.bot.send_message(
                        chat_id=chat_id,
                        text=summary_text,
                        parse_mode="Markdown"
                    )
                except Exception as e:
                    logger.error(f"Failed to send daily summary to {chat_id}: {e}")
                    
        except Exception as e:
            logger.error(f"Error sending daily summary: {e}")
    
    async def start_bot_polling(self):
        """Запуск бота в фоновом режиме"""
        if not self._initialized or not self.bot:
            logger.warning("Cannot start polling: bot not initialized")
            return
        
        try:
            # Запускаем в отдельной задаче
            task = asyncio.create_task(self.bot.start_polling())
            logger.info("Bot polling task started")
            return task
        except Exception as e:
            logger.error(f"Failed to start bot polling: {e}")
            return None
    
    def is_enabled(self) -> bool:
        """Проверяет, включен ли бот"""
        return self._initialized and self.bot is not None
    
    async def stop(self):
        """Остановка бота"""
        if self.bot:
            await self.bot.stop()
            logger.info("Bot service stopped")

# Глобальный экземпляр сервиса
bot_service = BotService()
