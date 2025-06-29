"""
Telegram Bot для анализа коммерческой недвижимости
"""
import asyncio
import logging
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from typing import List, Optional
from core.models import Lot, Offer
from core.config import CONFIG

logger = logging.getLogger(__name__)

class RealEstateBot:
    def __init__(self, token: str):
        try:
            from bot.message_formatter import MessageFormatter
            self.message_formatter = MessageFormatter()
            logger.info(f"Initializing bot with token: {token[:20]}...")
            self.bot = Bot(token=token)
            self.dp = Dispatcher()
            self.subscribers = set()  # Множество chat_id подписчиков
            self.setup_handlers()
            logger.info(f"Bot initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize bot: {e}")
            raise
    
    def setup_handlers(self):
        """Настройка обработчиков команд"""
        self.dp.message(Command("start"))(self.start_command)
        self.dp.message(Command("subscribe"))(self.subscribe_command)
        self.dp.message(Command("unsubscribe"))(self.unsubscribe_command)
        self.dp.message(Command("analogs"))(self.analogs_command)
        self.dp.message(Command("help"))(self.help_command)
        self.dp.callback_query()(self.handle_callback)
    
    async def start_command(self, message: types.Message):
        """Обработчик команды /start"""
        welcome_text = (
            "🏢 *Добро пожаловать в бот анализа коммерческой недвижимости!*\n\n"
            "Я помогу вам отслеживать выгодные лоты на торгах и находить аналоги для анализа.\n\n"
            "*Доступные команды:*\n"
            "• /subscribe - подписаться на уведомления о новых лотах\n"
            "• /unsubscribe - отписаться от уведомлений\n"
            "• /analogs [адрес] - найти аналоги для указанного адреса\n"
            "• /help - справка по командам\n\n"
            "Используйте /subscribe чтобы получать уведомления о новых выгодных лотах!"
        )
        
        await message.answer(welcome_text, parse_mode="Markdown")
    
    async def subscribe_command(self, message: types.Message):
        """Подписка на уведомления"""
        chat_id = message.chat.id
        self.subscribers.add(chat_id)
        
        await message.answer(
            "✅ Вы успешно подписались на уведомления о новых лотах!\n\n"
            "Теперь вы будете получать анализ каждого нового лота с:\n"
            "• Финансовыми показателями\n"
            "• Рыночной оценкой\n"
            "• Рекомендациями ИИ\n"
            "• Ссылками на торги"
        )
        
        # Сохраняем подписчиков
        await self._save_subscribers()
    
    async def unsubscribe_command(self, message: types.Message):
        """Отписка от уведомлений"""
        chat_id = message.chat.id
        self.subscribers.discard(chat_id)
        
        await message.answer("❌ Вы отписались от уведомлений о новых лотах.")
        await self._save_subscribers()
    
    async def analogs_command(self, message: types.Message):
        """Поиск аналогов для указанного адреса"""
        # Извлекаем адрес из сообщения
        command_parts = message.text.split(' ', 1)
        if len(command_parts) < 2:
            await message.answer(
                "📍 Укажите адрес для поиска аналогов:\n"
                "Пример: `/analogs Москва, ул. Тверская, 1`",
                parse_mode="Markdown"
            )
            return
        
        address = command_parts[1].strip()
        
        # Отправляем сообщение о начале поиска
        search_message = await message.answer(
            f"🔍 Ищу аналоги для адреса: {address}\n"
            "Это может занять несколько секунд..."
        )
        
        try:
            # Импортируем функции поиска
            from parser.cian_minimal import fetch_nearby_offers
            # from parser.geo_utils import filter_offers_by_distance  # Может быть недоступно
            from .analog_search import AnalogSearchService
            
            # Поиск аналогов
            offers = await AnalogSearchService.find_analogs_for_address(address, radius_km=3.0)
            
            if offers:
                # Форматируем результат
                from .message_formatter import MessageFormatter
                analogs_text = MessageFormatter.format_analogs_list(offers)
                
                await search_message.edit_text(
                    f"📍 *Аналоги для: {address}*\n\n{analogs_text}",
                    parse_mode="Markdown"
                )
            else:
                await search_message.edit_text(
                    f"❌ Аналоги для адреса '{address}' не найдены.\n"
                    "Попробуйте указать более общий адрес или другой район."
                )
                
        except Exception as e:
            logger.error(f"Error searching analogs for {address}: {e}")
            await search_message.edit_text(
                "❌ Произошла ошибка при поиске аналогов. Попробуйте позже."
            )
    
    async def help_command(self, message: types.Message):
        """Справка по командам"""
        help_text = (
            "🤖 *Справка по командам бота*\n\n"
            "*Основные команды:*\n"
            "• `/start` - начать работу с ботом\n"
            "• `/subscribe` - подписаться на уведомления\n"
            "• `/unsubscribe` - отписаться от уведомлений\n"
            "• `/analogs [адрес]` - найти аналоги\n"
            "• `/help` - эта справка\n\n"
            "*Примеры использования:*\n"
            "• `/analogs Москва, Тверская улица, 1`\n"
            "• `/analogs Московская область, Химки`\n\n"
            "*О боте:*\n"
            "Бот анализирует лоты коммерческой недвижимости на торгах "
            "и предоставляет детальную аналитику с рекомендациями."
        )
        
        await message.answer(help_text, parse_mode="Markdown")
    
    async def handle_callback(self, callback: types.CallbackQuery):
        """Обработчик callback кнопок"""
        data = callback.data
        
        if data.startswith("analogs_"):
            # Извлекаем UUID лота
            lot_uuid = data.replace("analogs_", "")
            
            await callback.answer("🔍 Ищу аналоги для лота...")
            
            try:
                # Отправляем сообщение о начале поиска
                search_message = await callback.message.answer(
                    f"🔍 Ищу аналоги для лота {lot_uuid[:8]}...\n"
                    "Проверяю базу данных аналогов..."
                )
                
                # Поиск аналогов для конкретного лота по UUID
                from .analog_search import AnalogSearchService
                offers = await AnalogSearchService.find_analogs_for_lot_uuid(lot_uuid, radius_km=3.0)
                
                if offers:
                    # Ищем информацию о самом лоте для отображения
                    from parser.google_sheets import find_lot_by_uuid
                    lot = find_lot_by_uuid(lot_uuid)
                    
                    # Форматируем результат
                    from .message_formatter import MessageFormatter
                    analogs_text = MessageFormatter.format_analogs_list(offers)
                    
                    lot_info = ""
                    if lot:
                        lot_info = f"📍 {lot.address}\n📐 {lot.area:,.0f} м²\n\n"
                    
                    await search_message.edit_text(
                        f"📊 *Найдены аналоги*\n"
                        f"{lot_info}{analogs_text}",
                        parse_mode="Markdown"
                    )
                else:
                    # Пытаемся найти информацию о лоте для более подробного ответа
                    from parser.google_sheets import find_lot_by_uuid
                    lot = find_lot_by_uuid(lot_uuid)
                    
                    lot_info = ""
                    if lot:
                        lot_info = f"📍 Адрес: {lot.address}\n📐 Площадь: {lot.area:,.0f} м²\n\n"
                    
                    await search_message.edit_text(
                        f"❌ Аналоги для лота не найдены.\n\n"
                        f"{lot_info}"
                        "Возможные причины:\n"
                        "• Нет сохраненных аналогов в базе данных\n"
                        "• Недоступность онлайн-поиска\n"
                        "• Слишком специфичный объект"
                    )
                    
            except Exception as e:
                logger.error(f"Error in analogs callback for UUID {lot_uuid}: {e}")
                await callback.message.answer(
                    "❌ Произошла ошибка при поиске аналогов.\n"
                    "Попробуйте позже или используйте команду `/analogs [адрес]`"
                )
        else:
            await callback.answer()
    
    async def send_lot_analysis(self, lot: Lot, chat_ids: List[int] = None):
        """Отправка анализа лота подписчикам"""
        if chat_ids is None:
            chat_ids = list(self.subscribers)
        
        if not chat_ids:
            logger.info("No subscribers to send lot analysis")
            return
        
        from .message_formatter import MessageFormatter
        message_text = MessageFormatter.format_lot_analysis(lot)
        
        # Создаем кнопки для взаимодействия
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔗 Открыть торги", url=lot.auction_url)],
            [InlineKeyboardButton(text="📊 Найти аналоги", callback_data=f"analogs_{lot.uuid}")]
        ])
        
        # Отправляем всем подписчикам
        failed_chats = []
        for chat_id in chat_ids:
            try:
                await self.bot.send_message(
                    chat_id=chat_id,
                    text=message_text,
                    parse_mode="Markdown",
                    reply_markup=keyboard,
                    disable_web_page_preview=True
                )
                await asyncio.sleep(0.1)  # Избегаем rate limit
            except Exception as e:
                logger.error(f"Failed to send message to chat {chat_id}: {e}")
                failed_chats.append(chat_id)
        
        # Удаляем неактивных подписчиков
        for chat_id in failed_chats:
            self.subscribers.discard(chat_id)
        
        if failed_chats:
            await self._save_subscribers()
        
        logger.info(f"Lot analysis sent to {len(chat_ids) - len(failed_chats)} subscribers")
    
    async def notify_new_lots(self, lots: List[Lot]):
        """Уведомления о новых лотах"""
        if not lots or not self.subscribers:
            return
        
        logger.info(f"Sending notifications about {len(lots)} new lots to {len(self.subscribers)} subscribers")
        
        for lot in lots:
            # Отправляем только если лот имеет хорошие показатели
            if self._should_notify_about_lot(lot):
                await self.send_lot_analysis(lot)
                await asyncio.sleep(1)  # Пауза между лотами
    
    def _should_notify_about_lot(self, lot: Lot) -> bool:
        """Определяет, стоит ли уведомлять о лоте - ИСПРАВЛЕННАЯ ВЕРСИЯ"""
        
        # НОВАЯ ЛОГИКА: уведомляем на основе плюсиков
        plus_count = getattr(lot, 'plus_count', 0)
        
        # Если есть хотя бы 1 плюсик - уведомляем
        if plus_count >= 1:
            return True
        
        # Дополнительная проверка для безопасности (старая логика как fallback)
        yield_threshold = CONFIG.get('market_yield_threshold', 10)
        if yield_threshold > 1:
            yield_threshold = yield_threshold / 100
        
        # Проверяем доходность
        annual_yield = getattr(lot, 'annual_yield_percent', 0)
        if annual_yield >= yield_threshold:
            return True
        
        # Проверяем капитализацию
        capitalization_percent = getattr(lot, 'capitalization_percent', 0)
        if capitalization_percent >= 0.15:  # 15% как 0.15
            return True
        
        return False
    
    async def _save_subscribers(self):
        """Сохранение списка подписчиков"""
        try:
            import json
            import os
            
            # Создаем директорию для данных бота если её нет
            bot_data_dir = "bot_data"
            if not os.path.exists(bot_data_dir):
                os.makedirs(bot_data_dir)
            
            # Сохраняем подписчиков в файл
            subscribers_file = os.path.join(bot_data_dir, "subscribers.json")
            with open(subscribers_file, 'w') as f:
                json.dump(list(self.subscribers), f)
            
            logger.info(f"Subscribers saved: {len(self.subscribers)} total")
        except Exception as e:
            logger.error(f"Error saving subscribers: {e}")
    
    async def _load_subscribers(self):
        """Загрузка списка подписчиков"""
        try:
            import json
            import os
            
            # Создаем директорию если её нет
            bot_data_dir = "bot_data"
            if not os.path.exists(bot_data_dir):
                os.makedirs(bot_data_dir)
                logger.info("Created bot_data directory")
            
            subscribers_file = os.path.join(bot_data_dir, "subscribers.json")
            
            if os.path.exists(subscribers_file):
                with open(subscribers_file, 'r') as f:
                    subscribers_list = json.load(f)
                    # ИСПРАВЛЕНО: убеждаемся, что это числа
                    self.subscribers = set(int(sub) for sub in subscribers_list)
                    logger.info(f"✅ Loaded {len(self.subscribers)} subscribers: {list(self.subscribers)}")
            else:
                self.subscribers = set()
                logger.info("No existing subscribers file, starting fresh")
                
                # Создаем пустой файл
                with open(subscribers_file, 'w') as f:
                    json.dump([], f)
                logger.info("Created empty subscribers file")
                
        except Exception as e:
            logger.error(f"Error loading subscribers: {e}")
            self.subscribers = set()
    
    async def start_polling(self):
        """Запуск бота в режиме polling"""
        await self._load_subscribers()
        logger.info("Starting Telegram bot polling...")
        
        try:
            await self.dp.start_polling(self.bot)
        except Exception as e:
            logger.error(f"Bot polling error: {e}")
            raise
    
    async def stop(self):
        """Остановка бота"""
        await self.bot.session.close()
        logger.info("Telegram bot stopped")
