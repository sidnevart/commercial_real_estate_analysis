"""
Форматирование сообщений для Telegram бота
"""
import logging
from typing import List
from core.models import Lot, Offer

logger = logging.getLogger(__name__)

class MessageFormatter:
    @staticmethod
    def format_lot_analysis(lot: Lot) -> str:
        """Форматирует анализ лота по образцу из ТЗ"""
        
        # Основная информация
        message = f"🔷 *Лот №{lot.id} сегодня*\n\n"
        
        # Описание объекта
        message += f"🏢 *Лот:* {lot.name}\n"
        message += f"📍 *Адрес:* {lot.address}\n"
        
        # Категория из классификации
        category = "Не определена"
        if hasattr(lot, 'classification') and lot.classification:
            category = lot.classification.category or "Не определена"
        message += f"🏗️ *Категория:* {category}\n\n"
        
        # Финансовые показатели
        message += "📊 *Финансовые показатели*\n"
        message += f"• *Площадь:* {lot.area:,.0f} м²\n"
        
        # Цена за м² (текущая)
        current_price_per_sqm = lot.price / lot.area if lot.area > 0 else 0
        message += f"• *Цена за м² (текущая):* {current_price_per_sqm:,.0f} ₽\n"
        
        # Рыночная цена за м²
        market_price_per_sqm = getattr(lot, 'market_price_per_sqm', 0)
        message += f"• *Рыночная цена за м²:* {market_price_per_sqm:,.0f} ₽\n"
        
        # Общие цены
        message += f"• *Текущая цена:* {lot.price:,.0f} ₽\n"
        
        market_value = getattr(lot, 'market_value', 0)
        message += f"• *Рыночная оценка:* {market_value:,.0f} ₽\n"
        
        # ГАП и доходность
        monthly_gap = getattr(lot, 'monthly_gap', 0)
        message += f"• *ГАП:* {monthly_gap:,.0f} ₽/мес\n"
        
        # ИСПРАВЛЕНО: доходность как процент
        annual_yield = getattr(lot, 'annual_yield_percent', 0)
        annual_yield_display = annual_yield * 100 if annual_yield < 1 else annual_yield
        message += f"• *Доходность:* {annual_yield_display:.1f}%\n"
        
        # ДОБАВЛЕНО: Капитализация в рублях И процентах
        capitalization_rub = getattr(lot, 'capitalization_rub', 0)
        capitalization_percent = getattr(lot, 'capitalization_percent', 0)
        capitalization_percent_display = capitalization_percent * 100 if capitalization_percent < 1 else capitalization_percent
        message += f"• *Капитализация:* {capitalization_rub:,.0f} ₽ ({capitalization_percent_display:.1f}%)\n"
        
        # ИСПРАВЛЕНО: Отклонение от рынка (разница между рыночной и текущей ценой)
        if market_price_per_sqm > 0:
            market_deviation_percent = ((current_price_per_sqm - market_price_per_sqm) / market_price_per_sqm) * 100
            deviation_emoji = "📉" if market_deviation_percent < 0 else "📈"
            message += f"• *Отклонение от рынка:* {deviation_emoji} {market_deviation_percent:.1f}%\n\n"
        else:
            message += f"• *Отклонение от рынка:* ❓ Нет данных\n\n"
        
        # Информация о торгах
        message += "🏛️ *Инфо о торгах*\n"
        message += f"• *Начальная цена:* {lot.price:,.0f} ₽\n"
        message += f"• *Аукцион:* {lot.auction_type}\n"
        message += f"• *Документ:* {lot.notice_number}\n\n"
        
        # ИСПРАВЛЕНО: Рекомендация на основе плюсиков
        plus_count = getattr(lot, 'plus_count', 0)
        plus_rental = getattr(lot, 'plus_rental', 0)
        plus_sale = getattr(lot, 'plus_sale', 0)
        
        if plus_count == 2:
            recommendation_emoji = "🔥"
            recommendation_text = "идеальный лот!"
            recommendation_reason = "Отличные показатели по аренде и продаже"
        elif plus_count == 1:
            recommendation_emoji = "✅"
            if plus_rental:
                recommendation_text = "хороший лот"
                recommendation_reason = "Высокая доходность аренды"
            else:
                recommendation_text = "хороший лот"
                recommendation_reason = "Выгодная цена покупки"
        else:
            recommendation_emoji = "❌"
            recommendation_text = "НЕ рекомендовано"
            recommendation_reason = "Показатели ниже пороговых значений"
        
        message += f"🧠 *Мнение ИИ:* {recommendation_emoji} {recommendation_text}\n"
        message += f"💡 *Причина:* {recommendation_reason}\n"
        
        # ДОБАВЛЕНО: Показываем плюсики для понимания
        if plus_count > 0:
            message += f"⭐ *Плюсы:* {plus_count}/2 (аренда: {'✅' if plus_rental else '❌'}, продажа: {'✅' if plus_sale else '❌'})\n"
        
        message += "\n"
        
        # Ссылка на торги (будет добавлена как кнопка)
        message += f"🔗 [Лот на torgi.gov.ru]({lot.auction_url})"
        
        return message
    
    @staticmethod
    def format_analogs_list(offers: List[Offer]) -> str:
        """Форматирует список аналогов"""
        if not offers:
            return "❌ Аналоги не найдены"
        
        # Разделяем по типу предложений
        sale_offers = [o for o in offers if getattr(o, 'type', 'sale') == 'sale']
        rent_offers = [o for o in offers if getattr(o, 'type', 'rent') == 'rent']
        
        message = ""
        
        # Продажа
        if sale_offers:
            message += "🔷 *Продажа:*\n"
            for i, offer in enumerate(sale_offers[:3], 1):  # Максимум 3 объявления
                price_per_sqm = offer.price / offer.area if offer.area > 0 else 0
                
                message += f"{i}. 📍 {offer.address}\n"
                message += f"   • {offer.area:,.0f} м²\n"
                message += f"   • {price_per_sqm:,.0f} ₽/м²\n"
                message += f"   • *Цена:* {offer.price:,.0f} ₽\n"
                
                # Расстояние если есть
                if hasattr(offer, 'distance_to_lot') and offer.distance_to_lot:
                    message += f"   • *Расстояние:* {offer.distance_to_lot:.1f} км\n"
                
                # Ссылка если есть
                if hasattr(offer, 'url') and offer.url:
                    message += f"   🔗 [Ссылка]({offer.url})\n"
                
                message += "\n"
        
        # Аренда
        if rent_offers:
            message += "🔷 *Аренда:*\n"
            for i, offer in enumerate(rent_offers[:3], 1):  # Максимум 3 объявления
                price_per_sqm = offer.price / offer.area if offer.area > 0 else 0
                
                message += f"{i}. 📍 {offer.address}\n"
                message += f"   • {offer.area:,.0f} м²\n"
                message += f"   • {price_per_sqm:,.0f} ₽/м²/мес\n"
                message += f"   • *Общая аренда:* {offer.price:,.0f} ₽/мес\n"
                
                # Расстояние если есть
                if hasattr(offer, 'distance_to_lot') and offer.distance_to_lot:
                    message += f"   • *Расстояние:* {offer.distance_to_lot:.1f} км\n"
                
                # Ссылка если есть
                if hasattr(offer, 'url') and offer.url:
                    message += f"   🔗 [Ссылка]({offer.url})\n"
                
                message += "\n"
        
        # Статистика
        total_offers = len(offers)
        message += f"📊 *Всего найдено:* {total_offers} объявлений"
        if sale_offers and rent_offers:
            message += f" ({len(sale_offers)} продажа, {len(rent_offers)} аренда)"
        
        return message
    
    @staticmethod
    def format_error_message(error_text: str) -> str:
        """Форматирует сообщение об ошибке"""
        return f"❌ *Ошибка:* {error_text}\n\nПопробуйте позже или обратитесь к администратору."
    
    @staticmethod
    def format_subscription_stats(total_lots: int, recommended_lots: int) -> str:
        """Форматирует статистику по подписке"""
        return (
            f"📊 *Статистика за сегодня:*\n"
            f"• Всего лотов: {total_lots}\n"
            f"• Рекомендованных: {recommended_lots}\n"
            f"• Эффективность фильтра: {(recommended_lots/total_lots*100):.1f}%" 
            if total_lots > 0 else "📊 Сегодня новых лотов не было"
        )

    @staticmethod
    def format_short_lot_summary(lot) -> str:
        """Краткое описание лота для списков"""
        try:
            yield_percent = getattr(lot, 'annual_yield_percent', 0)
            price_per_sqm = lot.price / lot.area if lot.area > 0 else 0
            
            summary = f"🏢 {lot.name[:50]}{'...' if len(lot.name) > 50 else ''}\n"
            summary += f"📍 {lot.address}\n"
            summary += f"💰 {price_per_sqm:,.0f} ₽/м² • {lot.area:,.0f} м²"
            
            if yield_percent > 0:
                summary += f" • {yield_percent:.1f}% доходность"
            
            return summary
            
        except Exception as e:
            logger.error(f"Error formatting lot summary: {e}")
            return f"❌ Ошибка форматирования лота {getattr(lot, 'id', 'unknown')}"
