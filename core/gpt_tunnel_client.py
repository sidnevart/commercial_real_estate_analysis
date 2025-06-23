# core/gpt_tunnel_client.py
import logging
import json
import asyncio
from typing import Dict, Any, List, Optional
from core.models import Lot, Offer
from core.config import CONFIG
import os
import httpx


logger = logging.getLogger(__name__)



async def calculate_metrics_with_gpt(
    lot: Lot, 
    filtered_sale_offers: List[Offer], 
    filtered_rent_offers: List[Offer]
) -> Optional[Dict[str, Any]]:
    """
    Рассчитывает финансовые метрики для лота с использованием GPT.
    
    Args:
        lot: Объект лота с торгов
        filtered_sale_offers: Отфильтрованные предложения о продаже
        filtered_rent_offers: Отфильтрованные предложения об аренде
        
    Returns:
        Словарь с расчетными метриками или None в случае ошибки
    """
    try:
        # Проверка наличия данных
        if not filtered_sale_offers and not filtered_rent_offers:
            logger.warning(f"Нет данных для анализа лота {lot.id}. Невозможно рассчитать метрики.")
            return None
            
        # Подготавливаем данные о продажах
        sale_data = []
        for offer in filtered_sale_offers:
            if offer.area > 0 and offer.price > 0:
                price_per_sqm = offer.price / offer.area
                sale_data.append({
                    "area": offer.area,
                    "price": offer.price,
                    "price_per_sqm": price_per_sqm,
                    "district": getattr(offer, 'district', ''),
                    "distance": getattr(offer, 'distance_to_lot', 0)
                })
        
        # Подготавливаем данные об аренде
        rent_data = []
        for offer in filtered_rent_offers:
            if offer.area > 0 and offer.price > 0:
                price_per_sqm = offer.price / offer.area
                rent_data.append({
                    "area": offer.area,
                    "price": offer.price,
                    "price_per_sqm": price_per_sqm,
                    "district": getattr(offer, 'district', ''),
                    "distance": getattr(offer, 'distance_to_lot', 0)
                })
        
        # Подготавливаем данные о текущем лоте
        lot_data = {
            "id": lot.id,
            "name": getattr(lot, 'name', ''),
            "area": lot.area,
            "price": lot.price,
            "district": getattr(lot, 'district', ''),
            "category": getattr(lot, 'property_category', '')
        }
        
        # Вычисляем медианные показатели предложений для включения в промпт
        market_price_per_sqm = 0
        if sale_data:
            prices = [item["price_per_sqm"] for item in sale_data]
            market_price_per_sqm = sorted(prices)[len(prices) // 2]
        
        avg_rent_price_per_sqm = 0
        if rent_data:
            prices = [item["price_per_sqm"] for item in rent_data]
            avg_rent_price_per_sqm = sorted(prices)[len(prices) // 2]
        
        # Формируем промпт для GPT на основе шаблона из конфигурации
        prompt = CONFIG.get("gpt_metrics_template", "").format(
            lot_id=lot_data["id"],
            name=lot_data["name"],
            area=lot_data["area"],
            price=lot_data["price"],
            district=lot_data["district"],
            category=lot_data["category"],
            market_price_per_sqm=market_price_per_sqm,
            avg_rent_price_per_sqm=avg_rent_price_per_sqm,
            sale_offers_count=len(sale_data),
            rent_offers_count=len(rent_data)
        )
        
        # Добавляем примеры предложений (до 5 каждого типа)
        prompt += "\n\nПРИМЕРЫ ПРЕДЛОЖЕНИЙ О ПРОДАЖЕ:\n"
        for i, item in enumerate(sale_data[:5], 1):
            prompt += f"{i}. Площадь: {item['area']} м², Цена: {item['price']:,} руб ({item['price_per_sqm']:,.0f} руб/м²), Район: {item['district']}\n"
        
        prompt += "\nПРИМЕРЫ ПРЕДЛОЖЕНИЙ ОБ АРЕНДЕ:\n"
        for i, item in enumerate(rent_data[:5], 1):
            prompt += f"{i}. Площадь: {item['area']} м², Цена: {item['price']:,} руб/мес ({item['price_per_sqm']:,.0f} руб/м²/мес), Район: {item['district']}\n"
        
        # Логируем информацию о запросе
        logger.info(f"Отправка запроса к GPT для расчета метрик лота {lot.id}")
        logger.debug(f"Промпт: {prompt[:200]}...")
        
        # Модель для запроса
        model = "gpt-4o-mini"  # Более экономичная модель
        
        # Модифицируем сообщение системы для GPT, чтобы обеспечить правильный формат ответа
        system_message = """
        Вы - финансовый аналитик, специализирующийся на оценке коммерческой недвижимости. 
        Ваша задача - рассчитать финансовые показатели на основе предоставленных данных.
        
        ВАЖНО: Вы должны вернуть ТОЛЬКО валидный JSON без комментариев и пояснений. Формат:
        
        {
          "market_price_per_sqm": число,
          "market_value": число,
          "capitalization_rub": число,
          "capitalization_percent": число,
          "average_rent_price_per_sqm": число,
          "monthly_gap": число,
          "annual_income": число, 
          "annual_yield_percent": число,
          "plus_sale": 0 или 1,
          "plus_rental": 0 или 1,
          "plus_count": 0, 1 или 2,
          "status": "discard", "review" или "approved",
          "market_value_method": "sales", "rent" или "none"
        }
        """
        
        # Вызываем GPT API с улучшенной инструкцией
        response = await chat(
            model=model,
            messages=[
                {"role": "system", "content": system_message},
                {"role": "user", "content": prompt}
            ],
            max_tokens=800
        )
        
        logger.debug(f"Ответ GPT: {response[:200]}...")
        
        # Извлекаем JSON из ответа
        metrics = extract_json_from_response(response)
        
        if not metrics:
            logger.error(f"Не удалось извлечь JSON из ответа GPT для лота {lot.id}")
            return None
            
        # Проверяем корректность рыночных ставок
        min_prices = {
            "Красносельский": 200000,
            "Тверской": 300000,
            "Басманный": 200000,
            "Пресненский": 250000,
            "Сокол": 150000
        }
        
        district = lot_data["district"]
        if district in min_prices:
            market_price = float(metrics.get('market_price_per_sqm', 0))
            min_price = min_prices[district]
            
            if market_price < min_price:
                logger.warning(f"⚠️ Рыночная цена для района {district} слишком низкая: {market_price:,.0f} руб/м²")
                logger.warning(f"Увеличиваем до минимального порога: {min_price:,.0f} руб/м²")
                metrics['market_price_per_sqm'] = min_price
                
                # Пересчитываем зависимые метрики
                metrics['market_value'] = min_price * lot.area
                metrics['capitalization_rub'] = metrics['market_value'] - lot.price
                if lot.price > 0:
                    metrics['capitalization_percent'] = metrics['capitalization_rub'] / lot.price * 100
                metrics['plus_sale'] = 1 if metrics['capitalization_percent'] >= 0 else 0
                metrics['plus_count'] = metrics['plus_sale'] + metrics.get('plus_rental', 0)
                
                # Обновляем статус объекта
                if metrics['plus_count'] == 0:
                    metrics['status'] = "discard"
                elif metrics['plus_count'] == 1:
                    metrics['status'] = "review"
                else:
                    metrics['status'] = "approved"
        
        # Проверка и корректировка арендной ставки для района Сокол
        if district == "Сокол" and float(metrics.get('average_rent_price_per_sqm', 0)) < 800:
            logger.warning(f"⚠️ Средняя арендная ставка для района Сокол слишком низкая")
            metrics['average_rent_price_per_sqm'] = 1200
            metrics['monthly_gap'] = metrics['average_rent_price_per_sqm'] * lot.area
            metrics['annual_income'] = metrics['monthly_gap'] * 12
            if lot.price > 0:
                metrics['annual_yield_percent'] = metrics['annual_income'] / lot.price * 100
            metrics['plus_rental'] = 1 if metrics['annual_yield_percent'] >= 10 else 0
            metrics['plus_count'] = metrics['plus_sale'] + metrics['plus_rental']
        
        logger.info(f"✅ Успешно получены метрики от GPT для лота {lot.id}")
        return metrics
        
    except Exception as e:
        logger.error(f"❌ Ошибка при вызове GPT для расчета метрик лота {lot.id}: {e}")
        return None
"""
ore.gpt_tunnel_client: Нет данных для анализа лота 21000002210000007065_1. Невозможно рассчитать метрики.
2025-06-23 22:34:44,010 WARNING root: ⚠️ Не удалось получить метрики от GPT для лота 21000002210000007065_1, используем стандартный расчет
2025-06-23 22:34:44,010 INFO root: Лот 21000002210000007065_1: Найдено 0 валидных объявлений о продаже
"""
def extract_json_from_response(response: str) -> Optional[Dict[str, Any]]:
    """
    Извлекает JSON из ответа GPT с обработкой разных форматов.
    """
    try:
        # Проверяем, содержит ли ответ JSON-блок в формате ```json ... ```
        import re
        json_pattern = r'```(?:json)?(.*?)```'
        match = re.search(json_pattern, response, re.DOTALL)
        if match:
            json_str = match.group(1).strip()
            return json.loads(json_str)
        
        # Если нет, ищем первый блок в фигурных скобках
        brace_pattern = r'{.*}'
        match = re.search(brace_pattern, response, re.DOTALL)
        if match:
            json_str = match.group(0)
            return json.loads(json_str)
        
        # Если и это не помогло, пробуем загрузить весь ответ как JSON
        return json.loads(response)
    except Exception as e:
        logger.error(f"Ошибка при извлечении JSON из ответа: {e}")
        logger.debug(f"Исходный ответ: {response}")
        return None

def sync_calculate_metrics_with_gpt(lot: Lot, filtered_sale_offers: List[Offer], filtered_rent_offers: List[Offer]) -> Optional[Dict[str, Any]]:
    """Синхронная обертка для вызова асинхронной функции из синхронного кода."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            logger.warning("Невозможно запустить асинхронный запрос в синхронном контексте с работающим циклом")
            return None
        else:
            return loop.run_until_complete(calculate_metrics_with_gpt(lot, filtered_sale_offers, filtered_rent_offers))
    except Exception as e:
        logger.error(f"Ошибка при синхронном вызове GPT: {e}")
        return None
    

API_KEY  = os.getenv("OPENAI_API_KEY")                 # ваш ключ shds-…
BASE_URL = "https://gptunnel.ru/v1"

HEADERS  = {"Authorization": API_KEY}
TIMEOUT  = httpx.Timeout(60.0, connect=15.0)
import json

async def chat(model: str, messages: list[dict], max_tokens: int = 500) -> str:
    """
    Исправленная функция для запросов к GPT API.
    model – "gpt-4o-mini", "gpt-3.5-turbo" и т.п.
    messages – [{"role":"system","content":"..."}, {"role":"user","content":"..."}]
    """
    payload = {
        "model": model,
        "messages": messages,
        "max_tokens": max_tokens
    }
    
    # Проверяем правильность заголовка Authorization
    headers = {"Authorization": API_KEY}
    
    # Вывод отладочной информации
    logging.debug(f"API Request to {BASE_URL}/chat/completions")
    logging.debug(f"Payload: {json.dumps(payload)}")
    
    try:
        async with httpx.AsyncClient(base_url=BASE_URL, headers=headers, timeout=TIMEOUT) as client:
            r = await client.post("/chat/completions", json=payload)
            
            # Детальное логирование ошибки
            if r.status_code != 200:
                logging.error(f"API error {r.status_code}: {r.text}")
                
            r.raise_for_status()
            data = r.json()
            text = data["choices"][0]["message"]["content"]
            return text
    except httpx.HTTPStatusError as e:
        logging.error(f"HTTP Error: {e}")
        logging.error(f"Response content: {e.response.text if hasattr(e, 'response') else 'No response'}")
        raise
    except Exception as e:
        logging.error(f"API connection error: {str(e)}")
        raise
