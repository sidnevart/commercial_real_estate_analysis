import re
import logging
import orjson
import asyncio
from typing import Optional

from core.config import CONFIG
from core.models import PropertyClassification, Lot
from core.gpt_tunnel_client import chat
import json

logger = logging.getLogger(__name__)
MODEL = "gpt-4o-mini"  
BRAKES = re.compile(r"```(?:json)?|```", re.I)   
BRACKS = re.compile(r"{[\s\S]*}", re.M)         

def _extract_json(txt: str) -> str:
    txt = BRAKES.sub("", txt)            
    m = BRACKS.search(txt)
    if not m:
        raise ValueError("JSON not found in GPT reply")
    return m.group(0)

async def classify_property(lot: Lot) -> PropertyClassification:
    """Исправленная функция классификации объекта недвижимости."""
    try:
        if not CONFIG["gpt_analysis_enabled"]:
            logger.info("GPT анализ отключен в настройках")
            return PropertyClassification()
            
        description = f"{lot.name}\nПлощадь: {lot.area} м²\nАдрес: {lot.address}\nТип: {lot.property_category}"
        
        prompt = CONFIG["gpt_prompt_template"].format(
            name=lot.name,
            description=description,
            area=lot.area,
            category=lot.property_category
        )
        
        # Добавим логирование запроса
        logger.info(f"Отправляем запрос в GPT для лота {lot.id}")
        
        raw = await chat(
            MODEL,
            [{"role": "user", "content": prompt}],
            max_tokens=300,  # Увеличиваем максимальное количество токенов
        )
        
        # Добавим логирование ответа
        logger.info(f"Получен ответ от GPT для лота {lot.id}: {raw[:100]}...")
        
        # Попытка исправить проблему с JSON
        try:
            # Сначала попробуем найти только JSON-часть ответа
            json_pattern = r'({[\s\S]*?})'
            json_match = re.search(json_pattern, raw)
            
            if json_match:
                classification_data = json.loads(json_match.group(1))
            else:
                # Если регулярное выражение не помогло, попробуем найти простые ключи
                category = re.search(r'"category":\s*"([^"]+)"', raw)
                size = re.search(r'"size_category":\s*"([^"]+)"', raw)
                basement = re.search(r'"has_basement":\s*(true|false)', raw)
                top_floor = re.search(r'"is_top_floor":\s*(true|false)', raw)
                
                classification_data = {
                    "category": category.group(1) if category else "",
                    "size_category": size.group(1) if size else "",
                    "has_basement": basement.group(1).lower() == "true" if basement else False,
                    "is_top_floor": top_floor.group(1).lower() == "true" if top_floor else False
                }
        except Exception as e:
            logger.error(f"Ошибка при извлечении JSON из ответа GPT: {e}")
            logger.debug(f"Сырой ответ GPT: {raw}")
            return PropertyClassification()
        
        # Создаем и возвращаем классификацию
        classification = PropertyClassification(
            category=classification_data.get("category", ""),
            size_category=classification_data.get("size_category", ""),
            has_basement=classification_data.get("has_basement", False),
            is_top_floor=classification_data.get("is_top_floor", False)
        )
        
        logger.info(f"Лот {lot.id} классифицирован как {classification.category}, размер: {classification.size_category}")
        return classification
        
    except Exception as e:
        logger.error(f"Ошибка классификации лота {lot.id}: {e}")
        return PropertyClassification()

def classify_property_sync(lot: Lot) -> Optional[PropertyClassification]:
    """
    Синхронная обертка для вызова асинхронной функции.
    Для случаев, когда нельзя использовать await.
    """
    loop = asyncio.get_event_loop()
    if loop.is_running():
        logger.warning("Невозможно запустить асинхронный запрос в синхронном контексте с работающим циклом")
        return None
    else:
        return loop.run_until_complete(classify_property(lot))