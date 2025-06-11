import re
import logging
import orjson
import asyncio
from typing import Optional

from core.config import CONFIG
from core.models import PropertyClassification, Lot
from core.gpt_tunnel_client import chat

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

async def classify_property(lot: Lot) -> Optional[PropertyClassification]:
    """
    Классифицирует объект недвижимости с помощью GPT через туннель.
    """
    if not CONFIG["gpt_analysis_enabled"]:
        logger.info("GPT анализ отключен в настройках")
        return None
    
    try:
        description = f"{lot.name}\nПлощадь: {lot.area} м²\nТип: {lot.property_category}"
        
        prompt = CONFIG["gpt_prompt_template"].format(
            name=lot.name,
            description=description,
            area=lot.area,
            category=lot.property_category
        )
        
        raw = await chat(
            MODEL,
            [{"role": "user", "content": prompt}],
            max_tokens=200,
        )
        
        try:
            clean = _extract_json(raw)
            classification_data = orjson.loads(clean)
            
            classification = PropertyClassification(
                category=classification_data.get("category", ""),
                size_category=classification_data.get("size_category", ""),
                has_basement=classification_data.get("has_basement", False),
                is_top_floor=classification_data.get("is_top_floor", False)
            )
            
            logger.info(f"Лот {lot.id} успешно классифицирован как {classification.category}")
            return classification
            
        except Exception as e:
            logger.error(f"Ошибка парсинга ответа GPT для лота {lot.id}: {e}")
            logger.debug(f"Сырой ответ GPT: {raw[:300]}")
            return None
            
    except Exception as e:
        logger.error(f"Ошибка классификации лота {lot.id}: {e}")
        return None

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