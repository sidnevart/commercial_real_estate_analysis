# core/gpt_tunnel_client.py
import os, httpx, asyncio, logging
logger = logging.getLogger(__name__)

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
