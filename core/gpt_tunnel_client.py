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

def sync_chat(model: str, messages: list[dict], max_tokens: int = 500) -> str:
    """
    Синхронная версия функции chat для вызова из синхронного кода.
    model – "gpt-4o-mini", "gpt-3.5-turbo" и т.п.
    messages – [{"role":"system","content":"..."}, {"role":"user","content":"..."}]
    """
    try:
        # Проверяем, есть ли уже запущенный event loop
        try:
            loop = asyncio.get_running_loop()
            # Если есть активный loop, создаем задачу в нем
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(_run_in_new_loop, model, messages, max_tokens)
                result = future.result(timeout=30)  # 30 секунд таймаут
                return result
        except RuntimeError:
            # Нет активного loop, можем создать новый
            return asyncio.run(chat(model, messages, max_tokens))
            
    except Exception as e:
        logging.error(f"Ошибка при синхронном вызове GPT API: {e}")
        return "{}"  # Return empty JSON to prevent parsing errors

def _run_in_new_loop(model: str, messages: list[dict], max_tokens: int = 500) -> str:
    """Запускает async функцию в новом event loop в отдельном потоке."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(chat(model, messages, max_tokens))
    finally:
        loop.close()