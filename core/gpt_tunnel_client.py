# core/gpt_tunnel_client.py
import os, httpx, asyncio, logging
logger = logging.getLogger(__name__)

API_KEY  = os.getenv("OPENAI_API_KEY")                 # ваш ключ shds-…
BASE_URL = "https://gptunnel.ru/v1"

HEADERS  = {"Authorization": API_KEY}
TIMEOUT  = httpx.Timeout(60.0, connect=15.0)

async def chat(model: str, messages: list[dict], max_tokens: int = 500) -> str:
    """
    model – "gpt-4o-mini", "gpt-3.5-turbo" и т.п.
    messages – [{"role":"system","content":"…"}, {"role":"user","content":"…"}]
    Возвращает сырое текстовое содержимое assistant.
    """
    payload = {
        "model": model,
        "max_tokens": max_tokens,
        "messages": messages,
    }
    async with httpx.AsyncClient(base_url=BASE_URL, headers=HEADERS, timeout=TIMEOUT) as client:
        r = await client.post("/chat/completions", json=payload)
        r.raise_for_status()
        data = r.json()
        text = data["choices"][0]["message"]["content"]
        return text
