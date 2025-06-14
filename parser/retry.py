import asyncio, logging, random, time
from functools import wraps
from inspect import iscoroutinefunction
from typing import Callable, Any

def retry(retries: int = 5, base_delay: float = 0.5, *, exceptions: tuple[type[Exception], ...] = (Exception,)):
    """Unified sync / async retry with exponential backoff."""
    def decorator(func: Callable[..., Any]):
        if iscoroutinefunction(func):
            @wraps(func)
            async def async_wrap(*args, **kwargs):
                delay = base_delay
                for attempt in range(1, retries + 1):
                    try:
                        return await func(*args, **kwargs)
                    except exceptions as e:
                        if attempt == retries:
                            raise
                        logging.warning("%s failed: %s. Retry %s/%s in %.1fs", func.__name__, e, attempt, retries, delay)
                        await asyncio.sleep(delay + random.random())
                        delay *= 2
            return async_wrap
        else:
            @wraps(func)
            def sync_wrap(*args, **kwargs):
                delay = base_delay
                for attempt in range(1, retries + 1):
                    try:
                        return func(*args, **kwargs)
                    except exceptions as e:
                        if attempt == retries:
                            raise
                        logging.warning("%s failed: %s. Retry %s/%s in %.1fs", func.__name__, e, attempt, retries, delay)
                        time.sleep(delay + random.random())
                        delay *= 2
            return sync_wrap
    return decorator