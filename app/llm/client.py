"""Клиент к внешнему LLM API. КТО ходит к модели: этот модуль, вызываемый ВОРКЕРОМ.
Возвращает async-генератор токенов (стриминг). API-слой сюда НЕ ходит."""
import json
import httpx
from ..config import settings

# Переиспользуемый клиент (keep-alive к LLM): создавать AsyncClient на каждый вызов —
# лишние TCP+TLS-хендшейки. Ленивый, чтобы создаваться внутри event loop воркера.
_client: httpx.AsyncClient | None = None


def _get_client() -> httpx.AsyncClient:
    global _client
    if _client is None:
        # ТАЙМАУТЫ обязательны: без read-таймаута зависшее соединение с моделью
        # блокирует воркер навсегда (и стопорит FIFO-гейт диалога). read — это
        # максимальная пауза МЕЖДУ токенами, а не общее время ответа.
        _client = httpx.AsyncClient(timeout=httpx.Timeout(
            connect=settings.llm_connect_timeout,
            read=settings.llm_read_timeout,
            write=settings.llm_write_timeout,
            pool=settings.llm_connect_timeout,
        ))
    return _client


async def stream_completion(messages: list[dict], max_tokens: int | None = None,
                            model: str | None = None):
    """Отправляет контекст в модель и отдаёт токены по мере генерации.
    messages: [{'role': 'user'|'assistant'|'system', 'content': str}, ...]
    max_tokens: жёсткий потолок ответа. None -> settings.max_response_tokens.
    Всегда задаём его: без max_tokens длина ответа не ограничена (стоимость,
    латентность, риск упереться в контекст на длинных диалогах).
    model: переопределение модели (None -> settings.llm_model). Нужно, напр., чтобы
    авто-название делать отдельной НЕ-reasoning моделью (см. title_llm_model)."""
    payload = {
        "model": model or settings.llm_model,
        "messages": messages,
        "stream": True,
        "max_tokens": max_tokens or settings.max_response_tokens,
    }
    headers = {"Authorization": f"Bearer {settings.llm_api_key}"}

    async with _get_client().stream("POST", settings.llm_api_url,
                                    json=payload, headers=headers) as resp:
        resp.raise_for_status()
        async for line in resp.aiter_lines():
            # OpenAI-совместимый SSE: строки вида "data: {...}"
            if not line or not line.startswith("data: "):
                continue
            data = line[len("data: "):]
            if data == "[DONE]":
                break
            chunk = json.loads(data)
            choices = chunk.get("choices") or []
            if not choices:                      # напр. кадр только с usage
                continue
            delta = (choices[0].get("delta") or {}).get("content")
            if delta:
                yield delta


async def complete(messages: list[dict], max_tokens: int | None = None,
                   model: str | None = None) -> str:
    """Нестриминговый вызов: собираем весь ответ в строку.
    Нужен суммаризатору — ему стриминг токенов не нужен, нужен готовый текст.
    max_tokens пробрасывается, чтобы ограничить объём резюме; model — чтобы
    делать заголовок отдельной моделью (см. title_llm_model)."""
    parts = [token async for token in stream_completion(messages, max_tokens, model)]
    return "".join(parts)
