"""Подсчёт и обрезка токенов — ЕДИНЫЙ модуль контроля длины.

Зачем отдельный модуль: api, llm_worker и summarizer должны мерить длину
ОДИНАКОВО, иначе один соберёт промпт «по бюджету», а другой посчитает иначе.
Считаем через tiktoken; для неизвестных моделей падаем на cl100k_base
(достаточно точная оценка для бюджетирования — нам не нужна побитовая точность,
нужен предсказуемый потолок).
"""
from functools import lru_cache

import tiktoken

from .config import settings

# Служебная надбавка на обёртку каждого сообщения в chat-формате (роль, разметка).
# Эвристика OpenAI (~4 токена/сообщение) — берём с запасом, чтобы НЕ занижать.
_PER_MESSAGE_OVERHEAD = 4
# Надбавка на «затравку» ответа ассистента в конце промпта.
_PER_REQUEST_OVERHEAD = 3


# Грубая оценка, если tiktoken недоступен: ~3 символа на токен. Берём 3 (а не 4),
# чтобы СКОРЕЕ завысить длину — для бюджета безопаснее переоценить, чем недооценить.
_CHARS_PER_TOKEN = 3


@lru_cache(maxsize=8)
def _encoding(model: str):
    """Энкодер tiktoken или None, если его не удалось получить.

    ВАЖНО для закрытого контура: tiktoken при первом вызове может ХОДИТЬ В СЕТЬ
    за BPE-рангами. В офлайн-окружении это падает — тогда возвращаем None и
    считаем токены грубой эвристикой, а не роняем весь сервис. Чтобы считать
    точно, положите кэш рангов в образ и задайте TIKTOKEN_CACHE_DIR."""
    try:
        return tiktoken.encoding_for_model(model)
    except KeyError:
        pass
    except Exception:
        return None
    try:
        return tiktoken.get_encoding("cl100k_base")
    except Exception:
        return None


def _approx(text: str) -> int:
    return max(1, len(text) // _CHARS_PER_TOKEN)


def count_text(text: str, model: str | None = None) -> int:
    """Число токенов в строке (точно через tiktoken, иначе — эвристика)."""
    text = text or ""
    enc = _encoding(model or settings.llm_model)
    if enc is None:
        return _approx(text)
    try:
        return len(enc.encode(text))
    except Exception:
        return _approx(text)


def count_message(msg: dict, model: str | None = None) -> int:
    """Токены одного chat-сообщения с учётом служебной обёртки роли."""
    return count_text(msg.get("content", ""), model) + _PER_MESSAGE_OVERHEAD


def count_messages(messages: list[dict], model: str | None = None) -> int:
    """Токены всего списка сообщений (как их увидит модель)."""
    return sum(count_message(m, model) for m in messages) + _PER_REQUEST_OVERHEAD


def truncate_text(text: str, max_tokens: int, model: str | None = None) -> str:
    """Жёстко обрезать строку до max_tokens (аварийный предохранитель).
    Режем по токенам; при недоступном tiktoken — по символам (та же эвристика 3:1)."""
    text = text or ""
    enc = _encoding(model or settings.llm_model)
    if enc is None:
        return text[:max_tokens * _CHARS_PER_TOKEN]
    try:
        ids = enc.encode(text)
    except Exception:
        return text[:max_tokens * _CHARS_PER_TOKEN]
    if len(ids) <= max_tokens:
        return text
    return enc.decode(ids[:max_tokens])


def chunk_messages(messages: list[dict], max_tokens: int,
                   model: str | None = None) -> list[list[dict]]:
    """Разбить список сообщений на чанки, каждый <= max_tokens.
    Нужно суммаризатору: большой накопившийся хвост нельзя свернуть одним
    промптом (сам промпт превысит контекст) — вкатываем его в резюме по частям.
    Сообщение, которое само больше чанка, выносится в отдельный чанк (обрежется
    ниже по стеку)."""
    chunks: list[list[dict]] = []
    current: list[dict] = []
    current_tokens = 0
    for msg in messages:
        cost = count_message(msg, model)
        if current and current_tokens + cost > max_tokens:
            chunks.append(current)
            current, current_tokens = [], 0
        current.append(msg)
        current_tokens += cost
    if current:
        chunks.append(current)
    return chunks
