"""Общие примитивы Redis-замков (owner-aware): продление и снятие.

Замок берётся как `SET key <token> NX EX ttl`, где token уникален на владельца
(имя консьюмера). ПРОБЛЕМА безусловного `DEL key` при снятии: если TTL истёк на лету
(например event loop встал на паузу и heartbeat не успел продлить), замок мог перехватить
ДРУГОЙ воркер — и наш `DEL` снёс бы ЧУЖОЙ замок, разрешив параллельную обработку одного
диалога (ровно то, что замок и должен исключать).

Снимаем атомарно и только СВОЙ замок: сравниваем значение и удаляем одним Lua-скриптом
(GET+DEL под одной операцией — без гонки между проверкой и удалением). Продлеваем тоже
только СВОЙ замок (проверка владения перед EXPIRE)."""
import asyncio

from .log import get_logger

log = get_logger("chat.locks")

# KEYS[1] = ключ замка, ARGV[1] = ожидаемый токен владельца.
_RELEASE_LUA = (
    "if redis.call('get', KEYS[1]) == ARGV[1] "
    "then return redis.call('del', KEYS[1]) else return 0 end"
)


async def release_lock(r, key: str, token: str) -> bool:
    """Снять замок key, только если он всё ещё наш (значение == token).
    Возвращает True, если реально сняли; False — если замок уже не наш (истёк/перехвачен)."""
    try:
        return bool(await r.eval(_RELEASE_LUA, 1, key, token))
    except Exception as e:                       # снятие замка не должно ронять обработку
        log.warning("release_lock error key=%s: %r", key, e)
        return False


async def heartbeat_lock(r, key: str, token: str, ttl: int, interval: int) -> None:
    """Держим СВОЙ замок живым во время долгой работы: раз в interval продлеваем TTL,
    пока владеем им. Без этого работа дольше TTL освободила бы замок на лету, и другой
    воркер начал бы дублирующую обработку. Как только замок уже не наш — выходим.
    Запускается фоновой задачей и отменяется вызывающим в finally (до release_lock)."""
    while True:
        await asyncio.sleep(interval)
        if await r.get(key) == token:
            await r.expire(key, ttl)
        else:
            return                               # замок уже не наш — прекращаем продление
