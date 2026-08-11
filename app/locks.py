"""Безопасное снятие Redis-замков (owner-aware release).

Замок берётся как `SET key <token> NX EX ttl`, где token уникален на владельца
(имя консьюмера). ПРОБЛЕМА безусловного `DEL key` при снятии: если TTL истёк на лету
(например event loop встал на паузу и heartbeat не успел продлить), замок мог перехватить
ДРУГОЙ воркер — и наш `DEL` снёс бы ЧУЖОЙ замок, разрешив параллельную обработку одного
диалога (ровно то, что замок и должен исключать).

Снимаем атомарно и только СВОЙ замок: сравниваем значение и удаляем одним Lua-скриптом
(GET+DEL под одной операцией — без гонки между проверкой и удалением)."""

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
        print("release_lock error:", repr(e))
        return False
