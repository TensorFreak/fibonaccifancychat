"""Минимальный in-memory async-«Redis» для юнит-тестов.

Покрывает ровно те команды, что трогают тестируемые функции (order-gate, dispatch).
Никакой сети/сервера — только детерминированная семантика."""


class FakeRedis:
    def __init__(self):
        self.store: dict[str, str] = {}
        self.lists: dict[str, list[str]] = {}     # горячее окно ctx:{id} и т.п.
        self.xadds: list[tuple[str, dict]] = []   # (stream, fields) для проверок
        self.acked: list[str] = []

    async def get(self, key):
        return self.store.get(key)

    # --- списки (горячее окно): семантика Redis для отрицательных индексов ---
    @staticmethod
    def _slice(lst, start, end):
        n = len(lst)
        if start < 0:
            start = max(n + start, 0)
        if end < 0:
            end = n + end
        if end >= n:
            end = n - 1
        if n == 0 or start > end:
            return []
        return lst[start:end + 1]

    async def lrange(self, key, start, end):
        return list(self._slice(self.lists.get(key, []), start, end))

    async def rpush(self, key, *values):
        self.lists.setdefault(key, []).extend(str(v) for v in values)
        return len(self.lists[key])

    async def ltrim(self, key, start, end):
        self.lists[key] = list(self._slice(self.lists.get(key, []), start, end))
        return True

    async def set(self, key, value, nx=False, ex=None):
        if nx and key in self.store:
            return False
        self.store[key] = str(value)
        return True

    async def incr(self, key):
        self.store[key] = str(int(self.store.get(key, 0)) + 1)
        return int(self.store[key])

    async def incrby(self, key, amount):
        self.store[key] = str(int(self.store.get(key, 0)) + amount)
        return int(self.store[key])

    async def expire(self, key, ttl):
        return key in self.store

    async def delete(self, *keys):
        n = 0
        for k in keys:
            n += 1 if self.store.pop(k, None) is not None else 0
        return n

    async def xadd(self, stream, fields, maxlen=None, approximate=False):
        self.xadds.append((stream, dict(fields)))
        return f"{len(self.xadds)}-0"

    async def xack(self, stream, group, msg_id):
        self.acked.append(msg_id)
        return 1

    async def eval(self, script, numkeys, *args):
        """Эмулируем ЕДИНСТВЕННЫЙ eval в коде — _ENQUEUE_LUA (атомарный приём #1):
        INCR seq -> EXPIRE -> XADD в inbound с seq в полях, возвращаем seq. Мутируем store
        и xadds только если дошли до конца: так эмуляция сохраняет свойство «всё или ничего»
        реального Lua-скрипта (сбой не оставляет ни seq, ни записи в стриме).
        KEYS=[seq_key, stream], ARGV=[ttl, maxlen, conv_id, user_id, message_id, text]."""
        seq_key, stream = args[0], args[1]
        _ttl, _maxlen, conv, user, mid, text = args[numkeys:numkeys + 6]
        seq = int(self.store.get(seq_key, 0)) + 1
        self.store[seq_key] = str(seq)
        self.xadds.append((stream, {
            "conversation_id": conv, "user_id": user,
            "message_id": mid, "seq": str(seq), "text": text}))
        return seq
