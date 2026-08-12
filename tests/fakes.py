"""Минимальный in-memory async-«Redis» для юнит-тестов.

Покрывает ровно те команды, что трогают тестируемые функции (order-gate, dispatch).
Никакой сети/сервера — только детерминированная семантика."""


class FakeRedis:
    def __init__(self):
        self.store: dict[str, str] = {}
        self.xadds: list[tuple[str, dict]] = []   # (stream, fields) для проверок
        self.acked: list[str] = []

    async def get(self, key):
        return self.store.get(key)

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

    async def publish(self, channel, data):
        return 0
