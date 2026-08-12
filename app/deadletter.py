"""Dead-letter для «отравленных» (постоянно падающих) задач стрима.

Задача, которая всегда падает в обработке, никогда не ackается и висит в PEL — reclaim
переобрабатывал бы её бесконечно (и зря жёг бы ресурсы). После max_deliveries попыток
переносим её в поток `<stream>:dead` и подтверждаем (XACK) оригинал, чтобы очередь жила,
а «ядовитую» задачу можно было разобрать вручную (`XRANGE <stream>:dead - +`)."""
from .log import get_logger

log = get_logger("chat.deadletter")


async def times_delivered(r, stream: str, group: str, msg_id: str) -> int:
    """Сколько раз задачу уже пытались доставить (счётчик PEL). Растёт при каждом
    XREADGROUP/XAUTOCLAIM — по нему и определяем «отравленную» задачу."""
    pend = await r.xpending_range(stream, group, min=msg_id, max=msg_id, count=1)
    return pend[0]["times_delivered"] if pend else 0


async def dead_letter(r, stream: str, group: str, msg_id: str,
                      fields: dict, maxlen: int) -> None:
    """Перенести задачу в `<stream>:dead` (с MAXLEN) и подтвердить оригинал."""
    await r.xadd(f"{stream}:dead",
                 {**fields, "_orig_id": str(msg_id), "_reason": "max_deliveries"},
                 maxlen=maxlen, approximate=True)
    await r.xack(stream, group, msg_id)
    log.error("dead-letter: stream=%s msg_id=%s conv=%s — превышен лимит доставок, "
              "задача снята с очереди", stream, msg_id, fields.get("conversation_id"))
