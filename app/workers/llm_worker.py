"""LLM-ВОРКЕР = исполнитель №2 (асинхронный фоновый процесс). ЭТО НЕ FastAPI.

Запускается отдельной командой: `python -m app.workers.llm_worker`.
Крутится 24/7, висит на consumer group Redis Stream. Именно он:
  - КТО выполняет переносы данных между Redis и Postgres,
  - КТО ходит к модели (через app.llm.client),
  - КТО возвращает ответ пользователю (публикуя токены в durable-ленту).

Масштабируется горизонтально: запусти N копий с одной consumer group —
Redis раздаст им разные сообщения (каждое обработается ровно одним воркером).
Порядок внутри диалога держит гейт по seq (см. process), а не только замок.
"""
import asyncio
import json
import os
import signal
import socket
import time
import uuid

from redis.exceptions import ResponseError

from ..config import settings
from ..redis_client import get_redis
from ..migrate import migrate
from .. import keys, db, tokens
from ..locks import release_lock, heartbeat_lock
from ..deadletter import times_delivered, dead_letter
from ..log import setup_logging, get_logger
from ..llm.client import stream_completion

log = get_logger("chat.llm_worker")

# Уникальное имя консьюмера на процесс: hostname+pid+rnd. Иначе при масштабировании
# все реплики выглядят как один консьюмер, и PEL/XAUTOCLAIM работают некорректно.
CONSUMER_NAME = f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"


async def ensure_group(r):
    """Создаём consumer group один раз (idempotent). mkstream — если стрима ещё нет."""
    try:
        await r.xgroup_create(settings.inbound_stream,
                              settings.consumer_group, id="0", mkstream=True)
    except ResponseError as e:
        if "BUSYGROUP" not in str(e):  # группа уже существует — это норма
            raise


# ---------- ГОРЯЧИЙ КОНТЕКСТ: cache-aside между Redis и Postgres ----------

async def load_context(r, conversation_id: str) -> list[dict]:
    """Читаем окно из Redis. Если пусто (cache miss) — РЕГИДРАТАЦИЯ из Postgres."""
    key = keys.ctx_key(conversation_id)
    cached = await r.lrange(key, 0, -1)
    if cached:
        return [json.loads(x) for x in cached]

    # cache miss: юзер вернулся после простоя, контекст протух -> тянем из БД
    msgs = await db.load_recent_messages(conversation_id, settings.ctx_max_messages)
    if msgs:
        await r.rpush(key, *[json.dumps(m) for m in msgs])
        await r.expire(key, settings.ctx_ttl_seconds)  # прогрели кэш с новым TTL
    return msgs


async def append_context(r, conversation_id: str, message: dict) -> bool:
    """Идемпотентно добавляем сообщение в горячее окно (ltrim держит его коротким).

    При ретрае обработки то же сообщение (по `mid`+`role`) НЕ задваивается: сканируем
    текущее окно и, если оно уже там, ничего не делаем. Всё происходит под замком
    диалога, поэтому гонок внутри диалога нет. Возвращает True, если реально добавили
    (по этому флагу решаем, слать ли эхо / крутить ли счётчик суммаризации).

    Сообщения, выпавшие из окна, не теряются: они в Postgres и позже свернутся в summary."""
    key = keys.ctx_key(conversation_id)
    mid = message.get("mid")
    if mid:
        for raw in await r.lrange(key, 0, -1):
            e = json.loads(raw)
            if e.get("mid") == mid and e.get("role") == message.get("role"):
                return False                             # уже в окне (ретрай)
    await r.rpush(key, json.dumps(message))
    await r.ltrim(key, -settings.ctx_max_messages, -1)   # оставляем только хвост
    await r.expire(key, settings.ctx_ttl_seconds)        # продлеваем жизнь при активности
    return True


async def get_summary(r, conversation_id: str) -> str | None:
    """Резюме старой части диалога: из кэша Redis, при промахе — из Postgres."""
    cached = await r.get(keys.sum_key(conversation_id))
    if cached is not None:
        return cached or None
    summary, _ = await db.get_summary(conversation_id)
    if summary:
        await r.set(keys.sum_key(conversation_id), summary, ex=settings.ctx_ttl_seconds)
    return summary


async def build_prompt(r, conversation_id: str) -> list[dict]:
    """Финальный промпт = [summary как system] + горячее окно последних сообщений,
    собранный ПО ТОКЕН-БЮДЖЕТУ (не по числу сообщений).

    Контроль длины:
      1) summary кладём как system-голову и вычитаем его стоимость из бюджета;
      2) окно набираем С КОНЦА (свежие сообщения важнее), пока укладываемся;
      3) аварийный предохранитель: если даже одно последнее сообщение не влезает,
         берём его обрезанным — модель должна получить хотя бы текущий вопрос.
    Старое, не попавшее в бюджет, не теряется: оно в Postgres и свёрнуто в summary."""
    window = await load_context(r, conversation_id)
    summary = await get_summary(r, conversation_id)

    budget = settings.prompt_token_budget
    head: list[dict] = []
    if summary:
        # summary уже ограничен summary_max_tokens суммаризатором, но подстрахуемся
        summary = tokens.truncate_text(summary, settings.summary_max_tokens)
        head = [{"role": "system",
                 "content": f"Краткое содержание предыдущей части диалога:\n{summary}"}]
        budget -= tokens.count_messages(head)

    selected: list[dict] = []
    for msg in reversed(window):                 # с конца: новое приоритетнее
        cost = tokens.count_message(msg)
        if cost > budget:
            break
        budget -= cost
        selected.append(msg)
    selected.reverse()                           # вернуть хронологический порядок

    # предохранитель: окно пустое (сообщения слишком длинные) -> берём последнее,
    # обрезав до остатка бюджета, чтобы промпт гарантированно не превысил лимит
    if not selected and window:
        room = max(settings.prompt_token_budget - tokens.count_messages(head), 0)
        last = dict(window[-1])
        last["content"] = tokens.truncate_text(last["content"], room)
        selected = [last]

    # проекция к чистым {role, content}: во внутренних записях окна есть служебный
    # `mid` — в LLM его слать нельзя.
    return [{"role": m["role"], "content": m["content"]} for m in (head + selected)]


# ---------- ОБРАБОТКА ОДНОГО ВХОДЯЩЕГО СООБЩЕНИЯ ----------

async def _mark_applied(r, conversation_id: str, applied_value: int) -> None:
    """Отметить applied и КОГЕРЕНТНО продлить TTL счётчиков порядка (M1).

    Ставим applied и seq одним TTL в один момент -> они истекают вместе. Важно, что seq
    продлевается воркером НЕ РАНЬШЕ applied (здесь — одновременно), а api при INCR может
    лишь продлить seq дальше. Значит seq никогда не протухнет раньше applied, и «остывший»
    диалог не окажется в состоянии applied>seq (что дало бы ложный дроп seq<=applied на
    первом же новом сообщении). После простоя оба протухают вместе -> счёт с нуля, безопасно."""
    ttl = settings.conv_counter_ttl_seconds
    await r.set(keys.conv_applied(conversation_id), applied_value, ex=ttl)
    await r.expire(keys.conv_seq(conversation_id), ttl)


async def _order_gate(r, conversation_id: str, seq: int, fields: dict) -> bool:
    """FIFO-гейт по диалогу. Возвращает True, если сообщение можно применять СЕЙЧАС.

    Применяем строго по возрастанию seq (его назначает api через INCR). Так порядок
    сохраняется даже при N воркерах, где замок предотвращает одновременность, но НЕ
    переупорядочивание. Логика:
      - seq <= applied      -> уже применено (дубль/переигровка): пропустить (ack);
      - seq > applied + 1    -> предшественник ещё не применён: вернуть задачу в хвост
                                очереди и подождать. LIVENESS: если ждём дольше
                                order_gap_timeout_ms, считаем предшественника потерянным
                                (напр. api сделал INCR, но XADD упал) и ПРОПУСКАЕМ разрыв,
                                иначе сообщение кружило бы по очереди вечно;
      - seq == applied + 1   -> наша очередь: применять."""
    if not seq:
        return True                                  # старый формат без порядка
    applied = int(await r.get(keys.conv_applied(conversation_id)) or 0)
    if seq <= applied:
        return False
    if seq > applied + 1:
        now_ms = time.time() * 1000
        gate_since = float(fields.get("gate_since") or 0)
        if gate_since and now_ms - gate_since > settings.order_gap_timeout_ms:
            # предшественник, похоже, потерян -> перешагиваем разрыв (восстанавливаем
            # liveness). Потерянное сообщение, если придёт позже, отсеётся как seq<=applied.
            # ЭТО СОБЫТИЕ ПОТЕРИ (H3): сообщения [applied+1 .. seq-1] пропускаются
            # безвозвратно. Если это ложное срабатывание под backlog (предшественник ещё
            # ждал вычитки, а не потерян) — это потеря хода. Логируем ГРОМКО, чтобы
            # отслеживать в метриках/алертах и при частых срабатываниях поднять
            # order_gap_timeout_ms или разгрузить очередь.
            await _mark_applied(r, conversation_id, seq - 1)   # + продлить TTL (M1)
            log.warning("[order][LOSS] conv=%s: gap timeout after %dms, dropping "
                        "seq %d..%d, applying seq=%d", conversation_id,
                        settings.order_gap_timeout_ms, applied + 1, seq - 1, seq)
            return True
        if not gate_since:
            fields = {**fields, "gate_since": str(now_ms)}   # засекаем первое ожидание
        await r.xadd(settings.inbound_stream, fields,        # вернуть в хвост (с MAXLEN)
                     maxlen=settings.inbound_stream_maxlen, approximate=True)
        await asyncio.sleep(0.05)
        return False
    return True


async def process(r, fields: dict):
    conversation_id = fields["conversation_id"]
    text = fields["text"]
    message_id = fields.get("message_id") or None       # None -> дедупа нет
    seq = int(fields.get("seq", 0) or 0)
    channel = keys.conv_channel(conversation_id)

    # Порядок: применяем сообщения диалога строго по seq (см. _order_gate).
    if not await _order_gate(r, conversation_id, seq, fields):
        return

    # Замок по диалогу: сериализует обработку (пользователь мог прислать несколько
    # сообщений подряд). Вместе с гейтом порядка даёт FIFO без гонок контекста.
    # TTL с запасом + heartbeat -> замок не отпустится даже во время долгой генерации.
    lock = keys.conv_lock(conversation_id)
    while not await r.set(lock, CONSUMER_NAME, nx=True,
                          ex=settings.conv_lock_ttl_seconds):
        await asyncio.sleep(0.2)
    hb = asyncio.create_task(heartbeat_lock(
        r, lock, CONSUMER_NAME,
        settings.conv_lock_ttl_seconds, settings.lock_heartbeat_seconds))

    try:
        # ПЕРЕПРОВЕРКА ПОРЯДКА ПОД ЗАМКОМ: пока ждали замок, этот тур мог быть
        # применён другим воркером (reclaim/редоставка длинной генерации). Тогда
        # не повторяем — иначе была бы лишняя генерация (второй вызов LLM).
        if seq and seq <= int(await r.get(keys.conv_applied(conversation_id)) or 0):
            return

        # ИДЕМПОТЕНТНОСТЬ ВСЕГО ХОДА (M2): если ответ ассистента на этот message_id уже
        # лежит в Postgres (воркер упал ПОСЛЕ вставки ответа, но ДО отметки applied, и
        # задачу переобработали через reclaim) — НЕ генерируем заново. Иначе был бы второй
        # платный вызов LLM и расхождение: клиент увидел бы новый ответ, а в БД остался бы
        # прежний (вставка идемпотентна по (message_id,'assistant') -> DO NOTHING). Ответ
        # уже сгенерирован, сохранён и (при первой попытке) отстримлен — просто
        # досдвигаем applied и выходим.
        if message_id and await db.assistant_exists(conversation_id, message_id):
            if seq:
                await _mark_applied(r, conversation_id, seq)
            return

        # 1) ПЕРЕНОС hot<-: сообщение юзера. БД идемпотентна (ON CONFLICT по
        #    message_id) — это источник истины. Окно тоже идемпотентно (по mid),
        #    эхо шлём ТОЛЬКО при реальном добавлении -> без дублей у клиента.
        await db.insert_message(conversation_id, "user", text, message_id)
        added = await append_context(
            r, conversation_id, {"role": "user", "content": text, "mid": message_id})
        if added:
            await r.publish(channel, json.dumps(
                {"type": "user_message", "content": text, "message_id": message_id}))

        # 2) собираем ПРОМПТ = summary (из Redis/PG) + горячее окно (по токен-бюджету)
        prompt = await build_prompt(r, conversation_id)

        # 3) генерация в DURABLE-ленту gen:{id}:{mid}. Лента ВСЕГДА завершается
        #    записью end и получает TTL — даже при ошибке LLM (иначе она утекла бы
        #    в Redis навсегда, а клиенты висели бы без assistant_end). Сама генерация
        #    НЕ идемпотентна: ретрай после ошибки — это осознанно новый ответ.
        gen_message_id = str(uuid.uuid4())
        gen_key = keys.gen_stream(conversation_id, gen_message_id)
        await r.set(keys.active_gen(conversation_id), gen_message_id,
                    ex=settings.gen_ttl_seconds)
        await r.publish(channel, json.dumps(
            {"type": "gen_start", "message_id": gen_message_id}))

        parts: list[str] = []
        try:
            async for token in stream_completion(prompt):
                parts.append(token)
                await r.xadd(gen_key, {"t": "token", "c": token})
                # H1: держим TTL на ленте, ПОКА она наполняется. Финальный gen_ttl
                # ставится только после end; при жёстком падении воркера (SIGKILL/OOM)
                # до end лента осталась бы БЕЗ TTL навсегда -> утечка Redis. Первый токен
                # ставит active-TTL, дальше изредка продлеваем (один EXPIRE на 256 токенов
                # — пренебрежимо дёшево). Брошенная лента протухнет через gen_active_ttl.
                if len(parts) == 1 or len(parts) % 256 == 0:
                    await r.expire(gen_key, settings.gen_active_ttl_seconds)
        except Exception:
            await r.xadd(gen_key, {"t": "end", "error": "1"})   # завершить ленту
            await r.expire(gen_key, settings.gen_ttl_seconds)
            await r.delete(keys.active_gen(conversation_id))
            raise                                               # -> не ackать, ретрай
        answer = "".join(parts)
        empty = not answer.strip()
        # Завершаем ленту: с пометкой ошибки, если ответ ПУСТОЙ (все дельты пустые или
        # ошибка в теле ответа со статусом 200) — иначе клиент увидел бы финализированный
        # пустой бабл без признака проблемы.
        await r.xadd(gen_key, {"t": "end", "error": "1"} if empty else {"t": "end"})
        await r.expire(gen_key, settings.gen_ttl_seconds)
        await r.delete(keys.active_gen(conversation_id))

        if empty:
            # Пустой ответ: НЕ сохраняем пустого ассистента (не мусорим историю/контекст)
            # и не триггерим суммаризацию/название. Ход завершён — помечаем applied и
            # выходим (сообщение будет ack'нуто), чтобы не ретраить бесконечно. Сообщение
            # юзера уже сохранено, клиент увидел индикатор ошибки в assistant_end.
            log.warning("empty completion conv=%s message_id=%s — ответ не сохранён",
                        conversation_id, message_id)
            if seq:
                await _mark_applied(r, conversation_id, seq)
            return

        # 4) ПЕРЕНОС ->cold: ответ ассистента. message_id ассистента = inbound
        #    message_id (роль различает строки в БД) -> ретрай той же обработки не
        #    задваивает. Триггер суммаризации крутим ТОЛЬКО при реальной вставке,
        #    чтобы счётчик не накрутился дважды на ретрае.
        inserted = await db.insert_message(
            conversation_id, "assistant", answer, message_id)
        await append_context(r, conversation_id,
                             {"role": "assistant", "content": answer, "mid": message_id})
        if inserted:
            n = await r.incrby(keys.since_sum_key(conversation_id), 2)
            await r.expire(keys.since_sum_key(conversation_id),   # TTL счётчика (M1)
                           settings.conv_counter_ttl_seconds)
            if n >= settings.summary_trigger_messages:
                await r.set(keys.since_sum_key(conversation_id), 0,
                            ex=settings.conv_counter_ttl_seconds)
                await r.xadd(settings.summarize_stream,
                             {"conversation_id": conversation_id},
                             maxlen=settings.summarize_stream_maxlen, approximate=True)

            # АВТО-НАЗВАНИЕ: один раз на диалог (NX-маркер дедупит). Отдельная фоновая
            # задача в тот же стрим — заголовок генерит суммаризатор отдельным LLM-вызовом.
            if await r.set(keys.title_enq(conversation_id), "1", nx=True,
                           ex=settings.title_enqueue_ttl_seconds):
                await r.xadd(settings.summarize_stream,
                             {"conversation_id": conversation_id, "task": "title"},
                             maxlen=settings.summarize_stream_maxlen, approximate=True)

        # Порядок: помечаем seq применённым СТРОГО после успеха (до снятия замка).
        # _mark_applied заодно когерентно продлевает TTL счётчиков порядка (M1).
        if seq:
            await _mark_applied(r, conversation_id, seq)
    finally:
        hb.cancel()
        # Снимаем ТОЛЬКО свой замок (owner-aware). Безусловный DEL мог бы снести замок,
        # который уже перехватил другой воркер после истечения TTL на лету (C2).
        await release_lock(r, lock, CONSUMER_NAME)


# ---------- ВОССТАНОВЛЕНИЕ ЗАВИСШИХ ЗАДАЧ (PEL) ----------

async def reclaim_stale(r):
    """Забрать у «мёртвых» воркеров задачи, висящие в PEL дольше reclaim_min_idle_ms,
    и обработать их. Без этого сообщение, у которого воркер умер между чтением и
    XACK, зависло бы навсегда (и застопорило бы гейт порядка диалога)."""
    try:
        res = await r.xautoclaim(
            settings.inbound_stream, settings.consumer_group, CONSUMER_NAME,
            min_idle_time=settings.reclaim_min_idle_ms, start_id="0-0", count=10,
        )
    except ResponseError:
        return
    # XAUTOCLAIM возвращает [cursor, messages, (deleted)] — 3 элемента на Redis 7,
    # 2 на Redis 6.2. Берём messages устойчиво к версии.
    entries = res[1] if isinstance(res, (list, tuple)) and len(res) > 1 else []
    for msg_id, fields in entries:
        if not fields:                      # запись уже удалена из стрима -> просто ack
            await r.xack(settings.inbound_stream, settings.consumer_group, msg_id)
            continue
        # DEAD-LETTER: «отравленная» задача (падает раз за разом) — после лимита доставок
        # снимаем её с очереди, чтобы не крутить вечно (см. deadletter.py).
        if await times_delivered(r, settings.inbound_stream, settings.consumer_group,
                                 msg_id) > settings.max_deliveries:
            await dead_letter(r, settings.inbound_stream, settings.consumer_group,
                              msg_id, fields, settings.dead_letter_maxlen)
            continue
        try:
            await process(r, fields)
            await r.xack(settings.inbound_stream, settings.consumer_group, msg_id)
        except Exception:
            log.exception("reclaim process error msg_id=%s", msg_id)


# ---------- ГЛАВНЫЙ ЦИКЛ ВОРКЕРА ----------

def _install_stop(stop: asyncio.Event):
    """SIGINT/SIGTERM -> мягкая остановка: дообработать текущее сообщение и выйти."""
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:         # напр. не-Unix
            pass


async def main():
    setup_logging(settings.log_level)
    await migrate()                 # схема БД актуальна (идемпотентно)
    r = get_redis()
    await ensure_group(r)
    stop = asyncio.Event()
    _install_stop(stop)
    log.info("llm_worker started as %s, listening on %s",
             CONSUMER_NAME, settings.inbound_stream)

    iters = 0
    while not stop.is_set():
        iters += 1
        # периодически забираем зависшие задачи у мёртвых воркеров
        if iters % settings.reclaim_every_iters == 0:
            await reclaim_stale(r)

        # блокирующее чтение новых сообщений (">" = только ещё не выданные группе)
        resp = await r.xreadgroup(
            settings.consumer_group, CONSUMER_NAME,
            {settings.inbound_stream: ">"}, count=1, block=5000,
        )
        if not resp:
            continue
        for _stream, entries in resp:
            for msg_id, fields in entries:
                try:
                    await process(r, fields)
                    # XACK — подтверждаем обработку; иначе сообщение зависнет в PEL
                    await r.xack(settings.inbound_stream, settings.consumer_group, msg_id)
                except Exception:
                    # не ackаем: сообщение останется в PEL и будет переобработано
                    # (нашим reclaim_stale или другим воркером после таймаута).
                    log.exception("process error msg_id=%s", msg_id)

    log.info("llm_worker stopping: %s", CONSUMER_NAME)


if __name__ == "__main__":
    asyncio.run(main())
