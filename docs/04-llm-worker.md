# 04. LLM-воркер

**Файл:** [`app/workers/llm_worker.py`](../app/workers/llm_worker.py) · **Исполнитель:** №2 (фоновый процесс)
**Запуск:** `python -m app.workers.llm_worker`

> Правки надёжности этого воркера — идемпотентность (дедуп по `message_id`), FIFO-гейт
> по `seq`, гарантированное завершение ленты при ошибке LLM, реклейм PEL, уникальное имя
> консьюмера, мягкая остановка — описаны в [10. Харденинг](10-hardening.md). Ниже —
> базовая механика; там, где поведение изменилось, стоят ссылки.

## Роль

Воркер — асинхронный фоновый процесс, крутится 24/7 на consumer group `chat:inbound`. Именно он:

- **ходит к модели** (через `app.llm.client`);
- **переносит данные** между Redis и Postgres;
- **возвращает ответ** пользователю, публикуя токены в durable-ленту.

Это НЕ FastAPI. api и воркер общаются только через Redis. Масштабируется горизонтально: N копий в одной группе делят входящий поток, каждое сообщение обрабатывается ровно один раз.

## Главный цикл

```python
await ensure_group(r)                     # создать consumer group (идемпотентно)
while True:
    resp = await r.xreadgroup(group, name, {inbound: ">"}, count=1, block=5000)
    for _stream, entries in resp:
        for msg_id, fields in entries:
            try:
                await process(r, fields)
                await r.xack(inbound, group, msg_id)   # подтвердить
            except Exception:
                log.exception("process error msg_id=%s", msg_id)  # НЕ ackать -> PEL -> повтор
```

Логи — читаемый однострочный формат в stdout (`app/log.py`); ошибки — с трейсбеком
(`log.exception`). «Отравленную» задачу, падающую раз за разом, `reclaim_stale` после
`max_deliveries` уводит в `chat:inbound:dead` (dead-letter, [10](10-hardening.md), R5-4).

Про `xreadgroup`, `">"`, `XACK` и PEL — см. [03. Очередь входящих](03-inbound-queue.md).

## Обработка одного сообщения: `process()`

### Шаг 0 — замок диалога

```python
lock = keys.conv_lock(conversation_id)
while not await r.set(lock, CONSUMER_NAME, nx=True, ex=settings.conv_lock_ttl_seconds):
    await asyncio.sleep(0.2)
hb = asyncio.create_task(heartbeat_lock(             # продлеваем TTL во время генерации
    r, lock, CONSUMER_NAME, settings.conv_lock_ttl_seconds, settings.lock_heartbeat_seconds))
# ... в finally: hb.cancel(); await release_lock(r, lock, CONSUMER_NAME)
```

Пользователь мог прислать 2–3 сообщения подряд, пока шла генерация. Их разберут **разные** воркеры. Без сериализации они бы перемешали контекст (ответ на второе сообщение попал бы в историю раньше первого). Замок `lock:conv:{id}` гарантирует: сообщения одного диалога обрабатываются строго по очереди.

- `SET ... NX EX` — атомарный захват с TTL `conv_lock_ttl_seconds` (300 c). Значение — уникальный `CONSUMER_NAME` владельца.
- **Heartbeat:** долгая генерация (большой контекст) может идти дольше TTL; фоновая задача (`heartbeat_lock` из `app/locks.py`) продлевает замок, пока **владеем** им, — иначе замок истёк бы на лету и другой воркер начал бы дублирующую обработку ([10](10-hardening.md)).
- Не смогли захватить → ждём 0.2 c и пробуем снова (простой честный spin-wait).
- Снимается в `finally` через **`release_lock`** — owner-aware (Lua compare-and-delete: снимаем ТОЛЬКО свой замок, не чужой, перехваченный после истечения TTL на лету, [10](10-hardening.md), C2). Heartbeat отменяется там же.

> Параллелизм — **между** диалогами. **Внутри** одного диалога — строго последовательно. Это by design.

> **Важно:** замок предотвращает *одновременность*, но не *переупорядочивание* при
> нескольких воркерах. Порядок FIFO обеспечивает отдельный гейт по `seq` (применяем
> только `seq == applied+1`, иначе возвращаем задачу в хвост). См. [10](10-hardening.md), H1.

### Шаг 1 — сохранить сообщение пользователя

```python
await db.insert_message(conversation_id, "user", text, message_id)   # Postgres (истина), идемпотентно
added = await append_context(r, conversation_id,
                             {"role": "user", "content": text, "mid": message_id})  # ctx
if added:                                                            # эхо только при реальной вставке
    await r.publish(channel, json.dumps(
        {"type": "user_message", "content": text, "message_id": message_id}))
```

Сообщение уходит в Postgres (навсегда, вставка идемпотентна по `message_id`), в горячее окно `ctx:{id}` (для промпта) и эхом в Pub/Sub — чтобы **другие** устройства пользователя увидели его реплику. Эхо шлётся **только если** сообщение реально добавилось в окно (`added`), иначе на ретрае был бы дубль у клиента. Про `append_context` — см. [05. Горячий контекст](05-hot-context.md).

### Шаг 2 — собрать промпт

```python
prompt = await build_prompt(r, conversation_id)
```

Промпт = `[summary как system]` + горячее окно, собранные **по токен-бюджету** (не по числу сообщений). Логика бюджета, обрезки и предохранителей — в [08. Контроль длины](08-token-budget.md).

### Шаг 3 — генерация в durable-ленту

Перед генерацией — идемпотентность ВСЕГО хода: если ответ на этот `message_id` уже в БД
(воркер упал после вставки ответа, но до отметки `applied`, и задачу переиграли), НЕ
генерируем повторно — иначе второй платный вызов LLM и расхождение клиент/БД ([10](10-hardening.md), R3-M2):

```python
if message_id and await db.assistant_exists(conversation_id, message_id):
    if seq: await _mark_applied(r, conversation_id, seq)   # досдвигаем порядок и выходим
    return

gen_message_id = str(uuid.uuid4())                        # отдельный id этой генерации
gen_key = keys.gen_stream(conversation_id, gen_message_id)
await r.set(keys.active_gen(conversation_id), gen_message_id, ex=settings.gen_ttl_seconds)
await r.publish(channel, json.dumps({"type": "gen_start", "message_id": gen_message_id}))

parts = []
async for token in stream_completion(prompt):
    parts.append(token)
    await r.xadd(gen_key, {"t": "token", "c": token})     # durable дельта
    if len(parts) == 1 or len(parts) % 256 == 0:          # H1: active-TTL пока лента растёт
        await r.expire(gen_key, settings.gen_active_ttl_seconds)
answer = "".join(parts)
empty = not answer.strip()                                # пустой ответ -> помечаем ошибкой
await r.xadd(gen_key, {"t": "end", "error": "1"} if empty else {"t": "end"})   # терминал
await r.expire(gen_key, settings.gen_ttl_seconds)
await r.delete(keys.active_gen(conversation_id))
```

Токены идут в durable Redis Stream, а не в эфемерный Pub/Sub — это делает поток resumable. `active_gen` + `gen_start` дают клиентам понять, какую ленту тейлить. Полный разбор — [07. Resumable streams](07-resumable-streams.md). Генерация обёрнута в `try/except`: при ошибке LLM лента завершается записью `end` с `error=1` и получает TTL — не утекает ([10](10-hardening.md), H3). **Пустой ответ** модели помечается тем же `error=1` и НЕ сохраняется как сообщение ассистента ([10](10-hardening.md), R5-2).

### Шаг 4 — сохранить ответ

```python
inserted = await db.insert_message(conversation_id, "assistant", answer, message_id)  # тот же message_id, роль различает строки
await append_context(r, conversation_id, {"role": "assistant", "content": answer, "mid": message_id})
```

Финальный ответ — в Postgres (истина, идемпотентно по `(message_id, 'assistant')`) и в горячее окно. `inserted` (реально вставлено, а не дубль-ретрай) решает, крутить ли триггер суммаризации и авто-название (шаг 5).

### Шаг 5 — триггер суммаризации

```python
if inserted:                                                 # только при реальной вставке (не на ретрае)
    n = await r.incrby(keys.since_sum_key(conversation_id), 2)   # +user +assistant
    await r.expire(keys.since_sum_key(conversation_id), settings.conv_counter_ttl_seconds)
    if n >= settings.summary_trigger_messages:
        await r.set(keys.since_sum_key(conversation_id), 0, ex=settings.conv_counter_ttl_seconds)
        await r.xadd(settings.summarize_stream, {"conversation_id": conversation_id})
    # АВТО-НАЗВАНИЕ: один раз на диалог (NX-маркер title:enq дедупит)
    if await r.set(keys.title_enq(conversation_id), "1", nx=True, ex=settings.title_enqueue_ttl_seconds):
        await r.xadd(settings.summarize_stream, {"conversation_id": conversation_id, "task": "title"})
# порядок: помечаем seq применённым строго после успеха (до снятия замка)
if seq: await _mark_applied(r, conversation_id, seq)
```

Считаем новые сообщения (за ход +2, только на реальной вставке — чтобы ретрай не накрутил счётчик). Накопилось достаточно → ставим задачу в **отдельную** очередь и сбрасываем счётчик. Первый ответ ассистента заодно ставит задачу авто-названия (`task: "title"`). Саму свёртку и заголовок делает summarizer, **не блокируя** ответ пользователю. См. [06. Суммаризация](06-summarization.md).

## Кто пишет что: сводка переносов

| Данные | Куда | Когда |
|---|---|---|
| сообщение юзера | Postgres + `ctx:{id}` | шаг 1 |
| эхо `user_message` | Pub/Sub `conv:{id}` | шаг 1 |
| токены ответа | Stream `gen:{id}:{mid}` | шаг 3 |
| финальный ответ | Postgres + `ctx:{id}` | шаг 4 |
| задача суммаризации | Stream `chat:summarize` | шаг 5 (по триггеру) |

## Отказы и восстановление

- **Воркер упал в середине `process`** до `XACK` → запись в PEL, будет переобработана (at-least-once) нашим `reclaim_stale` или другим воркером. Замок (TTL 300 c, heartbeat умер вместе с воркером) сам отпустится. Дублей нет: вставки идемпотентны по `message_id` ([10](10-hardening.md), C3).
- **Воркер упал во время генерации** → лента `gen:{id}:{mid}` осталась без записи `end`; `active_gen` протухнет по TTL. Клиентский тейл добьёт до TTL и выйдет; полный ответ восстановится из Postgres при перезагрузке истории (но частичной генерации в Postgres нет — туда пишется только законченный ответ на шаге 4).

## Параметры

| Параметр | Значение | Где влияет |
|---|---|---|
| `conv_lock_ttl_seconds` | 300 c (+heartbeat) | TTL замка диалога; продлевается во время генерации |
| `summary_trigger_messages` | 20 | порог запуска суммаризации |
| `gen_ttl_seconds` / `gen_active_ttl_seconds` | 300 / 900 | жизнь ленты генерации после `end` / во время |
| `reclaim_min_idle_ms` | 300000 | порог реклейма PEL (валидатор: `>= conv_lock_ttl*1000`) |
| `CONSUMER_NAME` | `{hostname}-{pid}-{rnd}` | имя консьюмера — уникально на процесс (для PEL/`XAUTOCLAIM`) |
