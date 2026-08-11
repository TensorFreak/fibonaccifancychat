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
            except Exception as e:
                print("process error:", repr(e))       # НЕ ackать -> PEL -> повтор
```

Про `xreadgroup`, `">"`, `XACK` и PEL — см. [03. Очередь входящих](03-inbound-queue.md).

## Обработка одного сообщения: `process()`

### Шаг 0 — замок диалога

```python
lock = keys.conv_lock(conversation_id)
while not await r.set(lock, CONSUMER_NAME, nx=True, ex=settings.conv_lock_ttl_seconds):
    await asyncio.sleep(0.2)
hb = asyncio.create_task(_heartbeat_lock(r, lock))   # продлеваем TTL во время генерации
```

Пользователь мог прислать 2–3 сообщения подряд, пока шла генерация. Их разберут **разные** воркеры. Без сериализации они бы перемешали контекст (ответ на второе сообщение попал бы в историю раньше первого). Замок `lock:conv:{id}` гарантирует: сообщения одного диалога обрабатываются строго по очереди.

- `SET ... NX EX` — атомарный захват с TTL `conv_lock_ttl_seconds` (300 c).
- **Heartbeat:** долгая генерация (большой контекст) может идти дольше TTL; фоновая задача продлевает замок, пока владеем им, — иначе замок истёк бы на лету и другой воркер начал бы дублирующую обработку ([10](10-hardening.md)).
- Не смогли захватить → ждём 0.2 c и пробуем снова (простой честный spin-wait).
- Снимается в `finally` (`DELETE`), heartbeat отменяется там же.

> Параллелизм — **между** диалогами. **Внутри** одного диалога — строго последовательно. Это by design.

> **Важно:** замок предотвращает *одновременность*, но не *переупорядочивание* при
> нескольких воркерах. Порядок FIFO обеспечивает отдельный гейт по `seq` (применяем
> только `seq == applied+1`, иначе возвращаем задачу в хвост). См. [10](10-hardening.md), H1.

### Шаг 1 — сохранить сообщение пользователя

```python
await db.insert_message(conversation_id, "user", text)        # Postgres (истина)
await append_context(r, conversation_id, {"role": "user", "content": text})  # ctx
await r.publish(channel, json.dumps({"type": "user_message", "content": text}))
```

Сообщение уходит в Postgres (навсегда), в горячее окно `ctx:{id}` (для промпта) и эхом в Pub/Sub — чтобы **другие** устройства пользователя увидели его реплику. Про `append_context` — см. [05. Горячий контекст](05-hot-context.md).

### Шаг 2 — собрать промпт

```python
prompt = await build_prompt(r, conversation_id)
```

Промпт = `[summary как system]` + горячее окно, собранные **по токен-бюджету** (не по числу сообщений). Логика бюджета, обрезки и предохранителей — в [08. Контроль длины](08-token-budget.md).

### Шаг 3 — генерация в durable-ленту

```python
message_id = str(uuid.uuid4())
gen_key = keys.gen_stream(conversation_id, message_id)
await r.set(keys.active_gen(conversation_id), message_id, ex=settings.gen_ttl_seconds)
await r.publish(channel, json.dumps({"type": "gen_start", "message_id": message_id}))

parts = []
async for token in stream_completion(prompt):
    parts.append(token)
    await r.xadd(gen_key, {"t": "token", "c": token})     # durable дельта
answer = "".join(parts)
await r.xadd(gen_key, {"t": "end"})                       # терминальная запись
await r.expire(gen_key, settings.gen_ttl_seconds)
await r.delete(keys.active_gen(conversation_id))
```

Токены идут в durable Redis Stream, а не в эфемерный Pub/Sub — это делает поток resumable. `active_gen` + `gen_start` дают клиентам понять, какую ленту тейлить. Полный разбор — [07. Resumable streams](07-resumable-streams.md). Генерация обёрнута в `try/except`: при ошибке LLM лента завершается записью `end` с `error=1` и получает TTL — не утекает ([10](10-hardening.md), H3).

### Шаг 4 — сохранить ответ

```python
await db.insert_message(conversation_id, "assistant", answer)
await append_context(r, conversation_id, {"role": "assistant", "content": answer})
```

Финальный ответ — в Postgres (истина) и в горячее окно.

### Шаг 5 — триггер суммаризации

```python
n = await r.incrby(keys.since_sum_key(conversation_id), 2)   # +user +assistant
if n >= settings.summary_trigger_messages:
    await r.set(keys.since_sum_key(conversation_id), 0)
    await r.xadd(settings.summarize_stream, {"conversation_id": conversation_id})
```

Считаем новые сообщения (за ход +2). Накопилось достаточно → ставим задачу в **отдельную** очередь и сбрасываем счётчик. Саму свёртку делает summarizer, **не блокируя** ответ пользователю. См. [06. Суммаризация](06-summarization.md).

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
| `gen_ttl_seconds` | 300 | жизнь ленты генерации |
| `CONSUMER_NAME` | `worker-1` | имя консьюмера (в проде — уникальное) |
