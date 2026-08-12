# 02. WebSocket API

**Файл:** [`app/api/main.py`](../app/api/main.py) · **Исполнитель:** `api` (FastAPI, №1)

> Изменения по безопасности и надёжности этого слоя (авторизация, `seq`, сериализация
> отправки, валидация входа) собраны в [10. Харденинг](10-hardening.md).

## Роль

API-слой — синхронный горячий путь. Задачи:

0. **Авторизация:** проверить токен и владение диалогом (см. [10](10-hardening.md), C1).
1. **Вход:** принять текст из сокета → присвоить `seq` (`INCR`) → `XADD` в Redis Stream.
2. **Выход:** доставить в сокет всё, что относится к диалогу — control-события из Pub/Sub и токены ответа из durable-ленты (вся отправка — через общий `send()` под `asyncio.Lock`).

Чего api **не делает**: не ходит к LLM, не пишет ответы в Postgres, не собирает промпт. Он тонкий: буферизует вход и ретранслирует выход. Это позволяет держать много дешёвых api-инстансов за балансировщиком.

## Что такое «вебсокет» здесь

Вебсокет — TCP-соединение между **браузером одного устройства** и **одним процессом uvicorn/FastAPI**, на который его направил балансировщик. Два устройства пользователя = два разных сокета, возможно на двух разных инстансах. Ключ маршрутизации — `conversation_id` в пути: `/ws/{conversation_id}`.

## Входной цикл: сокет → очередь

```python
while True:
    raw = await ws.receive_text()          # тело несёт ТОЛЬКО {"text": "..."}
    data = json.loads(raw)
    text = data["text"]                    # валидация: не пусто / не длиннее max_message_chars
    if not await _allow_ws_message(r, user_id):   # рейтлимит per-user (H2), ДО расхода seq
        await send(json.dumps({"type": "error", "error": "rate_limited"})); continue
    seq = await r.incr(keys.conv_seq(conversation_id))            # монотонный номер (FIFO)
    await r.expire(keys.conv_seq(conversation_id), settings.conv_counter_ttl_seconds)
    await r.xadd(settings.inbound_stream, {
        "conversation_id": conversation_id,
        "user_id": user_id,                # из JWT (authenticate), НЕ из тела сообщения
        "message_id": str(uuid.uuid4()),
        "seq": str(seq),
        "text": text,
    }, maxlen=settings.inbound_stream_maxlen, approximate=True)
```

`XADD` кладёт задачу в durable-очередь и мгновенно возвращает управление. Отсюда важное свойство: **пользователь может слать новые сообщения, не дожидаясь ответа LLM** — каждое просто становится ещё одной записью в стриме. Порядок обработки для одного диалога потом гарантирует гейт по `seq` + замок в воркере (см. [04](04-llm-worker.md)). Блок обращений к Redis обёрнут в `try/except`: транзиентный сбой Redis не роняет сокет — клиенту уходит `server_busy` ([10](10-hardening.md), R5-1).

## Выходной путь: два источника

После рефакторинга под resumable streams выход состоит из **двух** источников, и это важно понимать:

### 1. Control-канал — Pub/Sub `conv:{id}`

Через `pubsub.listen()` приходят события управления:

- `user_message` — эхо реплики пользователя (чтобы **другие** его устройства увидели, что он написал). Пересылается в сокет как есть.
- `gen_start {message_id}` — сигнал «началась генерация». api **не** пересылает его в браузер, а запускает тейл durable-ленты этой генерации (см. ниже).

```python
async def pump_out():
    while True:                               # переживаем рестарт Redis: переподписка
        try:
            await pubsub.subscribe(keys.conv_channel(conversation_id))
            active_mid = await r.get(keys.active_gen(conversation_id))   # догон после (пере)подписки
            if active_mid:
                start_tail(active_mid)
            async for msg in pubsub.listen():
                if msg["type"] != "message":
                    continue
                evt = json.loads(msg["data"])
                if evt.get("type") == "gen_start":
                    start_tail(evt["message_id"])   # токены придут из durable-ленты
                else:
                    await send(msg["data"])         # user_message и прочий control (send под общим Lock)
        except asyncio.CancelledError:
            raise
        except Exception:                     # обрыв Redis -> пауза и переподписка
            await asyncio.sleep(1.0)
```

Pub/Sub — fire-and-forget: доставляется только тем, кто подписан сейчас. Для control-событий это приемлемо (сигнал `gen_start` дублируется маркером `active_gen`). При рестарте/обрыве Redis `pump_out` переподписывается и повторяет догон по `active_gen`, компенсируя пропущенный `gen_start` ([10](10-hardening.md), «Resubscribe Pub/Sub»). Вся отправка в сокет идёт через общий `send()` под `asyncio.Lock`.

### 2. Поток токенов — durable-лента `gen:{id}:{mid}`

Сами токены ответа идут не через Pub/Sub, а из Redis Stream — это и делает поток resumable. За чтение отвечает `tail_generation`: `XREAD` с `"0"` отдаёт ленту с начала (реплей), затем блокирующе ждёт новые токены (live-хвост). Детали — в [07. Resumable streams](07-resumable-streams.md).

## Догон на подключении

Сразу после подписки api проверяет, не идёт ли уже генерация:

```python
active_mid = await r.get(keys.active_gen(conversation_id))
if active_mid:
    start_tail(active_mid)     # переиграть ленту с начала + продолжить live
```

Это закрывает сценарий реконнекта посреди генерации: клиент восстанавливает `message_id` активной генерации из `active_gen:{id}` и переигрывает её с нуля, даже если `gen_start` он пропустил (был отключён).

## Жизненный цикл соединения

```
accept
  ├─ subscribe conv:{id}
  ├─ active_gen? → start_tail (догон)
  ├─ pump_out task  (control-канал → сокет / запуск тейлов)
  └─ входной цикл: receive_text → XADD chat:inbound
        …
disconnect (finally):
  ├─ cancel pump_out
  ├─ cancel все tail-задачи
  └─ unsubscribe + aclose pubsub
```

Все фоновые задачи (`pump_out`, тейлы генераций) отменяются в `finally`, ресурсы Pub/Sub закрываются — утечки подписок нет.

## Протокол для клиента

Что браузер получает в сокет (все — JSON-строки):

| Событие | Поля | Когда |
|---|---|---|
| `user_message` | `content` | пользователь (с другого устройства) отправил реплику |
| `assistant_start` | `message_id` | началась доставка ответа (эмитит тейл ленты) |
| `token` | `content`, `message_id` | очередной токен ответа |
| `assistant_end` | `message_id`, `error` | ответ завершён (`error="1"` — обрыв генерации или пустой ответ) |
| `error` | `error` | отклонено: `bad_message` / `empty_text` / `too_long` / `rate_limited` (лимит) / `server_busy` (сбой Redis) |

Что браузер отправляет: `{"text": "..."}`. Токен — в query: `?token=…`. `user_id`
берётся из токена сервером, из тела он больше не читается.

> **Важно для клиента:** при реконнекте `assistant_start`/`token` для активной генерации прилетят заново с начала. Рендерить ответ нужно **идемпотентно по `message_id`** (перерисовать весь ответ этого `message_id`), а не дописывать вслепую.

## Health-check

`GET /healthz` → `{"ok": true}` — для проб балансировщика/оркестратора.

## Связанные документы

- [03. Очередь входящих](03-inbound-queue.md) — куда уходит `XADD`.
- [07. Resumable streams](07-resumable-streams.md) — как устроен тейл ленты.
- [01. Обзор](01-architecture-overview.md) — общий поток.
