# 02. WebSocket API

**Файл:** [`app/api/main.py`](../app/api/main.py) · **Исполнитель:** `api` (FastAPI, №1)

> Изменения по безопасности и надёжности этого слоя (авторизация, `seq`, сериализация
> отправки, валидация входа) собраны в [10. Харденинг](10-hardening.md).

## Роль

API-слой — синхронный горячий путь. Задачи:

0. **Авторизация:** проверить токен и владение диалогом (см. [10](10-hardening.md), C1).
1. **Вход:** принять текст из сокета → присвоить `seq` (`INCR`) → `XADD` в Redis Stream.
2. **Выход:** доставить в сокет всё, что относится к диалогу — control-события из durable-ленты `events:{id}` и токены ответа из durable-ленты `gen:{id}:{mid}` (вся отправка — через общий `send()` под `asyncio.Lock`).

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

Выход состоит из **двух** durable-лент Redis Stream (Pub/Sub не используется), и это важно понимать:

### 1. Control-канал — durable-лента `events:{id}`

Через `XREAD BLOCK` тем же приёмом, что и токены, тейлим ленту control-событий:

- `user_message` — эхо реплики пользователя (чтобы **другие** его устройства увидели, что он написал). Пересылается в сокет как есть (клиент рендерит идемпотентно по `message_id`).
- `gen_start {message_id}` — сигнал «началась генерация». api запускает тейл durable-ленты этой генерации **и** пересылает событие клиенту (тот использует его только чтобы сдвинуть свой курсор догона — рендер идёт из `assistant_start`/`token`).

Каждое событие несёт клиенту поле `eid` (id записи в ленте) — по нему клиент двигает `last_event_id` и на реконнекте догоняет пропущенное.

```python
async def pump_out(last_id):                  # last_id = last_event_id клиента (или "$")
    ev_key = keys.events_stream(conversation_id)
    while True:
        try:
            active_mid = await r.get(keys.active_gen(conversation_id))   # догон активной генерации
            if active_mid:
                start_tail(active_mid)
            while True:
                resp = await r.xread({ev_key: last_id}, block=5000, count=100)
                if not resp:
                    continue                  # таймаут XREAD -> ждём дальше
                for _key, entries in resp:
                    for entry_id, fields in entries:
                        last_id = entry_id
                        evt = json.loads(fields["data"]); evt["eid"] = entry_id
                        if evt.get("type") == "gen_start":
                            start_tail(evt["message_id"])   # токены — из gen:{}-ленты
                        await send(json.dumps(evt))         # send под общим Lock
        except asyncio.CancelledError:
            raise
        except Exception:                     # обрыв Redis -> пауза и повторный XREAD
            await asyncio.sleep(1.0)
```

В отличие от прежнего Pub/Sub (fire-and-forget), лента **durable**: событие переживает обрыв и переигрывается. При рестарте/обрыве Redis `pump_out` продолжает `XREAD` с того же `last_id` — ничего не теряется. `gen_start`, случившийся **до** `last_id` (генерация уже шла на момент коннекта), в ленте не переиграется — его ловит догон по `active_gen` (см. ниже). Вся отправка в сокет идёт через общий `send()` под `asyncio.Lock`.

### 2. Поток токенов — durable-лента `gen:{id}:{mid}`

Токены ответа идут отдельной лентой на каждую генерацию — это и делает поток resumable. За чтение отвечает `tail_generation`: `XREAD` с `"0"` отдаёт ленту с начала (реплей), затем блокирующе ждёт новые токены (live-хвост). Детали — в [07. Resumable streams](07-resumable-streams.md).

### Снапшот-консистентный старт (`events_cursor`)

Первый коннект к диалогу не должен ни потерять событие, ни задвоить его относительно только что загруженной истории. Эндпоинт истории (`GET /api/conversations/{id}/messages`, первая страница) возвращает **`events_cursor`** — конец ленты `events:{id}`, прочитанный **строго до** выборки сообщений из Postgres. Клиент передаёт его как `?last_event_id=` при подключении: события **до/на** курсоре уже в загруженной истории (воркер пишет сообщение в Postgres **до** `XADD` события), события **после** — дотейлит вебсокет. Порядок «cursor-first» закрывает гонку «снапшот истории ↔ подписка», которую с Pub/Sub закрыть было нельзя.

## Догон на подключении

Сразу после подписки api проверяет, не идёт ли уже генерация:

```python
active_mid = await r.get(keys.active_gen(conversation_id))
if active_mid:
    start_tail(active_mid)     # переиграть ленту с начала + продолжить live
```

Это закрывает сценарий реконнекта посреди генерации: клиент восстанавливает `message_id` активной генерации из `active_gen:{id}` и переигрывает её с нуля, даже если `gen_start` он пропустил (был отключён или тот случился до `last_event_id`).

## Жизненный цикл соединения

```
accept
  ├─ last_event_id = query["last_event_id"] или "$"
  ├─ pump_out(last_event_id) task
  │     ├─ active_gen? → start_tail (догон активной генерации)
  │     └─ XREAD events:{id} с last_event_id → сокет / запуск тейлов
  └─ входной цикл: receive_text → XADD chat:inbound
        …
disconnect (finally):
  ├─ cancel pump_out
  └─ cancel все tail-задачи
```

Все фоновые задачи (`pump_out`, тейлы генераций) отменяются в `finally`. Подписок Pub/Sub больше нет — закрывать нечего, `XREAD` живёт на общем клиенте Redis.

## Протокол для клиента

Что браузер получает в сокет (все — JSON-строки):

| Событие | Поля | Когда |
|---|---|---|
| `user_message` | `content`, `message_id`, `eid` | пользователь (с другого устройства) отправил реплику |
| `gen_start` | `message_id`, `eid` | началась генерация; клиент двигает `last_event_id` (рендер — из `assistant_start`) |
| `assistant_start` | `message_id` | началась доставка ответа (эмитит тейл ленты) |
| `token` | `content`, `message_id` | очередной токен ответа |
| `assistant_end` | `message_id`, `error` | ответ завершён (`error="1"` — обрыв генерации или пустой ответ) |
| `error` | `error` | отклонено: `bad_message` / `empty_text` / `too_long` / `rate_limited` (лимит) / `server_busy` (сбой Redis) |

События ленты `events:{id}` (`user_message`, `gen_start`) несут `eid` — id записи; события тейла токенов (`assistant_*`, `token`) его не несут.

Что браузер отправляет: `{"text": "..."}`. Токен — в query: `?token=…`. Там же курсор
догона: `?last_event_id=…` (id последнего применённого события ленты; на первом коннекте —
`events_cursor` из истории). `user_id` берётся из токена сервером, из тела он не читается.

> **Важно для клиента:** при реконнекте события переигрываются. `assistant_start`/`token` активной генерации прилетят заново с начала — рендерить ответ нужно **идемпотентно по `message_id`** (перерисовать весь ответ этого `message_id`, не дописывать вслепую). Эхо `user_message` тоже нужно применять **идемпотентно по `message_id`** (без дублей при повторной доставке). Клиент двигает `last_event_id` по `eid` каждого события ленты.

## Health-check

`GET /healthz` → `{"ok": true}` — для проб балансировщика/оркестратора.

## Связанные документы

- [03. Очередь входящих](03-inbound-queue.md) — куда уходит `XADD`.
- [07. Resumable streams](07-resumable-streams.md) — как устроен тейл ленты.
- [01. Обзор](01-architecture-overview.md) — общий поток.
