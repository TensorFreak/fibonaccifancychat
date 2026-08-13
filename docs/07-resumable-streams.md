# 07. Resumable streams

**Ключи:** `gen:{conversation_id}:{message_id}` (Stream), `active_gen:{conversation_id}` (String)
**Код:** [`app/api/main.py`](../app/api/main.py) (`tail_generation`), [`app/workers/llm_worker.py`](../app/workers/llm_worker.py) (`process`)

## Проблема

Сетевой обрыв, refresh вкладки или переключение устройства не должны терять уже сгенерированный ответ. Наивная реализация публикует токены в Redis Pub/Sub — но Pub/Sub эфемерен (fire-and-forget): кто не подписан **сейчас**, тот токен потерял. Реконнект посреди генерации → пользователь видит ответ «с середины», начало утеряно до перезагрузки истории.

## Решение

Токены ответа пишутся не в Pub/Sub, а в **durable Redis Stream на каждую генерацию** — `gen:{conversation_id}:{message_id}`. Stream хранит записи, поэтому их можно **переиграть** (`XREAD` с начала), а затем продолжить в реальном времени. Это и есть resumable stream.

Роли разделены:

| Канал | Что несёт | Свойство |
|---|---|---|
| Stream `events:{id}` | control: `gen_start`, `user_message` | durable, реплеится, TTL (догон по `last_event_id`) |
| Stream `gen:{id}:{mid}` | токены ответа ассистента | durable, реплеится, TTL |
| String `active_gen:{id}` | id идущей сейчас генерации | догон на коннекте |

Обе ленты — Redis Stream, читаются одним и тем же `XREAD BLOCK`; Pub/Sub не используется. Ниже разобрана лента токенов; лента событий устроена так же (см. [02](02-websocket-api.md)).

## Сторона воркера (продюсер)

```python
gen_message_id = str(uuid.uuid4())                       # отдельный id ЭТОЙ генерации
gen_key = keys.gen_stream(conversation_id, gen_message_id)

await r.set(keys.active_gen(conversation_id), gen_message_id, ex=settings.gen_ttl_seconds)
await _emit_event(r, conversation_id, {"type": "gen_start", "message_id": gen_message_id})  # events:{id}

parts = []
async for token in stream_completion(prompt):
    parts.append(token)
    await r.xadd(gen_key, {"t": "token", "c": token})    # durable дельта
    # H1: пока лента наполняется, держим на ней active-TTL (gen_active_ttl_seconds),
    # чтобы при жёстком падении воркера она не осталась без TTL навсегда.
    if len(parts) == 1 or len(parts) % 256 == 0:
        await r.expire(gen_key, settings.gen_active_ttl_seconds)

empty = not "".join(parts).strip()                       # пустой ответ -> помечаем ошибкой
await r.xadd(gen_key, {"t": "end", "error": "1"} if empty else {"t": "end"})  # терминал
await r.expire(gen_key, settings.gen_ttl_seconds)
await r.delete(keys.active_gen(conversation_id))
```

- `active_gen:{id}` — маркер «идёт генерация X». По нему подключающийся клиент узнаёт, какую ленту догонять; он же служит признаком «генерация ещё активна» для тейла (см. ниже). Свой TTL — чтобы не завис, если воркер умрёт.
- `gen_start` в durable-ленте `events:{id}` — сигнал сокетам начать тейл; durable, поэтому переживает обрыв и переигрывается по `last_event_id`. Для генерации, начавшейся **до** курсора клиента (уже шла на момент коннекта), сигнал в ленте не переиграется — тогда клиент восстановит `message_id` из `active_gen` при подключении (дублирующая гарантия).
- Каждый токен → запись `{"t":"token","c":...}`. Финал → запись `{"t":"end"}` (с `error=1`, если ответ модели пустой — [10](10-hardening.md), R5-2).
- Во время генерации у ленты active-TTL `gen_active_ttl_seconds` (900 c), после завершения — `gen_ttl_seconds` (300 c): в этом окне реконнект переиграет **даже полностью готовый** ответ. Затем `EXPIRE` её убирает.

## Сторона api (консьюмер): `tail_generation`

```python
# send — сериализованная отправка в сокет (общий asyncio.Lock, см. [02])
async def tail_generation(ws, r, conversation_id, message_id, tailing, send):
    if message_id in tailing:      # защита от двойного тейла одной генерации
        return
    tailing.add(message_id)
    gen_key = keys.gen_stream(conversation_id, message_id)
    try:
        await send(json.dumps({"type": "assistant_start", "message_id": message_id}))
        last_id = "0"                                    # "0" => лента с начала
        while True:
            resp = await r.xread({gen_key: last_id}, block=5000, count=200)
            if not resp:
                # Ленты нет по ДВУМ разным причинам — их нельзя путать:
                #   1) она ЕЩЁ не создана — первый токен не пришёл (TTFT дольше block,
                #      норма для большого контекста/медленной модели);
                #   2) она уже ПРОТУХЛА (истёк gen_ttl после завершения).
                # Сдаёмся только во 2-м случае: признак «генерация ещё идёт» —
                # active_gen указывает на ИМЕННО эту генерацию (R4-H1). Иначе клиент,
                # подключившийся до первого токена, бросил бы тейл и завис бы с пустым баблом.
                if not await r.exists(gen_key):
                    if await r.get(keys.active_gen(conversation_id)) != message_id:
                        return
                continue
            for _key, entries in resp:
                for entry_id, fields in entries:
                    last_id = entry_id
                    if fields.get("t") == "end":
                        await send(json.dumps({"type": "assistant_end",
                                               "message_id": message_id,
                                               "error": fields.get("error", "")}))
                        return
                    await send(json.dumps({"type": "token",
                                           "content": fields.get("c", ""),
                                           "message_id": message_id}))
    finally:
        tailing.discard(message_id)
```

Один цикл делает **и реплей, и live-хвост**:

1. `last_id = "0"` — первый `XREAD` вернёт **все** уже накопленные токены (реплей с начала).
2. `last_id` двигается на id последней прочитанной записи; следующий `XREAD BLOCK` ждёт **новые** токены (live).
3. Запись `end` → шлём `assistant_end` (с полем `error`) и выходим.
4. Таймаут + ленты нет: если генерация **ещё активна** (`active_gen` = наш `message_id`) —
   ждём дальше (лента ещё не создана, TTFT); если уже **не активна** — лента истекла по
   TTL, выходим ([10](10-hardening.md), R4-H1).

## Два триггера тейла

Тейл запускается в двух ситуациях (обе в [`ws_endpoint`](02-websocket-api.md)):

1. **Догон на подключении** — если генерация уже идёт:
   ```python
   active_mid = await r.get(keys.active_gen(conversation_id))
   if active_mid:
       start_tail(active_mid)
   ```
2. **Генерация началась при подключённом сокете** — по `gen_start` из ленты `events:{id}`:
   ```python
   if evt.get("type") == "gen_start":
       start_tail(evt["message_id"])
   ```

Множество `tailing` не даёт запустить два тейла одной генерации, если сработали оба триггера.

## Сценарии

| Сценарий | Поведение |
|---|---|
| Обычная генерация при живом сокете | `gen_start` → тейл с `"0"` → реплей (пусто) + live-хвост |
| Подключение/тейл **до первого токена** (долгий TTFT) | ленты ещё нет, но `active_gen`=наш id → тейл **ждёт**, не сдаётся (R4-H1) |
| Реконнект **посреди** генерации | `active_gen` есть → тейл с `"0"` → реплей уже сгенерённого + live |
| Реконнект **после** завершения (в пределах TTL) | лента ещё жива → реплей полного ответа с `assistant_end` |
| Реконнект после TTL | ленты нет → тейл не стартует; полный ответ берётся перезагрузкой истории из Postgres |
| Несколько устройств | каждый сокет тейлит ту же ленту независимо — все видят один поток |

> **Клиент на реконнекте ВСЕГДА ресинкает историю (M1-review).** `gen_ttl_seconds` (300 c)
> короче `events_ttl_seconds` (900 c), поэтому в окне 300..900 c переигранный `gen_start` мог
> указывать на уже протухшую ленту токенов — раньше это оставляло вечный «пишет…»-бабл.
> Теперь [`chat.html:reconnect`](../app/static/chat.html) на каждом авто-реконнекте делает
> `loadHistory → connect`: снапшот из Postgres (готовый ответ уже там) + до-тейл живой
> генерации по `active_gen`. Та же последовательность, что при первом открытии чата. Сам
> реконнект — с джиттером и экспоненциальным бэкоффом (N1-review), чтобы массовый обрыв не
> дал всплеск GET-истории; корректность ресинка это не меняет, только тайминг.

## Протокол и требование к клиенту

События в сокет: `assistant_start` → `token`* → `assistant_end`, все с `message_id`.

> При реконнекте эти события **прилетают заново с начала** (реплей). Клиент обязан рендерить ответ **идемпотентно по `message_id`**: перерисовать весь ответ этого id, а не дописывать к тому, что уже на экране. Иначе после реконнекта текст задвоится.

> **Изоляция диалогов на клиенте (критично).** У страницы ОДИН сокет и общий на все чаты рендер-стейт (`bubbles`, лог). При переключении чата старый сокет нельзя оставлять живым: `selectChat` делает `teardownWs()` (снимает ВСЕ обработчики и закрывает сокет) СРАЗУ, до `await loadHistory`, а `onmessage` дополнительно проверяет `if(id !== activeId) return`. Иначе сокет прежнего чата, оставаясь открытым во время загрузки истории нового, рендерил свои токены в открытый сейчас диалог — межчатовая утечка стрима. Гасить только `onclose` (как было) мало: `onmessage` продолжал срабатывать.

## Границы

- **`user_message`-эхо resumable** — идёт durable-лентой `events:{id}` наравне с токенами. При обрыве реплика пользователя с другого устройства переигрывается по `last_event_id` клиента; за пределами TTL ленты (`events_ttl_seconds`) — восстанавливается перезагрузкой истории из Postgres (та же граница, что у `gen:{}`). Клиент применяет эхо идемпотентно по `message_id`.
- **Ошибка LLM посреди генерации** — лента **всегда** завершается: воркер пишет
  `{"t":"end","error":"1"}`, ставит TTL, снимает `active_gen` и пробрасывает ошибку
  (уйдёт в ретрай). Клиент получает `assistant_end` с `error="1"`. Лента не утекает.
  См. [10. Харденинг](10-hardening.md), H3.
- **Незавершённая лента** (воркер умер до `end`, без штатного `try/except`) — тейл
  добьёт до TTL и выйдет по `not exists`; полный результат появится, когда задачу
  переиграет `reclaim_stale`/другой воркер.
- **Порядок при нескольких генерациях** — каждая генерация = своя лента и свой `message_id`; замок диалога ([04](04-llm-worker.md)) гарантирует, что они не идут одновременно.

## Параметры

| Параметр | Значение | Смысл |
|---|---|---|
| `gen_ttl_seconds` | 300 | жизнь ленты после завершения (окно реплея готового ответа) |
