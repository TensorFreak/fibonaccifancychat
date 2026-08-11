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
| Pub/Sub `conv:{id}` | control: `gen_start`, `user_message` | эфемерный |
| Stream `gen:{id}:{mid}` | токены ответа ассистента | durable, реплеится, TTL |
| String `active_gen:{id}` | id идущей сейчас генерации | догон на коннекте |

## Сторона воркера (продюсер)

```python
message_id = str(uuid.uuid4())
gen_key = keys.gen_stream(conversation_id, message_id)

await r.set(keys.active_gen(conversation_id), message_id, ex=settings.gen_ttl_seconds)
await r.publish(channel, json.dumps({"type": "gen_start", "message_id": message_id}))

async for token in stream_completion(prompt):
    await r.xadd(gen_key, {"t": "token", "c": token})   # durable дельта
await r.xadd(gen_key, {"t": "end"})                     # терминальная запись
await r.expire(gen_key, settings.gen_ttl_seconds)
await r.delete(keys.active_gen(conversation_id))
```

- `active_gen:{id}` — маркер «идёт генерация X». По нему подключающийся клиент узнаёт, какую ленту догонять. Свой TTL — чтобы не завис, если воркер умрёт.
- `gen_start` в Pub/Sub — сигнал уже подключённым сокетам начать тейл. Если сигнал потеряется (Pub/Sub), клиент восстановит `message_id` из `active_gen` при подключении — дублирующая гарантия.
- Каждый токен → запись `{"t":"token","c":...}`. Финал → запись `{"t":"end"}`.
- После завершения лента живёт `gen_ttl_seconds` (300 c): в этом окне реконнект переиграет **даже полностью готовый** ответ. Затем `EXPIRE` её убирает.

## Сторона api (консьюмер): `tail_generation`

```python
async def tail_generation(ws, r, conversation_id, message_id, tailing):
    if message_id in tailing:      # защита от двойного тейла одной генерации
        return
    tailing.add(message_id)
    gen_key = keys.gen_stream(conversation_id, message_id)
    try:
        await ws.send_text(json.dumps({"type": "assistant_start", "message_id": message_id}))
        last_id = "0"                                    # "0" => лента с начала
        while True:
            resp = await r.xread({gen_key: last_id}, block=5000, count=200)
            if not resp:
                if not await r.exists(gen_key):          # ленты нет (истёк TTL) -> выходим
                    return
                continue
            for _key, entries in resp:
                for entry_id, fields in entries:
                    last_id = entry_id
                    if fields.get("t") == "end":
                        await ws.send_text(json.dumps({"type": "assistant_end",
                                                       "message_id": message_id}))
                        return
                    await ws.send_text(json.dumps({"type": "token",
                                                   "content": fields.get("c", ""),
                                                   "message_id": message_id}))
    finally:
        tailing.discard(message_id)
```

Один цикл делает **и реплей, и live-хвост**:

1. `last_id = "0"` — первый `XREAD` вернёт **все** уже накопленные токены (реплей с начала).
2. `last_id` двигается на id последней прочитанной записи; следующий `XREAD BLOCK` ждёт **новые** токены (live).
3. Запись `end` → шлём `assistant_end` и выходим.
4. Таймаут + ленты уже нет (`not exists`) → генерация давно завершена и лента истекла по TTL → выходим.

## Два триггера тейла

Тейл запускается в двух ситуациях (обе в [`ws_endpoint`](02-websocket-api.md)):

1. **Догон на подключении** — если генерация уже идёт:
   ```python
   active_mid = await r.get(keys.active_gen(conversation_id))
   if active_mid:
       start_tail(active_mid)
   ```
2. **Генерация началась при подключённом сокете** — по `gen_start` из Pub/Sub:
   ```python
   if evt.get("type") == "gen_start":
       start_tail(evt["message_id"])
   ```

Множество `tailing` не даёт запустить два тейла одной генерации, если сработали оба триггера.

## Сценарии

| Сценарий | Поведение |
|---|---|
| Обычная генерация при живом сокете | `gen_start` → тейл с `"0"` → реплей (пусто) + live-хвост |
| Реконнект **посреди** генерации | `active_gen` есть → тейл с `"0"` → реплей уже сгенерённого + live |
| Реконнект **после** завершения (в пределах TTL) | лента ещё жива → реплей полного ответа с `assistant_end` |
| Реконнект после TTL | ленты нет → тейл не стартует; полный ответ берётся перезагрузкой истории из Postgres |
| Несколько устройств | каждый сокет тейлит ту же ленту независимо — все видят один поток |

## Протокол и требование к клиенту

События в сокет: `assistant_start` → `token`* → `assistant_end`, все с `message_id`.

> При реконнекте эти события **прилетают заново с начала** (реплей). Клиент обязан рендерить ответ **идемпотентно по `message_id`**: перерисовать весь ответ этого id, а не дописывать к тому, что уже на экране. Иначе после реконнекта текст задвоится.

## Границы

- **`user_message`-эхо не resumable** — идёт эфемерным Pub/Sub. При обрыве реплика пользователя с другого устройства может не отобразиться до перезагрузки истории. Чтобы сделать resumable и её, нужна durable-лента событий диалога (`events:{id}`) — намеренно не сделано в скелете.
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
