# 05. Горячий контекст

**Ключ:** `ctx:{conversation_id}` (Redis LIST) · **Код:** [`app/workers/llm_worker.py`](../app/workers/llm_worker.py)

## Зачем

Чтобы собрать промпт, модели нужны последние сообщения диалога. Тянуть их из Postgres на каждый ход — лишняя латентность на горячем пути. Поэтому последние N сообщений держим «горячими» в Redis LIST `ctx:{id}` — быстрый доступ, а Postgres остаётся source of truth. Это классический **cache-aside**.

## Что лежит

Redis LIST, элементы — JSON вида `{"role": "user"|"assistant", "content": "..."}`, в хронологическом порядке (старые слева, свежие справа). Длина ограничена `ctx_max_messages` (40, см. инвариант ниже). TTL — `ctx_ttl_seconds` (1800 c = 30 мин).

## Запись: `append_context`

```python
async def append_context(r, conversation_id, message):   # message = {role, content, mid}
    key = keys.ctx_key(conversation_id)
    if message.get("mid"):                               # ИДЕМПОТЕНТНОСТЬ: при ретрае
        for raw in await r.lrange(key, 0, -1):           # то же (mid, role) не задваиваем
            e = json.loads(raw)
            if e.get("mid") == message["mid"] and e.get("role") == message["role"]:
                return False                             # уже в окне
    await r.rpush(key, json.dumps(message))              # добавить в хвост
    await r.ltrim(key, -settings.ctx_max_messages, -1)   # держать только последние N
    await r.expire(key, settings.ctx_ttl_seconds)        # продлить жизнь при активности
    return True                                          # реально добавили
```

На каждое сообщение:

0. **Дедуп по `mid`+`role`** — при переобработке (at-least-once) то же сообщение не
   задваивается в окне. Возвращаемый флаг (`True` = реально добавили) решает, слать ли
   эхо `user_message` и крутить ли счётчик суммаризации. Всё под замком диалога — гонок нет.
1. `RPUSH` — дописать сообщение в конец окна.
2. `LTRIM (-N, -1)` — оставить только последние N. Всё, что «выпало» из окна, **не теряется**: оно в Postgres и позже будет свёрнуто суммаризатором в `summary`.
3. `EXPIRE` — продлить TTL. Пока диалог активен — окно живёт; 30 мин тишины — протухает.

Вызывается дважды за ход: после сообщения пользователя (шаг 1) и после ответа ассистента (шаг 4) в [`process`](04-llm-worker.md).

## Чтение с регидратацией: `load_context`

```python
async def load_context(r, conversation_id):
    key = keys.ctx_key(conversation_id)
    cached = await r.lrange(key, 0, -1)
    if cached:
        return [json.loads(x) for x in cached]           # cache hit

    # cache miss: контекст протух -> РЕГИДРАТАЦИЯ из Postgres
    msgs = await db.load_recent_messages(conversation_id, settings.ctx_max_messages)
    if msgs:
        await r.rpush(key, *[json.dumps(m) for m in msgs])
        await r.expire(key, settings.ctx_ttl_seconds)    # прогрели кэш заново
    return msgs
```

- **Cache hit** — окно в Redis есть, читаем `LRANGE` и возвращаем.
- **Cache miss** — пользователь вернулся после простоя, окно протухло по TTL. Тянем последние N сообщений из Postgres (`load_recent_messages`), прогреваем кэш и ставим новый TTL. Со следующего хода снова быстрый путь.

Регидратация из Postgres опирается на индекс `idx_messages_conv_id (conversation_id, id)`:
`load_recent_messages` берёт последние N по **`id DESC`** (монотонный, устойчив к одинаковым
таймстампам) и разворачивает в хронологию. См. [09. Модель хранения](09-storage-model.md).

## Место в сборке промпта

Горячее окно — вторая половина промпта. Первая — `summary` (свёрнутая старая часть). Итог:

```
prompt = [summary как system] + горячее окно
```

Важно: `build_prompt` берёт из окна **не все 20 сообщений безусловно**, а столько, сколько влезает в токен-бюджет — окно набирается с конца. Полная логика — в [08. Контроль длины](08-token-budget.md). Само окно `ctx:{id}` ограничено по **числу** сообщений; бюджет по **токенам** накладывается уже при сборке промпта.

## Как это связано с суммаризацией — ИНВАРИАНТ окна

| Параметр | Значение | Роль |
|---|---|---|
| `ctx_max_messages` | 40 | размер горячего окна |
| `summary_recent_keep` | 20 | сколько последних НЕ сворачивать |
| `summary_trigger_messages` | 20 | каждые N новых — пересжатие |

**Критично:** окно должно вмещать **всё ещё не свёрнутое** в `summary`, иначе между
суммаризациями возникает дыра. Суммаризация запускается не после каждого сообщения, а
раз в `summary_trigger_messages`, поэтому окно (которое сдвигается на каждом сообщении)
«уезжает» вперёд summary. Объём несвёрнутого доходит до `summary_recent_keep +
summary_trigger_messages`, поэтому:

```
ctx_max_messages  >=  summary_recent_keep + summary_trigger_messages   (40 = 20 + 20)
```

Если окно меньше — сообщения, уже выпавшие из окна, но ещё не попавшие в summary,
исчезают из контекста (модель их «забывает»). Инвариант проверяется валидатором в
[`config.py`](../app/config.py): заниженное значение автоматически поднимается. Небольшой
overlap (часть окна уже в summary) допустим — это лишь дублирование, не потеря.
См. [06. Суммаризация](06-summarization.md).

## Границы

- Окно ограничено **числом** сообщений, а не токенами — очень длинные сообщения раздувают промпт. Это компенсируется токен-бюджетом на этапе `build_prompt` ([08](08-token-budget.md)), но само окно об этом «не знает».
- TTL общий на весь диалог: одна активность продлевает всё окно. Отдельного вытеснения по возрасту сообщения нет — только `LTRIM` по количеству.
