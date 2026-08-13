# 06. Суммаризация

**Файл:** [`app/workers/summarizer.py`](../app/workers/summarizer.py) · **Исполнитель:** №3 (фоновый процесс)
**Запуск:** `python -m app.workers.summarizer` · **Очередь:** `chat:summarize` / группа `summarizers`

## Зачем

Диалог растёт бесконечно, а контекст модели конечен. Держать всю историю в промпте нельзя. Решение: старую часть диалога **сворачивать в краткое резюме** (`summary`), а дословно оставлять только последние N сообщений. Тогда промпт = `summary` + горячее окно, и он остаётся коротким при сколь угодно длинном диалоге.

Суммаризация вынесена в **отдельный процесс со своей очередью**, чтобы не мешать горячему пути ответа: пользователь не ждёт свёртку. Тот же процесс обслуживает и **авто-названия** диалогов (см. ниже) — обе задачи это фоновые LLM-вызовы, не на горячем пути.

## Авто-название диалога

Заголовок для сайдбара генерируется **отдельным LLM-запросом** (`generate_title`), а не обрезкой первого сообщения — чтобы выглядел естественно. Поток:

1. `llm_worker` после **первого** ответа ассистента ставит в `chat:summarize` задачу
   `{conversation_id, task: "title"}`. Дедуп — NX-маркер `title:enq:{id}` (ставится один раз).
2. Суммаризатор (`handle` диспетчеризует по `task`) вызывает `generate_title`: берёт первое
   сообщение пользователя, просит модель дать короткое название (3–5 слов) и сохраняет его
   через `set_conversation_title` (idempotent: пишет только если `title IS NULL`).
3. Клиент подхватывает готовое название, перезапросив список диалогов после `assistant_end`.

> **Пустой ответ модели → фолбэк на первое сообщение, а НЕ строка «Новый чат».** Reasoning-модели при малом `title_max_tokens` часто отдают пустой `content` (весь бюджет ушёл в `reasoning_content`). Записать буквальную заглушку «Новый чат» нельзя: она непустая, поэтому в UI неотличима от «без названия», но при этом навсегда блокирует перегенерацию (`set` пишет лишь `WHERE title IS NULL`) и повторную постановку задачи (NX-маркер живёт сутки) — чат застревает. Поэтому при пустом ответе `generate_title` берёт осмысленное имя из первого сообщения пользователя. Регрессия — в `tests/test_generate_title.py`.

Задача идёт через тот же стрим/группу, что и суммаризация; поле `task` их различает.

## Триггер (со стороны llm_worker)

Воркер ответа считает новые сообщения и по порогу ставит задачу:

```python
n = await r.incrby(keys.since_sum_key(conversation_id), 2)     # +user +assistant
if n >= settings.summary_trigger_messages:                     # 20
    await r.set(keys.since_sum_key(conversation_id), 0)
    await r.xadd(settings.summarize_stream, {"conversation_id": conversation_id})
```

Счётчик `since_sum:{id}` копит «сколько новых сообщений с последней свёртки». Достиг 20 — задача в `chat:summarize`, счётчик сброшен.

## Главный цикл

Тот же паттерн consumer group, что и у llm_worker, но своя группа `summarizers` (независимость от горячего пути):

```python
resp = await r.xreadgroup(summarize_group, CONSUMER_NAME,   # уникально на процесс: hostname-pid-rnd
                          {summarize_stream: ">"}, count=1, block=5000)
# ... handle(conversation_id) ... XACK   (handle диспетчеризует summarize / generate_title)
```

Как и llm_worker, суммаризатор периодически делает `reclaim_stale` (`XAUTOCLAIM`) по
зависшим в PEL задачам, а «отравленную» (постоянно падающую) после `max_deliveries`
переносит в `chat:summarize:dead` (см. [10. Харденинг](10-hardening.md), R5-4).

## Замок суммаризации

```python
lock = f"lock:sum:{conversation_id}"
if not await r.set(lock, CONSUMER_NAME, nx=True, ex=settings.summarize_lock_ttl_seconds):
    return                                   # уже сворачивается -> просто выходим
hb = asyncio.create_task(heartbeat_lock(     # продлеваем замок во время долгой свёртки
    r, lock, CONSUMER_NAME, settings.summarize_lock_ttl_seconds, settings.lock_heartbeat_seconds))
# ... в finally: hb.cancel(); await release_lock(r, lock, CONSUMER_NAME)
```

Отдельный замок, **не пересекается** с `lock:conv:{id}` из llm_worker (это разные
подсистемы). Нужен, чтобы две задачи по одному диалогу не свернули одно и то же дважды.
Значение замка — уникальный `CONSUMER_NAME`, снимаем его owner-aware (`release_lock`, Lua
compare-and-delete), а во время долгой свёртки большого хвоста замок продлевается
heartbeat'ом — иначе истёк бы на лету и второй суммаризатор дублировал бы работу
([10. Харденинг](10-hardening.md), C2/R4-M1). Важное отличие от воркера ответа: здесь при
занятом замке мы **не ждём**, а сразу выходим — свёртка не срочная, следующий триггер её и
так запустит.

## Что именно сворачивается

```python
old_summary, upto_id = await db.get_summary(conversation_id)
# ещё не в summary (id > watermark), с лимитом summary_max_fetch на заход (защита от OOM,
# если суммаризатор далеко отстал; хвост длиннее — дозапустит сам себя)
pending = await db.load_messages_since(conversation_id, upto_id, settings.summary_max_fetch)
to_fold = pending[:-settings.summary_recent_keep] if \
    len(pending) > settings.summary_recent_keep else []
if not to_fold:
    return
```

- `summary_upto_id` — watermark: `id` последнего свёрнутого сообщения (монотонный BIGINT,
  а не `created_at` — устойчив к одинаковым таймстампам, см. [10. Харденинг](10-hardening.md), M1).
- `pending` — всё, что новее watermark'а (`id > summary_upto_id`), ещё не в резюме.
- `to_fold` — из pending **откладываем последние `summary_recent_keep` (20)** и сворачиваем только то, что старше. Эти последние остаются несвёрнутыми и должны целиком помещаться в горячее окно — поэтому окно `ctx_max_messages` (40) обязано быть **не меньше** `summary_recent_keep + summary_trigger_messages`, иначе между суммаризациями в контексте возникает дыра. Инвариант и его проверка — в [05. Горячий контекст](05-hot-context.md). Если суммаризатор отстал и `pending` уже длиннее окна (дыра образовалась), это фиксируется в лог `[summary][LAG]` и durable-поток `chat:summarize:lag` для алерта (R7-3); свёртка ниже двигает watermark и лечит отставание.

## Свёртка с контролем длины: `fold`

Раньше здесь был один промпт на весь `to_fold`. Проблема: если хвост большой (воркер простаивал, накопилось много), сам промпт суммаризатора превышал контекст модели. Теперь свёртка **итеративная по чанкам**:

```python
async def fold(old_summary, messages):
    summary = old_summary
    for chunk in tokens.chunk_messages(messages, settings.summary_fold_chunk_tokens):
        prompt = [
            {"role": "system", "content": "... Уложись примерно в "
                                          f"{settings.summary_max_tokens} токенов."},
            {"role": "user", "content": f"Текущее резюме:\n{summary or '(пусто)'}\n\n"
                                        f"Новые сообщения:\n{render(chunk)}\n\n"
                                        f"Обновлённое резюме:"},
        ]
        result = await complete(prompt, max_tokens=settings.summary_max_tokens)
        if result and result.strip():                         # пустой ответ НЕ затираем накопленным
            summary = tokens.truncate_text(result, settings.summary_max_tokens)  # предохранитель длины
    return summary or ""
```

> **Пустой ответ модели не обнуляет резюме.** Если `complete()` на каком-то чанке вернул пусто (сбой/фильтр), мы НЕ перезаписываем `summary` пустой строкой — иначе следующий чанк свернулся бы поверх «(пусто)», а `old_summary` и уже свёрнутые чанки пропали бы (при этом watermark в `summarize()` всё равно сдвигается — сообщения считались бы свёрнутыми). Пропускаем проблемный чанк, сохраняя прежнее `summary`: деградация локальна (один чанк), а не тотальна. Регрессия — в `tests/test_summarize_fold.py`.

Три уровня контроля длины (см. [08. Контроль длины](08-token-budget.md)):

1. **Чанкинг входа** — `chunk_messages` бьёт хвост на части ≤ `summary_fold_chunk_tokens` (8000), каждая вкатывается в резюме отдельным шагом. Промпт суммаризатора больше не взрывается.
2. **Потолок в промпте** — модель просят уложиться в `summary_max_tokens` (2000).
3. **Жёсткая обрезка** — `truncate_text` подстраховывает, если модель проигнорила инструкцию. Так `summary` не разрастается от свёртки к свёртке (а он идёт в **каждый** промпт ответа).

## Сохранение результата

```python
new_summary = await fold(old_summary, to_fold)
new_upto_id = to_fold[-1]["id"]                   # двигаем watermark по id
await db.save_summary(conversation_id, new_summary, new_upto_id)   # Postgres (истина, upsert)
await r.set(keys.sum_key(conversation_id), new_summary, ex=settings.ctx_ttl_seconds)  # кэш
```

- В Postgres — новое `summary` и сдвинутый `summary_upto_id`. `save_summary` — это
  `INSERT … ON CONFLICT DO UPDATE` (upsert): резюме сохраняется, даже если строки
  диалога не было (раньше `UPDATE` молча обновлял 0 строк — см. [10](10-hardening.md), C2).
- В Redis `sum:{id}` — свежий кэш резюме с TTL, чтобы llm_worker сразу видел обновление, не читая БД.

## Как резюме попадает в промпт

`llm_worker.get_summary` читает `sum:{id}` (кэш), при промахе — из Postgres и прогревает кэш. Затем `build_prompt` кладёт резюме как system-голову, дополнительно обрезав до `summary_max_tokens`. См. [04](04-llm-worker.md) и [08](08-token-budget.md).

## Параметры

| Параметр | Значение | Смысл |
|---|---|---|
| `summary_trigger_messages` | 20 | порог запуска (в сообщениях) |
| `summary_recent_keep` | 20 | сколько последних не сворачивать |
| `summary_max_tokens` | 2000 | потолок объёма резюме |
| `summary_fold_chunk_tokens` | 8000 | размер чанка при свёртке большого хвоста |
| `summary_max_fetch` | 5000 | потолок вычитки несвёрнутого хвоста за заход |
| `summarize_lock_ttl_seconds` | 300 | TTL замка суммаризации (+heartbeat) |

## Границы

- Итеративная свёртка по чанкам — это последовательные вызовы LLM; при очень большом единоразовом хвосте свёртка займёт время (но она фоновая, горячий путь не блокирует).
- Качество резюме зависит от модели: жёсткая обрезка `truncate_text` может оборвать текст на полуслове — это аварийный предохранитель, а не штатный путь (штатно модель сама укладывается в лимит).
