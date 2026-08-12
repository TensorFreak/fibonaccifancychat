# 03. Очередь входящих (Redis Stream)

**Ключ:** `chat:inbound` (`settings.inbound_stream`) · **Группа:** `llm-workers` (`settings.consumer_group`)
**Пишет:** [`app/api/main.py`](../app/api/main.py) · **Читает:** [`app/workers/llm_worker.py`](../app/workers/llm_worker.py)

## Зачем очередь между api и воркером

api и воркер — разные процессы, часто на разных машинах, масштабируются независимо. Между ними нужна развязка, которая:

- **не теряет задачи**, если воркер занят или временно упал (durability);
- **раздаёт работу** нескольким воркерам без дублирования (балансировка);
- **не блокирует** api на время генерации (async handoff).

Redis Stream + consumer group закрывает всё три пункта. Pub/Sub здесь не подошёл бы: он эфемерный (нет durability) и broadcast (нет эксклюзивной раздачи).

## Продюсер: api

Единственная операция api по входу:

```python
seq = await r.incr(keys.conv_seq(conversation_id))   # монотонный номер (FIFO-порядок)
await r.expire(keys.conv_seq(conversation_id), settings.conv_counter_ttl_seconds)  # idle-TTL счётчика
await r.xadd(settings.inbound_stream, {
    "conversation_id": ..., "user_id": ..., "message_id": ..., "seq": str(seq), "text": ...,
}, maxlen=settings.inbound_stream_maxlen, approximate=True)   # MAXLEN ~ ограничивает рост стрима
```

`user_id` берёт **сервер** из JWT (не из тела сообщения — тело несёт только `{text}`);
`message_id` — свежий UUID (ключ идемпотентности). Всё обращение к Redis обёрнуто так, что
транзиентный сбой Redis не роняет вебсокет (клиенту уходит `server_busy`, [10](10-hardening.md), R5-1).

Поле `seq` — монотонный номер сообщения в диалоге; воркер применяет их строго по
возрастанию (FIFO-гейт, см. [04](04-llm-worker.md) и [10. Харденинг](10-hardening.md), H1).

`XADD` durable и мгновенный. api не ждёт обработки — вернул управление и готов принимать следующее сообщение.

## Консьюмер: воркеры в consumer group

Все воркеры читают из одной группы `llm-workers`:

```python
resp = await r.xreadgroup(
    settings.consumer_group, CONSUMER_NAME,
    {settings.inbound_stream: ">"}, count=available, block=5000,
)
```

- `">"` — «отдай только записи, которые ещё никому в группе не выдавались».
- Consumer group гарантирует: **каждая запись достаётся ровно одному воркеру**. Запусти N копий — Redis раздаст им разные сообщения. Это и есть горизонтальное масштабирование обработки.
- `count=available` — читаем ровно столько, сколько свободных слотов параллелизма
  (`worker_concurrency − len(inflight)`); прочитанные раздаются фоновым задачам, а не
  обрабатываются по одной ([04](04-llm-worker.md), [10](10-hardening.md), R6-1).
- `block=5000` — блокирующее ожидание до 5 c: нет сообщений → цикл повторяется, CPU не жжётся.

`CONSUMER_NAME` уникален на процесс — `f"{hostname}-{pid}-{rnd}"` — иначе PEL/`XAUTOCLAIM` двух реплик смешались бы.

## Создание группы (идемпотентно)

```python
await r.xgroup_create(settings.inbound_stream, settings.consumer_group,
                      id="0", mkstream=True)
# BUSYGROUP -> группа уже есть, это норма
```

`mkstream=True` создаёт стрим, если его ещё нет (первый запуск до первого `XADD`). `id="0"` — читать с самого начала. Ошибка `BUSYGROUP` глотается: группа уже создана другим воркером.

## At-least-once и PEL

Модель доставки — **at-least-once**. Механика подтверждения:

```python
try:
    await process(r, fields)
    await r.xack(settings.inbound_stream, settings.consumer_group, msg_id)
except Exception:
    log.exception("process error msg_id=%s", msg_id)   # НЕ ackаем (лог с трейсбеком)
```

- Успех → `XACK` убирает запись из **PEL** (Pending Entries List — список выданных, но не подтверждённых).
- Ошибка → **не** ackаем. Запись остаётся в PEL и может быть переобработана.

Отсюда следствие: обработка должна быть устойчива к повтору. Идемпотентность обеспечена: вставки в Postgres идут через `INSERT … ON CONFLICT` по уникальному `(message_id, role)`, горячее окно дедуплицируется по `mid`, а повторное применение уже применённого `seq` отсекается гейтом порядка. Разбор — в [10. Харденинг](10-hardening.md), C3.

**Dead-letter:** если задача падает раз за разом («отравленная»), она бы вечно висела в PEL. `reclaim_stale` считает попытки доставки (счётчик PEL) и после `max_deliveries` уводит задачу в `chat:inbound:dead` + `XACK` — очередь не застревает. Разбирать: `XRANGE chat:inbound:dead - +`. См. [10](10-hardening.md), R5-4.

## Восстановление зависших задач

Если воркер упал **между** захватом записи и `XACK`, запись «висит» в его PEL. Теперь
это восстанавливается автоматически: [`llm_worker.reclaim_stale`](../app/workers/llm_worker.py)
периодически (раз в `reclaim_every_iters` итераций) делает `XAUTOCLAIM` по записям,
простаивающим дольше `reclaim_min_idle_ms`, и дообрабатывает их. Это же не даёт залипнуть
FIFO-гейту порядка, если предшественник застрял в PEL умершего воркера. Разбор ответа
устойчив к версии Redis (2 или 3 элемента). Детали — [10. Харденинг](10-hardening.md), L1.

`CONSUMER_NAME` теперь уникален на процесс (`hostname-pid-rnd`) — иначе PEL/`XAUTOCLAIM`
для нескольких реплик работали бы некорректно.

## Вторая очередь: суммаризация

Тот же паттерн, отдельный стрим `chat:summarize` и группа `summarizers` — чтобы фоновая свёртка не конкурировала с горячим путём. Продюсер — `llm_worker` (`XADD` по триггеру), консьюмер — `summarizer`. См. [06. Суммаризация](06-summarization.md).

## Параметры

| Параметр | Значение | Смысл |
|---|---|---|
| `inbound_stream` | `chat:inbound` | имя стрима входящих |
| `consumer_group` | `llm-workers` | группа воркеров ответа |
| `summarize_stream` | `chat:summarize` | стрим задач суммаризации |
| `summarize_group` | `summarizers` | группа суммаризаторов |

## Уже реализовано (было в «точках роста»)

- **Ограничение длины стрима** — `XADD ... MAXLEN ~ inbound_stream_maxlen` (100000). Без него обработанные записи копили бы память (`XACK` убирает из PEL, но не из стрима). Рейтлимит ws-сообщений per-user ([10](10-hardening.md), H2) не даёт одному клиенту переполнить общий стрим и вытеснить чужие сообщения.
- **Reaper зависших задач** (`XAUTOCLAIM`), **dead-letter** отравленных и уникальный `CONSUMER_NAME` — см. [10. Харденинг](10-hardening.md).

## Точки роста

- **Шардирование.** Один стрим — единая точка нагрузки. На масштабе: `chat:inbound:{hash(conversation_id) % K}`, K групп/наборов воркеров. (FIFO-порядок уже обеспечен гейтом по `seq`, но единый стрим остаётся точкой нагрузки.) Не реализовано — осознанное упрощение.
