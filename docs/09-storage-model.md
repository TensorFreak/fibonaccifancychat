# 09. Модель хранения

**Файлы:** [`migrations/`](../migrations), [`app/db.py`](../app/db.py), [`app/keys.py`](../app/keys.py)

Принцип: **Postgres — source of truth (истина, вечно), Redis — быстрая проекция (с TTL)**. Всё в Redis можно потерять и восстановить из Postgres; наоборот — нет.

## Postgres: холодное хранилище

Схема задаётся миграциями в [`migrations/`](../migrations) (см. [13. Миграции](13-migrations.md)); базовая — `0001_init`.

### `users`
```sql
id UUID PK, created_at TIMESTAMPTZ
```
Пользователи. Минимально.

### `conversations`
```sql
id UUID PK, user_id UUID -> users(id), created_at TIMESTAMPTZ,
title TEXT,                      -- авто-название (отдельный LLM-запрос; NULL = «Новый чат»)
summary TEXT,                    -- бегущее резюме старой части диалога
summary_upto_id BIGINT DEFAULT 0 -- id последнего сообщения, свёрнутого в summary
```
`title` заполняет суммаризатор (`generate_title`) отдельным коротким LLM-запросом по
первому сообщению — см. [06. Суммаризация](06-summarization.md). Миграция `0003`.
`summary` + `summary_upto_id` — состояние суммаризации. Watermark — монотонный `id`
сообщения (а не `created_at`): без коллизий на границе. Строки `conversations`/`users`
создаются при подключении ([10](10-hardening.md), C1). См. [06. Суммаризация](06-summarization.md).

### `messages`
```sql
id BIGINT IDENTITY PK, conversation_id UUID, role TEXT CHECK(user|assistant|system),
content TEXT, message_id TEXT, created_at TIMESTAMPTZ
```
Вся история диалогов навсегда. Пишется в llm_worker (сообщение юзера и ответ ассистента).
`message_id` — id входящего сообщения, ключ идемпотентности (см. индексы).

### Индексы
```sql
idx_messages_conv_id ON messages (conversation_id, id)
idx_messages_dedup   ON messages (message_id, role) WHERE message_id IS NOT NULL  -- UNIQUE
```
Первый — для регидратации горячего окна и выборки несвёрнутого хвоста для суммаризатора
(порядок по монотонному `id`). Второй — **частичный уникальный** дедуп-индекс: гарантирует
идемпотентность вставок (`INSERT … ON CONFLICT`), сообщения без `message_id` в дедупе не
участвуют. Разбор — [10](10-hardening.md), C3.

## Слой доступа: `app/db.py`

Async через `asyncpg`, пул соединений (`min_size=1, max_size=10`).

| Функция | Назначение |
|---|---|
| `ensure_conversation(conv, user_id)` | создать `users`/`conversations` и проверить владение (→ bool) |
| `insert_message(conv, role, content, message_id)` | вставка сообщения, идемпотентная (`ON CONFLICT`), → bool |
| `load_recent_messages(conv, limit)` | последние N (по `id`) в хронологии — регидратация окна (с `mid`) |
| `load_messages_page(conv, limit, before_id)` | keyset-страница сообщений — **message_cursor** ([14](14-pagination.md)) |
| `list_conversations_page(user, limit, before)` | keyset-страница списка диалогов (с `title`) — **chat_cursor** ([14](14-pagination.md)) |
| `set_conversation_title(conv, title)` | сохранить авто-название (только если `title IS NULL`) |
| `get_first_user_message(conv)` | первое сообщение юзера — основа авто-названия |
| `get_summary(conv)` | `(summary, summary_upto_id)` диалога |
| `load_messages_since(conv, since_id, limit)` | сообщения с `id > since_id` (с лимитом) — для суммаризатора |
| `save_summary(conv, summary, upto_id)` | upsert резюме и сдвиг watermark |

Индекс `idx_conversations_user_created (user_id, created_at DESC, id DESC)` (миграция
`0002`) обслуживает keyset-пагинацию списка диалогов.

## Redis: горячая проекция

Все имена ключей — в одном модуле [`app/keys.py`](../app/keys.py) (единый источник: api и воркеры обязаны использовать одинаковые имена). Клиент — `decode_responses=True` (работаем со `str`).

| Ключ | Тип | Содержимое | TTL | Документ |
|---|---|---|---|---|
| `chat:inbound` | Stream | входящие задачи | до `XACK` | [03](03-inbound-queue.md) |
| `chat:summarize` | Stream | задачи суммаризации | до `XACK` | [06](06-summarization.md) |
| `ctx:{id}` | List | горячее окно последних сообщений | 1800 c | [05](05-hot-context.md) |
| `sum:{id}` | String | кэш текущего резюме | 1800 c | [06](06-summarization.md) |
| `since_sum:{id}` | String (счётчик) | сколько сообщений с последней свёртки | — | [06](06-summarization.md) |
| `conv:{id}` | Pub/Sub | control-события (`gen_start`, `user_message`) | не хранится | [02](02-websocket-api.md) |
| `gen:{id}:{mid}` | Stream | токены одной генерации | 300 c после `end` | [07](07-resumable-streams.md) |
| `active_gen:{id}` | String | id идущей сейчас генерации | 300 c | [07](07-resumable-streams.md) |
| `lock:conv:{id}` | String | замок диалога (сериализация) | 300 c + heartbeat | [04](04-llm-worker.md) |
| `lock:sum:{id}` | String | замок суммаризации | 120 c | [06](06-summarization.md) |
| `seq:conv:{id}` | String (счётчик) | монотонный номер сообщения (FIFO) | — | [10](10-hardening.md) |
| `applied:conv:{id}` | String | номер последнего применённого сообщения | — | [10](10-hardening.md) |

Идемпотентность вставок обеспечивается не Redis-ключом, а частичным уникальным индексом
`(message_id, role)` в Postgres (`INSERT … ON CONFLICT`) — см. [10](10-hardening.md), C3.

Функции-конструкторы ключей в `keys.py`: `ctx_key`, `conv_channel`, `conv_lock`, `sum_key`, `since_sum_key`, `gen_stream`, `active_gen`, `conv_seq`, `conv_applied`. Стримы/группы — в `config.py`.

## Что где живёт: сводка по данным

| Данные | Истина (Postgres) | Горячая копия (Redis) | Восстановление |
|---|---|---|---|
| сообщения | `messages` | `ctx:{id}` (последние N) | `load_recent_messages` → прогрев `ctx` |
| резюме | `conversations.summary` | `sum:{id}` | `get_summary` → прогрев `sum` |
| watermark свёртки | `conversations.summary_upto_id` | — | всегда из Postgres |
| токены генерации | — (только готовый ответ в `messages`) | `gen:{id}:{mid}` | перезагрузка истории после TTL |
| счётчик до свёртки | — | `since_sum:{id}` | пересоздаётся (потеря = лишь сдвиг триггера) |

Ключевое следствие: **потеря Redis не рушит данные**. Контекст и резюме регидратируются из Postgres при следующем обращении (cache-aside). Теряется только «в полёте»: незавершённые генерации и текущий счётчик до суммаризации — некритично.

## Проекция и согласованность

- `messages` (Postgres) — первично; `ctx:{id}` (Redis) — производно, обновляется тем же воркером синхронно с записью в БД.
- `conversations.summary` (Postgres) — первично; `sum:{id}` (Redis) — кэш, обновляется суммаризатором сразу после `save_summary`.
- Рассинхрон возможен лишь кратковременно (кэш протух → регидратация), устойчивого расхождения нет: писатель всегда обновляет обе стороны.
