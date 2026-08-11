"""Слой Postgres — SOURCE OF TRUTH. Здесь лежит вся история навсегда.
Используем asyncpg напрямую (в реальном проекте можно SQLAlchemy).

conversation_id и user_id — UUID (передаём строками, asyncpg их разбирает).
Водяной знак суммаризации — id последнего свёрнутого сообщения (монотонный BIGINT),
а не created_at: id не даёт коллизий на границе при одинаковых таймстампах."""
import uuid

import asyncpg
from .config import settings

_pool: asyncpg.Pool | None = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(settings.postgres_dsn, min_size=1, max_size=10)
    return _pool


def _uid(value: str) -> uuid.UUID:
    """Строка -> UUID (единый разбор для conversation_id / user_id)."""
    return uuid.UUID(str(value))


# ---------- Пользователи (веб-регистрация) ----------

async def create_user(email: str, password_hash: str) -> str:
    """Создать пользователя, вернуть его id (UUID-строку)."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """INSERT INTO users (email, password_hash) VALUES ($1, $2)
               RETURNING id""",
            email, password_hash,
        )
    return str(row["id"])


async def get_user_by_email(email: str) -> dict | None:
    """Найти пользователя по email -> {id, password_hash} или None."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, password_hash FROM users WHERE email = $1", email)
    if row is None:
        return None
    return {"id": str(row["id"]), "password_hash": row["password_hash"]}


# ---------- Диалоги и владение ----------

async def create_conversation(user_id: str) -> str:
    """Создать новый диалог за пользователем, вернуть его id."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO conversations (user_id) VALUES ($1) RETURNING id",
            _uid(user_id),
        )
    return str(row["id"])


async def list_conversations_page(user_id: str, limit: int,
                                  before: tuple | None = None) -> tuple[list[dict], bool]:
    """Страница диалогов пользователя (свежие сверху) — KEYSET-пагинация (chat_cursor).

    Сортировка (created_at DESC, id DESC); курсор — пара (created_at, id) последнего
    показанного диалога. `before=None` -> первая страница (самые свежие). Индекс
    idx_conversations_user_created делает выборку без OFFSET/seq-scan.

    Возвращает (rows, has_more): rows = [{id, created_at}], has_more — есть ли ещё
    более старые. Чтобы понять has_more, берём limit+1 и лишний обрезаем."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        if before is None:
            rows = await conn.fetch(
                """SELECT id, created_at, title FROM conversations
                   WHERE user_id = $1
                   ORDER BY created_at DESC, id DESC
                   LIMIT $2""",
                _uid(user_id), limit + 1,
            )
        else:
            before_ts, before_id = before
            rows = await conn.fetch(
                """SELECT id, created_at, title FROM conversations
                   WHERE user_id = $1 AND (created_at, id) < ($2, $3)
                   ORDER BY created_at DESC, id DESC
                   LIMIT $4""",
                _uid(user_id), before_ts, _uid(before_id), limit + 1,
            )
    has_more = len(rows) > limit
    rows = rows[:limit]
    return ([{"id": str(r["id"]), "created_at": r["created_at"], "title": r["title"]}
             for r in rows], has_more)


async def set_conversation_title(conversation_id: str, title: str) -> None:
    """Сохранить авто-название. Только если ещё не задано (title IS NULL) — так
    повторная/дублирующая title-задача не перезатирает уже сгенерированное."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE conversations SET title = $2 WHERE id = $1 AND title IS NULL",
            _uid(conversation_id), title)


async def get_first_user_message(conversation_id: str) -> str | None:
    """Первое сообщение пользователя — основа для авто-названия."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """SELECT content FROM messages
               WHERE conversation_id = $1 AND role = 'user'
               ORDER BY id LIMIT 1""",
            _uid(conversation_id))
    return row["content"] if row else None


async def user_owns_conversation(user_id: str, conversation_id: str) -> bool:
    """Принадлежит ли диалог пользователю (для проверки доступа к истории)."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        owner = await conn.fetchval(
            "SELECT user_id FROM conversations WHERE id = $1", _uid(conversation_id))
    return owner is not None and owner == _uid(user_id)

async def ensure_conversation(conversation_id: str, user_id: str) -> bool:
    """Идемпотентно гарантируем строки users и conversations и ПРОВЕРЯЕМ владение.

    Возвращает True, если диалог принадлежит user_id (существующий — за ним, или
    только что создан за ним). False -> диалог принадлежит другому пользователю
    (доступ запретить). Это устраняет доступ к чужим диалогам и заодно создаёт
    строку conversations, без которой резюме некуда было сохранять."""
    cid, uid = _uid(conversation_id), _uid(user_id)
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "INSERT INTO users (id) VALUES ($1) ON CONFLICT (id) DO NOTHING", uid)
            await conn.execute(
                """INSERT INTO conversations (id, user_id) VALUES ($1, $2)
                   ON CONFLICT (id) DO NOTHING""", cid, uid)
            owner = await conn.fetchval(
                "SELECT user_id FROM conversations WHERE id = $1", cid)
    return owner is not None and owner == uid


# ---------- Сообщения ----------

async def insert_message(conversation_id: str, role: str, content: str,
                         message_id: str | None = None) -> bool:
    """Пишем одно сообщение в холодное надёжное хранилище. ИДЕМПОТЕНТНО:
    повторная вставка с тем же (message_id, role) отсекается ON CONFLICT.
    Возвращает True, если строка реально вставлена; False — если это дубль (ретрай).
    message_id=None -> дедупа нет (частичный индекс NULL не покрывает)."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """INSERT INTO messages (conversation_id, role, content, message_id)
               VALUES ($1, $2, $3, $4)
               ON CONFLICT (message_id, role) WHERE message_id IS NOT NULL
               DO NOTHING
               RETURNING id""",
            _uid(conversation_id), role, content, message_id,
        )
    return row is not None


async def load_recent_messages(conversation_id: str, limit: int) -> list[dict]:
    """Регидратация: тянем хвост истории, чтобы пересобрать горячий контекст.
    Порядок — по id (монотонный), а не created_at: устойчив к одинаковым таймстампам.
    message_id (как `mid`) нужен, чтобы регидратированное окно оставалось дедуп-совместимым."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT role, content, message_id FROM messages
               WHERE conversation_id = $1
               ORDER BY id DESC
               LIMIT $2""",
            _uid(conversation_id), limit,
        )
    # вернулись в обратном порядке (свежие сверху) — разворачиваем в хронологию
    return [{"role": r["role"], "content": r["content"], "mid": r["message_id"]}
            for r in reversed(rows)]


async def load_messages_page(conversation_id: str, limit: int,
                             before_id: int | None = None) -> tuple[list[dict], bool]:
    """Страница сообщений чата — KEYSET-пагинация (message_cursor) для прокрутки ВВЕРХ.

    Сортируем по id DESC (свежие первыми), курсор — `before_id` (грузим строго
    старше него). Индекс (conversation_id, id) делает это O(log n + limit) без OFFSET.
    `before_id=None` -> последняя (самая свежая) страница.

    Возвращает (messages, has_more): messages в ХРОНОЛОГИЧЕСКОМ порядке (старые слева)
    с полем `id` (курсор клиента), has_more — есть ли ещё более старые сообщения."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        if before_id is None:
            rows = await conn.fetch(
                """SELECT id, role, content FROM messages
                   WHERE conversation_id = $1
                   ORDER BY id DESC LIMIT $2""",
                _uid(conversation_id), limit + 1,
            )
        else:
            rows = await conn.fetch(
                """SELECT id, role, content FROM messages
                   WHERE conversation_id = $1 AND id < $2
                   ORDER BY id DESC LIMIT $3""",
                _uid(conversation_id), before_id, limit + 1,
            )
    has_more = len(rows) > limit
    rows = rows[:limit]
    # rows: свежие->старые; разворачиваем в хронологию для рендера
    messages = [{"id": r["id"], "role": r["role"], "content": r["content"]}
                for r in reversed(rows)]
    return messages, has_more


# ---------- Суммаризация ----------

async def get_summary(conversation_id: str):
    """Текущее резюме диалога и id последнего свёрнутого сообщения (watermark).
    Диалога может ещё не быть -> (None, 0)."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT summary, summary_upto_id FROM conversations WHERE id = $1",
            _uid(conversation_id),
        )
    if row is None:
        return None, 0
    return row["summary"], row["summary_upto_id"]


async def load_messages_since(conversation_id: str, since_id: int,
                              limit: int | None = None) -> list[dict]:
    """Сообщения, ещё НЕ вошедшие в summary (id > since_id), в порядке id.
    Возвращаем c id, чтобы воркер знал, до какого места двигать watermark.
    limit — потолок за один заход (защита от OOM, если суммаризатор далеко отстал);
    если хвост длиннее, суммаризатор дообработает его следующей задачей."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        if limit is None:
            rows = await conn.fetch(
                """SELECT id, role, content FROM messages
                   WHERE conversation_id = $1 AND id > $2 ORDER BY id""",
                _uid(conversation_id), int(since_id or 0),
            )
        else:
            rows = await conn.fetch(
                """SELECT id, role, content FROM messages
                   WHERE conversation_id = $1 AND id > $2 ORDER BY id LIMIT $3""",
                _uid(conversation_id), int(since_id or 0), int(limit),
            )
    return [dict(r) for r in rows]


async def save_summary(conversation_id: str, summary: str, upto_id: int) -> None:
    """Сохраняем резюме и сдвигаем watermark. UPSERT: страхуемся на случай,
    если строки conversations почему-то нет (иначе UPDATE молча обновит 0 строк
    и резюме потеряется)."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO conversations (id, summary, summary_upto_id)
               VALUES ($1, $2, $3)
               ON CONFLICT (id) DO UPDATE
                   SET summary = EXCLUDED.summary,
                       summary_upto_id = EXCLUDED.summary_upto_id""",
            _uid(conversation_id), summary, int(upto_id),
        )
