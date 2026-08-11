"""API-СЛОЙ = FastAPI (исполнитель №1, синхронный горячий путь).

Что такое здесь "вебсокет": это TCP-соединение между БРАУЗЕРОМ пользователя
(одно устройство = одно соединение) и ОДНИМ процессом uvicorn/FastAPI, на который
его направил балансировщик. У пользователя два устройства -> два разных сокета,
возможно на двух разных инстансах FastAPI.

Задача этого слоя:
  1) АВТОРИЗАЦИЯ: проверить токен и владение диалогом (иначе доступ к чужим данным);
  2) ВХОД:  принять текст из сокета -> XADD в Redis Stream (отдать работу воркеру);
  3) ВЫХОД: control-события из Pub/Sub + токены ответа из durable-ленты -> в сокет.

FastAPI НЕ ходит к LLM и НЕ пишет историю в Postgres. Единственное обращение к БД —
проверка владения диалогом при подключении (источник истины по доступу).
"""
import asyncio
import base64
import json
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

from fastapi import (Body, Depends, FastAPI, Header, HTTPException, WebSocket,
                     WebSocketDisconnect)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from ..config import settings
from ..redis_client import get_redis
from ..migrate import migrate
from .. import keys, db, auth
from ..auth import authenticate


@asynccontextmanager
async def lifespan(app: FastAPI):
    # При старте применяем миграции БД (идемпотентно, под advisory-lock — безопасно
    # даже если несколько инстансов стартуют одновременно).
    await migrate()
    yield


app = FastAPI(lifespan=lifespan)

# CORS: страница и API на одном origin, но оставляем настраиваемым (для теста "*").
app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in settings.cors_allow_origins.split(",") if o.strip()],
    allow_methods=["*"],
    allow_headers=["*"],
)

_STATIC = Path(__file__).resolve().parent.parent / "static"


# ---------- Аутентификация HTTP-эндпоинтов (Bearer) ----------

async def current_user(authorization: str = Header(default="")) -> str:
    """Достаём user_id из заголовка Authorization: Bearer <jwt>."""
    token = authorization[7:].strip() if authorization.lower().startswith("bearer ") else ""
    user_id = await authenticate(token)
    if not user_id:
        raise HTTPException(status_code=401, detail="unauthorized")
    return user_id


# ---------- Регистрация / вход ----------

def _norm_credentials(body: dict, min_len: int = 0) -> tuple[str, str]:
    email = (body.get("email") or "").strip().lower()
    password = body.get("password") or ""
    if not email or not password:
        raise HTTPException(status_code=400, detail="email and password required")
    if len(password) > settings.password_max_chars:
        raise HTTPException(status_code=400, detail="password too long")
    if min_len and len(password) < min_len:
        raise HTTPException(status_code=400, detail=f"password too short (min {min_len})")
    return email, password


@app.post("/api/register")
async def register(body: dict = Body(...)):
    email, password = _norm_credentials(body, min_len=6)   # политика длины — при регистрации
    if await db.get_user_by_email(email):
        raise HTTPException(status_code=409, detail="email already registered")
    user_id = await db.create_user(email, await auth.hash_password(password))
    return {"token": auth.make_token(user_id), "user_id": user_id, "email": email}


@app.post("/api/login")
async def login(body: dict = Body(...)):
    email, password = _norm_credentials(body)   # на входе минимум не навязываем
    user = await db.get_user_by_email(email)
    if not user or not user["password_hash"] or \
            not await auth.verify_password(password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="invalid email or password")
    return {"token": auth.make_token(user["id"]), "user_id": user["id"], "email": email}


# ---------- Пагинация: helpers ----------

def _clamp_limit(limit: int | None, default: int) -> int:
    if not limit or limit < 1:
        return default
    return min(limit, settings.page_size_max)


def _encode_chat_cursor(created_at: datetime, conv_id: str) -> str:
    """Непрозрачный курсор списка диалогов: (created_at, id) -> base64."""
    raw = f"{created_at.isoformat()}|{conv_id}".encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def _decode_chat_cursor(cursor: str) -> tuple[datetime, str]:
    try:
        raw = base64.urlsafe_b64decode(cursor.encode("ascii")).decode("utf-8")
        ts, conv_id = raw.rsplit("|", 1)
        return datetime.fromisoformat(ts), conv_id
    except Exception:
        raise HTTPException(status_code=400, detail="bad cursor")


# ---------- Диалоги ----------

@app.get("/api/conversations")
async def list_conversations(user_id: str = Depends(current_user),
                             cursor: str | None = None, limit: int | None = None):
    """Список диалогов пользователя, keyset-пагинация (chat_cursor).
    Ответ: {conversations: [id...], next_cursor, has_more}. next_cursor -> в ?cursor=
    для следующей (более старой) страницы."""
    n = _clamp_limit(limit, settings.conversations_page_size)
    before = _decode_chat_cursor(cursor) if cursor else None
    rows, has_more = await db.list_conversations_page(user_id, n, before)
    next_cursor = _encode_chat_cursor(rows[-1]["created_at"], rows[-1]["id"]) \
        if has_more and rows else None
    return {
        "conversations": [
            {"id": r["id"], "title": r["title"],
             "created_at": r["created_at"].isoformat()} for r in rows
        ],
        "next_cursor": next_cursor, "has_more": has_more,
    }


@app.post("/api/conversations")
async def new_conversation(user_id: str = Depends(current_user)):
    return {"id": await db.create_conversation(user_id)}


@app.get("/api/conversations/{conversation_id}/messages")
async def conversation_history(conversation_id: str,
                               user_id: str = Depends(current_user),
                               before: int | None = None, limit: int | None = None):
    """История чата, keyset-пагинация вверх (message_cursor).
    `before` — id, строго старше которого грузим (для прокрутки к старым сообщениям).
    Ответ: {messages:[{id,role,content}...] (хронологически), next_cursor, has_more}.
    next_cursor -> в ?before= для следующей (более старой) страницы."""
    try:
        uuid.UUID(conversation_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="bad conversation id")
    if not await db.user_owns_conversation(user_id, conversation_id):
        raise HTTPException(status_code=403, detail="forbidden")
    n = _clamp_limit(limit, settings.messages_page_size)
    messages, has_more = await db.load_messages_page(conversation_id, n, before)
    # next_cursor = id самого старого сообщения страницы (грузить строго старше него)
    next_cursor = messages[0]["id"] if has_more and messages else None
    return {"messages": messages, "next_cursor": next_cursor, "has_more": has_more}


# ---------- Веб-страницы ----------

@app.get("/")
async def page_index():
    return FileResponse(_STATIC / "index.html")


@app.get("/chat")
async def page_chat():
    return FileResponse(_STATIC / "chat.html")


async def _allow_ws_message(r, user_id: str) -> bool:
    """Рейтлимит входящих ws-сообщений per-user, фиксированное окно (H2).

    Единый inbound-стрим общий на всех пользователей; без лимита один клиент может
    залить его быстрее, чем воркеры разгребают, и приблизительный MAXLEN начнёт
    выбрасывать ещё НЕ доставленные сообщения ДРУГИХ пользователей (тихая потеря).
    INCR+EXPIRE в окне — атомарно, дёшево (2 команды) и общее между инстансами api.
    Проверяем ДО расхода seq, чтобы отклонённое сообщение не создавало дыру в порядке."""
    window = int(time.time()) // settings.ws_rate_window_seconds
    key = keys.ws_rate(user_id, window)
    n = await r.incr(key)
    if n == 1:
        await r.expire(key, settings.ws_rate_window_seconds)
    return n <= settings.ws_rate_max_messages


async def tail_generation(ws: WebSocket, r, conversation_id: str,
                          message_id: str, tailing: set[str], send):
    """RESUMABLE-ядро: переигрываем durable-ленту gen:{id}:{mid} в этот сокет.

    XREAD стартует с "0" -> сначала прилетают ВСЕ уже сгенерированные токены
    (догон/replay), затем тот же цикл блокирующе ждёт новые (live-хвост). Так
    клиент, подключившийся посреди генерации ИЛИ после обрыва, видит ответ
    целиком с начала и продолжает в реальном времени.

    send — сериализованная отправка в сокет (общий lock), tailing — защита от
    двойного тейла одной генерации."""
    if message_id in tailing:
        return
    tailing.add(message_id)
    gen_key = keys.gen_stream(conversation_id, message_id)
    try:
        await send(json.dumps({"type": "assistant_start", "message_id": message_id}))
        last_id = "0"                       # "0" => отдать ленту с самого начала
        while True:
            resp = await r.xread({gen_key: last_id}, block=5000, count=200)
            if not resp:
                # таймаут ожидания: если ленты уже нет (истёк gen_ttl) — выходим
                if not await r.exists(gen_key):
                    return
                continue
            for _key, entries in resp:
                for entry_id, fields in entries:
                    last_id = entry_id
                    if fields.get("t") == "end":
                        await send(json.dumps({
                            "type": "assistant_end",
                            "message_id": message_id,
                            "error": fields.get("error", ""),
                        }))
                        return
                    await send(json.dumps({
                        "type": "token",
                        "content": fields.get("c", ""),
                        "message_id": message_id,
                    }))
    except WebSocketDisconnect:
        pass
    finally:
        tailing.discard(message_id)


@app.websocket("/ws/{conversation_id}")
async def ws_endpoint(ws: WebSocket, conversation_id: str):
    await ws.accept()
    r = get_redis()

    # --- АВТОРИЗАЦИЯ: токен -> user_id, затем проверка владения диалогом ---
    # Токен передаётся как query-параметр: ws://.../ws/{id}?token=...
    user_id = await authenticate(ws.query_params.get("token"))
    if not user_id:
        await ws.close(code=1008)          # policy violation
        return
    try:
        uuid.UUID(conversation_id)          # conversation_id обязан быть UUID
    except ValueError:
        await ws.close(code=1008)
        return
    if not await db.ensure_conversation(conversation_id, user_id):
        await ws.close(code=1008)          # диалог принадлежит другому пользователю
        return

    tasks: set[asyncio.Task] = set()
    tailing: set[str] = set()             # message_id генераций, уже тейлящихся

    # Единый сериализованный вывод в сокет: pump_out и несколько tail-задач пишут
    # в ОДИН сокет; без общего замка Starlette может словить concurrent send.
    send_lock = asyncio.Lock()

    async def send(data: str):
        async with send_lock:
            await ws.send_text(data)

    def start_tail(message_id: str):
        t = asyncio.create_task(
            tail_generation(ws, r, conversation_id, message_id, tailing, send))
        tasks.add(t)
        t.add_done_callback(tasks.discard)

    # --- Control-канал: conv:{id} (Pub/Sub) несёт события управления:
    #     эхо user_message и сигнал gen_start. Токены ассистента — durable-лентой. ---
    pubsub = r.pubsub()

    async def pump_out():
        """Устойчив к рестарту/обрыву Redis: при ошибке переподписывается. После
        (пере)подписки ДЕЛАЕМ ДОГОН по active_gen — так ловим и генерацию, идущую с
        момента коннекта, и ту, что стартовала в окно, пока Pub/Sub был отвалившимся
        (пропущенный gen_start компенсируется этой проверкой)."""
        while True:
            try:
                await pubsub.subscribe(keys.conv_channel(conversation_id))
                active_mid = await r.get(keys.active_gen(conversation_id))
                if active_mid:
                    start_tail(active_mid)
                async for msg in pubsub.listen():
                    if msg["type"] != "message":
                        continue
                    evt = json.loads(msg["data"])
                    if evt.get("type") == "gen_start":
                        start_tail(evt["message_id"])   # токены — из durable-ленты
                    else:
                        await send(msg["data"])         # user_message и прочий control
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print("pubsub reconnect:", repr(e))
                try:
                    await pubsub.unsubscribe(keys.conv_channel(conversation_id))
                except Exception:
                    pass
                await asyncio.sleep(1.0)                # пауза перед переподпиской

    out_task = asyncio.create_task(pump_out())

    try:
        # --- ВХОДНОЙ цикл: сокет -> Redis Stream ---
        while True:
            raw = await ws.receive_text()

            # ВАЛИДАЦИЯ входа: кривой JSON / нет текста / слишком длинно -> не роняем
            # соединение, отвечаем ошибкой и ждём следующее сообщение.
            try:
                data = json.loads(raw)
                text = data["text"]
            except (json.JSONDecodeError, KeyError, TypeError):
                await send(json.dumps({"type": "error", "error": "bad_message"}))
                continue
            if not isinstance(text, str) or not text.strip():
                await send(json.dumps({"type": "error", "error": "empty_text"}))
                continue
            if len(text) > settings.max_message_chars:
                await send(json.dumps({"type": "error", "error": "too_long"}))
                continue

            # РЕЙТЛИМИТ (H2): режем флуд ДО расхода seq и XADD, чтобы один клиент не
            # заливал общий inbound-стрим и не вытеснял из него сообщения других юзеров.
            if not await _allow_ws_message(r, user_id):
                await send(json.dumps({"type": "error", "error": "rate_limited"}))
                continue

            # seq — монотонный номер сообщения в диалоге (FIFO-порядок в воркере).
            # Продлеваем TTL счётчика на активности (M1): в паре с продлением в воркере
            # (_mark_applied) это держит seq живым не меньше applied, а после простоя оба
            # протухают вместе -> счёт с нуля, без ложных дропов.
            seq = await r.incr(keys.conv_seq(conversation_id))
            await r.expire(keys.conv_seq(conversation_id), settings.conv_counter_ttl_seconds)

            # XADD кладёт задачу в durable-очередь и мгновенно возвращает управление.
            # Пользователь может слать новые сообщения, не дожидаясь ответа LLM.
            # MAXLEN ~ ограничивает рост стрима (XACK убирает из PEL, но не из стрима).
            await r.xadd(settings.inbound_stream, {
                "conversation_id": conversation_id,
                "user_id": user_id,
                "message_id": str(uuid.uuid4()),
                "seq": str(seq),
                "text": text,
            }, maxlen=settings.inbound_stream_maxlen, approximate=True)
    except WebSocketDisconnect:
        pass
    finally:
        out_task.cancel()
        for t in tasks:
            t.cancel()
        await pubsub.unsubscribe(keys.conv_channel(conversation_id))
        await pubsub.aclose()


@app.get("/healthz")
async def healthz():
    return {"ok": True}
