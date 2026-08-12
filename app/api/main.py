"""API-СЛОЙ = FastAPI (исполнитель №1, синхронный горячий путь).

Что такое здесь "вебсокет": это TCP-соединение между БРАУЗЕРОМ пользователя
(одно устройство = одно соединение) и ОДНИМ процессом uvicorn/FastAPI, на который
его направил балансировщик. У пользователя два устройства -> два разных сокета,
возможно на двух разных инстансах FastAPI.

Задача этого слоя:
  1) АВТОРИЗАЦИЯ: проверить токен и владение диалогом (иначе доступ к чужим данным);
  2) ВХОД:  принять текст из сокета -> XADD в Redis Stream (отдать работу воркеру);
  3) ВЫХОД: control-события из durable-ленты events:{id} + токены ответа из durable-ленты
     gen:{id}:{mid} -> в сокет. Pub/Sub не используется: обе ленты переживают обрыв и
     переигрываются на реконнекте (resumable).

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
from ..log import setup_logging, get_logger

log = get_logger("chat.api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Читаемые логи в stdout (см. app/log.py) — до всего остального.
    setup_logging(settings.log_level)
    # При старте применяем миграции БД (идемпотентно, под advisory-lock — безопасно
    # даже если несколько инстансов стартуют одновременно).
    await migrate()
    log.info("api ready")
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
    if len(email) > settings.email_max_chars:
        raise HTTPException(status_code=400, detail="email too long")
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

    # СНАПШОТ-КОНСИСТЕНТНЫЙ ДОГОН СОБЫТИЙ (только для первой страницы, before=None):
    # курсор ленты events:{id} читаем СТРОГО ДО выборки сообщений. Тогда любое событие
    # ПОСЛЕ курсора клиент дотейлит по вебсокету (?last_event_id=events_cursor), а любое
    # событие ДО/НА курсоре уже отражено в этой истории — воркер пишет сообщение в Postgres
    # ДО XADD события, поэтому событие в ленте => строка уже была в БД к моменту чтения
    # курсора => попала в снапшот. Порядок «cursor-first» закрывает дыру между снапшотом и
    # подпиской, которую с Pub/Sub закрыть было нельзя. Пустая лента -> "0" (тейл с начала).
    events_cursor = None
    if before is None:
        r = get_redis()
        last = await r.xrevrange(keys.events_stream(conversation_id), count=1)
        events_cursor = last[0][0] if last else "0"

    messages, has_more = await db.load_messages_page(conversation_id, n, before)
    # next_cursor = id самого старого сообщения страницы (грузить строго старше него)
    next_cursor = messages[0]["id"] if has_more and messages else None
    return {"messages": messages, "next_cursor": next_cursor,
            "has_more": has_more, "events_cursor": events_cursor}


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
                # Ленты нет по ДВУМ разным причинам, и их нельзя путать:
                #   1) она ЕЩЁ не создана — первый токен не пришёл (TTFT дольше block:
                #      на большом контексте/медленной модели это норма);
                #   2) она уже ПРОТУХЛА (истёк gen_ttl после завершения).
                # Сдаёмся только во 2-м случае. Признак «генерация ещё идёт» — active_gen
                # указывает на ИМЕННО эту генерацию (ставится ДО gen_start, снимается по
                # завершении). Иначе клиент, подключившийся до первого токена, бросал бы
                # тейл и навсегда завис бы с пустым «пишет…» баблом, хотя ответ идёт.
                if not await r.exists(gen_key):
                    if await r.get(keys.active_gen(conversation_id)) != message_id:
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

    # Курсор догона control-событий. Клиент передаёт last_event_id — id последнего
    # ПРИМЕНЁННОГО события ленты events:{id}: при первом коннекте это events_cursor из
    # снапшота истории (догон строго после снапшота, без дыр/дублей), при реконнекте —
    # последнее виденное событие (переиграем ровно пропущенное за время обрыва).
    # Пусто -> "$": только новые события с этого момента.
    last_event_id = ws.query_params.get("last_event_id") or "$"

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

    # --- Control-канал: durable-лента events:{id} (Redis Stream) ВМЕСТО Pub/Sub.
    #     Несёт эхо user_message и сигнал gen_start. Токены ассистента — отдельной
    #     durable-лентой gen:{id}:{mid}. Читаем тем же XREAD BLOCK, что и токены. ---
    async def pump_out(last_id: str):
        """Тейлит ленту событий. Durable => переживает обрыв Redis/сети: на реконнекте
        продолжаем с last_id и переигрываем пропущенное. Каждое событие несём клиенту с
        полем eid (id записи) — по нему клиент двигает свой курсор догона (last_event_id).

        ДОГОН АКТИВНОЙ ГЕНЕРАЦИИ: gen_start, случившийся ДО last_id (генерация уже шла на
        момент коннекта — например, началась между снапшотом истории и подпиской, либо шла
        ещё до открытия чата), в ленте не переиграется. Его ловит active_gen (ставится ДО
        gen_start, снимается по завершении): проверяем при старте и после каждого
        переподключения. tail_generation идемпотентен по message_id — двойного тейла нет."""
        ev_key = keys.events_stream(conversation_id)
        while True:
            try:
                active_mid = await r.get(keys.active_gen(conversation_id))
                if active_mid:
                    start_tail(active_mid)
                while True:
                    resp = await r.xread({ev_key: last_id}, block=5000, count=100)
                    if not resp:
                        continue                        # таймаут XREAD -> ждём дальше
                    for _key, entries in resp:
                        for entry_id, fields in entries:
                            last_id = entry_id
                            evt = json.loads(fields["data"])
                            evt["eid"] = entry_id       # курсор догона для клиента
                            if evt.get("type") == "gen_start":
                                start_tail(evt["message_id"])   # токены — из gen:{}-ленты
                            await send(json.dumps(evt))  # gen_start клиент шлёт лишь для eid
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("events tail reconnect conv=%s: %r", conversation_id, e)
                await asyncio.sleep(1.0)                # пауза перед повторным XREAD

    out_task = asyncio.create_task(pump_out(last_event_id))

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

            # Все обращения к Redis для приёма сообщения — под одним try: транзиентный
            # сбой Redis НЕ должен ронять вебсокет (иначе моргание Redis отключило бы
            # все активные сокеты разом). Сообщаем клиенту server_busy и ждём следующее.
            # WebSocketDisconnect пробрасываем — это уже обрыв самого сокета.
            try:
                # РЕЙТЛИМИТ (H2): режем флуд ДО расхода seq и XADD, чтобы один клиент не
                # заливал общий inbound-стрим и не вытеснял сообщения других юзеров.
                if not await _allow_ws_message(r, user_id):
                    await send(json.dumps({"type": "error", "error": "rate_limited"}))
                    continue

                # seq — монотонный номер сообщения в диалоге (FIFO-порядок в воркере).
                # Продлеваем TTL счётчика на активности (M1): в паре с продлением в воркере
                # (_mark_applied) это держит seq живым не меньше applied, а после простоя
                # оба протухают вместе -> счёт с нуля, без ложных дропов.
                seq = await r.incr(keys.conv_seq(conversation_id))
                await r.expire(keys.conv_seq(conversation_id),
                               settings.conv_counter_ttl_seconds)

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
                raise
            except Exception as e:
                # Redis моргнул: сокет живой, сообщение не принято. Клиент может повторить.
                # Возможный расход seq без XADD самозалечит FIFO-гейт по order_gap_timeout.
                log.warning("inbound redis error conv=%s: %r", conversation_id, e)
                await send(json.dumps({"type": "error", "error": "server_busy"}))
                continue
    except WebSocketDisconnect:
        pass
    finally:
        out_task.cancel()
        for t in tasks:
            t.cancel()


@app.get("/healthz")
async def healthz():
    return {"ok": True}
