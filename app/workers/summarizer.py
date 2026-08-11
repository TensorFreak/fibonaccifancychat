"""СУММАРИЗАТОР = исполнитель №3 (отдельный фоновый процесс). ЭТО НЕ FastAPI.

Запуск: `python -m app.workers.summarizer`.
Свой Redis Stream (`chat:summarize`) и своя consumer group — независим от LLM-воркера
и не мешает горячему пути ответа пользователю.

Что делает: берёт задачу {conversation_id}, читает из Postgres сообщения, которые
ещё НЕ вошли в summary, сворачивает их через LLM в обновлённое резюме, сохраняет
его в Postgres (source of truth) и обновляет кэш `sum:{id}` в Redis. Последние
`summary_recent_keep` сообщений НЕ сворачивает — они остаются дословно в горячем окне.
"""
import asyncio
import os
import signal
import socket
import uuid

from redis.exceptions import ResponseError

from ..config import settings
from ..redis_client import get_redis
from ..migrate import migrate
from .. import keys, db, tokens
from ..locks import release_lock
from ..llm.client import complete

# Уникальное имя консьюмера на процесс (см. пояснение в llm_worker).
CONSUMER_NAME = f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"


async def ensure_group(r):
    try:
        await r.xgroup_create(settings.summarize_stream,
                              settings.summarize_group, id="0", mkstream=True)
    except ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


def render(messages: list[dict]) -> str:
    return "\n".join(f"{m['role']}: {m['content']}" for m in messages)


async def fold(old_summary: str | None, messages: list[dict]) -> str:
    """Свернуть messages в резюме поверх old_summary — ИТЕРАТИВНО по чанкам.

    Контроль длины: большой накопившийся хвост нельзя отдать одним промптом —
    сам промпт суммаризатора превысит контекст модели. Поэтому бьём хвост на
    чанки по summary_fold_chunk_tokens и последовательно вкатываем каждый в
    резюме. Результат каждого шага ограничен summary_max_tokens (и потолком в
    промпте, и жёсткой обрезкой) — так summary не разрастается от раза к разу."""
    summary = old_summary
    chunks = tokens.chunk_messages(messages, settings.summary_fold_chunk_tokens)
    for chunk in chunks:
        prompt = [
            {"role": "system",
             "content": "Ты сжимаешь историю диалога в краткое содержание. "
                        "Обнови существующее резюме с учётом новых сообщений. "
                        "Пиши сжато, сохраняй факты, имена, договорённости и решения. "
                        f"Уложись примерно в {settings.summary_max_tokens} токенов."},
            {"role": "user",
             "content": f"Текущее резюме:\n{summary or '(пусто)'}\n\n"
                        f"Новые сообщения:\n{render(chunk)}\n\n"
                        f"Обновлённое резюме:"},
        ]
        summary = await complete(prompt, max_tokens=settings.summary_max_tokens)
        # предохранитель от модели, проигнорившей инструкцию про длину
        summary = tokens.truncate_text(summary, settings.summary_max_tokens)
    return summary or ""


async def summarize(r, conversation_id: str):
    # лёгкий замок именно на суммаризацию (не пересекается с замком ответа в llm_worker),
    # чтобы две задачи по одному диалогу не сворачивали одно и то же дважды.
    # Значение — уникальный токен владельца (CONSUMER_NAME), чтобы снять ТОЛЬКО свой
    # замок (см. release_lock, C2): свёртка большого хвоста несколькими LLM-вызовами
    # может превысить TTL, и тогда безусловный DEL снёс бы чужой замок.
    lock = f"lock:sum:{conversation_id}"
    if not await r.set(lock, CONSUMER_NAME, nx=True, ex=120):
        return
    try:
        old_summary, upto_id = await db.get_summary(conversation_id)

        # сообщения, ещё не вошедшие в summary (id > watermark). Ограничиваем заход
        # summary_max_fetch — защита от OOM, если суммаризатор далеко отстал.
        pending = await db.load_messages_since(
            conversation_id, upto_id, settings.summary_max_fetch)
        capped = len(pending) >= settings.summary_max_fetch
        # последние N оставляем дословно в окне — не сворачиваем
        to_fold = pending[:-settings.summary_recent_keep] if \
            len(pending) > settings.summary_recent_keep else []
        if not to_fold:
            return  # пока нечего сворачивать

        # КТО ходит к модели здесь: суммаризатор (нестриминговый вызов).
        # fold сам разобьёт большой хвост на чанки и ограничит длину резюме.
        new_summary = await fold(old_summary, to_fold)

        # двигаем watermark до id последнего свёрнутого сообщения
        new_upto_id = to_fold[-1]["id"]

        # ПЕРЕНОС ->cold: резюме в Postgres (source of truth)
        await db.save_summary(conversation_id, new_summary, new_upto_id)
        # обновляем горячий кэш резюме, чтобы llm_worker сразу видел свежее
        await r.set(keys.sum_key(conversation_id), new_summary,
                    ex=settings.ctx_ttl_seconds)

        # хвост был обрезан лимитом -> ещё есть что сворачивать: дозапускаем себя
        if capped:
            await r.xadd(settings.summarize_stream,
                         {"conversation_id": conversation_id},
                         maxlen=settings.summarize_stream_maxlen, approximate=True)
    finally:
        await release_lock(r, lock, CONSUMER_NAME)   # снять только свой замок (C2)


async def generate_title(r, conversation_id: str):
    """Авто-название диалога ОТДЕЛЬНЫМ LLM-запросом (не обрезка первого сообщения).
    Идемпотентно: set_conversation_title пишет только если title ещё NULL."""
    first = await db.get_first_user_message(conversation_id)
    if not first:
        return
    prompt = [
        {"role": "system",
         "content": "Придумай очень короткое название для диалога по первому сообщению "
                    "пользователя: 3–5 слов, одна строка, на языке пользователя, без "
                    "кавычек и без точки в конце."},
        {"role": "user", "content": first[:2000]},
    ]
    raw = (await complete(prompt, max_tokens=settings.title_max_tokens)).strip()
    title = raw.splitlines()[0].strip().strip('"').strip("«»").strip() if raw else ""
    await db.set_conversation_title(conversation_id, (title or "Новый чат")[:120])


async def handle(r, fields: dict):
    """Диспетчер задач стрима: title -> генерация названия, иначе -> суммаризация."""
    if fields.get("task") == "title":
        await generate_title(r, fields["conversation_id"])
    else:
        await summarize(r, fields["conversation_id"])


async def reclaim_stale(r):
    """Забрать у «мёртвого» суммаризатора задачи из PEL и доработать их (как в
    llm_worker). Без этого задача умершего процесса зависла бы в PEL навсегда."""
    try:
        res = await r.xautoclaim(
            settings.summarize_stream, settings.summarize_group, CONSUMER_NAME,
            min_idle_time=settings.reclaim_min_idle_ms, start_id="0-0", count=10,
        )
    except ResponseError:
        return
    entries = res[1] if isinstance(res, (list, tuple)) and len(res) > 1 else []
    for msg_id, fields in entries:
        if not fields:
            await r.xack(settings.summarize_stream, settings.summarize_group, msg_id)
            continue
        try:
            await handle(r, fields)
            await r.xack(settings.summarize_stream, settings.summarize_group, msg_id)
        except Exception as e:
            print("summarize reclaim error:", repr(e))


def _install_stop(stop: asyncio.Event):
    """SIGINT/SIGTERM -> мягкая остановка: дообработать текущую задачу и выйти."""
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass


async def main():
    await migrate()                 # схема БД актуальна (идемпотентно)
    r = get_redis()
    await ensure_group(r)
    stop = asyncio.Event()
    _install_stop(stop)
    print(f"summarizer started as {CONSUMER_NAME}, listening on", settings.summarize_stream)

    iters = 0
    while not stop.is_set():
        iters += 1
        if iters % settings.reclaim_every_iters == 0:
            await reclaim_stale(r)
        resp = await r.xreadgroup(
            settings.summarize_group, CONSUMER_NAME,
            {settings.summarize_stream: ">"}, count=1, block=5000,
        )
        if not resp:
            continue
        for _stream, entries in resp:
            for msg_id, fields in entries:
                try:
                    await handle(r, fields)
                    await r.xack(settings.summarize_stream,
                                 settings.summarize_group, msg_id)
                except Exception as e:
                    print("summarize error:", repr(e))

    print("summarizer stopping:", CONSUMER_NAME)


if __name__ == "__main__":
    asyncio.run(main())
