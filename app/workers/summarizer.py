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
import re
import signal
import socket
import uuid

from redis.exceptions import ResponseError

from ..config import settings
from ..redis_client import get_redis
from ..migrate import migrate
from .. import keys, db, tokens
from ..locks import release_lock, heartbeat_lock
from ..deadletter import times_delivered, dead_letter
from ..log import setup_logging, get_logger
from ..llm.client import complete

log = get_logger("chat.summarizer")

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
    # tiktoken синхронный/CPU-затратный -> считаем в потоке, чтобы не блокировать
    # event loop суммаризатора (H-2, как в llm_worker).
    chunks = await asyncio.to_thread(
        tokens.chunk_messages, messages, settings.summary_fold_chunk_tokens)
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
        result = await complete(prompt, max_tokens=settings.summary_max_tokens)
        # Пустой ответ модели (сбой/фильтр/пустая генерация) НЕ затираем накопленным.
        # Иначе следующий чанк свернулся бы поверх «(пусто)», а old_summary и уже свёрнутые
        # ранее чанки пропали бы — при том что watermark в summarize() всё равно сдвинется
        # (сообщения считались бы свёрнутыми). Пропускаем проблемный чанк, сохраняя прежнее
        # summary: деградация локальна (потеря одного чанка), а не тотальна (потеря всего).
        if result and result.strip():
            # предохранитель от модели, проигнорившей инструкцию про длину
            summary = await asyncio.to_thread(
                tokens.truncate_text, result, settings.summary_max_tokens)
    return summary or ""


async def summarize(r, conversation_id: str):
    # лёгкий замок именно на суммаризацию (не пересекается с замком ответа в llm_worker),
    # чтобы две задачи по одному диалогу не сворачивали одно и то же дважды.
    # Значение — уникальный токен владельца (CONSUMER_NAME), чтобы снять ТОЛЬКО свой
    # замок (см. release_lock, C2). Свёртка большого хвоста идёт несколькими LLM-вызовами
    # и может быть долгой, поэтому замок продлеваем heartbeat'ом (как в llm_worker): без
    # этого он истёк бы на лету, и второй суммаризатор дублировал бы работу (лишний LLM).
    lock = f"lock:sum:{conversation_id}"
    if not await r.set(lock, CONSUMER_NAME, nx=True,
                       ex=settings.summarize_lock_ttl_seconds):
        return
    hb = asyncio.create_task(heartbeat_lock(
        r, lock, CONSUMER_NAME,
        settings.summarize_lock_ttl_seconds, settings.lock_heartbeat_seconds))
    try:
        old_summary, upto_id = await db.get_summary(conversation_id)

        # сообщения, ещё не вошедшие в summary (id > watermark). Ограничиваем заход
        # summary_max_fetch — защита от OOM, если суммаризатор далеко отстал.
        pending = await db.load_messages_since(
            conversation_id, upto_id, settings.summary_max_fetch)
        capped = len(pending) >= settings.summary_max_fetch

        # АЛЕРТ на «дыру» в контексте (H2). ИНВАРИАНТ отсутствия дыры: горячее окно
        # (ctx_max_messages) должно вмещать ВСЁ ещё не свёрнутое в summary. Если
        # несвёрнутый хвост стал ДЛИННЕЕ окна — часть сообщений уже выпала из окна, но
        # ещё не попала в summary, и промпт между суммаризациями теряет середину диалога.
        # В норме since_sum-триггер держит хвост ~= окну; длинный хвост = суммаризатор
        # отставал/падал (в т.ч. dead-letter). Свёртка ниже это ВЫЛЕЧИТ (сдвинет
        # watermark), но факт отставания фиксируем в durable-аудит для алерта — иначе
        # деградация контекста была бы молчаливой.
        if len(pending) > settings.ctx_max_messages:
            log.warning("[summary][LAG] conv=%s: %d unsummarized messages exceed hot "
                        "window (%d) — context hole possible until fold catches up",
                        conversation_id, len(pending), settings.ctx_max_messages)
            try:
                await r.xadd(keys.summary_lag_stream(settings.summarize_stream),
                             {"conversation_id": conversation_id,
                              "pending": str(len(pending)),
                              "window": str(settings.ctx_max_messages)},
                             maxlen=settings.dead_letter_maxlen, approximate=True)
            except Exception as e:                   # аудит не должен ломать суммаризацию
                log.warning("summary lag audit xadd failed conv=%s: %r",
                            conversation_id, e)

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
        hb.cancel()                                  # остановить продление до снятия
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
    # Срезаем reasoning-разметку, если гибридная модель (напр. deepseek-v4) встроила «мысли»
    # в content: заголовок идёт ПОСЛЕ <think>…</think>. Закрытые блоки вырезаем; висящий
    # незакрытый (ответ обрезан по лимиту токенов) отбрасываем до конца строки.
    cleaned = re.sub(r"<think.*?</think>", "", raw, flags=re.DOTALL | re.IGNORECASE)
    cleaned = re.sub(r"<think.*", "", cleaned, flags=re.DOTALL | re.IGNORECASE).strip()
    lines = [ln.strip() for ln in cleaned.splitlines() if ln.strip()]
    title = lines[0].strip('"').strip("«»").strip() if lines else ""
    # ФОЛБЭК, если модель не дала заголовок (частая причина — reasoning съел весь бюджет
    # токенов, видимый content пуст). НЕ пишем строку-заглушку «Новый чат»: в UI она
    # неотличима от «без названия», но НЕПУСТАЯ — поэтому навсегда блокирует и перегенерацию
    # (set_conversation_title пишет лишь WHERE title IS NULL), и повторную постановку задачи
    # (NX-маркер живёт сутки). Логируем СЫРОЙ ответ модели, чтобы было видно, что она вернула
    # (пусто -> мало токенов на titul; текст -> проблема парсинга), и берём осмысленное имя
    # из первого сообщения пользователя.
    if not title:
        log.warning("empty title conv=%s, raw LLM output=%r", conversation_id, raw)
        title = " ".join(first.split())[:60]
    await db.set_conversation_title(conversation_id, title[:120])


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
        # DEAD-LETTER: постоянно падающую задачу снимаем с очереди после лимита доставок.
        if await times_delivered(r, settings.summarize_stream, settings.summarize_group,
                                 msg_id) > settings.max_deliveries:
            await dead_letter(r, settings.summarize_stream, settings.summarize_group,
                              msg_id, fields, settings.dead_letter_maxlen)
            continue
        try:
            await handle(r, fields)
            await r.xack(settings.summarize_stream, settings.summarize_group, msg_id)
        except Exception:
            log.exception("summarize reclaim error msg_id=%s", msg_id)


def _install_stop(stop: asyncio.Event):
    """SIGINT/SIGTERM -> мягкая остановка: дообработать текущую задачу и выйти."""
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass


async def main():
    setup_logging(settings.log_level)
    await migrate()                 # схема БД актуальна (идемпотентно)
    # Прогреваем tiktoken на старте, вне горячего пути (H-2).
    await asyncio.to_thread(tokens.warm_encoder)
    r = get_redis()
    await ensure_group(r)
    stop = asyncio.Event()
    _install_stop(stop)
    log.info("summarizer started as %s, listening on %s",
             CONSUMER_NAME, settings.summarize_stream)

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
                except Exception:
                    log.exception("summarize error msg_id=%s", msg_id)

    log.info("summarizer stopping: %s", CONSUMER_NAME)


if __name__ == "__main__":
    asyncio.run(main())
