"""Учёт фоновых задач воркера (H-1): _dispatch кладёт задачу в inflight, а по её
завершении callback её оттуда убирает. Это тот самый счётчик ёмкости, по которому
главный цикл решает, сколько сообщений вычитывать из стрима."""
import asyncio

from app.workers import llm_worker as w

from .fakes import FakeRedis


async def test_dispatch_tracks_and_acks(monkeypatch):
    started = asyncio.Event()
    release = asyncio.Event()

    async def fake_process(r, fields):
        started.set()
        await release.wait()

    monkeypatch.setattr(w, "process", fake_process)
    r = FakeRedis()
    inflight: set = set()

    w._dispatch(r, "10-0", {"conversation_id": "c"}, inflight)
    assert len(inflight) == 1                     # слот занят сразу

    await started.wait()
    assert len(inflight) == 1                     # пока process не завершился — держим слот

    release.set()
    async with asyncio.timeout(2):
        while inflight:
            await asyncio.sleep(0.01)
    assert len(inflight) == 0                     # слот освобождён по завершении
    assert r.acked == ["10-0"]                    # успех -> XACK


async def test_dispatch_failure_does_not_ack(monkeypatch):
    async def boom(r, fields):
        raise RuntimeError("llm down")

    monkeypatch.setattr(w, "process", boom)
    r = FakeRedis()
    inflight: set = set()

    w._dispatch(r, "11-0", {"conversation_id": "c"}, inflight)
    async with asyncio.timeout(2):
        while inflight:
            await asyncio.sleep(0.01)
    # при ошибке НЕ ackаем -> сообщение останется в PEL и будет переобработано.
    assert r.acked == []
