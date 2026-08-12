"""Инварианты FIFO-гейта порядка (_order_gate) — то, что удерживает порядок ВНУТРИ
диалога, когда генерации разных диалогов идут параллельно (H-1)."""
import time

from app import keys
from app.config import settings
from app.workers import llm_worker as w

from .fakes import FakeRedis

CID = "11111111-1111-4111-8111-111111111111"


async def test_seq_zero_is_legacy_passthrough():
    r = FakeRedis()
    # seq=0 -> старый формат без порядка: применяем сразу.
    assert await w._order_gate(r, CID, 0, {}) is True


async def test_already_applied_is_skipped():
    r = FakeRedis()
    r.store[keys.conv_applied(CID)] = "4"
    # seq <= applied -> дубль/переигровка: пропустить.
    assert await w._order_gate(r, CID, 3, {}) is False
    assert await w._order_gate(r, CID, 4, {}) is False


async def test_next_in_order_passes():
    r = FakeRedis()
    r.store[keys.conv_applied(CID)] = "4"
    # seq == applied + 1 -> наша очередь.
    assert await w._order_gate(r, CID, 5, {}) is True


async def test_gap_requeues_and_waits():
    r = FakeRedis()
    r.store[keys.conv_applied(CID)] = "4"
    fields = {"conversation_id": CID, "seq": "7", "text": "hi"}
    # предшественник (5,6) ещё не применён -> вернуть в хвост и подождать.
    assert await w._order_gate(r, CID, 7, fields) is False
    assert len(r.xadds) == 1                      # ровно один requeue
    _stream, requeued = r.xadds[0]
    assert "gate_since" in requeued              # засекли начало ожидания
    # applied НЕ сдвинут (разрыв не потерян, просто ждём)
    assert r.store[keys.conv_applied(CID)] == "4"


async def test_gap_timeout_skips_lost_predecessor():
    r = FakeRedis()
    r.store[keys.conv_applied(CID)] = "4"
    stale = time.time() * 1000 - (settings.order_gap_timeout_ms + 5000)
    fields = {"conversation_id": CID, "seq": "7", "gate_since": str(stale)}
    # ждём дольше order_gap_timeout -> считаем 5,6 потерянными, перешагиваем разрыв.
    assert await w._order_gate(r, CID, 7, fields) is True
    # applied сдвинут до seq-1, чтобы 7 применилось как applied+1.
    assert r.store[keys.conv_applied(CID)] == "6"
    # Разрыв перешагнули, а не отложили: НЕ requeue'им в inbound-стрим. Единственный XADD —
    # аудит-событие потери хода в order_loss-ленту (C3), а не возврат задачи в очередь.
    assert not any(s == settings.inbound_stream for s, _ in r.xadds)
    assert [s for s, _ in r.xadds] == [keys.order_loss_stream(settings.inbound_stream)]
