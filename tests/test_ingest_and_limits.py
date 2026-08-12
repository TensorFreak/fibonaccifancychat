"""Приём сообщений (#1) и anti-abuse рейтлимиты (#2 ws-коннекты, #3 auth).

Покрывает регрессии из прод-ревью:
  #1 — атомарный приём: seq и запись в inbound-стрим появляются ВМЕСТЕ (или никак),
       иначе выжженный seq заморозил бы диалог на order_gap_timeout;
  #2 — рейтлимит частоты ws-подключений per-user (защита Postgres от флуда коннектов);
  #3 — рейтлимит register/login per-IP (брутфорс пароля + CPU-DoS через bcrypt).
"""
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from app.api import main
from app.config import settings
from app import keys

from .fakes import FakeRedis

CID = "11111111-1111-4111-8111-111111111111"
UID = "22222222-2222-4222-8222-222222222222"


def _req(host: str):
    """Мини-заглушка Request: нужен только request.client.host (реальный IP клиента)."""
    return SimpleNamespace(client=SimpleNamespace(host=host))


# ---------- #1 Атомарный приём (INCR seq + XADD одним шагом) ----------

async def test_enqueue_produces_seq_and_stream_entry_together():
    r = FakeRedis()
    seq = await main._enqueue_inbound(r, CID, UID, "hello")
    # seq выделен и монотонный с 1
    assert seq == 1
    assert r.store[keys.conv_seq(CID)] == "1"
    # ровно одна запись — в inbound-стрим, с тем же seq и корректными полями
    assert len(r.xadds) == 1
    stream, fields = r.xadds[0]
    assert stream == settings.inbound_stream
    assert fields["seq"] == "1"
    assert fields["conversation_id"] == CID
    assert fields["user_id"] == UID
    assert fields["text"] == "hello"
    assert fields["message_id"]                       # message_id проставлен


async def test_enqueue_seq_is_monotonic():
    r = FakeRedis()
    assert await main._enqueue_inbound(r, CID, UID, "a") == 1
    assert await main._enqueue_inbound(r, CID, UID, "b") == 2
    assert [f["seq"] for _s, f in r.xadds] == ["1", "2"]


async def test_enqueue_failure_consumes_neither_seq_nor_stream():
    """Суть фикса #1: сбой приёма не оставляет «дыру» — ни seq, ни записи в стриме,
    поэтому следующий приём не выглядит как разрыв порядка."""
    class BrokenRedis(FakeRedis):
        async def eval(self, *a, **k):
            raise RuntimeError("redis down")

    r = BrokenRedis()
    with pytest.raises(RuntimeError):
        await main._enqueue_inbound(r, CID, UID, "hello")
    assert keys.conv_seq(CID) not in r.store          # seq НЕ израсходован
    assert r.xadds == []                              # запись НЕ добавлена


# ---------- #2 Рейтлимит частоты ws-подключений (per-user) ----------

async def test_ws_connect_rate_allows_up_to_cap_then_blocks(monkeypatch):
    monkeypatch.setattr(main.time, "time", lambda: 1000.0)   # фиксируем окно
    r = FakeRedis()
    for _ in range(settings.ws_connect_rate_max):
        assert await main._allow_ws_connect(r, UID) is True
    assert await main._allow_ws_connect(r, UID) is False     # cap+1 -> отказ


async def test_ws_connect_rate_resets_next_window(monkeypatch):
    now = [1000.0]
    monkeypatch.setattr(main.time, "time", lambda: now[0])
    r = FakeRedis()
    for _ in range(settings.ws_connect_rate_max):
        await main._allow_ws_connect(r, UID)
    assert await main._allow_ws_connect(r, UID) is False
    now[0] += settings.ws_connect_rate_window_seconds        # новое окно
    assert await main._allow_ws_connect(r, UID) is True


async def test_ws_connect_rate_is_per_user(monkeypatch):
    monkeypatch.setattr(main.time, "time", lambda: 1000.0)
    r = FakeRedis()
    for _ in range(settings.ws_connect_rate_max):
        await main._allow_ws_connect(r, "user-a")
    assert await main._allow_ws_connect(r, "user-a") is False
    assert await main._allow_ws_connect(r, "user-b") is True  # чужой лимит не задет


# ---------- #3 Рейтлимит попыток auth (per-IP, per-action) ----------

async def test_auth_rate_raises_429_after_cap(monkeypatch):
    monkeypatch.setattr(main.time, "time", lambda: 1000.0)
    r = FakeRedis()
    monkeypatch.setattr(main, "get_redis", lambda: r)
    req = _req("1.2.3.4")
    for _ in range(settings.auth_rate_max):
        await main._rate_limit_auth(req, "login")            # в пределах лимита — ок
    with pytest.raises(HTTPException) as ei:
        await main._rate_limit_auth(req, "login")
    assert ei.value.status_code == 429


async def test_auth_rate_is_per_ip_and_per_action(monkeypatch):
    monkeypatch.setattr(main.time, "time", lambda: 1000.0)
    r = FakeRedis()
    monkeypatch.setattr(main, "get_redis", lambda: r)
    for _ in range(settings.auth_rate_max):
        await main._rate_limit_auth(_req("1.1.1.1"), "login")
    # тот же IP, то же действие — заблокировано
    with pytest.raises(HTTPException):
        await main._rate_limit_auth(_req("1.1.1.1"), "login")
    # другой IP — свой счётчик
    await main._rate_limit_auth(_req("2.2.2.2"), "login")
    # тот же IP, но другое действие (register) — отдельный счётчик
    await main._rate_limit_auth(_req("1.1.1.1"), "register")


async def test_auth_rate_fail_open_on_redis_error(monkeypatch):
    """Лимит — защита, а не критичный путь: моргание Redis НЕ должно ронять вход."""
    class BrokenRedis(FakeRedis):
        async def incr(self, key):
            raise RuntimeError("redis down")

    monkeypatch.setattr(main, "get_redis", lambda: BrokenRedis())
    req = _req("1.2.3.4")
    # даже сверх лимита не бросает исключений (fail-open)
    for _ in range(settings.auth_rate_max + 5):
        await main._rate_limit_auth(req, "login")
