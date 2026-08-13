"""Регрессия: потеря контекста после простоя диалога.

Горячее окно ctx:{id} живёт ctx_ttl_seconds (30 мин). После простоя оно протухает, и
следующий ход ДОЛЖЕН регидрироваться из Postgres (source of truth), а не начинаться с
чистого листа. Баг был в ПОРЯДКЕ шагов в process(): append_context клал текущее сообщение
в протухшее (пустое) окно ДО регидратации, load_context видел непустое окно и пропускал
загрузку истории -> в промпт уходило лишь текущее сообщение, вся прежняя история (хотя она
цела в Postgres) выпадала. Симптом: диалог «забывал всё» через полчаса паузы.

Фикс: load_context вызывается ПЕРВЫМ (шаг 0), пока окно ещё пусто.
"""
import pytest

from app import db
from app.workers import llm_worker as w
from tests.fakes import FakeRedis

# Прежняя история диалога, лежащая в Postgres (окно Redis протухло).
HISTORY = [
    {"role": "user", "content": "кто основал Anthropic?", "mid": "m1"},
    {"role": "assistant", "content": "Дарио и Даниэла Амодей", "mid": "m1"},
    {"role": "user", "content": "может быть братом и сестрой", "mid": "m2"},
    {"role": "assistant", "content": "да, брат и сестра", "mid": "m2"},
]


@pytest.fixture
def pg_history(monkeypatch):
    """Postgres отдаёт HISTORY; Redis-окно при этом пустое (симуляция простоя)."""
    async def fake_load_recent(conversation_id, limit):
        return list(HISTORY[-limit:])
    monkeypatch.setattr(db, "load_recent_messages", fake_load_recent)


async def test_cold_window_rehydrates_before_append(pg_history):
    """ФИКС: сначала регидратация (load_context), потом append текущего хода —
    прежняя история сохраняется, эхо пользователя не теряется."""
    r = FakeRedis()
    cid = "conv-fixed"

    # Порядок как в process() после фикса:
    await w.load_context(r, cid)                                   # шаг 0: прогреть из PG
    added = await w.append_context(
        r, cid, {"role": "user", "content": "а openai?", "mid": "m3"})

    assert added is True                                          # эхо user_message не подавлено
    window = await w.load_context(r, cid)
    contents = [m["content"] for m in window]
    assert "может быть братом и сестрой" in contents              # старый контекст на месте
    assert contents[-1] == "а openai?"                            # текущее — последним
    assert len(window) == len(HISTORY) + 1


async def test_append_before_rehydrate_loses_history(pg_history):
    """ГАРД ПОРЯДКА: старый (сломанный) порядок — append ДО регидратации — терял контекст.
    Тест фиксирует, ПОЧЕМУ порядок шагов в process() критичен: если его вернут назад,
    история снова пропадёт и этот тест упадёт."""
    r = FakeRedis()
    cid = "conv-broken"

    await w.append_context(                                        # положили в пустое окно
        r, cid, {"role": "user", "content": "а openai?", "mid": "m3"})
    window = await w.load_context(r, cid)                          # окно непусто -> НЕ регидрирует
    contents = [m["content"] for m in window]

    assert contents == ["а openai?"]                              # вся прежняя история потеряна
    assert "может быть братом и сестрой" not in contents


async def test_process_keeps_context_after_idle(pg_history, monkeypatch):
    """СКВОЗНОЙ гард самого process(): на холодном окне + истории в PG промпт, уходящий
    в LLM, ДОЛЖЕН содержать прежние сообщения. Если из process() убрать шаг-0 (load_context
    до append), этот тест упадёт — именно он ловит регрессию в реальном коде, а не только
    в примитивах."""
    r = FakeRedis()
    cid = "conv-proc"

    async def _true(*a, **k):
        return True

    async def _false(*a, **k):
        return False

    async def _get_summary(conversation_id):
        return (None, 0)

    monkeypatch.setattr(db, "insert_message", _true)
    monkeypatch.setattr(db, "assistant_exists", _false)
    monkeypatch.setattr(db, "get_summary", _get_summary)
    # db.load_recent_messages -> HISTORY уже из фикстуры pg_history

    captured = {}

    async def fake_stream(messages, max_tokens=None):
        captured["messages"] = messages
        yield "ответ"

    # мок LLM + обход owner-aware release (FakeRedis.eval эмулирует только ingest-скрипт)
    # и фонового heartbeat замка (он бы завис на sleep).
    monkeypatch.setattr(w, "stream_completion", fake_stream)

    async def _noop(*a, **k):
        return True

    monkeypatch.setattr(w, "release_lock", _noop)
    monkeypatch.setattr(w, "heartbeat_lock", _noop)

    # seq="0" -> гейт порядка пропускает сразу (if not seq: return True), без applied-логики
    await w.process(r, {"conversation_id": cid, "text": "а openai?",
                        "message_id": "m3", "seq": "0"})

    contents = [m["content"] for m in captured["messages"]]
    assert any("может быть братом и сестрой" in c for c in contents)   # контекст на месте
    assert contents[-1] == "а openai?"                                 # текущее — последним
