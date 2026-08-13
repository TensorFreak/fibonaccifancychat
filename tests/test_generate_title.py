"""Регрессия: авто-название не должно записывать строку-заглушку «Новый чат».

Баг (подтверждён на проде): при пустом ответе LLM (частое для reasoning-моделей с малым
max_tokens — бюджет уходит в reasoning_content, content пустой) generate_title писал в БД
буквальную строку «Новый чат». Она непустая, поэтому:
  - в UI неотличима от «без названия» (фронт рисует title || 'Новый чат');
  - блокирует перегенерацию (set_conversation_title пишет лишь WHERE title IS NULL) и
    повторную постановку задачи (NX-маркер живёт сутки) — чат застревает НАВСЕГДА.

Фикс: при пустом ответе LLM берём осмысленное имя из первого сообщения пользователя.
"""
from app import db
from app.workers import summarizer as s


async def test_empty_llm_title_falls_back_to_first_message(monkeypatch):
    captured = {}

    async def fake_first(conversation_id):
        return "Расскажи подробно про Чехова и его пьесы"

    async def fake_complete(prompt, max_tokens=None):
        return "   "                                   # модель вернула пусто/пробелы

    async def fake_set(conversation_id, title):
        captured["title"] = title

    monkeypatch.setattr(db, "get_first_user_message", fake_first)
    monkeypatch.setattr(s, "complete", fake_complete)
    monkeypatch.setattr(db, "set_conversation_title", fake_set)

    await s.generate_title(None, "cid")

    assert captured["title"]                            # не пусто
    assert captured["title"] != "Новый чат"            # НЕ строка-заглушка
    assert "Чехов" in captured["title"]                # осмысленно, из вопроса юзера


async def test_llm_title_used_and_cleaned_when_present(monkeypatch):
    captured = {}

    async def fake_first(conversation_id):
        return "любой вопрос"

    async def fake_complete(prompt, max_tokens=None):
        return '«Чехов: жизнь и творчество»\nлишняя строка'   # кавычки + перевод строки

    async def fake_set(conversation_id, title):
        captured["title"] = title

    monkeypatch.setattr(db, "get_first_user_message", fake_first)
    monkeypatch.setattr(s, "complete", fake_complete)
    monkeypatch.setattr(db, "set_conversation_title", fake_set)

    await s.generate_title(None, "cid")

    assert captured["title"] == "Чехов: жизнь и творчество"   # первая строка, кавычки сняты


async def test_no_title_written_when_no_user_message(monkeypatch):
    """Если сообщений пользователя нет — вообще ничего не пишем (не «Новый чат»)."""
    called = {"set": False}

    async def fake_first(conversation_id):
        return None

    async def fake_set(conversation_id, title):
        called["set"] = True

    monkeypatch.setattr(db, "get_first_user_message", fake_first)
    monkeypatch.setattr(db, "set_conversation_title", fake_set)

    await s.generate_title(None, "cid")
    assert called["set"] is False
