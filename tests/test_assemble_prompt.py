"""Сборка промпта по токен-бюджету (_assemble_prompt) — чистая CPU-функция, которую
воркер гоняет в потоке (H-2). Проверяем контроль длины (требование max context)."""
from app.config import settings
from app.workers import llm_worker as w


def test_selects_newest_within_budget(monkeypatch):
    # маленький бюджет: влезает только последнее короткое сообщение, длинное отсекается.
    monkeypatch.setattr(settings, "prompt_token_budget", 40)
    window = [
        {"role": "user", "content": "A" * 4000, "mid": "1"},      # много токенов
        {"role": "assistant", "content": "short", "mid": "1"},    # мало
    ]
    out = w._assemble_prompt(window, None)
    assert [m["content"] for m in out] == ["short"]
    # служебный `mid` НЕ должен утечь в LLM
    assert all("mid" not in m for m in out)


def test_summary_goes_first_as_system(monkeypatch):
    monkeypatch.setattr(settings, "prompt_token_budget", 500)
    window = [{"role": "user", "content": "hello", "mid": "1"}]
    out = w._assemble_prompt(window, "предыдущая часть диалога")
    assert out[0]["role"] == "system"
    assert "предыдущая часть диалога" in out[0]["content"]
    assert out[-1]["content"] == "hello"


def test_emergency_truncate_single_oversized_message(monkeypatch):
    # даже одно последнее сообщение не влезает -> берём его ОБРЕЗАННЫМ (не пустой промпт).
    monkeypatch.setattr(settings, "prompt_token_budget", 10)
    window = [{"role": "user", "content": "X" * 100000, "mid": "1"}]
    out = w._assemble_prompt(window, None)
    assert len(out) == 1
    assert 0 < len(out[0]["content"]) < 100000    # обрезано до бюджета
