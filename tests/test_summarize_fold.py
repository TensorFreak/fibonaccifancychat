"""Регрессия: fold() не должен терять накопленное резюме, если модель вернула ПУСТО
на одном из чанков. Баг: `summary = await complete(...)` затирал summary пустой строкой,
и следующий чанк сворачивался поверх «(пусто)» — old_summary и уже свёрнутые чанки
пропадали, хотя watermark в summarize() всё равно двигался (сообщения считались свёрнутыми).

Фикс: пустой результат complete() пропускаем, оставляя прежнее summary."""
import pytest

from app.config import settings
from app.workers import summarizer as s


async def test_fold_keeps_summary_when_a_chunk_returns_empty(monkeypatch):
    # маленький потолок чанка -> каждое сообщение попадает в СВОЙ чанк (нужно >1 чанка)
    monkeypatch.setattr(settings, "summary_fold_chunk_tokens", 5)

    calls = {"n": 0}

    async def fake_complete(prompt, max_tokens=None):
        calls["n"] += 1
        # 1-й чанк -> нормальное резюме; 2-й -> ПУСТО (сбой/фильтр модели)
        return "резюме после первого чанка" if calls["n"] == 1 else ""

    monkeypatch.setattr(s, "complete", fake_complete)

    messages = [
        {"role": "user", "content": "первое сообщение про Anthropic"},
        {"role": "assistant", "content": "второе сообщение про Амодеев"},
    ]
    out = await s.fold("старое резюме", messages)

    assert calls["n"] == 2                          # оба чанка обработаны
    assert out == "резюме после первого чанка"      # пустой 2-й чанк НЕ обнулил накопленное


async def test_fold_uses_last_nonempty_result(monkeypatch):
    """Контроль: когда все ответы непустые — берём результат последнего чанка (обычный путь)."""
    monkeypatch.setattr(settings, "summary_fold_chunk_tokens", 5)

    seq = iter(["после чанка 1", "после чанка 2"])

    async def fake_complete(prompt, max_tokens=None):
        return next(seq)

    monkeypatch.setattr(s, "complete", fake_complete)

    messages = [
        {"role": "user", "content": "первое сообщение про Anthropic"},
        {"role": "assistant", "content": "второе сообщение про Амодеев"},
    ]
    out = await s.fold(None, messages)
    assert out == "после чанка 2"


async def test_fold_all_empty_returns_old_summary(monkeypatch):
    """Если модель отдаёт пусто на ВСЕХ чанках — сохраняем исходное old_summary,
    а не превращаем резюме в пустую строку."""
    monkeypatch.setattr(settings, "summary_fold_chunk_tokens", 5)

    async def fake_complete(prompt, max_tokens=None):
        return "   "                                # пусто/пробелы на каждом чанке

    monkeypatch.setattr(s, "complete", fake_complete)

    messages = [
        {"role": "user", "content": "первое сообщение про Anthropic"},
        {"role": "assistant", "content": "второе сообщение про Амодеев"},
    ]
    out = await s.fold("исходное резюме", messages)
    assert out == "исходное резюме"
