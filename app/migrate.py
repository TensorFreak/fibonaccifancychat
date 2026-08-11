"""Мини-раннер миграций БД.

Применяет неприменённые SQL-файлы из каталога migrations/ строго по имени (0001, 0002, …),
каждый — в своей транзакции. Отслеживает применённое в таблице schema_migrations.

Безопасен при параллельном старте нескольких процессов: перед работой берёт
pg_advisory_lock, поэтому мигрирует ровно один процесс, остальные ждут и видят,
что всё уже применено. Идемпотентен: повторный запуск — no-op.

Запуск: `python -m app.migrate`. Также вызывается при старте api и воркеров.
"""
import asyncio
from pathlib import Path

import asyncpg

from .config import settings

_MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"
# Произвольный фиксированный ключ, чтобы сериализовать мигрирующие процессы.
_ADVISORY_LOCK_KEY = 728912


async def _connect_with_retry(attempts: int = 30, delay: float = 1.0):
    """Postgres при старте контейнера может быть ещё не готов — подождём."""
    last_err = None
    for _ in range(attempts):
        try:
            return await asyncpg.connect(settings.postgres_dsn)
        except (OSError, asyncpg.PostgresError) as e:
            last_err = e
            await asyncio.sleep(delay)
    raise last_err


async def migrate() -> list[str]:
    """Применяет все неприменённые миграции. Возвращает список применённых версий."""
    conn = await _connect_with_retry()
    applied_now: list[str] = []
    try:
        await conn.execute("SELECT pg_advisory_lock($1)", _ADVISORY_LOCK_KEY)
        try:
            await conn.execute(
                """CREATE TABLE IF NOT EXISTS schema_migrations (
                       version    TEXT PRIMARY KEY,
                       applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
                   )""")
            done = {r["version"] for r in
                    await conn.fetch("SELECT version FROM schema_migrations")}

            for path in sorted(_MIGRATIONS_DIR.glob("*.sql")):
                version = path.stem                      # напр. "0001_init"
                if version in done:
                    continue
                sql = path.read_text(encoding="utf-8")
                async with conn.transaction():           # DDL в Postgres транзакционен
                    await conn.execute(sql)
                    await conn.execute(
                        "INSERT INTO schema_migrations (version) VALUES ($1)", version)
                applied_now.append(version)
                print(f"[migrate] applied {version}")

            print("[migrate] up to date" if not applied_now
                  else f"[migrate] {len(applied_now)} migration(s) applied")
        finally:
            await conn.execute("SELECT pg_advisory_unlock($1)", _ADVISORY_LOCK_KEY)
    finally:
        await conn.close()
    return applied_now


if __name__ == "__main__":
    asyncio.run(migrate())
