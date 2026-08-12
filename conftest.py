"""Общая настройка pytest.

Задаём env ДО импорта app.config: валидаторы fail-closed'ят старт в не-dev режиме при
слабом секрете (_guard_auth_secret) И при дефолтном пароле Postgres (_guard_postgres_password) —
это by design для прода, поэтому тестам нужны валидный AUTH_SECRET и не-дефолтный POSTGRES_DSN.
Ставим через setdefault, чтобы не перетирать заданное снаружи.

Файл лежит в КОРНЕ проекта: pytest добавит его каталог в sys.path -> `import app` работает.
"""
import os

os.environ.setdefault("AUTH_DEV_MODE", "false")
os.environ.setdefault("AUTH_SECRET", "test-secret-" + "z" * 40)
os.environ.setdefault(
    "POSTGRES_DSN", "postgresql://app:test-" + "z" * 20 + "@localhost:5432/chat")
