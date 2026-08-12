"""Общая настройка pytest.

Задаём env ДО импорта app.config: валидатор _guard_auth_secret fail-closed'ит старт
при слабом секрете в не-dev режиме (это by design для прода), поэтому тестам нужен
валидный AUTH_SECRET. Ставим через setdefault, чтобы не перетирать заданный снаружи.

Файл лежит в КОРНЕ проекта: pytest добавит его каталог в sys.path -> `import app` работает.
"""
import os

os.environ.setdefault("AUTH_DEV_MODE", "false")
os.environ.setdefault("AUTH_SECRET", "test-secret-" + "z" * 40)
