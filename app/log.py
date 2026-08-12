"""Читаемое логирование для эксплуатации.

Единый однострочный человекочитаемый формат на все процессы (api, воркеры), в stdout —
чтобы удобно смотреть прямо на сервере: `docker compose logs -f`, `journalctl`, `tail`.
Это НЕ структурные JSON-логи под внешний сборщик, а простой текст «глазами».

Формат:  2026-08-12 14:23:01 INFO  [chat.llm_worker] текст сообщения
Уровень задаётся LOG_LEVEL (env) / settings.log_level, по умолчанию INFO.
"""
import logging
import sys

_CONFIGURED = False


def setup_logging(level: str = "INFO") -> None:
    """Идемпотентно настроить корневой логгер на читаемый формат в stdout.
    Вызывается один раз на старте каждого процесса (api lifespan / worker main)."""
    global _CONFIGURED
    if _CONFIGURED:
        return
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"))
    root = logging.getLogger()
    root.handlers[:] = [handler]          # наш единственный обработчик — предсказуемый вывод
    root.setLevel(level.upper())
    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    """Логгер компонента (напр. "chat.llm_worker")."""
    return logging.getLogger(name)
