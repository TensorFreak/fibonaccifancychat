"""Аутентификация: пароли (bcrypt) и токены (JWT).

Поток веб-входа: register/login проверяют пароль -> выдают JWT с sub=user_id.
Вебсокет и HTTP-эндпоинты передают этот токен; authenticate() его проверяет.

authenticate() СНАЧАЛА пробует разобрать токен как JWT (основной путь). Если это не
валидный JWT и включён auth_dev_mode — токен трактуется как личность (быстрый ручной
тест без формы). В проде: auth_dev_mode=False + заданный auth_secret.
"""
import asyncio
import time
import uuid

import bcrypt
import jwt

from .config import settings

_DEV_NS = uuid.UUID("c8a7bacc-0000-4000-8000-000000000000")
_ALG = "HS256"


# ---------- Пароли ----------
# bcrypt — синхронный и CPU-затратный (~100 мс). Гоняем в threadpool, чтобы не
# блокировать event loop api. Пароль обрезаем до 72 байт: bcrypt учитывает только их,
# а на более длинных вход некоторые версии кидают ValueError.

def _hash(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8")[:72], bcrypt.gensalt()).decode("utf-8")


def _verify(password: str, password_hash: str) -> bool:
    try:
        return bcrypt.checkpw(password.encode("utf-8")[:72],
                              password_hash.encode("utf-8"))
    except (ValueError, TypeError):
        return False


async def hash_password(password: str) -> str:
    return await asyncio.to_thread(_hash, password)


async def verify_password(password: str, password_hash: str) -> bool:
    return await asyncio.to_thread(_verify, password, password_hash)


# ---------- Токены (JWT) ----------

def make_token(user_id: str) -> str:
    now = int(time.time())
    payload = {"sub": str(user_id), "iat": now,
               "exp": now + settings.auth_token_ttl_seconds}
    return jwt.encode(payload, settings.auth_secret, algorithm=_ALG)


def _verify_jwt(token: str) -> str | None:
    try:
        payload = jwt.decode(token, settings.auth_secret, algorithms=[_ALG])
        sub = payload.get("sub")
        return str(sub) if sub else None
    except jwt.PyJWTError:
        return None


async def authenticate(token: str | None) -> str | None:
    """Токен -> user_id (UUID-строка) или None. Сначала JWT, затем dev-фолбэк."""
    if not token or not token.strip():
        return None
    uid = _verify_jwt(token)
    if uid:
        return uid
    if settings.auth_dev_mode:
        # DEV: стабильный UUID из строки токена (быстрый ручной тест без логина).
        return str(uuid.uuid5(_DEV_NS, token.strip()))
    return None
