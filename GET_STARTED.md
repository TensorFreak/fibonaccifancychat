# GET STARTED — запуск chat-backend

Пошаговый минимум, чтобы поднять проект. Подробности по компонентам — в [`docs/`](docs/README.md),
прод-готовность — в [`review.md`](review.md) и [`docs/10-hardening.md`](docs/10-hardening.md).

## 0. Требования

- **Docker** + **Docker Compose** (рекомендованный путь — всё поднимается одной командой).
- Ключ к внешнему LLM API (OpenAI-совместимый по формату).

## 1. Создать `.env`

```bash
cp .env.example .env
```

## 2. Заполнить обязательные переменные в `.env`

| Переменная | Что вписать |
|---|---|
| `LLM_API_KEY` | ваш ключ LLM API |
| `LLM_API_URL` / `LLM_MODEL` | эндпоинт и модель (по умолчанию OpenAI / `gpt-4o-mini`) |
| `AUTH_SECRET` | **длинный случайный секрет (≥32 символов)** — см. шаг 3 |
| `CONTEXT_WINDOW_TOKENS` | окно контекста ВАШЕЙ модели (напр. `128000`) |

> ⚠️ **Важно (fail-closed):** приложение **не стартует**, пока `AUTH_SECRET` не задан
> настоящим значением. По умолчанию `AUTH_DEV_MODE=false`, а старт-гард отвергает
> дефолтный/плейсхолдерный/короткий секрет. Это осознанная защита от деплоя «на дефолтах»
> с выключенной авторизацией. (Для ручных ws-тестов без формы входа можно временно
> поставить `AUTH_DEV_MODE=true` — тогда любой токен = личность. В проде — только `false`.)

## 3. Сгенерировать `AUTH_SECRET`

```bash
python -c "import secrets; print(secrets.token_urlsafe(48))"
```

Вставьте вывод в `.env` как `AUTH_SECRET=...`.

## 4. Поднять всё

```bash
docker compose up --build
```

Что произойдёт: поднимутся `postgres` и `redis`, одноразовый сервис `migrate` применит
миграции БД, затем запустятся `api` (FastAPI), `worker` (LLM-воркер) и `summarizer`.

## 5. Открыть в браузере

```
http://localhost:8000
```

Зарегистрируйтесь (любая почта, подтверждение не требуется; пароль от 6 символов) — и
попадёте в чат. Один аккаунт можно открыть с нескольких устройств, диалоги
синхронизируются.

Проверка живости API: `GET http://localhost:8000/healthz` → `{"ok": true}`.

## 6. Горизонтальное масштабирование

```bash
docker compose up --scale worker=4     # больше LLM-воркеров (одна consumer group)
```

Для нескольких инстансов `api` уберите фиксированный `ports: ["8000:8000"]` в
[`docker-compose.yml`](docker-compose.yml) и поставьте перед ними reverse-proxy
(Caddy/nginx), затем `--scale api=3`. Подробнее — [`docs/12-scaling.md`](docs/12-scaling.md).

## 7. Перед публичным продом (кратко)

- HTTPS-прокси перед `api`; `CORS_ALLOW_ORIGINS` сузить до вашего домена.
- Пароль на Redis; закрыть порты `5432`/`6379` наружу.
- Рейтлимит на `/api/login`·`/api/register` — на прокси (поток ws-сообщений уже лимитируется
  in-app, см. `WS_RATE_*`).
- `restart: unless-stopped`, сбор логов, метрики/алерты (в т.ч. на событие `[order][LOSS]`).
- `reclaim_min_idle_ms` и `conv_lock_ttl_seconds` держать больше максимального времени ответа модели.

Полный чек-лист и статус готовности — в [`review.md`](review.md).

---

### Локальный запуск без Docker (опционально)

Нужны запущенные Redis и Postgres, затем:

```bash
pip install -r requirements.txt
# в .env поставьте REDIS_URL / POSTGRES_DSN на localhost
python -m app.migrate                                   # применить миграции
uvicorn app.api.main:app --host 0.0.0.0 --port 8000     # терминал 1: API
python -m app.workers.llm_worker                        # терминал 2: LLM-воркер
python -m app.workers.summarizer                        # терминал 3: суммаризатор
```
