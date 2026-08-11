FROM python:3.12-slim
WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
COPY migrations ./migrations
# Не запускаем от root: создаём непривилегированного пользователя.
RUN useradd --create-home --uid 10001 appuser && chown -R appuser:appuser /srv
USER appuser
# Один образ обслуживает api, воркеры и раннер миграций — команду задаёт docker-compose.
