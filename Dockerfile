FROM python:3.12-slim
WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# Прекэшируем BPE-ранги tiktoken В ОБРАЗ, чтобы в проде НЕ было сетевого похода за ними
# ни на старте, ни (тем более) посреди генерации (H-2). Кладём в фикс. каталог и
# указываем его через ENV — tiktoken.warm_encoder на старте воркера найдёт готовый кэш.
# Тянем оба распространённых энкодера OpenAI (o200k_base — gpt-4o/-mini, cl100k_base —
# gpt-4/3.5 и фолбэк для неизвестных моделей).
ENV TIKTOKEN_CACHE_DIR=/opt/tiktoken_cache
RUN mkdir -p /opt/tiktoken_cache && python -c \
    "import tiktoken; tiktoken.get_encoding('o200k_base'); tiktoken.get_encoding('cl100k_base')"
COPY app ./app
COPY migrations ./migrations
# Не запускаем от root: создаём непривилегированного пользователя.
RUN useradd --create-home --uid 10001 appuser && chown -R appuser:appuser /srv
USER appuser
# Один образ обслуживает api, воркеры и раннер миграций — команду задаёт docker-compose.
