#!/usr/bin/env bash
# test_local_build.sh — локальный запуск всего стека chat-backend на одной машине
# (в т.ч. Mac Air M1: все образы мультиарх, arm64 нативно).
#
# Что делает:
#   1) генерирует секреты (AUTH_SECRET и сильный пароль Postgres) и пишет их в .env;
#   2) синхронно прописывает пароль в POSTGRES_DSN (приложение читает DSN);
#   3) поднимает docker compose, ждёт готовности и печатает URL для входа.
#
# Использование:
#   ./test_local_build.sh                       # локально: свежий .env + чистый старт (http://localhost)
#   ./test_local_build.sh --fresh               # пересоздать секреты И стереть тома БД
#   LLM_API_KEY=sk-... ./test_local_build.sh    # сразу подставить ключ LLM (без вопроса)
#
# ПУБЛИЧНЫЙ доступ (коллеги заходят с других машин):
#   ./test_local_build.sh --public chat.example.com   # свой домен → HTTPS (Let's Encrypt)
#   ./test_local_build.sh --public-ip 203.0.113.5     # БЕЗ домена → HTTPS через <ip>.sslip.io
#   (просто HTTP по IP без шифрования: не задавайте флаг, оставьте SITE_ADDRESS=:80 и
#    откройте порт 80 — но пароли/токены пойдут открытым текстом, только для «на 5 минут»)
# Флаги комбинируются: ./test_local_build.sh --fresh --public-ip 203.0.113.5
# ВАЖНО (сервер): порты 80 и 443 нужно открыть в firewall/security-group облака — этого
# скрипт сделать не может (зависит от провайдера).
#
# Для OpenAI-СОВМЕСТИМОГО провайдера (не api.openai.com) задайте ещё эндпоинт и модель:
#   LLM_API_KEY=... \
#   LLM_API_URL=https://ваш-провайдер/v1/chat/completions \
#   LLM_MODEL=имя-модели \
#   ./test_local_build.sh
# ВНИМАНИЕ: LLM_API_URL — это ПОЛНЫЙ URL эндпоинта (с путём /v1/chat/completions),
# а НЕ «base_url» — приложение шлёт POST прямо на него (см. app/llm/client.py).
#
# Повторный запуск БЕЗ --fresh переиспользует уже созданный .env (пароль БД не меняется —
# это важно: том Postgres инициализируется паролем ОДИН раз, при первом старте).
set -euo pipefail
cd "$(dirname "$0")"

# --- выбрать команду compose (Docker Desktop = "docker compose") ---
if docker compose version >/dev/null 2>&1; then
  DC="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
  DC="docker-compose"
else
  echo "ОШИБКА: нужен Docker. Установите Docker Desktop для Apple Silicon и запустите его." >&2
  exit 1
fi
docker info >/dev/null 2>&1 || { echo "ОШИБКА: демон Docker не запущен — откройте Docker Desktop." >&2; exit 1; }

FRESH=0
SITE_OVERRIDE=""            # если задан --public/--public-ip → пропишем в SITE_ADDRESS
while [[ $# -gt 0 ]]; do
  case "$1" in
    --fresh) FRESH=1; shift ;;
    --public)
      [[ -n "${2:-}" ]] || { echo "ОШИБКА: --public требует хост, напр. --public chat.example.com" >&2; exit 1; }
      SITE_OVERRIDE="$2"; shift 2 ;;
    --public-ip)
      [[ -n "${2:-}" ]] || { echo "ОШИБКА: --public-ip требует IPv4, напр. --public-ip 203.0.113.5" >&2; exit 1; }
      [[ "$2" =~ ^[0-9]{1,3}(\.[0-9]{1,3}){3}$ ]] || { echo "ОШИБКА: '$2' не похоже на IPv4-адрес" >&2; exit 1; }
      SITE_OVERRIDE="$2.sslip.io"; shift 2 ;;   # sslip.io резолвит <ip>.sslip.io → <ip> → реальный TLS
    *) echo "ОШИБКА: неизвестный аргумент '$1' (см. шапку скрипта)" >&2; exit 1 ;;
  esac
done

gen() { openssl rand -hex "$1"; }        # hex — URL-safe, годится и для DSN, и для секрета

# Заменить или добавить KEY=VALUE в .env. Разделитель sed — '|' (в hex/DSN его нет).
set_kv() {
  local key="$1" val="$2"
  if grep -qE "^${key}=" .env; then
    sed -i.bak "s|^${key}=.*|${key}=${val}|" .env && rm -f .env.bak
  else
    printf '%s=%s\n' "$key" "$val" >> .env
  fi
}

need_secrets=0
if [[ ! -f .env || $FRESH -eq 1 ]]; then
  need_secrets=1
  cp .env.example .env
  echo "→ создан свежий .env из .env.example"
else
  echo "→ .env уже есть — переиспользую его секреты (для пересоздания: ./test_local_build.sh --fresh)"
fi

if [[ $need_secrets -eq 1 ]]; then
  PGPASS="$(gen 24)"                     # 48 hex-символов
  SECRET="$(gen 32)"                     # 64 hex-символа (>= 32, проходит гард)
  set_kv POSTGRES_PASSWORD "$PGPASS"
  set_kv POSTGRES_DSN "postgresql://app:${PGPASS}@postgres:5432/chat"
  set_kv AUTH_SECRET "$SECRET"
  set_kv AUTH_DEV_MODE "false"           # реальный вход email+пароль (не dev-обход)
  echo "→ сгенерированы AUTH_SECRET и пароль Postgres, записаны в .env"

  # Ключ LLM: из переменной окружения, иначе спросить (можно оставить пустым)
  KEY="${LLM_API_KEY:-}"
  if [[ -z "$KEY" ]]; then
    read -r -p "   LLM_API_KEY (Enter — пропустить, генерация ответов не заработает): " KEY || true
  fi
  [[ -n "$KEY" ]] && set_kv LLM_API_KEY "$KEY"

  # Эндпоинт и модель LLM: для OpenAI-совместимого провайдера (не api.openai.com) задайте
  # их через окружение. LLM_API_URL — ПОЛНЫЙ URL (с /v1/chat/completions), не «base_url».
  # Если не заданы — остаются дефолты из .env.example (OpenAI, gpt-4o-mini).
  [[ -n "${LLM_API_URL:-}" ]] && set_kv LLM_API_URL "$LLM_API_URL"
  [[ -n "${LLM_MODEL:-}"   ]] && set_kv LLM_MODEL   "$LLM_MODEL"
fi

# Публичный адрес: прописываем SITE_ADDRESS всегда, когда задан флаг (в т.ч. при повторном
# запуске без --fresh — это осознанный override). Caddy по нему сам выпустит TLS-сертификат.
if [[ -n "$SITE_OVERRIDE" ]]; then
  set_kv SITE_ADDRESS "$SITE_OVERRIDE"
  echo "→ SITE_ADDRESS=${SITE_OVERRIDE} — Caddy выпустит TLS (нужны открытые порты 80 и 443)"
fi

# Предупреждение, если ключ LLM так и не задан (регистрация/логин работают всё равно)
if grep -qE '^LLM_API_KEY=(sk-\.\.\.)?$' .env; then
  echo "⚠  LLM_API_KEY не задан: регистрация и вход работать будут, но ответы модели — нет."
  echo "   Впишите ключ в .env (LLM_API_KEY=...) и перезапустите скрипт, когда нужна генерация."
fi

# Свежие секреты → стереть тома, чтобы Postgres переинициализировался под НОВЫЙ пароль
# (иначе старый том остался бы со старым паролем и БД не пустила бы приложение).
if [[ $need_secrets -eq 1 ]]; then
  echo "→ очищаю старые тома (down -v), чтобы БД поднялась под новым паролем…"
  $DC down -v --remove-orphans >/dev/null 2>&1 || true
fi

echo "→ сборка и запуск сервисов (первый раз может занять пару минут)…"
$DC up --build -d

# Ждём готовности ВНУТРИ контейнера api (healthz=200 => Redis+Postgres живы). Проверяем не
# через caddy: при публичном SITE_ADDRESS caddy отдаёт только этот хост и редиректит на HTTPS,
# поэтому http://localhost его бы не прошёл. Заходим python-ом внутрь api к localhost:8000.
echo -n "→ жду готовности сервисов "
ready=0
for i in $(seq 1 90); do
  if $DC exec -T api python -c \
      "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:8000/healthz',timeout=3).getcode()==200 else 1)" \
      >/dev/null 2>&1; then
    ready=1; echo " ✓"; break
  fi
  echo -n "."
  sleep 2
done
if [[ $ready -ne 1 ]]; then
  echo
  echo "⚠  Не дождался готовности за ~180с. Часто причина — ещё идёт сборка, занят порт 80/443"
  echo "   или упал api из-за конфига. Логи:  $DC logs -f"
  exit 1
fi

LLM_URL_EFF="$(grep -E '^LLM_API_URL=' .env | head -1 | cut -d= -f2-)"
LLM_MODEL_EFF="$(grep -E '^LLM_MODEL=' .env | head -1 | cut -d= -f2-)"
SITE_EFF="$(grep -E '^SITE_ADDRESS=' .env | head -1 | cut -d= -f2-)"

# URL для показа: ":80"/пусто → локальный HTTP; иначе публичный хост по HTTPS.
if [[ -z "$SITE_EFF" || "$SITE_EFF" == ":80" || "$SITE_EFF" == :* ]]; then
  URL="http://localhost"
  ACCESS_NOTE=" Другие в той же сети: http://<IP-этого-сервера>/  (откройте порт 80; без TLS —
   пароли/токены идут открытым текстом, только для короткого показа своим)."
else
  URL="https://${SITE_EFF}"
  ACCESS_NOTE=" Коллеги заходят по этому HTTPS-адресу. Сертификат Caddy выпустит сам при первом
   обращении (пара секунд). Нужны открытые в облаке порты 80 И 443."
fi

cat <<EOF

======================================================================
 Готово. Откройте в браузере:   ${URL}/
${ACCESS_NOTE}

 LLM-эндпоинт: ${LLM_URL_EFF}
 LLM-модель:   ${LLM_MODEL_EFF}
 (для OpenAI-совместимого провайдера задайте LLM_API_URL/LLM_MODEL — см. шапку скрипта)

 РАЗНЫЕ ПОЛЬЗОВАТЕЛИ с одной машины — да, легко:
   • токен хранится в localStorage КАЖДОГО браузера/окна отдельно;
   • зарегистрируйте разные email в разных браузерах (Chrome / Safari)
     ИЛИ в обычном и приватном/incognito окне одного браузера — это будут
     независимые пользователи, каждый со своими диалогами;
   • у одного пользователя можно открыть несколько вкладок/устройств —
     сообщения синхронизируются между ними (durable-события + resumable-стрим).

 Логи всех сервисов:   $DC logs -f
 Остановить (данные сохранятся):   $DC down
 Снести всё вместе с БД/историей:   $DC down -v
======================================================================
EOF
