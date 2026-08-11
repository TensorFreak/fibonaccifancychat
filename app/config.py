"""Централизованная конфигурация. Читается из переменных окружения / .env.
Один и тот же объект settings импортируют И api, И воркеры — это общий код."""
from pydantic import model_validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # инфраструктура
    redis_url: str = "redis://localhost:6379/0"
    postgres_dsn: str = "postgresql://app:app@localhost:5432/chat"

    # внешний LLM API (OpenAI-совместимый по формату; подставь свой)
    llm_api_url: str = "https://api.openai.com/v1/chat/completions"
    llm_api_key: str = ""
    llm_model: str = "gpt-4o-mini"

    # горячий контекст в Redis
    ctx_ttl_seconds: int = 1800      # 30 мин неактивности -> контекст протухает
    # Размер скользящего окна. ВАЖНЫЙ ИНВАРИАНТ: окно должно вмещать ВСЁ ещё не
    # свёрнутое в summary, иначе между суммаризациями в контексте появляется дыра
    # (сообщения, уже выпавшие из окна, но ещё не попавшие в summary). Несвёрнутое
    # доходит до summary_recent_keep + summary_trigger_messages, поэтому окно должно
    # быть НЕ МЕНЬШЕ этой суммы (см. валидатор ниже). 40 = 20 keep + 20 trigger.
    ctx_max_messages: int = 40       # >= summary_recent_keep + summary_trigger_messages

    # resumable streams: сколько живёт лента токенов генерации (gen:{id}:{mid})
    # после завершения. В этом окне реконнект переиграет даже готовый ответ.
    gen_ttl_seconds: int = 300       # 5 мин
    # TTL ленты ВО ВРЕМЯ генерации (H1). Лента получает финальный gen_ttl только после
    # записи end; при ЖЁСТКОМ падении воркера (SIGKILL/OOM) до end лента осталась бы БЕЗ
    # TTL навсегда -> утечка Redis. Ставим TTL сразу при создании и периодически
    # продлеваем в цикле токенов, поэтому лента брошенной генерации сама протухнет через
    # gen_active_ttl_seconds после последнего токена. Держите заметно больше паузы между
    # токенами (llm_read_timeout), чтобы не протухнуть под живым, но медленным стримом.
    gen_active_ttl_seconds: int = 900   # 15 мин

    # --- КОНТРОЛЬ ДЛИНЫ (бюджет токенов) ---
    # Полное контекстное окно модели. Теперь это НЕ справка, а реальный потолок:
    # валидатор ниже гарантирует prompt_token_budget + max_response_tokens + запас
    # <= context_window_tokens. Поставьте под свою модель.
    context_window_tokens: int = 256000
    # Резерв под ответ ассистента -> уходит в max_tokens запроса к LLM.
    max_response_tokens: int = 4096
    # Потолок на ВЕСЬ промпт запроса (summary + горячее окно). Горячее окно
    # набирается с конца, пока укладывается в бюджет. Это ПОТОЛОК: фактический
    # промпт обычно меньше (его размер задаёт горячее окно ctx_max_messages).
    # Значение <= 0 означает «авто»: занять максимум окна модели за вычетом ответа
    # и запаса. Иначе зажимается валидатором, чтобы влезть в context_window_tokens.
    prompt_token_budget: int = 0
    # Доля context_window под запас на неточность подсчёта токенов (tiktoken !=
    # токенизатор произвольной модели). 5% для 256k = ~12.8k токенов.
    token_safety_ratio: float = 0.05
    # Потолок на объём summary (в токенах): и в промпте суммаризатора, и обрезкой.
    summary_max_tokens: int = 2000
    # Размер чанка при сворачивании большого накопившегося хвоста в summary.
    summary_fold_chunk_tokens: int = 8000

    # Redis Stream (очередь входящих сообщений)
    inbound_stream: str = "chat:inbound"
    consumer_group: str = "llm-workers"

    # Суммаризация (отдельная очередь и воркер)
    summarize_stream: str = "chat:summarize"
    summarize_group: str = "summarizers"
    summary_trigger_messages: int = 20   # каждые N новых сообщений — пересжать
    summary_recent_keep: int = 20        # столько последних НЕ сворачиваем (== окно)

    # --- БЕЗОПАСНОСТЬ / ЛИМИТЫ ---
    # Dev-режим авторизации: любой токен трактуется как личность пользователя
    # (см. app/auth.py) — то есть auth ФАКТИЧЕСКИ ВЫКЛЮЧЕН. Дефолт False (fail-closed):
    # включать ЯВНО и ТОЛЬКО для локального ручного теста. Не делаем True по умолчанию,
    # чтобы деплой «на дефолтах» не оказался с открытым auth. В проде — реализовать
    # проверку корпоративного SSO в app/auth.authenticate.
    auth_dev_mode: bool = False
    # Максимальная длина одного входящего сообщения (символов) — защита от DoS.
    max_message_chars: int = 8000

    # Рейтлимит входящих сообщений вебсокета (H2). Единый inbound-стрим общий на всех;
    # без лимита один клиент может залить его быстрее, чем воркеры разгребают, и
    # приблизительный MAXLEN начнёт выбрасывать ЕЩЁ НЕ ДОСТАВЛЕННЫЕ сообщения ДРУГИХ
    # пользователей (тихая потеря). Лимитируем per-user (токен -> user_id), окно —
    # фиксированное (INCR+EXPIRE, дёшево и работает между инстансами). Значения щедрые
    # для человека, но режут флуд.
    ws_rate_max_messages: int = 30       # сообщений за окно на пользователя
    ws_rate_window_seconds: int = 10     # длина окна (сек)

    # Максимальная длина пароля (bcrypt режет на 72 байтах и кидает на длинных).
    password_max_chars: int = 128

    # --- ВЕБ / JWT ---
    # Секрет подписи JWT. В ПРОДЕ ОБЯЗАТЕЛЬНО задать своё длинное случайное значение.
    auth_secret: str = "dev-insecure-change-me"
    # Срок жизни токена (сек). По умолчанию 7 дней.
    auth_token_ttl_seconds: int = 604800
    # Разрешённые CORS-origin через запятую ("*" — любой; для теста ок,
    # для прода сузить до вашего домена).
    cors_allow_origins: str = "*"

    # --- ПАГИНАЦИЯ (keyset-курсоры) ---
    messages_page_size: int = 40          # сообщений на страницу истории чата
    conversations_page_size: int = 30     # диалогов на страницу списка
    page_size_max: int = 100              # верхний предел limit из запроса

    # --- НАДЁЖНОСТЬ ОБРАБОТКИ ---
    # Реклейм зависших задач: заберём у «мёртвого» воркера всё, что висит в PEL
    # дольше этого простоя (мс). ВАЖНО: держите заметно больше максимального времени
    # генерации ответа, иначе долгий (напр. на большом контексте) ответ,
    # ещё висящий в PEL, будет ошибочно реклеймлен и сгенерирован повторно.
    reclaim_min_idle_ms: int = 120000
    # Как часто (раз в N итераций основного цикла) запускать реклейм PEL.
    reclaim_every_iters: int = 20

    # Приблизительный потолок длины стримов (XADD ... MAXLEN ~). Без него стрим
    # растёт БЕЗ ГРАНИЦ (XACK убирает из PEL, но не из стрима) -> Redis OOM.
    inbound_stream_maxlen: int = 100000
    summarize_stream_maxlen: int = 50000

    # Замок диалога. TTL должен быть БОЛЬШЕ максимального времени генерации, иначе он
    # истечёт на лету и другой воркер начнёт дублирующую обработку. Во время генерации
    # замок продлевается heartbeat'ом, поэтому TTL — это запас на паузу heartbeat.
    conv_lock_ttl_seconds: int = 300
    lock_heartbeat_seconds: int = 30

    # FIFO-гейт: если предшественник (seq-1) не пришёл дольше этого времени, считаем
    # его потерянным и пропускаем разрыв — иначе диалог завис бы навсегда, а сообщение
    # бесконечно кружило бы по очереди.
    # ВНИМАНИЕ (H3): «не пришёл» и «ещё не вычитан из-за backlog» неразличимы. Под
    # завалом стрима предшественник может просто ждать вычитки; если таймаут меньше
    # худшей задержки очереди, гейт пропустит разрыв, а опоздавшее РЕАЛЬНОЕ сообщение
    # потом отсеётся как seq<=applied (тихая потеря хода). Поэтому дефолт консервативный
    # (держите его БОЛЬШЕ p99 задержки «XADD -> вычитка воркером» под пиковой нагрузкой),
    # а каждый факт пропуска логируется (см. _order_gate) — это событие потери, следите
    # за ним в метриках/алертах.
    order_gap_timeout_ms: int = 120000

    # Суммаризация: максимум сообщений, вычитываемых за один заход (защита от OOM,
    # если суммаризатор далеко отстал). Если хвост длиннее — доработается следующей
    # задачей (сам себя дозапустит).
    summary_max_fetch: int = 5000

    # --- LLM: таймауты (иначе зависшее соединение блокирует воркер навсегда) ---
    llm_connect_timeout: float = 10.0
    llm_read_timeout: float = 120.0     # макс. пауза между токенами
    llm_write_timeout: float = 30.0

    # --- Авто-название диалога (отдельный LLM-запрос) ---
    title_max_tokens: int = 24          # название короткое
    # dedup: помечаем, что название уже запрошено, чтобы не дёргать LLM каждый ход.
    title_enqueue_ttl_seconds: int = 86400

    @model_validator(mode="after")
    def _enforce_window_covers_unsummarized(self):
        """Гарантия отсутствия дыры в контексте: окно >= keep + trigger.
        Если задали меньше — поднимаем окно до безопасного минимума (лучше чуть
        больше токенов, чем терять сообщения между суммаризациями)."""
        need = self.summary_recent_keep + self.summary_trigger_messages
        if self.ctx_max_messages < need:
            self.ctx_max_messages = need
        return self

    @model_validator(mode="after")
    def _fit_prompt_budget_to_context(self):
        """РЕАЛЬНАЯ гарантия max_context_length: промпт + ответ + запас укладываются
        в окно модели.

        ceiling = context_window_tokens - max_response_tokens - запас.
        - prompt_token_budget <= 0  -> «авто»: берём весь ceiling (использовать окно
          по максимуму — актуально для больших моделей вроде 256k);
        - prompt_token_budget > ceiling -> зажимаем до ceiling (не дать превысить окно);
        - иначе — оставляем как задано (осознанное ограничение ради стоимости/латентности).
        Запас (token_safety_ratio) компенсирует неточность подсчёта токенов для
        не-OpenAI моделей и офлайн-эвристики."""
        safety = int(self.context_window_tokens * self.token_safety_ratio)
        ceiling = self.context_window_tokens - self.max_response_tokens - safety
        ceiling = max(ceiling, 512)   # деградация, но не абсурд
        if self.prompt_token_budget <= 0 or self.prompt_token_budget > ceiling:
            self.prompt_token_budget = ceiling
        return self

    @model_validator(mode="after")
    def _guard_auth_secret(self):
        """Фейл-фаст против катастрофической опечатки конфига: в НЕ-dev режиме со
        СЛАБЫМ секретом любой смог бы подписать JWT на любого пользователя.
        Не даём подняться, пока секрет действительно не задан (C1).

        Отвергаем не только дефолт, но и известные плейсхолдеры из .env.example и
        слишком короткие значения — иначе оператор, скопировавший пример и забывший
        сгенерировать секрет, стартовал бы с публично известным ключом."""
        if self.auth_dev_mode:
            return self
        weak = {"dev-insecure-change-me", "change-me-to-a-long-random-secret"}
        if self.auth_secret in weak or len(self.auth_secret) < 32:
            raise ValueError(
                "AUTH_SECRET must be a long random value (>=32 chars) when "
                "AUTH_DEV_MODE=false. Generate one with: "
                "python -c \"import secrets; print(secrets.token_urlsafe(48))\"")
        return self

    class Config:
        env_file = ".env"


settings = Settings()
