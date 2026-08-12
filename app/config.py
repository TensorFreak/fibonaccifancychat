"""Централизованная конфигурация. Читается из переменных окружения / .env.
Один и тот же объект settings импортируют И api, И воркеры — это общий код."""
from pydantic import model_validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # --- ЛОГИ / ЭКСПЛУАТАЦИЯ ---
    # Уровень логов (DEBUG/INFO/WARNING/ERROR). Формат — читаемый однострочный текст в
    # stdout (см. app/log.py): удобно смотреть на сервере `docker compose logs -f`.
    log_level: str = "INFO"

    # инфраструктура
    redis_url: str = "redis://localhost:6379/0"
    postgres_dsn: str = "postgresql://app:app@localhost:5432/chat"
    # Таймауты Postgres (M3): без них зависший/медленный запрос держит соединение из пула
    # бесконечно; несколько таких — и пул (max_size=10) исчерпан, процесс встал.
    # command_timeout — клиентская отмена запроса (сек); statement_timeout — серверный
    # предел на запрос (мс), отменяет его на стороне БД. Пул общий для api/воркеров;
    # раннер миграций идёт по ОТДЕЛЬНОМУ соединению (migrate.py) и под этот лимит не
    # попадает (длинный DDL не оборвётся). Держите оба > самого долгого штатного запроса.
    pg_command_timeout: float = 30.0
    pg_statement_timeout_ms: int = 30000
    # Размер пула соединений Postgres (на КАЖДЫЙ процесс api/воркера). Соединение берётся
    # на один запрос и сразу возвращается (во время генерации LLM оно НЕ удерживается),
    # поэтому пул должен покрывать пик ОДНОВРЕМЕННЫХ запросов = worker_concurrency (H1).
    # Валидатор требует pg_pool_max_size >= worker_concurrency. ВНИМАНИЕ при scale:
    # суммарно к БД = pg_pool_max_size * (число процессов api+воркеров) — держите ниже
    # max_connections Postgres (по умолчанию 100).
    pg_pool_min_size: int = 1
    pg_pool_max_size: int = 32

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

    # TTL счётчиков порядка диалога seq/applied/since_sum (M1). Без него эти ключи копятся
    # вечно — по одному триплету на КАЖДЫЙ диалог за всю жизнь сервиса -> медленный рост
    # памяти Redis без эвикции. Обновляются на активности диалога; после простоя протухают
    # ВМЕСТЕ (воркер продлевает seq не раньше applied — см. _mark_applied), поэтому
    # «остывший» диалог просто начнёт счёт заново с нуля — это БЕЗОПАСНО: seq/applied —
    # чисто redis-токены порядка, не связаны с id сообщений в Postgres. Держите заметно
    # больше времени обработки/бэклога и order_gap_timeout_ms (иначе счётчики протухнут
    # под живым, но медленным диалогом).
    conv_counter_ttl_seconds: int = 86400   # 1 сутки

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

    # resumable control-события: durable-лента events:{id} (Redis Stream) — ЗАМЕНА Pub/Sub.
    # Несёт gen_start и эхо user_message. TTL = окно догона на реконнекте: в его пределах
    # клиент переигрывает пропущенные события по своему last_event_id; после — восстановит
    # состояние перезагрузкой истории из Postgres (та же граница, что у gen:{}). Держите
    # >= типичного окна реконнекта клиента (обрыв сети/refresh вкладки).
    events_ttl_seconds: int = 900       # 15 мин
    # Приблизительный потолок длины ленты событий (XADD ... MAXLEN ~) — защита от роста
    # без границ (как у прочих стримов). События крошечные и редкие на фоне токенов.
    events_maxlen: int = 10000

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
    # TTL замка суммаризации. Свёртка большого хвоста идёт несколькими LLM-вызовами и
    # может быть долгой; замок продлевается heartbeat'ом (как в llm_worker), поэтому TTL —
    # это запас на паузу heartbeat. Без продления замок истекал бы на лету, и второй
    # суммаризатор дублировал бы работу (не потеря данных — save_summary идемпотентен по
    # диапазону, — но лишний расход LLM). Валидатор требует lock_heartbeat < этого TTL.
    summarize_lock_ttl_seconds: int = 300

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
    # Потолок ОДНОВРЕМЕННЫХ ws-соединений на пользователя (H2). Без него один токен
    # может открыть неограниченно сокетов: каждый коннект — транзакция в Postgres
    # (ensure_conversation) + фоновые задачи, т.е. вектор DoS на БД. Считаем per-user в
    # Redis (общий на все инстансы api), проверяем ДО обращения к БД. Значение щедрое для
    # человека с несколькими вкладками/устройствами, но режет флуд соединений.
    ws_max_connections_per_user: int = 20
    # TTL «свежести» записи о ws-соединении в учётном ZSET (H2). Каждое ЖИВОЕ соединение
    # продлевает свой дедлайн heartbeat'ом раз в lock_heartbeat_seconds; запись мёртвого
    # инстанса (не снявшего свой член) сама выпадает из счёта по истечении этого срока.
    # Это заменяет прежний INCR/DECR-счётчик под коротким TTL, который для ДОЛГОживущих
    # сокетов протухал на лету и уходил в минус (лимит переставал работать). Должен быть
    # заметно больше lock_heartbeat_seconds (валидатор требует), иначе живое соединение
    # выпадет между продлениями и учёт начнёт врать.
    ws_conn_ttl_seconds: int = 90

    # Максимальная длина пароля (bcrypt режет на 72 байтах и кидает на длинных).
    password_max_chars: int = 128
    # Максимальная длина email (RFC 5321 — адрес не длиннее 254 символов). Без лимита
    # клиент мог бы прислать мегабайтный «email» -> лишняя нагрузка/хранение.
    email_max_chars: int = 254

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
    # Сколько генераций РАЗНЫХ диалогов один воркер ведёт ОДНОВРЕМЕННО (H-1). Генерация
    # почти всё время ждёт сеть LLM, поэтому один asyncio-процесс спокойно мультиплексирует
    # десятки таких ожиданий на одном ядре — не нужно плодить процессы (и множить пулы
    # Postgres). Порядок ВНУТРИ диалога держат seq-гейт и замок диалога, поэтому
    # параллелятся только РАЗНЫЕ диалоги. Держите с запасом ниже, чем позволяет пул
    # Postgres (max_size) и лимиты Redis; при росте — сначала поднимите этот параметр,
    # а не число процессов. ИНВАРИАНТ (H1): worker_concurrency <= pg_pool_max_size
    # (иначе пик одновременных запросов упрётся в пул и они сериализуются на acquire).
    worker_concurrency: int = 24
    # Реклейм зависших задач: заберём у «мёртвого» воркера всё, что висит в PEL
    # дольше этого простоя (мс). ВАЖНО: держите заметно больше максимального времени
    # генерации ответа, иначе долгий (напр. на большом контексте) ответ,
    # ещё висящий в PEL, будет ошибочно реклеймлен, и второй воркер зря прокрутится на
    # замке всю генерацию (M2). Валидатор ниже требует reclaim_min_idle_ms >=
    # conv_lock_ttl_seconds*1000: не забирать задачу раньше, чем истёк бы замок мёртвого
    # воркера. Дефолт поднят до 300000 (=TTL замка), было 120000.
    reclaim_min_idle_ms: int = 300000
    # Как часто (раз в N итераций основного цикла) запускать реклейм PEL.
    reclaim_every_iters: int = 20

    # Dead-letter: после скольких доставок постоянно падающая («отравленная») задача
    # переносится в поток `<stream>:dead` и подтверждается — иначе она крутилась бы в
    # PEL вечно, переобрабатываясь reclaim'ом. Разбирать: `XRANGE <stream>:dead - +`.
    max_deliveries: int = 5
    dead_letter_maxlen: int = 10000

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
    # а каждый факт пропуска логируется И пишется в поток <inbound>:order_loss (см.
    # _order_gate) — это событие потери, заведите на него алерт.
    #
    # КРИТИЧНЫЙ ИНВАРИАНТ (C1): order_gap_timeout_ms > reclaim_min_idle_ms +
    # conv_lock_ttl_seconds*1000. Иначе сообщение, чей воркер УПАЛ после прохода гейта, но
    # до XACK (напр. ошибка LLM), висит в PEL и переобрабатывается только через
    # reclaim_min_idle; если гейт «сдаётся» раньше, преемник перешагнёт разрыв и это
    # сообщение потом отсеётся как seq<=applied -> ход потерян БЕЗВОЗВРАТНО. Порог должен
    # дать зависшему предшественнику успеть реклеймнуться И догенерироваться (ещё один
    # lock_ttl) прежде, чем гейт признает его потерянным. Валидатор ниже это требует.
    order_gap_timeout_ms: int = 900000

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
    def _check_lock_and_reclaim_timing(self):
        """Согласованность таймингов замка диалога и реклейма (M2). Фейл-фаст на
        конфиге, который тихо ломает сериализацию обработки:

        - heartbeat ОБЯЗАН продлевать замок раньше истечения его TTL, иначе замок умрёт
          между продлениями и два воркера возьмут один диалог;
        - реклейм не должен забирать задачу раньше, чем истёк бы замок мёртвого воркера
          (reclaim_min_idle_ms >= conv_lock_ttl_seconds*1000). Иначе задачу ЖИВОГО, но
          долго генерящего воркера реклеймят на лету, и второй воркер впустую крутится
          на замке всю генерацию (лишней генерации нет — её ловит перепроверка applied —
          но пропускная способность падает)."""
        if self.lock_heartbeat_seconds >= self.conv_lock_ttl_seconds:
            raise ValueError(
                "lock_heartbeat_seconds must be < conv_lock_ttl_seconds "
                "(heartbeat must renew the lock before its TTL expires)")
        if self.lock_heartbeat_seconds >= self.summarize_lock_ttl_seconds:
            raise ValueError(
                "lock_heartbeat_seconds must be < summarize_lock_ttl_seconds "
                "(heartbeat must renew the summarizer lock before its TTL expires)")
        if self.reclaim_min_idle_ms < self.conv_lock_ttl_seconds * 1000:
            raise ValueError(
                "reclaim_min_idle_ms must be >= conv_lock_ttl_seconds*1000 "
                "(don't reclaim a task before a dead worker's lock would expire)")
        if self.worker_concurrency < 1:
            raise ValueError("worker_concurrency must be >= 1")
        if self.ws_conn_ttl_seconds <= self.lock_heartbeat_seconds:
            raise ValueError(
                "ws_conn_ttl_seconds must be > lock_heartbeat_seconds (H2): a live "
                "connection refreshes its accounting entry every lock_heartbeat_seconds; "
                "a shorter TTL drops it between refreshes and the per-user cap drifts.")
        if self.worker_concurrency > self.pg_pool_max_size:
            raise ValueError(
                "worker_concurrency must be <= pg_pool_max_size (H1): the pool must "
                "cover the peak of simultaneous DB queries, else they serialize on "
                "pool.acquire(). Raise pg_pool_max_size or lower worker_concurrency.")
        # C1: гейт порядка не должен «сдаваться» раньше, чем зависший в PEL предшественник
        # успеет реклеймнуться (reclaim_min_idle_ms) И догенерироваться (ещё один lock_ttl).
        # Иначе преемник перешагнёт разрыв, а реальное сообщение потом отсеётся как дубль.
        min_gap = self.reclaim_min_idle_ms + self.conv_lock_ttl_seconds * 1000
        if self.order_gap_timeout_ms <= min_gap:
            raise ValueError(
                "order_gap_timeout_ms must be > reclaim_min_idle_ms + "
                "conv_lock_ttl_seconds*1000 (C1): give a stuck predecessor time to be "
                "reclaimed and regenerated before the gate declares it lost, otherwise "
                "a transient worker/LLM failure permanently drops that turn.")
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
