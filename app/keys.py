"""Единое место, где заданы имена ключей и каналов Redis.
Держать их в одном модуле важно: api и воркеры ДОЛЖНЫ использовать одинаковые
имена, иначе один пишет в один канал, а другой слушает другой."""


def ctx_key(conversation_id: str) -> str:
    # Redis LIST: горячее окно последних сообщений диалога (для сборки промпта)
    return f"ctx:{conversation_id}"


def events_stream(conversation_id: str) -> str:
    # Redis STREAM: durable-лента control-событий диалога (gen_start, эхо user_message).
    # Заменяет эфемерный Pub/Sub conv:{id}: события переживают обрыв и переигрываются —
    # (пере)подключившийся клиент догоняет пропущенное по своему last_event_id. MAXLEN+TTL
    # держат ленту ограниченной; после TTL догон невозможен -> клиент восстановит состояние
    # перезагрузкой истории из Postgres (та же граница, что у gen:{}).
    return f"events:{conversation_id}"


def conv_lock(conversation_id: str) -> str:
    # Redis ключ-замок: сериализует обработку одного диалога (см. воркер)
    return f"lock:conv:{conversation_id}"


def sum_key(conversation_id: str) -> str:
    # Redis STRING: кэш текущего summary диалога (source of truth — в Postgres)
    return f"sum:{conversation_id}"


def gen_stream(conversation_id: str, message_id: str) -> str:
    # Redis STREAM: durable-лента токенов ОДНОЙ генерации ассистента.
    # Именно она делает стрим resumable: каждый токен = запись, её можно
    # переиграть через XRANGE/XREAD при реконнекте. TTL после завершения.
    return f"gen:{conversation_id}:{message_id}"


def active_gen(conversation_id: str) -> str:
    # Redis STRING: message_id генерации, идущей ПРЯМО СЕЙЧАС в этом диалоге
    # (или отсутствует). По нему подключившийся клиент понимает, что догонять.
    return f"active_gen:{conversation_id}"


def since_sum_key(conversation_id: str) -> str:
    # Redis счётчик: сколько новых сообщений накопилось с последней суммаризации
    return f"since_sum:{conversation_id}"


def title_enq(conversation_id: str) -> str:
    # Redis-маркер «название уже запрошено»: чтобы не дёргать LLM за заголовком
    # на каждом ходу (ставится один раз на первом ответе ассистента).
    return f"title:enq:{conversation_id}"


def conv_seq(conversation_id: str) -> str:
    # Redis счётчик: монотонный номер сообщения в диалоге (порядок FIFO).
    # api делает INCR при приёме, воркер применяет сообщения строго по возрастанию.
    return f"seq:conv:{conversation_id}"


def ws_rate(user_id: str, window: int) -> str:
    # Redis счётчик рейтлимита входящих ws-сообщений пользователя в фикс. окне.
    # window — номер окна (epoch // window_seconds); INCR+EXPIRE, общий на все инстансы.
    return f"rl:ws:{user_id}:{window}"


def ws_connect_rate(user_id: str, window: int) -> str:
    # Redis счётчик рейтлимита ЧАСТОТЫ ws-подключений пользователя в фикс. окне (#2).
    # Отдельно от ws_rate (лимит сообщений): режет цикл connect/disconnect, плодящий
    # транзакции Postgres (ensure_conversation создаёт строку диалога на каждый коннект).
    return f"rl:wsconn:{user_id}:{window}"


def auth_rate(action: str, ident: str, window: int) -> str:
    # Redis счётчик рейтлимита попыток register/login per-IP в фикс. окне (#3): режет
    # брутфорс пароля и CPU-DoS (каждая попытка = ~100мс bcrypt) флудом эндпоинтов.
    return f"rl:auth:{action}:{ident}:{window}"


def ws_conns(user_id: str) -> str:
    # Redis ZSET ОДНОВРЕМЕННЫХ ws-соединений пользователя (H2): member = id соединения,
    # score = время протухания. ZADD при коннекте, ZREM при обрыве, heartbeat продлевает
    # score живого соединения; на входе выкидываем протухшие члены (ZREMRANGEBYSCORE) и
    # считаем живые (ZCARD). Общий на все инстансы api. В отличие от прежнего INCR/DECR
    # счётчика это НЕ уходит в минус и НЕ теряет учёт долгоживущих сокетов: запись мёртвого
    # инстанса сама выпадает по score.
    return f"ws:conns:{user_id}"


def order_loss_stream(inbound_stream: str) -> str:
    # Redis STREAM аудита событий потери хода в FIFO-гейте (C3): каждый пропуск разрыва
    # по order_gap_timeout пишется сюда для алертинга/разбора. Разбирать:
    # `XRANGE <inbound>:order_loss - +`.
    return f"{inbound_stream}:order_loss"


def summary_lag_stream(summarize_stream: str) -> str:
    # Redis STREAM аудита отставания суммаризации (H2): если несвёрнутый хвост уже длиннее
    # горячего окна, часть сообщений выпала из окна, но ещё НЕ попала в summary -> дыра в
    # контексте. Пишем сюда (как order_loss), чтобы завести алерт, а не терять молча.
    # Разбирать: `XRANGE <summarize>:lag - +`.
    return f"{summarize_stream}:lag"


def conv_applied(conversation_id: str) -> str:
    # Redis STRING: номер последнего УЖЕ применённого сообщения диалога.
    # Гейт порядка: обрабатываем только seq == applied + 1.
    # Идемпотентность вставок обеспечивается на уровне Postgres (ON CONFLICT по
    # message_id), поэтому отдельные Redis-маркеры дедупа больше не нужны.
    return f"applied:conv:{conversation_id}"
