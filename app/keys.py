"""Единое место, где заданы имена ключей и каналов Redis.
Держать их в одном модуле важно: api и воркеры ДОЛЖНЫ использовать одинаковые
имена, иначе один пишет в один канал, а другой слушает другой."""


def ctx_key(conversation_id: str) -> str:
    # Redis LIST: горячее окно последних сообщений диалога (для сборки промпта)
    return f"ctx:{conversation_id}"


def conv_channel(conversation_id: str) -> str:
    # Redis Pub/Sub канал: сюда воркер публикует токены, отсюда api их шлёт в сокеты
    return f"conv:{conversation_id}"


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


def conv_applied(conversation_id: str) -> str:
    # Redis STRING: номер последнего УЖЕ применённого сообщения диалога.
    # Гейт порядка: обрабатываем только seq == applied + 1.
    # Идемпотентность вставок обеспечивается на уровне Postgres (ON CONFLICT по
    # message_id), поэтому отдельные Redis-маркеры дедупа больше не нужны.
    return f"applied:conv:{conversation_id}"
