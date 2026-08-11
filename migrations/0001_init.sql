-- 0001_init: базовая схема source-of-truth в Postgres.
-- Здесь лежит ВСЯ история диалогов навсегда; Redis — лишь её быстрая проекция.
-- Все операции идемпотентны (IF NOT EXISTS): безопасно применить к БД, где схема
-- уже была создана прежним schema.sql.

CREATE TABLE IF NOT EXISTS users (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- email/пароль для веб-регистрации. Нулевые, если юзер создан не через форму
    -- (напр. dev-токеном). email уникален (без учёта регистра — храним в lower).
    email          TEXT UNIQUE,
    password_hash  TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS conversations (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id          UUID REFERENCES users(id),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- бегущее резюме старой части диалога (source of truth):
    summary          TEXT,
    -- id последнего сообщения, уже свёрнутого в summary (монотонный watermark).
    -- Именно id, а не created_at: id строго возрастает без коллизий, поэтому
    -- сообщения на границе не теряются при одинаковых таймстампах.
    summary_upto_id  BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS messages (
    id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    conversation_id  UUID NOT NULL,
    role             TEXT NOT NULL CHECK (role IN ('user', 'assistant', 'system')),
    content          TEXT NOT NULL,
    -- id входящего сообщения — ключ ИДЕМПОТЕНТНОСТИ. При ретрае обработки вставка
    -- с тем же (message_id, role) отсекается ON CONFLICT (exactly-once на уровне БД).
    message_id       TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ключевой индекс: по нему идёт регидратация (последние N сообщений диалога),
-- keyset-пагинация сообщений и выборка несвёрнутого хвоста для суммаризатора.
-- Порядок — по id (монотонный).
CREATE INDEX IF NOT EXISTS idx_messages_conv_id
    ON messages (conversation_id, id);

-- дедуп-индекс: одно сообщение (по message_id) в каждой роли — не более одного раза.
-- Частичный (WHERE message_id IS NOT NULL): сообщения старого формата без id в дедупе
-- не участвуют (несколько NULL допустимы).
CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_dedup
    ON messages (message_id, role) WHERE message_id IS NOT NULL;
