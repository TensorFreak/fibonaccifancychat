-- 0002_conversations_pagination: индекс под keyset-пагинацию списка диалогов.
--
-- Список чатов сортируется по (created_at DESC, id DESC) и листается курсором
-- WHERE (created_at, id) < (:ts, :id). Этот составной индекс делает и фильтр по
-- user_id, и такую выборку эффективной (без seq-scan и без OFFSET).
-- id добавлен вторым ключом, чтобы разрешать коллизии одинаковых created_at.

CREATE INDEX IF NOT EXISTS idx_conversations_user_created
    ON conversations (user_id, created_at DESC, id DESC);
