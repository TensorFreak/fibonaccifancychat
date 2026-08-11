-- 0003_conversation_title: авто-название диалога.
-- Заполняется отдельным LLM-запросом (см. summarizer.generate_title), чтобы выглядело
-- естественно, а не как обрезанное первое сообщение пользователя. NULL -> ещё не задано
-- (клиент показывает «Новый чат»).

ALTER TABLE conversations ADD COLUMN IF NOT EXISTS title TEXT;
