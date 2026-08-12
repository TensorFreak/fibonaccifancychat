# 08. Контроль длины (token budget)

**Код:** [`app/tokens.py`](../app/tokens.py), [`app/workers/llm_worker.py`](../app/workers/llm_worker.py) (`build_prompt`), [`app/workers/summarizer.py`](../app/workers/summarizer.py) (`fold`), [`app/llm/client.py`](../app/llm/client.py)

## Проблема

Контекст модели конечен. Раньше длина контролировалась только по **числу сообщений** (окно = 20 штук), а это не про токены:

1. **Длинные сообщения.** 20 сообщений по 10k токенов = 200k токенов → промпт улетает за лимит, API отвечает ошибкой. Счётчик сообщений об этом «не знает».
2. **Резюме без потолка.** `summary` шёл в каждый промпт, но ничем не ограничивался и пух от свёртки к свёртке.
3. **Большой хвост в суммаризаторе.** Если накопилось много несвёрнутых сообщений, сам промпт суммаризатора превышал контекст.
4. **Длина ответа не ограничена.** Без `max_tokens` модель могла генерировать до максимума — стоимость, латентность, риск упереться в контекст.

Этот документ описывает механизм, который закрывает всё четыре пункта.

## Единый модуль подсчёта: `app/tokens.py`

Чтобы api, воркер и суммаризатор мерили длину **одинаково**, весь подсчёт — в одном модуле поверх `tiktoken`. Для неизвестных/кастомных моделей — фолбэк на `cl100k_base` (оценка сверху; нам нужна не побитовая точность, а предсказуемый потолок).

> **Закрытый контур:** tiktoken при первом вызове может ходить в интернет за BPE-рангами.
> Если энкодер получить не удалось (офлайн), модуль переходит на грубую эвристику
> (~3 символа/токен, с запасом в бо́льшую сторону) — сервис **не падает**. Для точного
> подсчёта кэш рангов **уже вшит в образ**: [`Dockerfile`](../Dockerfile) прекачивает
> `o200k_base`+`cl100k_base` и задаёт `TIKTOKEN_CACHE_DIR`. См. [10](10-hardening.md), H4/R6-2.

> **Неблокирующий подсчёт (R6-2):** `tiktoken` синхронный и CPU-затратный, а первый вызов
> ещё и может сходить в сеть. Поэтому: (1) энкодер прогревается на старте воркера
> (`tokens.warm_encoder` через `asyncio.to_thread`) — возможный сетевой поход случается
> один раз и вне горячего пути; (2) сам подсчёт при сборке промпта и свёртке вынесен в
> поток. tiktoken (Rust) отпускает GIL, поэтому это реально параллелится по ядрам.
> См. [10](10-hardening.md), R6-2.

| Функция | Что делает |
|---|---|
| `count_text(text)` | токены строки |
| `count_message(msg)` | токены сообщения + служебная надбавка на обёртку роли (~4) |
| `count_messages(msgs)` | токены всего списка (как увидит модель) |
| `truncate_text(text, max_tokens)` | жёстко обрезать строку по токенам (аварийный предохранитель) |
| `chunk_messages(msgs, max_tokens)` | разбить список на чанки ≤ max_tokens (для суммаризатора) |

Обрезка режет **по токенам, а не по символам** — только так гарантированно уложишься в лимит.

## Параметры (config.py)

| Параметр | Значение | Роль |
|---|---|---|
| `context_window_tokens` | 256000 | полное окно модели — **реальный потолок** (см. ниже) |
| `max_response_tokens` | 4096 | резерв под ответ → `max_tokens` запроса |
| `prompt_token_budget` | 0 = авто | потолок промпта; 0 → занять окно по максимуму |
| `token_safety_ratio` | 0.05 | запас на неточность подсчёта токенов (доля окна) |
| `summary_max_tokens` | 2000 | потолок объёма резюме |
| `summary_fold_chunk_tokens` | 8000 | размер чанка при свёртке большого хвоста |

### `context_window_tokens` — гарантия, а не справка

Валидатор в [`config.py`](../app/config.py) выводит безопасный потолок промпта:

```
ceiling = context_window_tokens − max_response_tokens − context_window_tokens·token_safety_ratio
```

и приводит `prompt_token_budget` к нему:

- `prompt_token_budget <= 0` → **авто**: `prompt_token_budget = ceiling` (использовать окно
  по максимуму — для больших моделей вроде 256k);
- `prompt_token_budget > ceiling` → **зажимается** до `ceiling` (нельзя превысить окно);
- разумное явное значение — сохраняется (осознанное ограничение ради стоимости/латентности).

Так гарантируется `промпт + ответ + запас ≤ context_window_tokens`. Примеры (проверено):
окно 256k, ответ 4096, запас 5% → авто-бюджет **239 104**; заданные 300000 → зажаты до
239 104; окно 8k, ответ 1024 → бюджет 6 576.

`token_safety_ratio` компенсирует, что для не-OpenAI моделей и офлайн-режима подсчёт
токенов приблизительный — запас страхует от недооценки. Фактический промпт обычно **меньше**
бюджета: его размер задаёт горячее окно `ctx_max_messages` и длина сообщений
(`max_message_chars`). Чтобы держать в промпте больше истории дословно — поднимите
`ctx_max_messages` (соблюдая инвариант из [05](05-hot-context.md)).

## Правка 1 — сборка промпта по токен-бюджету

Было: `[summary] + все 20 сообщений окна`. Стало: собираем по бюджету.

> Ниже показана логика отбора. В коде она вынесена в чистую функцию `_assemble_prompt`,
> которую `build_prompt` вызывает через `asyncio.to_thread` — весь `tiktoken`-подсчёт идёт
> в потоке, не блокируя event loop (R6-2). Ввод-вывод (Redis/PG) остаётся асинхронным.

```python
def _assemble_prompt(window, summary):        # чистый CPU; вызывается через to_thread
    budget = settings.prompt_token_budget
    head = []
    if summary:
        summary = tokens.truncate_text(summary, settings.summary_max_tokens)  # подстраховка
        head = [{"role": "system", "content": f"Краткое содержание ...:\n{summary}"}]
        budget -= tokens.count_messages(head)

    selected = []
    for msg in reversed(window):            # с конца: свежее приоритетнее
        cost = tokens.count_message(msg)
        if cost > budget:
            break
        budget -= cost
        selected.append(msg)
    selected.reverse()                      # вернуть хронологию

    if not selected and window:             # предохранитель (правка 5)
        room = max(settings.prompt_token_budget - tokens.count_messages(head), 0)
        last = dict(window[-1])
        last["content"] = tokens.truncate_text(last["content"], room)
        selected = [last]

    return head + selected
```

Логика:

1. `summary` кладём как system-голову и **вычитаем его стоимость** из бюджета.
2. Окно набираем **с конца** (свежие сообщения важнее старых), пока укладываемся в остаток бюджета. Не влезло — прекращаем.
3. По построению промпт **не превышает бюджет**. Старое, не попавшее в окно, не теряется: оно в Postgres и свёрнуто в `summary`.

## Правка 2 — `max_tokens` на генерацию

`stream_completion` и `complete` всегда задают потолок ответа:

```python
async def stream_completion(messages, max_tokens=None):
    payload = {"model": ..., "messages": messages, "stream": True,
               "max_tokens": max_tokens or settings.max_response_tokens}
```

Без `max_tokens` длина ответа неограничена. Теперь: генерация ответа — `max_response_tokens` (4096), суммаризация — `summary_max_tokens` (2000, пробрасывается явно).

## Правка 3 — потолок на резюме

В суммаризаторе резюме ограничено **дважды**:

- в промпте: «Уложись примерно в `summary_max_tokens` токенов»;
- жёсткой обрезкой результата: `tokens.truncate_text(summary, settings.summary_max_tokens)` — на случай, если модель проигнорила инструкцию.

Так `summary` не разрастается: он идёт в каждый промпт ответа, и его раздувание било бы по всем ходам диалога.

## Правка 4 — чанкинг большого хвоста в суммаризаторе

`fold` сворачивает не одним промптом, а итеративно по чанкам:

```python
async def fold(old_summary, messages):
    summary = old_summary
    for chunk in tokens.chunk_messages(messages, settings.summary_fold_chunk_tokens):
        # промпт: текущее summary + render(chunk) -> обновлённое summary
        summary = await complete(prompt, max_tokens=settings.summary_max_tokens)
        summary = tokens.truncate_text(summary, settings.summary_max_tokens)
    return summary or ""
```

Большой накопившийся хвост нельзя отдать одним промптом — он сам превысит контекст. `chunk_messages` бьёт его на части ≤ `summary_fold_chunk_tokens`, каждая вкатывается в резюме отдельным шагом. Полный разбор суммаризатора — [06](06-summarization.md).

## Правка 5 — аварийная обрезка промпта

Крайний случай: даже одно последнее сообщение больше бюджета (окно после цикла осталось пустым). Нельзя отправить модели пустой промпт — берём последнее сообщение, обрезав до остатка бюджета:

```python
if not selected and window:
    room = max(settings.prompt_token_budget - tokens.count_messages(head), 0)
    last = dict(window[-1])
    last["content"] = tokens.truncate_text(last["content"], room)
    selected = [last]
```

Гарантия: модель всегда получает **хотя бы текущий вопрос** (пусть обрезанный), и промпт **никогда** не превышает лимит.

## Сводка: где какая правка

| Правка | Где | Закрывает проблему |
|---|---|---|
| 1. Токен-бюджет промпта | `build_prompt` | #1 длинные сообщения |
| 2. `max_tokens` генерации | `llm/client.py` | #4 длина ответа |
| 3. Потолок резюме | `summarizer.fold` | #2 разрастание summary |
| 4. Чанкинг хвоста | `summarizer.fold` + `chunk_messages` | #3 большой хвост |
| 5. Аварийная обрезка | `build_prompt` | #1 крайний случай |

## Зависимость

Добавлен `tiktoken==0.8.0` в [`requirements.txt`](../requirements.txt).

## Настройка под конкретную модель

Основной рычаг — три числа под вашу модель:

- `context_window_tokens` — из ТТХ модели;
- `max_response_tokens` — сколько максимум должен отвечать бот;
- `prompt_token_budget` ≈ `context_window_tokens − max_response_tokens −` запас (10–20%),
  либо `0` = авто (валидатор сам выведет потолок).

Меняются через переменные окружения / `.env` (см. [`app/config.py`](../app/config.py)).

## Границы

- Подсчёт токенов через `cl100k_base` для не-OpenAI моделей — **оценка**, не точное совпадение с токенизатором модели. Поэтому держим запас в бюджете.
- Обрезка `truncate_text` может оборвать текст на полуслове — это предохранитель для крайних случаев, а не штатный путь.
- Бюджет применяется к горячему окну; если само `summary` близко к `summary_max_tokens`, под окно останется меньше места — что корректно (резюме важнее старых деталей), но стоит держать `summary_max_tokens ≪ prompt_token_budget`.
