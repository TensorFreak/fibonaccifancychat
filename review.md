# Mission-critical review — chat-backend

_Date: 2026-08-12. Scope: full codebase (`app/`, `migrations/`, Dockerfile, compose,
frontend). Zero-trust pass — reviewed against the actual code, not prior reviews/docs._
_Auth model (email+password without verification) is out of scope by owner's decision._

## Verdict

The distributed core is genuinely strong: DB-level idempotency (`ON CONFLICT
(message_id, role)`), FIFO ordering by `seq` across N workers, resumable streaming with
`active_gen` catch-up, owner-aware locks with heartbeat, dead-letter, real context-window
ceiling, migrations under advisory-lock, keyset pagination, no SQL injection, no cross-user
leakage, XSS-safe client. Not a typical MVP.

This pass fixed the one HIGH availability/correctness bug and the two cheapest DoS gaps.
The rest below is the tracked backlog before full public prod.

---

## Fixed in this pass (verified by code)

### [#1 · HIGH] Atomic message ingest — `seq` could diverge from the stream → 15-min conversation freeze
`seq` was allocated by `INCR` **before** `XADD`. Any failure between them (`server_busy`
on a Redis blip, process kill) burned a seq number that never entered the stream; the
order-gate then treated it as a gap and waited `order_gap_timeout_ms` (default 900 s),
freezing the whole conversation and finally dropping the turn.
**Fix:** `INCR seq` + `EXPIRE` + `XADD` are now one atomic Lua script (`_ENQUEUE_LUA` in
`app/api/main.py`). Either both happen or neither — seq can no longer diverge from the
stream. Stale `server_busy` self-heal comment updated.

### [#2 · MEDIUM] WS connection-rate limit — unbounded conversation creation on Postgres
`ensure_conversation` creates the conversation row on connect; only *concurrent*
connections were capped, not connection *rate*, so a connect/disconnect loop over fresh
UUIDs was an unbounded transaction/row DoS on the shared DB.
**Fix:** per-user fixed-window connect limiter (`_allow_ws_connect`, keys `rl:wsconn:*`),
checked **before** the DB hit. Config: `WS_CONNECT_RATE_MAX=60` / window 60 s. Fail-open on
Redis error.

### [#3 · MEDIUM] Auth endpoint rate limit — brute-force + bcrypt CPU-DoS
`/api/register` and `/api/login` had no throttle: unthrottled password brute-force, and
each call runs bcrypt (~100 ms CPU) in the threadpool → flood = CPU-DoS; register also
created unbounded users.
**Fix:** per-IP fixed-window limiter (`_rate_limit_auth`, keys `rl:auth:*`) on both
endpoints, returns 429. Config: `AUTH_RATE_MAX=10` / window 60 s. Uses `request.client`
(real client IP via `--proxy-headers`). Fail-open on Redis error.

---

## Open backlog (not yet addressed)

### MEDIUM

- **[#4] JWT in the WebSocket URL query string.** The 7-day, non-revocable token rides in
  `ws://…/ws/{id}?token=<jwt>` and lands in Caddy/uvicorn access logs (query strings are
  logged where headers/bodies are not). Move the token to the first WS message, or issue a
  short-lived single-use ticket over HTTPS. (CSWSH itself is not exploitable — auth isn't
  cookie-based.)

- **[#5] `MAXLEN ~` on the shared inbound stream can silently drop un-processed messages.**
  Trimming removes oldest entries regardless of PEL/ack state; under a large backlog (100k+)
  those become dangling PEL refs that reclaim just acks and discards → silent turn loss. The
  per-user message rate limit bounds a single tenant, not the aggregate. Consider per-shard
  streams or ingress backpressure (reject) instead of trimming a durable queue.

- **[#6] Summarizer outage → silent "context hole".** Hot window holds only the last
  `ctx_max_messages` (40); older content must live in `summary`. If the summarizer is
  down/dead-lettered, messages that scroll out of the window but were never folded are
  silently absent from the LLM prompt — the model loses the middle of long conversations.
  Detected and written to `chat:summarize:lag`, but nothing self-heals while it's down and
  nothing is wired to alert. **Wire `:lag`, `chat:inbound:order_loss`, and `*:dead` streams
  to real alerting before prod** — otherwise these degradations are invisible.

- **[#7] Order-gate busy-requeue burns worker slots + Redis I/O.** While waiting on a real
  gap, each successor is re-`XADD`'d and `sleep(0.05)`'d, then immediately re-read — ~20
  XADD/s per stuck message for up to 900 s, each holding a `worker_concurrency` slot. A
  single stuck predecessor with queued successors degrades throughput. Prefer exponential
  backoff or a per-conversation "parked" set over hot-looping the shared stream.

- **[#8] Redis `appendfsync everysec` + fire-and-forget ingest.** Up to ~1 s of accepted
  `XADD`s / `INCR`s can be lost on a hard Redis crash, and the API returns nothing to the
  client after `XADD` — the user believes the message was sent. Contradicts "ultimate
  persistence" at the ingest boundary. Options: `appendfsync always` (throughput cost), or
  ack to the client only after a durable write.

### LOW / hardening

- **`save_summary` can create an orphan conversation with `NULL user_id`** — the UPSERT
  inserts `(id, summary, summary_upto_id)` with no `user_id` if the row is missing. Guard
  against inserting a new row, or backfill `user_id`.
- **Counter-TTL divergence** (edge): if `applied` (1-day TTL) expires while `seq` keeps
  being refreshed under a prolonged worker outage, the next message can look like a huge gap
  and drop a batch. The two TTLs are only conventionally coupled.
- **No Redis AUTH/TLS** — fine on single-host compose (ports unpublished); add `requirepass`
  before any multi-host deploy.
- **No email-format validation**; bcrypt silently truncates to 72 bytes.
- **`messages` has no FK to `conversations`** — orphan rows possible; no cascade (no delete
  feature exists yet, so latent).
- **JWT in `localStorage`** — standard tradeoff, but XSS = theft of a 7-day, non-revocable
  token. No `jti` / rotation / revocation list.
- **Tests** — now cover `_order_gate`, `_dispatch`, prompt assembly, the atomic-ingest
  path (#1, incl. the all-or-nothing failure case), and the #2/#3 rate limiters
  (`tests/test_ingest_and_limits.py`). Still **uncovered**: the resumable stream tail
  (`tail_generation`), reconnect/`pump_out` replay, and idempotent double-processing
  (`assistant_exists` / reclaim). Those are the next highest-value targets and need a real
  or faked Redis-stream harness.

---

## Verified solid (no action)

No SQL injection (parameterized + `uuid.UUID` coercion); no cross-user leakage (ownership
gates + conversation-namespaced stream keys); JWT fixed `algorithms=["HS256"]` with `exp`,
fail-closed, config refuses weak secret in non-dev; retry/reclaim idempotency prevents
double-charged LLM calls; owner-aware Lua lock release; startup config invariants
(lock/reclaim/gap timing, window ≥ keep+trigger, budget ≤ context window, pool ≥
concurrency); Postgres/Redis ports unpublished, non-root container, TLS via Caddy,
`--forwarded-allow-ips` scoped.
