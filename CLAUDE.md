# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

> This service is a **git submodule** of the `gridtokenx-coresystem` superproject. The root
> `CLAUDE.md` (one level up) holds the cross-service workflow rules — most importantly the
> mandatory **test-first, then summarize** rule after every code change, and the
> `code-review-graph` MCP-first exploration rule. Both apply here.

---

## What this service is

`gridtokenx-meter-service` — a small Axum service backing the trading UI's Smart Meter
dashboard. It owns no schema of its own: it reads/writes the **shared `gridtokenx` Postgres**
(`meters`, `meter_readings`, joined to `users` for the wallet). It does **no blockchain work** —
no minting, no Chain Bridge, no NATS, no Solana. It is a pure meter registry + reading store with
a realtime push stream. It *reads* the table's mint columns read-only to surface a
`mint_status` (`minted`/`pending`/`denied`) for the dashboard, but never writes them.

Responsibilities:

1. **Register a meter to a user account** — `POST /api/v1/meters`, scoped to the JWT user.
2. **Ingest readings** — `POST /api/v1/meters/{serial}/readings`, persisted to Postgres.
3. **Serve readings/stats** — list + aggregate endpoints for the dashboard, each carrying a
   read-only `mint_status` (and `minted`/`pending`/`denied` counts in stats).
4. **Realtime stream** — `GET /api/v1/meters/readings/stream` (Server-Sent Events): every
   submitted reading is fanned out to that user's open SSE subscribers.

All ingress is **HTTP (JWT-authed)** via APISIX. User scoping is by `user_id` from the JWT `sub`.

---

## Build, run, test

See **[TESTING.md](TESTING.md)** for the map of *critical point → exact test command*.

Each `gridtokenx-*` service is its **own Cargo workspace** — `cd` into this dir first; never
`cargo` from the superproject root.

```bash
cargo check                          # fast feedback
cargo build --release --bin meter-service
cargo test                           # unit tests (no infra needed)
cargo test -p meter-logic            # one crate
cargo test submit_falls_back_to_owner_wallet_when_request_blank -- --nocapture   # one test
```

**SQLx is runtime-checked here, not compile-time.** Queries use `sqlx::query_as::<_, T>(&sql)`
(string-built), *not* the `query!`/`query_as!` macros. So **no `DATABASE_URL` and no `.sqlx`
offline cache are needed to compile or run unit tests** — the DB is only touched at runtime.

Lints are strict (workspace `Cargo.toml`): `unsafe_code = deny`, `clippy::pedantic = warn`,
`clippy::unwrap_used = deny`, `missing_docs = warn`. Don't introduce `.unwrap()`.

**CI gate** (`.github/workflows/ci.yml`, always-on, infra-free). Hard gates:
`cargo fmt --all --check`, `cargo clippy --all-targets -- -D warnings`, and
`cargo test --workspace`. Keep all green — pedantic warnings and unformatted code **fail** the
build (run `cargo fmt` before pushing). The `#[ignore]` DB-gated e2e suite is **not** run in CI (shared
IAM-owned, partitioned schema); it runs against a live stack — see [TESTING.md](TESTING.md).

Coverage at a glance: `meter-logic` unit tests (validation, page clamp, wallet fallback,
serial norm, kWh `0.0` boundary), `meter-api` SSE-filter unit tests, two infra-free router
tests (auth → 401, malformed → 400/404), and the DB-gated `e2e_http` suite (all three
`mint_status` branches incl. synthetic minted/denied injection, cross-user authz, isolation,
pagination, aggregates). See [TESTING.md](TESTING.md) for the per-test map.

---

## Architecture — layered, trait-DI ("sync-ish core, async edges")

Dependency direction (never reverse): **`bin/meter-service` (server) → `meter-api` → `meter-logic` → `meter-persistence` → `meter-core`**.

| Crate | Role |
| --- | --- |
| `meter-core` | Domain models, `Config` (env), `ApiError`, and the **`MeterRepositoryTrait`** contract. |
| `meter-logic` | `MeterService` — business rules (kWh validation, page clamping, wallet fallback, serial normalization). Depends only on `meter-core`, so it's unit-testable with no DB. |
| `meter-persistence` | `MeterRepository` (SQLx/Postgres) — the concrete `MeterRepositoryTrait` impl. |
| `meter-api` | Axum handlers (thin), `AppState` DI container, JWT auth extractor, and the SSE realtime stream (`broadcast` channel). |
| `bin/meter-service` | `startup::run` wires `MeterRepository` as `Arc<dyn MeterRepositoryTrait>` into `MeterService`, creates the readings broadcast channel, builds the router, serves. Plus `telemetry`. |

Traits defined in `meter-core/src/traits.rs`, implemented in `meter-persistence`, wired in
`bin/meter-service/src/startup.rs`. Add new behavior by picking the crate per the dependency rule.

### Routes (`startup.rs`)
`GET /health` · `GET /api/v1/me/meters` · `POST /api/v1/meters` (register) ·
`GET /api/v1/meters/readings?limit&offset` · `GET /api/v1/meters/readings/stream` (SSE) ·
`GET /api/v1/meters/stats` · `POST /api/v1/meters/{serial}/readings`.

Domain field names mirror the trading UI contract (`types/meter.ts`) — keep them in sync.

---

## Critical invariants

- **`JWT_SECRET` is the only hard-required config.** Everything else has a default.
- **Reading is always persisted, then broadcast.** `submit_reading` saves the row, then publishes
  a `ReadingEvent` to the broadcast channel. A send error only means "no subscribers" and never
  fails the request.
- **SSE is filtered per-user.** `stream_readings` subscribes to the shared broadcast channel and
  emits only events whose `user_id` matches the authenticated user (`sse_event_for_user`). Lagged
  subscribers skip missed events rather than closing the stream.
- **Serial normalization.** Registration stores the trimmed serial; `submit_reading` trims the
  path serial before lookup/persist so a padded value still resolves the meter.
- **Wallet fallback.** A reading's credited wallet is the request `wallet_address`, falling back to
  the meter owner's registered wallet; blank-everywhere → `BadRequest`.
- **Mint status is read-only, derived in SQL.** The shared `meter_readings` table has `minted`,
  `mint_tx_signature`, `blockchain_*`, `on_chain_*` columns, populated by **other** services. This
  service never **writes** them (`insert_reading` writes `minted = false` only to satisfy the
  column default; no migration applied). It **reads** them to derive `mint_status` via
  `MINT_STATUS_CASE` (`repository/meter.rs`): `minted OR on_chain_confirmed` → `"minted"`,
  `blockchain_status='failed' OR blockchain_last_error IS NOT NULL` → `"denied"`, else `"pending"`.
  `user_stats` exposes the same predicates as `minted_count`/`pending_count`/`denied_count`. The
  realtime `submit → SSE` path always emits `"pending"` (the row is freshly inserted unminted);
  later mint transitions are **not** pushed (no consumer) — clients re-fetch list/stats.
- **No SSE push for mint/deny transitions — by design (won't-do).** This service does no
  blockchain work and runs no consumer of mint/deny events; the columns flip out-of-band in
  Postgres (written by other services), with no NATS/trigger/poll feeding back here. So a
  reading's `mint_status` changing `pending → minted`/`denied` is **never** broadcast on the SSE
  stream. The dashboard reconciles by re-fetching list/stats (it already polls every 30s, see the
  trading UI `useSmartMeter`). Adding a live push would require a new event source (DB
  `LISTEN/NOTIFY`, a NATS consumer, or a poll-and-diff loop) and is intentionally out of scope —
  revisit only if near-real-time mint feedback becomes a product requirement.

---

## Configuration (env, via `meter-core/src/config.rs`)

| Var | Default | Purpose |
| --- | --- | --- |
| `JWT_SECRET` | — (**required**) | HS256 secret; must equal the value IAM signs tokens with. |
| `DATABASE_URL` | `…@postgres:5432/gridtokenx` | Shared `gridtokenx` Postgres. |
| `METER_SERVICE_PORT` / `PORT` | `8080` | Bind port (binds `0.0.0.0`). |

Dockerfile: multi-stage `rust:1-bookworm` → `debian:bookworm-slim`, exposes `8080`, healthcheck on `/health`.
