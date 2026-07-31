# gridtokenx-meter-service

A small, **chain-light, read-mostly** Axum service backing the trading UI's Smart Meter dashboard.
It owns no schema of its own: it reads the shared `gridtokenx` Postgres (`meters`, `meter_readings`,
joined to `users` for the wallet). It does **no blockchain work** — no minting, no Chain Bridge, no
NATS, no Solana. It serves the dashboard read paths plus a realtime push stream, and reads the
`meter_readings` mint columns **read-only** to surface a `mint_status` (`minted`/`pending`/`denied`).

> Git submodule of the [`gridtokenx-coresystem`](https://github.com/NakaSato) superproject.

## Ingress

A single ingress: **HTTP (JWT-authed)** via APISIX. User scoping is by `user_id` from the JWT `sub`.

> **Reading ingest is NOT here.** Meter telemetry is ingested **only** via the Aggregator Bridge
> (Ed25519-signed IoT gateway → zone Redis Streams / InfluxDB / Kafka). This service does **not**
> accept direct reading writes — the former `POST /api/v1/meters/{serial}/readings` route and the
> repository's reading-insert path were removed. `meter_readings` rows are written by other services;
> this service only reads them. **Do not re-add a reading-ingest endpoint, NATS consumer, or mint
> path here** — minting now lives in the Aggregator Bridge settlement sink (Chain Bridge over NATS).

## Architecture — layered, trait-DI ("sync-ish core, async edges")

Dependency direction (never reverse):
`bin/meter-service` (server) → `meter-api` → `meter-logic` → `meter-persistence` → `meter-core`.

| Crate | Role |
| --- | --- |
| `meter-core` | Domain models, `Config` (env), `ApiError`, and the **`MeterRepositoryTrait`** contract. |
| `meter-logic` | `MeterService` — business rules (kWh validation, page clamping, wallet fallback, serial normalization). Depends only on `meter-core`, so it's unit-testable with no DB. |
| `meter-persistence` | `MeterRepository` (SQLx/Postgres) — the concrete `MeterRepositoryTrait` impl. |
| `meter-api` | Axum handlers (thin), `AppState` DI container, JWT auth extractor, and the SSE realtime stream (`broadcast` channel). |
| `bin/meter-service` | `startup::run` wires `MeterRepository` as `Arc<dyn MeterRepositoryTrait>` into `MeterService`, builds the router, spawns the mint-status SSE poller, serves. Plus `telemetry`. |

## Routes

Every caller-scoped route is canonical under the platform's `/api/v1/me` user-self base:

```
GET  /health
GET  /health/ready
GET  /metrics                                    # internal CIDRs only (APISIX-gated)
GET  /api/v1/me/meters                           # the caller's meters
POST /api/v1/me/meters                           # register
GET  /api/v1/me/meters/readings?limit&offset
GET  /api/v1/me/meters/readings/stream           # SSE (mint-status transitions)
GET  /api/v1/me/meters/stats
GET  /api/v1/meters/map                          # grid-wide — NOT caller-scoped
```

**Legacy aliases, still dual-served** by the identical handlers, so existing clients keep working:
`POST /api/v1/meters` · `GET /api/v1/meters/readings` · `GET /api/v1/meters/readings/stream` ·
`GET /api/v1/meters/stats`. Prefer the `/api/v1/me/meters*` forms in new code; the aliases go away
once no caller uses them. `GET /api/v1/meters/map` is *not* an alias — it is grid-wide by design
(every located meter across all users) and deliberately stays off the `/me` base.

There is **no** reading-ingest route and **no** mint route. Domain field names mirror the trading UI
contract (`types/meter.ts`) — keep them in sync.

## Critical invariants

- **`JWT_SECRET` is the only hard-required config.** Everything else has a default.
- **No reading-ingest path.** This service does not write `meter_readings` (no submit endpoint, no
  repository insert). Telemetry is ingested by the Aggregator Bridge; the only events the broadcast
  channel carries are mint-status transitions from the background poller (below).
- **Mint status is read-only, derived in SQL.** The shared `meter_readings` table has `minted`,
  `mint_tx_signature`, `blockchain_*`, `on_chain_*` columns, populated by **other** services. This
  service never **writes** them. It **reads** them to derive `mint_status` via `MINT_STATUS_CASE`
  (`repository/meter.rs`): `minted OR on_chain_confirmed` → `"minted"`, `blockchain_status='failed'
  OR blockchain_last_error IS NOT NULL` → `"denied"`, else `"pending"`. `user_stats` exposes the same
  predicates as `minted_count`/`pending_count`/`denied_count`.
- **Mint transitions reach SSE via a background poller** (`bin/meter-service/src/mint_poller.rs`).
  The mint columns flip out-of-band in Postgres (written by other services) and `meter_readings` is
  IAM-owned, so there's no DB trigger / `LISTEN-NOTIFY` to hook. The poller snapshots the newest
  resolved-mint readings every `METER_MINT_POLL_SECS` (default 15s; `0` disables), diffs each
  snapshot via the pure `diff_transitions`, and broadcasts only what changed onto the `readings_tx`
  channel the SSE handler filters by `user_id`. It primes its seen-set on startup without
  broadcasting (no backlog replay). Best-effort: a transition that lands while the service is down is
  not pushed — clients reconcile by re-fetching list/stats.
- **SSE is filtered per-user.** `stream_readings` emits only events whose `user_id` matches the
  authenticated user. Lagged subscribers skip missed events rather than closing the stream.
- **Serial normalization.** Registration stores the trimmed serial so lookups by a whitespace-padded
  serial still resolve the meter.

## Build, run, test

This service is its **own Cargo workspace** — `cd` into this dir first; never `cargo` from the
superproject root.

```bash
cargo check                          # fast feedback
cargo build --release --bin meter-service
cargo test                           # unit + infra-free router tests (no infra needed)
cargo test -p meter-logic            # one crate
```

**SQLx is runtime-checked here, not compile-time** (queries are string-built via
`sqlx::query_as::<_, T>(&sql)`). No `DATABASE_URL` and no `.sqlx` offline cache needed to compile or
run unit tests — the DB is only touched at runtime.

### Integration tests (require live infra, `#[ignore]` by default)

The DB-gated `e2e_http` suite (`bin/meter-service/tests/e2e_http.rs`) runs against a live stack
(register + read, all three `mint_status` branches incl. synthetic minted/denied injection,
multi-user list isolation, pagination, aggregates, and that the removed reading-ingest route 404s).
It is **not** run in CI (shared IAM-owned partitioned schema) — see [TESTING.md](TESTING.md).

Lints are strict: `unsafe_code = deny`, `clippy::pedantic = warn`, `clippy::unwrap_used = deny`,
`missing_docs = warn`. Don't introduce `.unwrap()`.

## Configuration (env, via `meter-core/src/config.rs`)

| Var | Default | Purpose |
| --- | --- | --- |
| `JWT_SECRET` | — (**required**) | HS256 secret; must equal the value IAM signs tokens with. |
| `DATABASE_URL` | `…@postgres:5432/gridtokenx` | Shared `gridtokenx` Postgres. |
| `METER_SERVICE_PORT` / `PORT` | `8080` | Bind port (binds `0.0.0.0`). |
| `METER_MINT_POLL_SECS` | `15` | Mint-status SSE poller interval; `0` disables it. |

## Docker

Multi-stage `rust:1-bookworm` → `debian:bookworm-slim`, exposes `8080`, healthcheck on `/health`.
