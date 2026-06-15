# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

> This service is a **git submodule** of the `gridtokenx-coresystem` superproject. The root
> `CLAUDE.md` (one level up) holds the cross-service workflow rules — most importantly the
> mandatory **test-first, then summarize** rule after every code change, and the
> `code-review-graph` MCP-first exploration rule. Both apply here.

---

## What this service is

`gridtokenx-meter-service` — a small, **chain-light** Axum service backing the trading UI's
Smart Meter dashboard. It owns no schema of its own: it reads/writes the **shared `gridtokenx`
Postgres** (`meters`, `meter_readings`, joined to `users` for the wallet). It does **not** depend
on Solana / blockchain-core; it mints by sending *intent* to **Chain Bridge over NATS** and
mirrors the wire types locally (see "Mint path" below).

Two ingress paths feed the same core service:

1. **HTTP (JWT-authed)** — the trading UI via APISIX. User scoping is by `user_id` from the JWT `sub`.
2. **NATS consumer (no auth)** — the **aggregator bridge** forwards verified, *mintable* readings
   (surplus generation, `net_kwh > 0`) on a subject. These carry no user, so they are attributed
   to the **registered owner of the meter serial**, then best-effort minted.

---

## Build, run, test

See **[TESTING.md](TESTING.md)** for the map of *critical point → exact test command*
(which test covers what, and the env each integration test needs).

Each `gridtokenx-*` service is its **own Cargo workspace** — `cd` into this dir first; never
`cargo` from the superproject root.

```bash
cargo check                          # fast feedback
cargo build --release --bin meter-service
cargo test                           # unit tests (no infra needed)
cargo test -p meter-persistence      # one crate
cargo test window_floors_to_15_min -- --nocapture   # one test
```

**SQLx is runtime-checked here, not compile-time.** Queries use `sqlx::query_as::<_, T>(&sql)`
(string-built), *not* the `query!`/`query_as!` macros. So **no `DATABASE_URL` and no `.sqlx`
offline cache are needed to compile or run unit tests** — the DB is only touched at runtime.

### Integration tests (require live infra, `#[ignore]` by default)

In `bin/meter-service/tests/`. Both need Postgres + NATS and a pre-registered meter serial:

```bash
# Stage 2B/3 — fake Chain Bridge (NATS responder), no Solana/Vault needed:
DATABASE_URL=postgresql://gridtokenx_user:gridtokenx_password@127.0.0.1:7001/gridtokenx \
NATS_URL=nats://127.0.0.1:9020 TEST_METER_SERIAL=<registered-serial> \
cargo test -p gridtokenx-meter-service --test mint_e2e -- --ignored --nocapture

# Stage 2C — REAL on-chain mint via live Chain Bridge + Solana validator
# (needs `./scripts/app.sh init` from the superproject first):
... cargo test -p gridtokenx-meter-service --test mint_onchain_e2e -- --ignored --nocapture
```

Lints are strict (workspace `Cargo.toml`): `unsafe_code = deny`, `clippy::pedantic = warn`,
`clippy::unwrap_used = deny`, `missing_docs = warn`. Don't introduce `.unwrap()`.

---

## Architecture — layered, trait-DI ("sync-ish core, async edges")

Dependency direction (never reverse): **`bin/meter-service` (server) → `meter-api` → `meter-logic` → `meter-persistence` → `meter-core`**.

| Crate | Role |
| --- | --- |
| `meter-core` | Domain models, `Config` (env), `ApiError`, and the **traits** (`MeterRepositoryTrait`, `MintGateway`). The contract everything else implements/consumes. |
| `meter-logic` | `MeterService` — business rules (kWh validation, page clamping, wallet fallback, idempotent ingest, best-effort mint). Depends only on `meter-core` traits, so it's unit-testable with no DB/NATS. |
| `meter-persistence` | Concrete impls: `MeterRepository` (SQLx/Postgres) + mint gateways (`NatsMintGateway`, `DisabledMintGateway`). |
| `meter-api` | Axum handlers (thin), `AppState` DI container, JWT auth extractor. |
| `bin/meter-service` | `startup::run` wires concrete impls as `Arc<dyn Trait>` into `MeterService`, builds the router, spawns the NATS consumer, serves. Plus `reading_consumer` + `telemetry`. |

Traits are defined in `meter-core/src/traits.rs`, implemented in `meter-persistence`, wired in
`bin/meter-service/src/startup.rs`. Add new behavior by picking the crate per the dependency rule.

### Routes (`startup.rs`)
`GET /health` · `GET /api/v1/me/meters` · `POST /api/v1/meters` (register) ·
`GET /api/v1/meters/readings?limit&offset` · `GET /api/v1/meters/stats` ·
`POST /api/v1/meters/{serial}/readings?auto_mint` · `POST /api/v1/meters/readings/{reading_id}/mint`.

Domain field names mirror the trading UI contract (`types/meter.ts`) — keep them in sync.

---

## Critical invariants (read before touching mint/ingest)

- **Degraded-by-design startup.** Missing/unreachable NATS never takes the HTTP API down: the
  reading consumer is skipped and the mint gateway falls back to `DisabledMintGateway` (503 on
  mint). Only `JWT_SECRET` is hard-required by `Config::from_env`.
- **Mint is always best-effort; the reading is always persisted first.** Both `submit_reading`
  (auto-mint) and `ingest_device_reading` save the row, then attempt the mint; a mint failure is
  recorded in `message` / logged, never lost — it can be minted later via the explicit endpoint.
- **Idempotency.** Device ingest uses `reading_id` as the row primary key (duplicate delivery =
  no-op insert). `mint_existing` rejects an already-minted reading (`Conflict`), so a redelivery
  never double-mints. On the chain side, the mint **idempotency key is `mint:{serial}:{window_start_ms}`**
  with a **15-minute window** (`WINDOW_MS` in `infra/mint.rs`) that **must match the aggregator's
  billing window** so the on-chain `(meter_id, window_start_ms)` PDA is stable per (meter, window).
- **Device-path wallet trust.** For NATS-forwarded readings the credited wallet is the registered
  owner's wallet (resolved by serial), **never a value off the wire**.
- **`blockchain_*` tracking columns.** `mark_reading_minted` writes the signature + slot; a Postgres
  `BEFORE UPDATE` trigger advances `blockchain_status` to `submitted`. On-chain finality
  (`confirmed`, `on_chain_confirmed`) is recorded later by a **separate confirmer**, not this service.

### SECURITY — known production gap
`NatsMintGateway` sends the mint envelope **unsigned** with a spoofable `service_identity`. The
bridge accepts this **only in dev** (signature enforcement off). In production the bridge MUST
enforce signing and meter-service MUST attach an `EnvelopeAuth` (its mTLS client cert). Tracked as
a production-hardening follow-up — see the SECURITY note in `crates/meter-persistence/src/infra/mint.rs`.

### Type mirroring (keep in sync — there's no shared crate by design)
To stay chain-light, two wire types are **duplicated** locally and must track their upstream:
- `MintForwardReading` (`reading_consumer.rs`) ↔ `aggregator_core::models::MintForwardReading`.
- `MintEnergyMessage` / `MintEnergyResultMessage` (`infra/mint.rs`) ↔ `gridtokenx_blockchain_core::rpc::nats_schema`.

---

## Configuration (env, via `meter-core/src/config.rs`)

| Var | Default | Purpose |
| --- | --- | --- |
| `JWT_SECRET` | — (**required**) | HS256 secret; must equal the value IAM signs tokens with. |
| `DATABASE_URL` | `…@postgres:5432/gridtokenx` | Shared `gridtokenx` Postgres. |
| `METER_SERVICE_PORT` / `PORT` | `8080` | Bind port (binds `0.0.0.0`). |
| `NATS_URL` | unset → consumer + mint disabled | NATS for the device consumer and Chain Bridge mint. |
| `METER_SERVICE_NATS_SUBJECT` | `meter.reading` | Subject the aggregator forwards mintable readings on. |
| `MINT_VIA_CHAIN_BRIDGE` | `false` | When true **and** `NATS_URL` set, mint via Chain Bridge; else minting is disabled (503). |
| `CHAIN_BRIDGE_SERVICE_IDENTITY` | `spiffe://gridtokenx.th/prod/meter-service` | SPIFFE identity asserted to Chain Bridge for mint RBAC. |

Dockerfile: multi-stage `rust:1-bookworm` → `debian:bookworm-slim`, exposes `8080`, healthcheck on `/health`.
