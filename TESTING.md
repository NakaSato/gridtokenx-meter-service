# TESTING.md — what to test, how

Map of **critical point → exact test command**, so the next change is verified at the
right spot instead of guessing. Test-first rule (root `CLAUDE.md`) applies: run the
narrowest test covering your change, widen if needed, report the real result.

> SQLx is runtime-checked here — unit tests need **no `DATABASE_URL`, no `.sqlx` cache**.
> This service has no NATS / blockchain / Solana dependency. Unit tests are pure; there
> is also **one DB-gated HTTP e2e test** (`#[ignore]` by default — see below).

## Quick

```bash
cargo check                  # compile only, fast feedback
cargo test                   # all unit tests (the e2e test is #[ignore], skipped here)
```

## Unit tests — where they live

### `meter-logic` — business rules (`src/meter_service.rs`, fake repo)

Pure, mock-free layer — no DB. `cargo test -p meter-logic` runs all of these.

| Critical point | What's asserted | Test |
| --- | --- | --- |
| page clamp | `limit` clamps to `1..=500`, negative `offset` → 0 | `list_readings_clamps_to_500_1_and_0` |
| register validation | blank serial → `BadRequest`; serial stored trimmed | `register_meter_rejects_empty_serial`, `register_meter_persists_trimmed_serial` |
| kWh validation | negative / non-finite kWh → `BadRequest` | `submit_rejects_negative_kwh`, `submit_rejects_non_finite_kwh` |
| meter ownership | unknown serial → `NotFound` | `submit_unknown_meter_is_not_found` |
| serial normalization | submit trims the path serial before lookup/persist (symmetry with registration) | `submit_trims_path_serial_before_persisting` |
| wallet fallback | blank request wallet → owner wallet; present → request wallet; none anywhere → `BadRequest` | `submit_falls_back_to_owner_wallet_when_request_blank`, `submit_uses_request_wallet_when_present`, `submit_rejects_when_no_wallet_anywhere` |

### `meter-api` — realtime SSE filter (`src/handlers/meter.rs`)

`cargo test -p meter-api` runs these.

| Critical point | What's asserted | Test |
| --- | --- | --- |
| SSE per-user filter | a reading for the owning user produces an event | `sse_event_emitted_for_owning_user` |
| SSE isolation | another user's reading is filtered out (`None`) | `sse_event_filtered_for_other_user` |

### `meter-persistence` — mint-status projection (`src/repository/meter.rs`)

`mint_status` is **derived read-only in SQL** from the shared table's dormant blockchain
columns (`MINT_STATUS_CASE`): `minted` → `"minted"`, `blockchain_status='failed'` /
`blockchain_last_error IS NOT NULL` → `"denied"`, else `"pending"`. `user_stats` exposes the
same predicates as `minted_count` / `pending_count` / `denied_count`. Pure-SQL, so it is **not**
unit-testable without a DB — covered by the e2e test below.

## HTTP e2e test (DB-gated, `#[ignore]`)

`bin/meter-service/tests/e2e_http.rs` drives the **real router in-process** (via
`tower::ServiceExt::oneshot`, no socket) against live Postgres: register → submit → SSE →
list → stats. Proves `mint_status` flows through every read path, including the realtime
stream. Self-contained — borrows an existing wallet-bearing user, uses a throwaway serial,
deletes the meter + readings it creates.

```bash
DATABASE_URL=postgresql://gridtokenx_user:gridtokenx_password@127.0.0.1:7001/gridtokenx \
JWT_SECRET=dev-jwt-secret-key-minimum-32-characters-long-for-development-2025 \
cargo test -p gridtokenx-meter-service --test e2e_http -- --ignored --nocapture
```

> `minted` / `pending` are exercised by whatever live data exists; the `denied` branch needs a
> reading with `blockchain_status='failed'` (none in normal flow) — inject one to prove it.

## Manual / runtime check (realtime stream)

The e2e test above covers SSE end-to-end. For an ad-hoc live smoke-test of a running service:

```bash
# Terminal 1 — subscribe to the stream
curl -N -H "Authorization: Bearer <jwt>" \
  http://localhost:8080/api/v1/meters/readings/stream

# Terminal 2 — submit a reading for the same user's meter; it should appear in terminal 1
curl -X POST -H "Authorization: Bearer <jwt>" -H 'content-type: application/json' \
  -d '{"kwh": 1.5}' \
  http://localhost:8080/api/v1/meters/<serial>/readings
```
