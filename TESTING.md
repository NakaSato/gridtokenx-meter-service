# TESTING.md — what to test, how

Map of **critical point → exact test command**, so the next change is verified at the
right spot instead of guessing. Test-first rule (root `CLAUDE.md`) applies: run the
narrowest test covering your change, widen if needed, report the real result.

> SQLx is runtime-checked here — unit tests need **no `DATABASE_URL`, no `.sqlx` cache**.
> This service has no NATS / blockchain / Solana dependency. Unit tests are pure. There is
> also a **DB-gated HTTP e2e suite** (`#[ignore]` by default — see below), plus two
> infra-free router tests (auth + malformed-body) that run with the unit tests.

## Quick

```bash
cargo check                  # compile only, fast feedback
cargo test                   # unit + infra-free router tests (DB-gated e2e is #[ignore], skipped)
```

## CI gate (`.github/workflows/ci.yml`)

Always-on, infra-free. **Hard gates**: `cargo fmt --all --check`,
`cargo clippy --all-targets -- -D warnings` (matches the repo `just clippy` bar), and
`cargo test --workspace`. The `#[ignore]` DB suite is **not** run here
(shared IAM-owned, partitioned schema) — it runs against a live stack (root e2e Tier-2 / manual).

## Unit tests — where they live

### `meter-logic` — business rules (`src/meter_service.rs`, fake repo)

Pure, mock-free layer — no DB. `cargo test -p meter-logic` runs all of these.

| Critical point | What's asserted | Test |
| --- | --- | --- |
| page clamp | `limit` clamps to `1..=500`, negative `offset` → 0 | `list_readings_clamps_to_500_1_and_0` |
| register validation | blank serial → `BadRequest`; serial stored trimmed | `register_meter_rejects_empty_serial`, `register_meter_persists_trimmed_serial` |
| kWh validation | negative / non-finite kWh → `BadRequest`; `0.0` is the accepted lower bound | `submit_rejects_negative_kwh`, `submit_rejects_non_finite_kwh`, `submit_accepts_zero_kwh_boundary` |
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
unit-testable without a DB — all three branches are covered by the DB-gated e2e suite below
(`http_e2e_mint_status_minted_and_denied` synthetically injects a minted + a denied row).

## HTTP e2e suite (DB-gated, `#[ignore]`)

`bin/meter-service/tests/e2e_http.rs` drives the **real router in-process** (via
`tower::ServiceExt::oneshot`, no socket) against live Postgres. Each test is self-contained —
borrows an existing wallet-bearing user, uses a throwaway serial, and deletes the meter +
readings it creates (even on assertion failure). Energy fields on injected rows are left `0`
so they never perturb other tests' SUM-aggregate deltas under parallel runs.

| Test | Critical point |
| --- | --- |
| `http_e2e_register_submit_stream_stats` | Full flow: register → submit → SSE → list → stats; fresh reading is `pending`. |
| `http_e2e_mint_status_minted_and_denied` | **minted + denied** branches: inject one minted (tx sig) + one denied (`blockchain_status='failed'`) row, assert list `mint_status` + `mint_tx_signature` and stats `minted_count` / `denied_count`. |
| `http_e2e_wallet_fallback_and_serial_norm` | Padded serial + blank wallet → stored row credits owner wallet, serial stored trimmed. |
| `http_e2e_my_meters` | Registered meter appears in `GET /me/meters`. |
| `http_e2e_error_paths` | Negative kWh → 400, unknown serial → 404, duplicate serial → 409. |
| `http_e2e_multi_user_isolation` | List scoping + SSE per-user filter (B's reading never reaches A). |
| `http_e2e_cross_user_submit_forbidden` | **Authz**: A submits to B's serial → 404, and no row leaks under B's meter. |
| `http_e2e_reading_fields_and_aggregates` | Optional energy fields project through; stats SUMs move by exact injected delta. |
| `http_e2e_pagination` | `limit` page cap, over-max clamp, negative offset clamp. |

Two more tests in the same file are **infra-free** (lazy pool, no live DB, run with `cargo test`):
`auth_rejects_bad_tokens` (missing / garbage / wrong-sig / expired JWT → 401) and
`malformed_body_and_unknown_route` (malformed JSON → 400, unknown route → 404).

```bash
DATABASE_URL=postgresql://gridtokenx_user:gridtokenx_password@127.0.0.1:7001/gridtokenx \
JWT_SECRET=dev-jwt-secret-key-minimum-32-characters-long-for-development-2025 \
cargo test -p gridtokenx-meter-service --test e2e_http -- --ignored --nocapture
```

> All three `mint_status` branches are now proven automatically — the `denied` row no longer
> needs hand-injection (`http_e2e_mint_status_minted_and_denied` forges it and cleans up).
> Tests `SKIP` (printing a notice, not failing) when the DB has no wallet-bearing user, or
> fewer than two for the isolation / cross-user tests.

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
