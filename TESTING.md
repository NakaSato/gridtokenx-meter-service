# TESTING.md — what to test, how

Map of **critical point → exact test command**, so the next change is verified at the
right spot instead of guessing. Test-first rule (root `CLAUDE.md`) applies: run the
narrowest test covering your change, widen if needed, report the real result.

> SQLx is runtime-checked here — unit tests need **no `DATABASE_URL`, no `.sqlx` cache**.
> Integration tests need live infra and are `#[ignore]` by default.

## Quick

```bash
cargo check                  # compile only, fast feedback
cargo test                   # all unit tests (no infra needed)
```

## Unit tests — where they live

### `meter-persistence` — mint id / window math (`src/infra/mint.rs`)

| Critical point | Why it matters | Command |
| --- | --- | --- |
| 15-min window floor | mint idempotency key `mint:{serial}:{window_start_ms}` + on-chain `(meter_id, window_start_ms)` PDA must be stable per (meter, window) and match the aggregator billing window (`WINDOW_MS`) | `cargo test -p meter-persistence window_floors_to_15_min -- --nocapture` |
| UUID serial → meter_id bytes | a UUID serial maps to its own 16 bytes | `cargo test -p meter-persistence meter_id_uses_uuid_bytes_when_serial_is_uuid` |
| non-UUID serial → stable v5 id | non-UUID serial maps to a stable, collision-distinct id | `cargo test -p meter-persistence meter_id_falls_back_to_stable_v5_for_non_uuid` |
| all mint-id / window logic | run the whole crate after touching `mint.rs` | `cargo test -p meter-persistence` |

### `meter-logic` — business rules (`src/meter_service.rs`, fake repo + mint)

Pure, mock-free layer — no DB/NATS. `cargo test -p meter-logic` runs all of these.

| Critical point | What's asserted | Test |
| --- | --- | --- |
| page clamp | `limit` clamps to `1..=500`, negative `offset` → 0 | `list_readings_clamps_to_500_1_and_0` |
| register validation | blank serial → `BadRequest`; serial stored trimmed (else NATS readings drop) | `register_meter_rejects_empty_serial`, `register_meter_persists_trimmed_serial` |
| kWh validation | negative / non-finite kWh → `BadRequest` (both paths) | `submit_rejects_negative_kwh`, `submit_rejects_non_finite_kwh`, `ingest_rejects_negative_kwh` |
| meter ownership | unknown serial → `NotFound`; unregistered device serial → `NotFound` | `submit_unknown_meter_is_not_found`, `ingest_unregistered_serial_is_not_found` |
| serial normalization | submit trims the path serial before lookup/persist (symmetry with registration) | `submit_trims_path_serial_before_persisting` |
| wallet fallback | blank request wallet → owner wallet; present → request wallet; none anywhere → `BadRequest` | `submit_falls_back_to_owner_wallet_when_request_blank`, `submit_uses_request_wallet_when_present`, `submit_rejects_when_no_wallet_anywhere` |
| device wallet trust | device path credits the **owner's** wallet, never an off-wire value | `ingest_credits_owner_wallet_not_wire_value` |
| mint best-effort | reading persisted even when mint fails; success marks minted + signature | `submit_auto_mint_failure_still_persists_reading`, `submit_with_auto_mint_marks_minted`, `submit_without_auto_mint_persists_unminted` |
| idempotent ingest | mint `Unavailable` → `false` (deferred); already-minted redelivery → `true` | `ingest_mint_unavailable_defers_but_succeeds`, `ingest_already_minted_is_idempotent_success` |
| mint guards | already minted → `Conflict`; empty wallet → `BadRequest` | `mint_existing_already_minted_conflicts`, `mint_existing_rejects_empty_wallet` |

## Integration tests — live infra, `#[ignore]`

In `bin/meter-service/tests/`. Package: `gridtokenx-meter-service`.

| Test | Verifies | Needs |
| --- | --- | --- |
| `mint_e2e::mint_path_writes_onchain_tracking_columns` | mint path persists reading then writes `blockchain_*` tracking columns | Postgres + NATS + registered serial (fake Chain Bridge responder) |
| `mint_onchain_e2e::real_onchain_mint_writes_tracking_columns` | REAL on-chain mint writes tracking columns | Postgres + NATS + live Chain Bridge + Solana validator (`./scripts/app.sh init` first) |

```bash
# Stage 2B/3 — fake Chain Bridge over NATS, no Solana/Vault:
DATABASE_URL=postgresql://gridtokenx_user:gridtokenx_password@127.0.0.1:7001/gridtokenx \
NATS_URL=nats://127.0.0.1:9020 TEST_METER_SERIAL=<registered-serial> \
cargo test -p gridtokenx-meter-service --test mint_e2e -- --ignored --nocapture

# Stage 2C — real on-chain mint:
DATABASE_URL=postgresql://gridtokenx_user:gridtokenx_password@127.0.0.1:7001/gridtokenx \
NATS_URL=nats://127.0.0.1:9020 TEST_METER_SERIAL=<registered-serial> \
cargo test -p gridtokenx-meter-service --test mint_onchain_e2e -- --ignored --nocapture
```
