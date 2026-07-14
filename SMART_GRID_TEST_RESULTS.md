# Smart-Grid Conformance — Test Results

> **Scope of this claim.** This is a *functional* test report for
> `gridtokenx-meter-service`. The tests verify behaviors that **align with**
> recognized smart-grid standard concepts (metering data model, measurement
> integrity, time encoding, secure identity, locational net flow, eventing).
> They are **not** an accredited conformance certification against any standard,
> and this service is one component — full-stack metering conformance (e.g.
> DLMS/COSEM wire protocol) is owned upstream by the Aggregator Bridge, noted
> per row below.

Generated: 2026-06-19 · Branch: `main` · Toolchain: `cargo test --workspace`
(runtime-checked SQLx, no DB needed to compile).

---

## 1. Aggregate result (real run)

| Suite | Passed | Failed | Ignored |
| --- | --- | --- | --- |
| `meter-logic` (business rules) | 15 | 0 | 0 |
| `meter-service` bin (`mint_poller`) | 5 | 0 | 0 |
| `meter-api` (SSE filter) | 2 | 0 | 0 |
| `meter-core` (config) | 1 | 0 | 0 |
| `e2e_http` (router, infra-free subset) | 7 | 0 | **18** |
| **Total** | **30** | **0** | **18** |

- **30 passed, 0 failed.** Gates green: `cargo fmt --all --check`,
  `cargo clippy --all-targets -- -D warnings` (pedantic, `unwrap_used = deny`).
- **17 ignored** = the `#[ignore]` DB-gated `e2e_http` cases. They require a live
  shared `gridtokenx` Postgres (IAM-owned, partitioned schema) and are **not run
  in CI** — so the standard areas that depend only on them are marked
  **DB-gated (not executed here)** below, honestly.

---

## 2. Standard-area conformance map

Each row: the standard concept, the behavior exercised, the backing tests, and
whether those tests actually ran in this report.

### 2.1 Metering data model — IEC 61968/61970 (CIM), ANSI C12.19 (table semantics)

Meter as an addressable asset with a unique serial, owner, type, location, and
geo-coordinates.

| Behavior | Tests | Ran |
| --- | --- | --- |
| Unique serial; empty/whitespace rejected; serial normalized (trimmed) and canonicalized | `register_meter_rejects_empty_serial`, `register_meter_persists_trimmed_serial` | ✅ |
| Geo-coordinates persisted with the asset | `http_e2e_register_persists_lat_lon` | ⏸ DB-gated |
| Owner-scoped asset listing | `http_e2e_my_meters` | ⏸ DB-gated |

### 2.2 Measurement integrity — IEC 62056 / DLMS-COSEM (data semantics), ANSI C12.19

Energy register values are validated before persistence. **Note:** the
DLMS/COSEM *wire protocol* itself is decoded upstream in the Aggregator Bridge;
this service validates the resulting register value.

| Behavior | Tests | Ran |
| --- | --- | --- |
| Reject negative kWh | `submit_rejects_negative_kwh` | ✅ |
| Reject non-finite (NaN/Inf) kWh | `submit_rejects_non_finite_kwh` | ✅ |
| Accept `0.0` kWh boundary (zero is valid, not an error) | `submit_accepts_zero_kwh_boundary` | ✅ |
| Reading rejected for unknown meter | `submit_unknown_meter_is_not_found` | ✅ |
| Path serial trimmed before lookup/persist (padded value still resolves) | `submit_trims_path_serial_before_persisting` | ✅ |
| Full reading field projection + aggregates | `http_e2e_reading_fields_and_aggregates` | ⏸ DB-gated |

### 2.3 Time encoding — RFC 3339 / ISO 8601 (referenced by IEEE 2030.5)

All timestamps rendered UTC RFC-3339; caller may supply an explicit reading
time, else server time.

| Behavior | Tests | Ran |
| --- | --- | --- |
| Explicit caller timestamp honored | `http_e2e_submit_explicit_timestamp` | ⏸ DB-gated |
| Newest-first ordering + `last_reading_time` | `http_e2e_list_ordering_and_last_reading_time` | ⏸ DB-gated |

### 2.4 Secure identity & access — NIST SP 1108 (NISTIR 7628 cybersecurity), IEEE 2030.5 identity

Per-user JWT identity; strict cross-tenant isolation.

| Behavior | Tests | Ran |
| --- | --- | --- |
| Bad/forged/expired token → 401 | `auth_rejects_bad_tokens` | ✅ |
| `JWT_SECRET` is hard-required; config defaults otherwise | `config::from_env_defaults_fallbacks_and_required_secret` | ✅ |
| SSE stream filtered to the owning user only | `sse_event_filtered_for_other_user`, `sse_event_emitted_for_owning_user` | ✅ |
| Cross-user reading submit forbidden | `http_e2e_cross_user_submit_forbidden` | ⏸ DB-gated |
| Multi-user data isolation | `http_e2e_multi_user_isolation` | ⏸ DB-gated |

### 2.5 Locational net flow — IEEE 1547 (DER interconnection), zone net metering, `M_zone` incentive signal

Per-zone production/consumption and **net flow** (`produced − consumed`): the
locational signal the `M_zone` incentive multiplier acts on. `net_flow > 0` =
net-exporter zone; `< 0` = net-importer.

| Behavior | Tests | Ran |
| --- | --- | --- |
| Per-zone flow surfaced; net-export vs net-import sign distinguished; unzoned group handled | `my_stats_surfaces_per_zone_flow` | ✅ |
| SQL `meter_readings`→`meters` join + `GROUP BY zone_id` net-flow arithmetic over real rows (exact per-zone totals, net-export/import sign, `net_flow = produced − consumed` invariant) | `http_e2e_stats_per_zone_net_flow` | ⏸ DB-gated |

### 2.6 Tokenization & settlement provenance — REC/GO custody chain (audit trail of mint status)

Read-only mint status derived from the shared table's blockchain columns;
transitions pushed to clients out-of-band.

| Behavior | Tests | Ran |
| --- | --- | --- |
| `minted`/`pending`/`denied` derivation + precedence | `http_e2e_mint_status_minted_and_denied`, `http_e2e_mint_status_alt_predicates_and_precedence` | ⏸ DB-gated |
| Transition detection is correct & idempotent (no replay) | `mint_poller::changed_status_is_a_transition`, `newly_resolved_reading_is_a_transition`, `unchanged_status_is_not_re_emitted`, `seen_map_is_bounded_to_current_snapshot`, `disabled_poller_is_a_noop` | ✅ |
| Transition reaches the SSE stream | `http_e2e_mint_poller_pushes_transition_to_sse` | ⏸ DB-gated |

### 2.7 Realtime eventing — IEEE 2030.5 subscription/notification pattern

Server-Sent Events fan-out, resilient to slow clients.

| Behavior | Tests | Ran |
| --- | --- | --- |
| Lagged subscriber skips missed events, stream not closed | `sse_lagged_subscriber_skips_not_closes` | ✅ |
| Multiple subscribers for one user all receive | `sse_multiple_subscribers_same_user_both_receive` | ✅ |

### 2.8 Availability & API robustness — NIST SP 1108 (availability), operational hardening

| Behavior | Tests | Ran |
| --- | --- | --- |
| Readiness reflects DB reachability (`200`/`503`) | `ready_returns_200_when_db_reachable`, `ready_returns_503_when_db_unreachable`, `check_ready_ok_when_store_reachable`, `check_ready_errors_when_store_unreachable` | ✅ |
| Liveness body + CORS | `health_ok_body_and_cors_header` | ✅ |
| Malformed body / unknown route → 400/404 | `malformed_body_and_unknown_route` | ✅ |
| DB error mapped to 500 (no leak) | `db_error_maps_to_500` | ✅ |
| Pagination metadata headers correct | `readings_headers_report_pagination`, `http_e2e_pagination` | partial (e2e ⏸) |

---

## 3. Honest gaps

1. **Zone net-flow SQL has both layers covered, but the DB layer is not run in
   CI.** Service pass-through/sign semantics run infra-free
   (`my_stats_surfaces_per_zone_flow`); the `GROUP BY m.zone_id` arithmetic + join
   are now asserted by `http_e2e_stats_per_zone_net_flow` — **DB-gated**, so it
   only validates against a live Postgres (`-- --ignored`), not in CI.
2. **DLMS/COSEM wire-protocol conformance is out of scope here** — owned by the
   Aggregator Bridge. This service tests register-value semantics only.
3. **No accredited certification.** These are functional tests aligned with
   standard *concepts*; they are not a formal conformance suite (e.g. IEEE 2030.5
   CSIP, ANSI C12.22 test vectors).
4. **18 e2e cases not executed in this report** (no live DB). Run against the
   stack per `TESTING.md` to validate the ⏸ rows end-to-end.

---

## 4. Reproduce

```bash
cd gridtokenx-meter-service
cargo test --workspace                 # the 30 infra-free tests in this report
cargo fmt --all --check
cargo clippy --all-targets -- -D warnings
# DB-gated e2e (live shared Postgres required — see TESTING.md):
# cargo test --workspace -- --ignored
```
