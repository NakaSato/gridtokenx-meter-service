-- CONTRACT (read-only) — public.user_wallet_read_model
--
-- This is NOT a migration and is applied by nothing. It is meter-service's copy
-- of a table it READS but does not own, kept byte-identical to the writer's
-- definition by `scripts/check-metering-ddl-sync.sh` (`just check-ddl-sync`) so
-- a shape change on the writing side fails a guard here instead of surfacing as
-- a runtime SQL error in this service.
--
-- Why this table is read at all
-- -----------------------------
-- Under DB-per-service Phase 2 rec-A the metering bounded context shares ONE
-- physical database (`gridtokenx_meter`). Inside it, meter-service is the sole
-- writer of `meters`/`meter_registry`/`meter_verification_attempts` (see
-- ../migrations/0001_meter_registry.sql) and the aggregator owns everything
-- else. Wallets, however, belong to neither: IAM owns them, and both services
-- need user -> primary wallet. Rather than run two Kafka consumers building two
-- near-identical projections of the same IAM events, the context keeps ONE
-- projection:
--
--   * WRITER  — gridtokenx-aggregator-bridge, `OwnerReadModelConsumer`
--               (crates/aggregator-persistence/src/infra/owner_read_model.rs),
--               fed by IAM `iam.user.events`. Canonical DDL:
--               gridtokenx-aggregator-bridge/migrations/0007_user_wallet_read_model.sql
--   * READERS — the aggregator (wallet fill in upsert_by_serial) and this
--               service (owner wallet in `meter_select` / `list_map_meters`,
--               crates/meter-persistence/src/repository/meter.rs).
--
-- meter-service reads this ONE table and no other table it does not own. It
-- deliberately does NOT read `meter_owner_read_model` — that is the aggregator's
-- private serial->(user, wallet) projection, built by consuming the very
-- `MeterRegistered` events this service emits, so reading it would be circular
-- (asking another service to re-derive our own `meters.user_id`) as well as
-- async-lagged. The owner join keys on the locally-owned `meters.user_id`.
--
-- Residual debt: a shared-DB contract table is still weaker than one-writer-
-- per-database. Full closure needs rec-B (separate physical DBs per service) —
-- tracked as TD-004 in ../../docs/exec-plans/tech-debt-tracker.md.

CREATE TABLE public.user_wallet_read_model (
    user_id         uuid NOT NULL,
    wallet_address  character varying(88),
    updated_at      timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT user_wallet_read_model_pkey PRIMARY KEY (user_id)
);

COMMENT ON TABLE public.user_wallet_read_model IS
    'Local read-model: user_id -> primary custodial wallet (base58). Written by IAM user events (UserOnboarded/UserWalletLinked/UserWalletPrimaryChanged/UserWalletUnlinked) independent of meter ownership, so a wallet event arriving before the user''s first meter row is never lost. Consulted by upsert_by_serial to fill wallets missing from meter events.';
COMMENT ON COLUMN public.user_wallet_read_model.wallet_address IS
    'Primary wallet (base58). NULLable: an unlink clears it; a NULL means "user currently has no mint-recipient wallet".';
