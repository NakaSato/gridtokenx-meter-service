//! `PostgreSQL` meter repository. Ownership is by `user_id` (`meters.user_id` → users.id).
//! Timestamps are rendered RFC-3339 in SQL so they map straight into `String`.

use async_trait::async_trait;
use sqlx::PgPool;
use uuid::Uuid;

use meter_core::domain::meter::{
    Meter, MeterMapPoint, MeterReading, MeterStats, RegisterMeterRequest, ZoneFlow,
};
use meter_core::error::{ApiError, Result};
use meter_core::traits::MeterRepositoryTrait;

const TS_FMT: &str = r#"YYYY-MM-DD"T"HH24:MI:SS.MS"Z""#;

/// Read-only derivation of a reading's token-mint status from the shared
/// table's dormant blockchain columns. This service never writes these columns
/// (other services / history do); it only projects them for the dashboard.
/// Order matters: minted wins over denied wins over `not_applicable` wins over
/// pending. `blockchain_status = 'no_surplus'` is written by the Aggregator
/// Bridge's settlement sink when a reading's 15-min billing window closes with
/// net consumption (nothing was ever going to mint) — without this branch such
/// a reading falls into the `ELSE 'pending'` case and never leaves "Pending" in
/// the trading UI (see `aggregator-persistence::infra::pg_readings::mark_no_surplus`).
const MINT_STATUS_CASE: &str = "CASE
    WHEN COALESCE(minted, false) OR COALESCE(on_chain_confirmed, false) THEN 'minted'
    WHEN blockchain_status = 'failed' OR blockchain_last_error IS NOT NULL THEN 'denied'
    WHEN blockchain_status = 'no_surplus' THEN 'not_applicable'
    ELSE 'pending'
 END";

/// Projection for a [`Meter`] row, joined to its owner for the wallet address.
fn meter_select(filter: &str) -> String {
    format!(
        "SELECT m.id,
                m.serial_number,
                COALESCE(m.meter_type, 'smart_meter')              AS meter_type,
                COALESCE(m.location, '')                           AS location,
                COALESCE(m.is_verified, false)                     AS is_verified,
                COALESCE(uw.wallet_address, '')                     AS wallet_address,
                m.latitude, m.longitude, m.zone_id
         FROM meters m
         -- DB-per-service Phase 2: the owner wallet comes from the durable
         -- user->primary-wallet edge, keyed on the LOCALLY-OWNED `meters.user_id`.
         --
         -- This deliberately does NOT join `meter_owner_read_model`. That table is
         -- the AGGREGATOR's private serial->(user, wallet) projection, which it
         -- builds by consuming the very `MeterRegistered` events this service
         -- emits — so reading it here is circular: meter-service asking another
         -- service for a re-derivation of its own `meters.user_id`. It is also
         -- fed ASYNC, so it is empty at the instant of registration and blanked a
         -- just-registered meter's wallet (the reason the user-edge fallback was
         -- added in c6aa96b). Keying on the local user_id removes both problems.
         --
         -- `user_wallet_read_model` is the one genuinely foreign fact left: IAM
         -- owns wallets. It is a metering-context SHARED contract table (written
         -- solely by the aggregator's IAM-event feed, read by both services) —
         -- see migrations/0001_meter_registry.sql and TD-004.
         LEFT JOIN user_wallet_read_model uw ON uw.user_id = m.user_id
         WHERE {filter}"
    )
}

/// Projection for a [`MeterReading`] row.
fn reading_select(filter: &str) -> String {
    format!(
        "SELECT id,
                COALESCE(meter_serial, '')                          AS meter_serial,
                COALESCE(kwh_amount, 0)::float8                     AS kwh,
                to_char(timestamp  AT TIME ZONE 'UTC', '{TS_FMT}')  AS timestamp,
                to_char(created_at AT TIME ZONE 'UTC', '{TS_FMT}')  AS submitted_at,
                energy_generated::float8                            AS energy_generated,
                energy_consumed::float8                             AS energy_consumed,
                voltage::float8                                     AS voltage,
                current::float8                                     AS current,
                surplus_energy::float8                              AS surplus_energy,
                deficit_energy::float8                              AS deficit_energy,
                power_factor::float8                                AS power_factor,
                frequency::float8                                   AS frequency,
                temperature::float8                                 AS temperature,
                battery_level::float8                               AS battery_level,
                weather_condition                                  AS weather_condition,
                latitude::float8                                    AS latitude,
                longitude::float8                                   AS longitude,
                rec_eligible                                       AS rec_eligible,
                carbon_offset::float8                               AS carbon_offset,
                max_sell_price::float8                              AS max_sell_price,
                max_buy_price::float8                               AS max_buy_price,
                meter_signature                                    AS meter_signature,
                meter_type                                         AS meter_type,
                {MINT_STATUS_CASE}                                  AS mint_status,
                mint_tx_signature                                   AS mint_tx_signature
         FROM meter_readings
         WHERE {filter}"
    )
}

/// Row for [`MeterRepositoryTrait::list_resolved_mint_readings`]: a reading plus
/// its owning `user_id` (which `MeterReading` itself does not carry).
#[derive(sqlx::FromRow)]
struct ResolvedMintRow {
    user_id: Uuid,
    #[sqlx(flatten)]
    reading: MeterReading,
}

/// Flat aggregate row for [`MeterRepositoryTrait::user_stats`]; the per-zone
/// `zones` vec is fetched separately and assembled into [`MeterStats`].
#[derive(sqlx::FromRow)]
struct StatsTotals {
    total_produced: f64,
    total_consumed: f64,
    last_reading_time: Option<String>,
    minted_count: i64,
    pending_count: i64,
    denied_count: i64,
}

/// SQLx-backed implementation of [`MeterRepositoryTrait`].
pub struct MeterRepository {
    pool: PgPool,
}

impl MeterRepository {
    /// Creates a new repository over the given connection pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl MeterRepositoryTrait for MeterRepository {
    async fn list_user_meters(&self, user_id: Uuid) -> Result<Vec<Meter>> {
        let sql = format!(
            "{} ORDER BY m.created_at DESC",
            meter_select("m.user_id = $1")
        );
        let meters = sqlx::query_as::<_, Meter>(&sql)
            .bind(user_id)
            .fetch_all(&self.pool)
            .await?;

        Ok(meters)
    }

    async fn list_map_meters(&self) -> Result<Vec<MeterMapPoint>> {
        // All users, located meters only (lat/lng present). Same owner join as
        // `meter_select`, but lat/lng are non-null here so they map to `f64`.
        let sql = "SELECT m.id,
                          m.serial_number,
                          COALESCE(m.meter_type, 'smart_meter') AS meter_type,
                          COALESCE(m.location, '')              AS location,
                          COALESCE(m.is_verified, false)        AS is_verified,
                          COALESCE(uw.wallet_address, '')       AS wallet_address,
                          m.latitude::float8                    AS latitude,
                          m.longitude::float8                   AS longitude,
                          m.zone_id
                   FROM meters m
                   -- Same owner join as `meter_select`: the user->wallet edge keyed
                   -- on the locally-owned m.user_id, never the aggregator's
                   -- serial-keyed projection. This site previously had NO fallback,
                   -- so a meter whose serial row had not yet been fed showed a blank
                   -- wallet on the map even when the owner's wallet was known.
                   LEFT JOIN user_wallet_read_model uw ON uw.user_id = m.user_id
                   WHERE m.latitude IS NOT NULL AND m.longitude IS NOT NULL
                   ORDER BY m.created_at DESC";
        let points = sqlx::query_as::<_, MeterMapPoint>(sql)
            .fetch_all(&self.pool)
            .await?;

        Ok(points)
    }

    async fn list_user_readings(
        &self,
        user_id: Uuid,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<MeterReading>> {
        let sql = format!(
            "{} ORDER BY timestamp DESC LIMIT $2 OFFSET $3",
            reading_select("user_id = $1")
        );
        let readings = sqlx::query_as::<_, MeterReading>(&sql)
            .bind(user_id)
            .bind(limit)
            .bind(offset)
            .fetch_all(&self.pool)
            .await?;

        Ok(readings)
    }

    async fn count_user_readings(&self, user_id: Uuid) -> Result<i64> {
        let total: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM meter_readings WHERE user_id = $1")
                .bind(user_id)
                .fetch_one(&self.pool)
                .await?;
        Ok(total)
    }

    async fn list_resolved_mint_readings(&self, limit: i64) -> Result<Vec<(Uuid, MeterReading)>> {
        // Same reading projection as `reading_select`, plus the owning user_id,
        // filtered to readings whose mint is resolved (minted, denied, or
        // settled with no surplus to mint).
        let sql = format!(
            "SELECT user_id,
                    id,
                    COALESCE(meter_serial, '')                          AS meter_serial,
                    COALESCE(kwh_amount, 0)::float8                     AS kwh,
                    to_char(timestamp  AT TIME ZONE 'UTC', '{TS_FMT}')  AS timestamp,
                    to_char(created_at AT TIME ZONE 'UTC', '{TS_FMT}')  AS submitted_at,
                    energy_generated::float8                            AS energy_generated,
                    energy_consumed::float8                             AS energy_consumed,
                    voltage::float8                                     AS voltage,
                    current::float8                                     AS current,
                    surplus_energy::float8                              AS surplus_energy,
                    deficit_energy::float8                              AS deficit_energy,
                    power_factor::float8                                AS power_factor,
                    frequency::float8                                   AS frequency,
                    temperature::float8                                 AS temperature,
                    battery_level::float8                               AS battery_level,
                    weather_condition                                  AS weather_condition,
                    latitude::float8                                    AS latitude,
                    longitude::float8                                   AS longitude,
                    rec_eligible                                       AS rec_eligible,
                    carbon_offset::float8                               AS carbon_offset,
                    max_sell_price::float8                              AS max_sell_price,
                    max_buy_price::float8                               AS max_buy_price,
                    meter_signature                                    AS meter_signature,
                    meter_type                                         AS meter_type,
                    {MINT_STATUS_CASE}                                  AS mint_status,
                    mint_tx_signature                                   AS mint_tx_signature
             FROM meter_readings
             WHERE COALESCE(minted, false) OR COALESCE(on_chain_confirmed, false)
                OR blockchain_status = 'failed' OR blockchain_last_error IS NOT NULL
                OR blockchain_status = 'no_surplus'
             ORDER BY timestamp DESC
             LIMIT $1"
        );
        let rows = sqlx::query_as::<_, ResolvedMintRow>(&sql)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;

        Ok(rows.into_iter().map(|r| (r.user_id, r.reading)).collect())
    }

    async fn user_stats(&self, user_id: Uuid) -> Result<MeterStats> {
        let sql = format!(
            "SELECT
                COALESCE(SUM(energy_generated), 0)::float8                                 AS total_produced,
                COALESCE(SUM(energy_consumed), 0)::float8                                  AS total_consumed,
                to_char(MAX(timestamp) AT TIME ZONE 'UTC', '{TS_FMT}')                     AS last_reading_time,
                COUNT(*) FILTER (WHERE COALESCE(minted, false) OR COALESCE(on_chain_confirmed, false))::int8                         AS minted_count,
                COUNT(*) FILTER (WHERE blockchain_status = 'failed' OR blockchain_last_error IS NOT NULL)::int8                       AS denied_count,
                COUNT(*) FILTER (WHERE NOT (COALESCE(minted, false) OR COALESCE(on_chain_confirmed, false))
                                   AND NOT COALESCE(blockchain_status = 'failed' OR blockchain_last_error IS NOT NULL, false)
                                   AND COALESCE(blockchain_status, '') != 'no_surplus')::int8 AS pending_count
             FROM meter_readings
             WHERE user_id = $1"
        );

        let totals = sqlx::query_as::<_, StatsTotals>(&sql)
            .bind(user_id)
            .fetch_one(&self.pool)
            .await?;

        // Per-zone energy flow: join each reading to its meter for the zone_id,
        // group by zone (unzoned meters group under NULL, ordered last).
        let zone_sql = "SELECT
                m.zone_id                                                          AS zone_id,
                COALESCE(SUM(r.energy_generated), 0)::float8                        AS total_produced,
                COALESCE(SUM(r.energy_consumed), 0)::float8                         AS total_consumed,
                (COALESCE(SUM(r.energy_generated), 0)
                    - COALESCE(SUM(r.energy_consumed), 0))::float8                  AS net_flow,
                COUNT(*)::int8                                                      AS reading_count
             FROM meter_readings r
             JOIN meters m ON m.user_id = r.user_id AND m.serial_number = r.meter_serial
             WHERE r.user_id = $1
             GROUP BY m.zone_id
             ORDER BY m.zone_id ASC NULLS LAST";
        let zones = sqlx::query_as::<_, ZoneFlow>(zone_sql)
            .bind(user_id)
            .fetch_all(&self.pool)
            .await?;

        Ok(MeterStats {
            total_produced: totals.total_produced,
            total_consumed: totals.total_consumed,
            last_reading_time: totals.last_reading_time,
            minted_count: totals.minted_count,
            pending_count: totals.pending_count,
            denied_count: totals.denied_count,
            zones,
        })
    }

    async fn register_meter(&self, user_id: Uuid, req: &RegisterMeterRequest) -> Result<Meter> {
        // is_verified = true: registration is JWT-scoped to an authenticated user and
        // the device streams Ed25519-signed telemetry the Aggregator Bridge verifies,
        // so the meter is verified at registration. No separate verification flow exists
        // (the old meter_verification_attempts schema is unwired), and the trading UI
        // gates "My Meters" on this flag — leaving it false shows every meter Unverified.
        let id: Uuid = sqlx::query_scalar(
            "INSERT INTO meters (user_id, serial_number, meter_type, location, latitude, longitude, is_verified)
             VALUES ($1, $2, $3, $4, $5, $6, true)
             RETURNING id",
        )
        .bind(user_id)
        .bind(&req.serial_number)
        .bind(req.meter_type.as_deref())
        .bind(req.location.as_deref())
        .bind(req.latitude)
        .bind(req.longitude)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| match &e {
            // 23505 = unique_violation (serial_number already registered)
            sqlx::Error::Database(db) if db.code().as_deref() == Some("23505") => {
                ApiError::Conflict(format!("meter '{}' already registered", req.serial_number))
            }
            _ => ApiError::from(e),
        })?;

        let meter = sqlx::query_as::<_, Meter>(&meter_select("m.id = $1"))
            .bind(id)
            .fetch_one(&self.pool)
            .await?;

        Ok(meter)
    }

    async fn find_meter_by_serial(&self, user_id: Uuid, serial: &str) -> Result<Option<Meter>> {
        let meter =
            sqlx::query_as::<_, Meter>(&meter_select("m.user_id = $1 AND m.serial_number = $2"))
                .bind(user_id)
                .bind(serial)
                .fetch_optional(&self.pool)
                .await?;

        Ok(meter)
    }

    async fn ping(&self) -> Result<()> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }
}
