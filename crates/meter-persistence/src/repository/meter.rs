//! `PostgreSQL` meter repository. Ownership is by `user_id` (`meters.user_id` → users.id).
//! Timestamps are rendered RFC-3339 in SQL so they map straight into `String`.

use async_trait::async_trait;
use sqlx::PgPool;
use uuid::Uuid;

use meter_core::domain::meter::{Meter, MeterReading, MeterStats, RegisterMeterRequest};
use meter_core::error::{ApiError, Result};
use meter_core::traits::MeterRepositoryTrait;

const TS_FMT: &str = r#"YYYY-MM-DD"T"HH24:MI:SS.MS"Z""#;

/// Read-only derivation of a reading's token-mint status from the shared
/// table's dormant blockchain columns. This service never writes these columns
/// (other services / history do); it only projects them for the dashboard.
/// Order matters: minted wins over denied wins over pending.
const MINT_STATUS_CASE: &str = "CASE
    WHEN COALESCE(minted, false) OR COALESCE(on_chain_confirmed, false) THEN 'minted'
    WHEN blockchain_status = 'failed' OR blockchain_last_error IS NOT NULL THEN 'denied'
    ELSE 'pending'
 END";

/// Projection for a [`Meter`] row, joined to its owner for the wallet address.
fn meter_select(filter: &str) -> String {
    format!(
        "SELECT m.id,
                m.serial_number,
                COALESCE(m.meter_type, 'smart_meter') AS meter_type,
                COALESCE(m.location, '')              AS location,
                COALESCE(m.is_verified, false)        AS is_verified,
                COALESCE(u.wallet_address, '')        AS wallet_address,
                m.latitude, m.longitude, m.zone_id
         FROM meters m
         JOIN users u ON u.id = m.user_id
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
        // filtered to readings whose mint is resolved (minted or denied).
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
                    {MINT_STATUS_CASE}                                  AS mint_status,
                    mint_tx_signature                                   AS mint_tx_signature
             FROM meter_readings
             WHERE COALESCE(minted, false) OR COALESCE(on_chain_confirmed, false)
                OR blockchain_status = 'failed' OR blockchain_last_error IS NOT NULL
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
                                   AND NOT (blockchain_status = 'failed' OR blockchain_last_error IS NOT NULL))::int8               AS pending_count
             FROM meter_readings
             WHERE user_id = $1"
        );

        let stats = sqlx::query_as::<_, MeterStats>(&sql)
            .bind(user_id)
            .fetch_one(&self.pool)
            .await?;

        Ok(stats)
    }

    async fn register_meter(&self, user_id: Uuid, req: &RegisterMeterRequest) -> Result<Meter> {
        let id: Uuid = sqlx::query_scalar(
            "INSERT INTO meters (user_id, serial_number, meter_type, location, latitude, longitude)
             VALUES ($1, $2, $3, $4, $5, $6)
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

    async fn insert_reading(
        &self,
        user_id: Uuid,
        meter_serial: &str,
        kwh: f64,
        wallet_address: &str,
        timestamp: Option<&str>,
    ) -> Result<MeterReading> {
        let id: Uuid = sqlx::query_scalar(
            "INSERT INTO meter_readings
                (id, user_id, wallet_address, meter_serial, kwh_amount,
                 timestamp, energy_generated, minted)
             VALUES
                (gen_random_uuid(), $1, $2, $3, $4,
                 COALESCE($5::timestamptz, now()), $4, false)
             RETURNING id",
        )
        .bind(user_id)
        .bind(wallet_address)
        .bind(meter_serial)
        .bind(kwh)
        .bind(timestamp)
        .fetch_one(&self.pool)
        .await?;

        let reading = sqlx::query_as::<_, MeterReading>(&reading_select("id = $1"))
            .bind(id)
            .fetch_one(&self.pool)
            .await?;

        Ok(reading)
    }

    async fn ping(&self) -> Result<()> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }
}
