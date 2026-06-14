//! PostgreSQL meter repository. Ownership is by `user_id` (meters.user_id → users.id).
//! Timestamps are rendered RFC-3339 in SQL so they map straight into `String`.

use async_trait::async_trait;
use sqlx::PgPool;
use uuid::Uuid;

use meter_core::domain::meter::{
    Meter, MeterReading, MeterStats, ReadingMintInfo, ReadingOwner, RegisterMeterRequest,
};
use meter_core::error::{ApiError, Result};
use meter_core::traits::MeterRepositoryTrait;

const TS_FMT: &str = r#"YYYY-MM-DD"T"HH24:MI:SS.MS"Z""#;

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
                COALESCE(minted, false)                             AS minted,
                mint_tx_signature                                   AS tx_signature,
                energy_generated::float8                            AS energy_generated,
                energy_consumed::float8                             AS energy_consumed,
                voltage::float8                                     AS voltage,
                current::float8                                     AS current
         FROM meter_readings
         WHERE {filter}"
    )
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

    async fn user_stats(&self, user_id: Uuid) -> Result<MeterStats> {
        let sql = format!(
            "SELECT
                COALESCE(SUM(energy_generated), 0)::float8                                 AS total_produced,
                COALESCE(SUM(energy_consumed), 0)::float8                                  AS total_consumed,
                to_char(MAX(timestamp) AT TIME ZONE 'UTC', '{TS_FMT}')                     AS last_reading_time,
                COALESCE(SUM(CASE WHEN minted THEN kwh_amount ELSE 0 END), 0)::float8      AS total_minted,
                COUNT(*) FILTER (WHERE minted)                                             AS total_minted_count,
                COALESCE(SUM(CASE WHEN NOT minted THEN kwh_amount ELSE 0 END), 0)::float8  AS pending_mint,
                COUNT(*) FILTER (WHERE NOT minted)                                         AS pending_mint_count
             FROM meter_readings
             WHERE user_id = $1"
        );

        let stats = sqlx::query_as::<_, MeterStats>(&sql)
            .bind(user_id)
            .fetch_one(&self.pool)
            .await?;

        Ok(stats)
    }

    async fn register_meter(
        &self,
        user_id: Uuid,
        req: &RegisterMeterRequest,
    ) -> Result<Meter> {
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

    async fn find_meter_by_serial(
        &self,
        user_id: Uuid,
        serial: &str,
    ) -> Result<Option<Meter>> {
        let meter = sqlx::query_as::<_, Meter>(&meter_select(
            "m.user_id = $1 AND m.serial_number = $2",
        ))
        .bind(user_id)
        .bind(serial)
        .fetch_optional(&self.pool)
        .await?;

        Ok(meter)
    }

    async fn find_owner_by_serial(&self, serial: &str) -> Result<Option<ReadingOwner>> {
        let owner = sqlx::query_as::<_, ReadingOwner>(
            "SELECT m.user_id                        AS user_id,
                    COALESCE(u.wallet_address, '')    AS wallet_address
             FROM meters m
             JOIN users u ON u.id = m.user_id
             WHERE m.serial_number = $1",
        )
        .bind(serial)
        .fetch_optional(&self.pool)
        .await?;

        Ok(owner)
    }

    async fn insert_device_reading(
        &self,
        reading_id: Uuid,
        user_id: Uuid,
        meter_serial: &str,
        kwh: f64,
        wallet_address: &str,
        timestamp_ms: i64,
    ) -> Result<bool> {
        // meter_readings is partitioned with a composite PK (id, reading_timestamp),
        // so `ON CONFLICT (id)` is unusable. Dedupe with an existence check on the
        // reading id: the consumer drains messages serially, so this is race-free
        // for a single instance, and the mint `minted` guard is the backstop
        // against any double on-chain mint.
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM meter_readings WHERE id = $1)")
                .bind(reading_id)
                .fetch_one(&self.pool)
                .await?;
        if exists {
            return Ok(false);
        }

        sqlx::query(
            "INSERT INTO meter_readings
                (id, user_id, wallet_address, meter_serial, kwh_amount,
                 timestamp, energy_generated, minted)
             VALUES
                ($1, $2, $3, $4, $5,
                 to_timestamp($6::float8 / 1000.0), $5, false)",
        )
        .bind(reading_id)
        .bind(user_id)
        .bind(wallet_address)
        .bind(meter_serial)
        .bind(kwh)
        .bind(timestamp_ms)
        .execute(&self.pool)
        .await?;

        Ok(true)
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

    async fn get_reading_for_mint(
        &self,
        user_id: Uuid,
        reading_id: Uuid,
    ) -> Result<Option<ReadingMintInfo>> {
        let info = sqlx::query_as::<_, ReadingMintInfo>(
            "SELECT id,
                    COALESCE(meter_serial, '')                 AS meter_serial,
                    COALESCE(wallet_address, '')               AS wallet_address,
                    COALESCE(kwh_amount, 0)::float8            AS kwh,
                    (EXTRACT(EPOCH FROM timestamp) * 1000)::int8 AS timestamp_ms,
                    COALESCE(minted, false)                    AS minted
             FROM meter_readings
             WHERE id = $1 AND user_id = $2",
        )
        .bind(reading_id)
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(info)
    }

    async fn mark_reading_minted(
        &self,
        reading_id: Uuid,
        signature: &str,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE meter_readings
             SET minted = true, mint_tx_signature = $2, updated_at = now()
             WHERE id = $1",
        )
        .bind(reading_id)
        .bind(signature)
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}
