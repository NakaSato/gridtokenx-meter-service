//! Core trait definitions for the meter service.
//!
//! These traits define the persistence and blockchain interfaces, allowing
//! decoupled implementations (wired in `bin/meter-service`) and easy mocking.

use async_trait::async_trait;
use uuid::Uuid;

use crate::domain::meter::{
    Meter, MeterReading, MeterStats, ReadingMintInfo, ReadingOwner, RegisterMeterRequest,
};
use crate::error::Result;

/// Read/write access to meters and meter readings, scoped by owning `user_id`.
#[async_trait]
pub trait MeterRepositoryTrait: Send + Sync {
    /// Lists meters owned by the given user.
    async fn list_user_meters(&self, user_id: Uuid) -> Result<Vec<Meter>>;

    /// Lists a page of the user's readings, newest first.
    async fn list_user_readings(
        &self,
        user_id: Uuid,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<MeterReading>>;

    /// Aggregates production / consumption / mint stats for the user.
    async fn user_stats(&self, user_id: Uuid) -> Result<MeterStats>;

    /// Registers a new meter for the user and returns the stored row.
    async fn register_meter(
        &self,
        user_id: Uuid,
        req: &RegisterMeterRequest,
    ) -> Result<Meter>;

    /// Looks up one of the user's meters by serial number.
    async fn find_meter_by_serial(
        &self,
        user_id: Uuid,
        serial: &str,
    ) -> Result<Option<Meter>>;

    /// Resolves the owning user + wallet for a meter serial, across all users.
    ///
    /// Used by the device-ingest path (NATS forward), which has no authenticated
    /// user. Returns `None` when the serial is not registered to any user.
    async fn find_owner_by_serial(&self, serial: &str) -> Result<Option<ReadingOwner>>;

    /// Inserts a device-forwarded reading using `reading_id` as the primary key,
    /// making ingest idempotent: a duplicate delivery (same id) is a no-op.
    ///
    /// Returns `true` when a new row was inserted, `false` when a row with that
    /// id already existed.
    async fn insert_device_reading(
        &self,
        reading_id: Uuid,
        user_id: Uuid,
        meter_serial: &str,
        kwh: f64,
        wallet_address: &str,
        timestamp_ms: i64,
    ) -> Result<bool>;

    /// Inserts a reading for the user and returns the stored row.
    async fn insert_reading(
        &self,
        user_id: Uuid,
        meter_serial: &str,
        kwh: f64,
        wallet_address: &str,
        timestamp: Option<&str>,
    ) -> Result<MeterReading>;

    /// Loads the fields needed to mint a reading, scoped to the owner.
    async fn get_reading_for_mint(
        &self,
        user_id: Uuid,
        reading_id: Uuid,
    ) -> Result<Option<ReadingMintInfo>>;

    /// Marks a reading minted, recording its on-chain signature.
    async fn mark_reading_minted(
        &self,
        reading_id: Uuid,
        signature: &str,
    ) -> Result<()>;
}

/// Blockchain mint gateway — submits a kWh mint and returns the tx signature.
///
/// Implemented over Chain Bridge (NATS request-reply). A disabled
/// implementation is wired when no blockchain backend is configured.
#[async_trait]
pub trait MintGateway: Send + Sync {
    /// Mints `kwh` energy tokens to `wallet_address` for the given meter.
    ///
    /// Returns the on-chain transaction signature on success.
    async fn mint(
        &self,
        wallet_address: &str,
        kwh: f64,
        meter_serial: &str,
        timestamp_ms: i64,
    ) -> Result<String>;
}
