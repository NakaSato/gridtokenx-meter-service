//! Core trait definitions for the meter service.
//!
//! Defines the persistence interface, allowing a decoupled implementation
//! (wired in `bin/meter-service`) and easy mocking.

use async_trait::async_trait;
use uuid::Uuid;

use crate::domain::meter::{Meter, MeterReading, MeterStats, RegisterMeterRequest};
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

    /// Aggregates production / consumption stats for the user.
    async fn user_stats(&self, user_id: Uuid) -> Result<MeterStats>;

    /// Registers a new meter for the user and returns the stored row.
    async fn register_meter(&self, user_id: Uuid, req: &RegisterMeterRequest) -> Result<Meter>;

    /// Looks up one of the user's meters by serial number.
    async fn find_meter_by_serial(&self, user_id: Uuid, serial: &str) -> Result<Option<Meter>>;

    /// Inserts a reading for the user and returns the stored row.
    async fn insert_reading(
        &self,
        user_id: Uuid,
        meter_serial: &str,
        kwh: f64,
        wallet_address: &str,
        timestamp: Option<&str>,
    ) -> Result<MeterReading>;
}
