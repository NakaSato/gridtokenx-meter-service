//! Core trait definitions for the meter service.
//!
//! Defines the persistence interface, allowing a decoupled implementation
//! (wired in `bin/meter-service`) and easy mocking.

use async_trait::async_trait;
use uuid::Uuid;

use crate::domain::meter::{
    Meter, MeterMapPoint, MeterReading, MeterStats, MeterVerificationAttempt, RegisterMeterRequest,
};
use crate::error::Result;

/// Read/write access to meters and meter readings, scoped by owning `user_id`.
#[async_trait]
pub trait MeterRepositoryTrait: Send + Sync {
    /// Lists meters owned by the given user.
    async fn list_user_meters(&self, user_id: Uuid) -> Result<Vec<Meter>>;

    /// Lists all located meters (latitude/longitude present) across every user,
    /// for the map view. **Not** scoped by user.
    async fn list_map_meters(&self) -> Result<Vec<MeterMapPoint>>;

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

    /// Counts readings attributed to `(user_id, serial)` that the Aggregator
    /// Bridge accepted the device signature for (`verification_status =
    /// 'verified'`), no older than `within_hours`.
    ///
    /// This is the evidence behind meter verification: the bridge writes a
    /// reading row only after Ed25519-verifying it against the device key
    /// provisioned for that serial, so a non-zero count is proof the physical
    /// meter is live, signing, and attributed to this owner.
    async fn count_attested_readings(
        &self,
        user_id: Uuid,
        serial: &str,
        within_hours: i64,
    ) -> Result<i64>;

    /// Flips `meters.is_verified` to true for the user's meter and returns the
    /// updated row. `None` when the user owns no meter with that serial — the
    /// UPDATE is owner-scoped, so this doubles as the authorization check and
    /// cannot verify someone else's device.
    async fn mark_meter_verified(&self, user_id: Uuid, serial: &str) -> Result<Option<Meter>>;

    /// Appends one row to the `meter_verification_attempts` audit trail.
    async fn record_verification_attempt(&self, attempt: &MeterVerificationAttempt) -> Result<()>;

    /// Total number of readings owned by the user (for pagination metadata).
    async fn count_user_readings(&self, user_id: Uuid) -> Result<i64>;

    /// Newest readings whose mint is **resolved** (`minted` or `denied`), each
    /// paired with its owning `user_id`. Drives the mint-status SSE poller, which
    /// diffs successive snapshots to detect transitions. Bounded by `limit`.
    async fn list_resolved_mint_readings(&self, limit: i64) -> Result<Vec<(Uuid, MeterReading)>>;

    /// Readiness probe: succeeds when the backing store is reachable.
    async fn ping(&self) -> Result<()>;
}
