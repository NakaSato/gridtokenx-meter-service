//! Shared application state injected into handlers via `State`.

use std::sync::Arc;

use tokio::sync::broadcast;
use uuid::Uuid;

use meter_core::domain::meter::MeterReading;
use meter_logic::MeterService;

/// A reading persisted for a user, fanned out to that user's realtime SSE
/// subscribers. Carries the owning `user_id` so the stream handler can filter
/// events to the authenticated user only.
#[derive(Debug, Clone)]
pub struct ReadingEvent {
    /// Owner the reading belongs to.
    pub user_id: Uuid,
    /// The persisted reading.
    pub reading: MeterReading,
}

/// Cloneable DI container passed to every handler.
#[derive(Clone)]
pub struct AppState {
    /// Meter read service.
    pub meter_service: MeterService,
    /// Shared HS256 secret used to verify IAM-issued JWTs.
    pub jwt_secret: Arc<str>,
    /// Broadcast channel for realtime readings. Handlers publish persisted
    /// readings here; the SSE endpoint subscribes and filters by `user_id`.
    pub readings_tx: broadcast::Sender<Arc<ReadingEvent>>,
}
