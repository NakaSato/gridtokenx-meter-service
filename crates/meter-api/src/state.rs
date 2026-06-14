//! Shared application state injected into handlers via `State`.

use std::sync::Arc;

use meter_logic::MeterService;

/// Cloneable DI container passed to every handler.
#[derive(Clone)]
pub struct AppState {
    /// Meter read service.
    pub meter_service: MeterService,
    /// Shared HS256 secret used to verify IAM-issued JWTs.
    pub jwt_secret: Arc<str>,
}
