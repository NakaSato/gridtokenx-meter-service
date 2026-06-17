//! System / liveness + readiness handlers.

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;

use crate::state::AppState;

/// GET /health — liveness. Always `200` if the process is up; does not touch the
/// database (so it never flaps on a transient DB blip).
// Axum handler: kept `async` for handler-signature consistency even though the
// body never awaits.
#[allow(clippy::unused_async)]
pub async fn health() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

/// GET /health/ready — readiness. `200` when Postgres is reachable, `503`
/// otherwise, so an orchestrator can gate traffic until the backing store is up.
pub async fn ready(State(state): State<AppState>) -> impl IntoResponse {
    match state.meter_service.check_ready().await {
        Ok(()) => (StatusCode::OK, "ready"),
        Err(_) => (StatusCode::SERVICE_UNAVAILABLE, "not ready"),
    }
}
