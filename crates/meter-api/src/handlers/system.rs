//! System / liveness handlers.

use axum::http::StatusCode;
use axum::response::IntoResponse;

/// GET /health
// Axum handler: kept `async` for handler-signature consistency even though the
// body never awaits.
#[allow(clippy::unused_async)]
pub async fn health() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}
