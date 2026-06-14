//! System / liveness handlers.

use axum::http::StatusCode;
use axum::response::IntoResponse;

/// GET /health
pub async fn health() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}
