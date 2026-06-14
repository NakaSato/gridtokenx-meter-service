//! Unified error type for the meter service, mappable to HTTP responses.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

/// A specialized Result type for meter operations.
pub type Result<T> = std::result::Result<T, ApiError>;

/// Application-level error returned across the API boundary.
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    /// Authentication failed (missing / invalid / expired token).
    #[error("unauthorized: {0}")]
    Unauthorized(String),

    /// Request was malformed or failed validation.
    #[error("bad request: {0}")]
    BadRequest(String),

    /// Requested resource does not exist (or is not owned by the caller).
    #[error("not found: {0}")]
    NotFound(String),

    /// Request conflicts with current state (e.g. already minted).
    #[error("conflict: {0}")]
    Conflict(String),

    /// A required backend dependency is unavailable (e.g. mint backend).
    #[error("service unavailable: {0}")]
    Unavailable(String),

    /// Persistence / database failure.
    #[error("database error")]
    Database(#[from] sqlx::Error),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            ApiError::Unauthorized(msg) => (StatusCode::UNAUTHORIZED, msg.clone()),
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg.clone()),
            ApiError::NotFound(msg) => (StatusCode::NOT_FOUND, msg.clone()),
            ApiError::Conflict(msg) => (StatusCode::CONFLICT, msg.clone()),
            ApiError::Unavailable(msg) => (StatusCode::SERVICE_UNAVAILABLE, msg.clone()),
            ApiError::Database(e) => {
                tracing::error!("db error: {e}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "database error".to_string(),
                )
            }
        };
        (status, Json(json!({ "error": message }))).into_response()
    }
}
