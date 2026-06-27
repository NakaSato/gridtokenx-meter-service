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

#[cfg(test)]
mod tests {
    use super::*;

    async fn body_json(resp: Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .expect("read body");
        serde_json::from_slice(&bytes).expect("body is json")
    }

    #[tokio::test]
    async fn client_errors_map_to_their_status_and_echo_message() {
        let cases = [
            (
                ApiError::Unauthorized("no token".into()),
                StatusCode::UNAUTHORIZED,
                "no token",
            ),
            (
                ApiError::BadRequest("bad serial".into()),
                StatusCode::BAD_REQUEST,
                "bad serial",
            ),
            (
                ApiError::NotFound("meter X".into()),
                StatusCode::NOT_FOUND,
                "meter X",
            ),
            (
                ApiError::Conflict("already minted".into()),
                StatusCode::CONFLICT,
                "already minted",
            ),
            (
                ApiError::Unavailable("mint backend down".into()),
                StatusCode::SERVICE_UNAVAILABLE,
                "mint backend down",
            ),
        ];
        for (err, want_status, want_msg) in cases {
            let resp = err.into_response();
            assert_eq!(resp.status(), want_status);
            assert_eq!(body_json(resp).await["error"], want_msg);
        }
    }

    #[tokio::test]
    async fn database_error_is_500_and_does_not_leak_internals() {
        // SECURITY: a DB failure must surface as a generic 500 — the inner sqlx
        // error (which can carry schema/query detail) must never reach the client.
        let resp = ApiError::Database(sqlx::Error::RowNotFound).into_response();
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = body_json(resp).await;
        assert_eq!(body["error"], "database error");
        // No "RowNotFound" / sqlx detail leaked.
        assert!(!body["error"].as_str().unwrap().to_lowercase().contains("row"));
    }

    #[test]
    fn display_carries_variant_prefix() {
        assert_eq!(
            ApiError::Conflict("dup".into()).to_string(),
            "conflict: dup"
        );
        assert_eq!(ApiError::Database(sqlx::Error::RowNotFound).to_string(), "database error");
    }
}
