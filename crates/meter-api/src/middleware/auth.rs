//! JWT auth — verifies the IAM-issued HS256 token and extracts the user id.
//!
//! APISIX forwards the `Authorization: Bearer <jwt>` header to this upstream.
//! We verify the signature + expiry with the shared `JWT_SECRET` (same value
//! IAM signs with) and read `claims.sub` as the user id.

use axum::extract::FromRequestParts;
use axum::http::header::AUTHORIZATION;
use axum::http::request::Parts;
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use uuid::Uuid;

use meter_core::error::ApiError;

use crate::state::AppState;

#[derive(Debug, Deserialize)]
struct Claims {
    sub: Uuid,
    #[allow(dead_code)]
    exp: usize,
}

/// Authenticated user extracted from the verified JWT.
pub struct AuthUser {
    /// Subject (`claims.sub`) — the IAM user id.
    pub user_id: Uuid,
}

impl FromRequestParts<AppState> for AuthUser {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let header = parts
            .headers
            .get(AUTHORIZATION)
            .and_then(|h| h.to_str().ok())
            .ok_or_else(|| ApiError::Unauthorized("Missing Authorization header".into()))?;

        let token = header
            .strip_prefix("Bearer ")
            .ok_or_else(|| ApiError::Unauthorized("Invalid Authorization header".into()))?;

        let mut validation = Validation::new(Algorithm::HS256);
        validation.validate_exp = true;
        validation.validate_aud = false;
        // IAM tokens carry sub/exp/iat/iss; only exp is required for validation.
        validation.set_required_spec_claims(&["exp"]);

        let data = decode::<Claims>(
            token,
            &DecodingKey::from_secret(state.jwt_secret.as_bytes()),
            &validation,
        )
        .map_err(|e| ApiError::Unauthorized(format!("Invalid token: {e}")))?;

        Ok(AuthUser {
            user_id: data.claims.sub,
        })
    }
}
