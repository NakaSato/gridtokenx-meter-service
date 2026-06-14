//! Meter endpoints. Thin — validate input, call the service, return JSON.

use axum::extract::{Path, Query, State};
use axum::Json;
use serde::Deserialize;
use uuid::Uuid;

use meter_core::domain::meter::{
    Meter, MeterReading, MeterStats, MintResponse, RegisterMeterRequest, RegisterMeterResponse,
    SubmitReadingRequest,
};
use meter_core::error::Result;

use crate::middleware::AuthUser;
use crate::state::AppState;

/// GET /api/v1/me/meters
///
/// # Errors
/// Returns an error if the query fails.
pub async fn get_my_meters(
    State(state): State<AppState>,
    user: AuthUser,
) -> Result<Json<Vec<Meter>>> {
    let meters = state.meter_service.list_my_meters(user.user_id).await?;
    Ok(Json(meters))
}

/// Query params for the readings listing.
#[derive(Debug, Deserialize)]
pub struct ReadingsQuery {
    #[serde(default = "default_limit")]
    limit: i64,
    #[serde(default)]
    offset: i64,
}

fn default_limit() -> i64 {
    50
}

/// GET /api/v1/meters/readings?limit&offset
///
/// # Errors
/// Returns an error if the query fails.
pub async fn get_my_readings(
    State(state): State<AppState>,
    user: AuthUser,
    Query(q): Query<ReadingsQuery>,
) -> Result<Json<Vec<MeterReading>>> {
    let readings = state
        .meter_service
        .list_my_readings(user.user_id, q.limit, q.offset)
        .await?;
    Ok(Json(readings))
}

/// GET /api/v1/meters/stats
///
/// # Errors
/// Returns an error if the query fails.
pub async fn get_meter_stats(
    State(state): State<AppState>,
    user: AuthUser,
) -> Result<Json<MeterStats>> {
    let stats = state.meter_service.my_stats(user.user_id).await?;
    Ok(Json(stats))
}

/// POST /api/v1/meters — register a meter for the authenticated user.
///
/// # Errors
/// Returns an error on invalid input, duplicate serial, or query failure.
pub async fn register_meter(
    State(state): State<AppState>,
    user: AuthUser,
    Json(req): Json<RegisterMeterRequest>,
) -> Result<Json<RegisterMeterResponse>> {
    let resp = state.meter_service.register_meter(user.user_id, &req).await?;
    Ok(Json(resp))
}

/// Query params controlling auto-mint on submit.
#[derive(Debug, Deserialize)]
pub struct SubmitQuery {
    #[serde(default)]
    auto_mint: bool,
}

/// POST /api/v1/meters/{serial}/readings?auto_mint — submit a reading.
///
/// # Errors
/// Returns an error on invalid input, unknown meter, or mint-backend failure.
pub async fn submit_reading(
    State(state): State<AppState>,
    user: AuthUser,
    Path(serial): Path<String>,
    Query(q): Query<SubmitQuery>,
    Json(req): Json<SubmitReadingRequest>,
) -> Result<Json<MeterReading>> {
    let reading = state
        .meter_service
        .submit_reading(user.user_id, &serial, &req, q.auto_mint)
        .await?;
    Ok(Json(reading))
}

/// POST /api/v1/meters/readings/{reading_id}/mint — mint a stored reading.
///
/// # Errors
/// Returns an error if the reading is missing, already minted, or the mint
/// backend fails.
pub async fn mint_reading(
    State(state): State<AppState>,
    user: AuthUser,
    Path(reading_id): Path<Uuid>,
) -> Result<Json<MintResponse>> {
    let resp = state.meter_service.mint_reading(user.user_id, reading_id).await?;
    Ok(Json(resp))
}
