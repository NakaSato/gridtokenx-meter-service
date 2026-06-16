//! Meter endpoints. Thin — validate input, call the service, return JSON.

use std::convert::Infallible;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::Json;
use futures::stream::{Stream, StreamExt};
use serde::Deserialize;
use tokio_stream::wrappers::BroadcastStream;

use meter_core::domain::meter::{
    Meter, MeterReading, MeterStats, RegisterMeterRequest, RegisterMeterResponse,
    SubmitReadingRequest,
};
use meter_core::error::Result;

use crate::middleware::AuthUser;
use crate::state::{AppState, ReadingEvent};

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

/// POST /api/v1/meters/{serial}/readings — submit a reading.
///
/// On success the persisted reading is published to the realtime stream so any
/// open SSE subscribers for this user receive it immediately.
///
/// # Errors
/// Returns an error on invalid input or unknown meter.
pub async fn submit_reading(
    State(state): State<AppState>,
    user: AuthUser,
    Path(serial): Path<String>,
    Json(req): Json<SubmitReadingRequest>,
) -> Result<Json<MeterReading>> {
    let reading = state
        .meter_service
        .submit_reading(user.user_id, &serial, &req)
        .await?;

    // Fan out to realtime subscribers. A send error just means no subscribers;
    // it never fails the request.
    let _ = state.readings_tx.send(Arc::new(ReadingEvent {
        user_id: user.user_id,
        reading: reading.clone(),
    }));

    Ok(Json(reading))
}

/// GET /api/v1/meters/readings/stream — realtime reading stream (SSE).
///
/// Subscribes to the broadcast channel and emits the authenticated user's
/// readings as `data:` events as they are submitted. Lagged events (slow
/// client) are skipped rather than closing the stream.
pub async fn stream_readings(
    State(state): State<AppState>,
    user: AuthUser,
) -> Sse<impl Stream<Item = std::result::Result<Event, Infallible>>> {
    let user_id = user.user_id;
    let stream = BroadcastStream::new(state.readings_tx.subscribe())
        .filter_map(move |res| async move { res.ok().and_then(|ev| sse_event_for_user(&ev, user_id)) });

    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// Builds an SSE event for a broadcast reading, but only when it belongs to
/// `user_id`. Returns `None` for another user's reading (filtered out) or if the
/// reading fails to serialize.
fn sse_event_for_user(
    event: &ReadingEvent,
    user_id: uuid::Uuid,
) -> Option<std::result::Result<Event, Infallible>> {
    if event.user_id != user_id {
        return None;
    }
    let data = serde_json::to_string(&event.reading).ok()?;
    Some(Ok(Event::default().event("reading").data(data)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use meter_core::domain::meter::MeterReading;
    use uuid::Uuid;

    fn reading() -> MeterReading {
        MeterReading {
            id: Uuid::nil(),
            meter_serial: "M1".to_string(),
            kwh: 1.0,
            timestamp: String::new(),
            submitted_at: String::new(),
            energy_generated: None,
            energy_consumed: None,
            voltage: None,
            current: None,
            mint_status: "pending".to_string(),
            mint_tx_signature: None,
        }
    }

    #[test]
    fn sse_event_emitted_for_owning_user() {
        let user = Uuid::new_v4();
        let ev = ReadingEvent { user_id: user, reading: reading() };
        assert!(sse_event_for_user(&ev, user).is_some());
    }

    #[test]
    fn sse_event_filtered_for_other_user() {
        let owner = Uuid::new_v4();
        let other = Uuid::new_v4();
        let ev = ReadingEvent { user_id: owner, reading: reading() };
        assert!(sse_event_for_user(&ev, other).is_none());
    }
}
