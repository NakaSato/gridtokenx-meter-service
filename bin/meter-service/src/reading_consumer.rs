//! NATS consumer for verified device readings forwarded by the aggregator bridge.
//!
//! The aggregator bridge persists every reading itself and forwards only the
//! mintable ones (surplus generation, `net_kwh > 0`) to meter-service on a NATS
//! subject. This task subscribes to that subject, attributes each reading to its
//! registered meter owner, persists it, and best-effort mints it.
//!
//! Degraded by design: if NATS is unreachable the task logs a warning and the
//! rest of the service runs normally (the HTTP API is unaffected).

use anyhow::Context;
use futures::StreamExt;
use meter_core::error::ApiError;
use meter_logic::MeterService;
use serde::Deserialize;
use uuid::Uuid;

/// Wire shape of a reading forwarded by the aggregator bridge.
///
/// Mirrors `aggregator_core::models::MintForwardReading` without depending on the
/// aggregator crate — meter-service stays chain-light and decoupled. Keep the
/// field names in sync with that type.
#[derive(Debug, Deserialize)]
struct MintForwardReading {
    /// Stable reading id — the idempotency key; becomes the reading row's id.
    reading_id: Uuid,
    /// Aggregator device id (diagnostic only).
    #[serde(default)]
    device_id: String,
    /// Physical meter serial — resolves the owning user + wallet for the mint.
    meter_serial: String,
    /// Net surplus energy in kWh to mint.
    energy_kwh: f64,
    /// Reading timestamp as epoch milliseconds.
    timestamp_ms: i64,
}

/// Connects to NATS and consumes forwarded readings until the subscription ends.
///
/// # Errors
/// Returns an error if the NATS connection or subscription cannot be established.
pub async fn run(nats_url: String, subject: String, service: MeterService) -> anyhow::Result<()> {
    let client = async_nats::connect(&nats_url)
        .await
        .with_context(|| format!("connect NATS {nats_url}"))?;
    let mut sub = client
        .subscribe(subject.clone())
        .await
        .with_context(|| format!("subscribe {subject}"))?;
    tracing::info!("📥 meter-service consuming forwarded readings on NATS '{subject}'");

    while let Some(msg) = sub.next().await {
        let reading: MintForwardReading = match serde_json::from_slice(&msg.payload) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("dropping malformed reading on '{subject}': {e}");
                continue;
            }
        };

        let serial = reading.meter_serial.trim();
        if serial.is_empty() {
            tracing::warn!(
                "dropping reading {} with empty serial (device_id={})",
                reading.reading_id,
                reading.device_id
            );
            continue;
        }

        match service
            .ingest_device_reading(
                reading.reading_id,
                serial,
                reading.energy_kwh,
                reading.timestamp_ms,
            )
            .await
        {
            Ok(minted) => tracing::info!(
                "ingested reading {} for meter '{}' ({} kWh, minted={})",
                reading.reading_id,
                serial,
                reading.energy_kwh,
                minted
            ),
            // Unregistered meter: nothing to attribute the reading to. Skip
            // rather than erroring — registration is the fix.
            Err(ApiError::NotFound(_)) => {
                tracing::warn!("skipping reading for unregistered meter '{serial}'");
            }
            Err(e) => tracing::error!("failed to ingest reading for '{serial}': {e}"),
        }
    }

    tracing::warn!("NATS reading subscription on '{subject}' ended");
    Ok(())
}
