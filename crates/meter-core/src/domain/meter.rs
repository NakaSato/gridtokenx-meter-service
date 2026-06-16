//! Response models — field names mirror the trading UI contract (`types/meter.ts`).

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// UI `MeterResponse`.
#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct Meter {
    /// Meter primary key.
    pub id: Uuid,
    /// Physical serial number.
    pub serial_number: String,
    /// Meter type (e.g. `smart_meter`).
    pub meter_type: String,
    /// Human-readable location label.
    pub location: String,
    /// Whether the meter has been verified on-chain.
    pub is_verified: bool,
    /// Owning user's wallet address.
    pub wallet_address: String,
    /// Latitude, if geocoded.
    pub latitude: Option<f64>,
    /// Longitude, if geocoded.
    pub longitude: Option<f64>,
    /// Zone partition id.
    pub zone_id: Option<i32>,
}

/// UI `MeterReading`. `Clone` so it can be fanned out to realtime SSE subscribers.
#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct MeterReading {
    /// Reading primary key.
    pub id: Uuid,
    /// Serial of the meter this reading belongs to.
    pub meter_serial: String,
    /// Energy in kWh.
    pub kwh: f64,
    /// Reading timestamp (RFC-3339 UTC).
    pub timestamp: String,
    /// Time the reading was persisted (RFC-3339 UTC).
    pub submitted_at: String,
    /// Energy generated this interval.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub energy_generated: Option<f64>,
    /// Energy consumed this interval.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub energy_consumed: Option<f64>,
    /// Measured voltage.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub voltage: Option<f64>,
    /// Measured current.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current: Option<f64>,
    /// Token-mint status, derived read-only from the shared table's dormant
    /// blockchain columns: `"minted"` (settled on-chain), `"denied"` (mint
    /// failed), or `"pending"` (not yet minted). This service never writes it.
    pub mint_status: String,
    /// On-chain mint transaction signature, when `mint_status == "minted"`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mint_tx_signature: Option<String>,
}

/// UI `MeterStats`.
#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct MeterStats {
    /// Total energy produced across all readings.
    pub total_produced: f64,
    /// Total energy consumed across all readings.
    pub total_consumed: f64,
    /// Timestamp of the most recent reading (RFC-3339 UTC).
    pub last_reading_time: Option<String>,
    /// Readings whose tokens are minted on-chain.
    pub minted_count: i64,
    /// Readings not yet minted.
    pub pending_count: i64,
    /// Readings whose mint failed.
    pub denied_count: i64,
}

/// UI `registerMeter` request body (`POST /api/v1/meters`).
#[derive(Debug, Deserialize)]
pub struct RegisterMeterRequest {
    /// Physical serial number (unique).
    pub serial_number: String,
    /// Meter type; defaults to `smart_meter` when omitted.
    pub meter_type: Option<String>,
    /// Human-readable location label.
    pub location: Option<String>,
    /// Latitude, if geocoded.
    pub latitude: Option<f64>,
    /// Longitude, if geocoded.
    pub longitude: Option<f64>,
}

/// UI `RegisterMeterResponse`.
#[derive(Debug, Serialize)]
pub struct RegisterMeterResponse {
    /// Whether registration succeeded.
    pub success: bool,
    /// Human-readable status message.
    pub message: String,
    /// The newly registered meter, when successful.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meter: Option<Meter>,
}

/// UI `submitMeterData` request body (`POST /api/v1/meters/{serial}/readings`).
#[derive(Debug, Deserialize)]
pub struct SubmitReadingRequest {
    /// Energy in kWh for this reading.
    pub kwh: f64,
    /// Wallet to credit; falls back to the meter owner's wallet when omitted.
    pub wallet_address: Option<String>,
    /// Reading timestamp (RFC-3339); defaults to now when omitted.
    pub timestamp: Option<String>,
}
