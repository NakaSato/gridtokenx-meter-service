//! Response models — field names mirror the trading UI contract (`types/meter.ts`).

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// UI `MeterResponse`.
#[derive(Debug, Serialize, sqlx::FromRow)]
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

/// UI `MeterReading`.
#[derive(Debug, Serialize, sqlx::FromRow)]
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
    /// Whether the reading has been minted on-chain.
    pub minted: bool,
    /// Mint transaction signature, if minted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx_signature: Option<String>,
    /// Optional status message (e.g. set after an auto-mint).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[sqlx(default)]
    pub message: Option<String>,
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
}

/// UI `MeterStats`.
#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct MeterStats {
    /// Total energy produced across all readings.
    pub total_produced: f64,
    /// Total energy consumed across all readings.
    pub total_consumed: f64,
    /// Timestamp of the most recent reading (RFC-3339 UTC).
    pub last_reading_time: Option<String>,
    /// Total kWh minted.
    pub total_minted: f64,
    /// Count of minted readings.
    pub total_minted_count: i64,
    /// Total kWh awaiting mint.
    pub pending_mint: f64,
    /// Count of readings awaiting mint.
    pub pending_mint_count: i64,
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

/// UI `mintReading` response (`POST /api/v1/meters/readings/{id}/mint`).
#[derive(Debug, Serialize)]
pub struct MintResponse {
    /// Human-readable status message.
    pub message: String,
    /// On-chain transaction signature of the mint.
    pub transaction_signature: String,
    /// Minted amount in kWh (string, mirroring the UI contract).
    pub kwh_amount: String,
    /// Wallet credited by the mint.
    pub wallet_address: String,
}

/// Owner identity for a meter, resolved by serial number across all users.
///
/// Used by the device-ingest path (NATS forward from the aggregator bridge),
/// which carries no authenticated user — readings are attributed to the
/// registered owner of the meter serial.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ReadingOwner {
    /// Owning user id.
    pub user_id: Uuid,
    /// Owner's registered wallet address (may be empty if the owner has none).
    pub wallet_address: String,
}

/// Fields needed to mint a stored reading on-chain.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ReadingMintInfo {
    /// Reading primary key.
    pub id: Uuid,
    /// Serial of the meter this reading belongs to.
    pub meter_serial: String,
    /// Wallet to credit.
    pub wallet_address: String,
    /// Energy in kWh.
    pub kwh: f64,
    /// Reading timestamp as epoch milliseconds.
    pub timestamp_ms: i64,
    /// Whether the reading has already been minted.
    pub minted: bool,
}
