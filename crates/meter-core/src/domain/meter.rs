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

/// Map marker for a located meter. Unlike [`Meter`], `latitude`/`longitude` are
/// required — the map query filters to meters that have coordinates. Returned
/// across all users for the dashboard map view (not user-scoped).
#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct MeterMapPoint {
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
    /// Latitude (always present — query filters to located meters).
    pub latitude: f64,
    /// Longitude (always present — query filters to located meters).
    pub longitude: f64,
    /// Zone partition id.
    pub zone_id: Option<i32>,
}

/// UI `MeterReading`. `Clone` so it can be fanned out to realtime SSE subscribers.
#[derive(Debug, Clone, Default, Serialize, sqlx::FromRow)]
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
    /// Energy exported to the grid this interval (surplus).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub surplus_energy: Option<f64>,
    /// Energy drawn from the grid this interval (deficit).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deficit_energy: Option<f64>,
    /// Power factor.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub power_factor: Option<f64>,
    /// Line frequency (Hz).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub frequency: Option<f64>,
    /// Meter temperature.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
    /// Battery charge level.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub battery_level: Option<f64>,
    /// Weather condition label.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub weather_condition: Option<String>,
    /// Latitude, if geocoded.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latitude: Option<f64>,
    /// Longitude, if geocoded.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub longitude: Option<f64>,
    /// Whether this reading is REC-eligible.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rec_eligible: Option<bool>,
    /// Carbon offset attributed to this reading.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub carbon_offset: Option<f64>,
    /// Owner's max sell price hint.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_sell_price: Option<f64>,
    /// Owner's max buy price hint.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_buy_price: Option<f64>,
    /// Device signature over the reading.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meter_signature: Option<String>,
    /// Meter type (e.g. `smart_meter`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meter_type: Option<String>,
    /// Token-mint status, derived read-only from the shared table's dormant
    /// blockchain columns: `"minted"` (settled on-chain), `"denied"` (mint
    /// failed), or `"pending"` (not yet minted). This service never writes it.
    pub mint_status: String,
    /// On-chain mint transaction signature, when `mint_status == "minted"`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mint_tx_signature: Option<String>,
}

/// UI `MeterStats`. The flat totals are unchanged; `zones` adds a per-zone
/// energy-flow breakdown (the locational signal `M_zone` incentives act on).
/// Not `FromRow` — `zones` is assembled from a second query in the repository.
#[derive(Debug, Clone, Serialize)]
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
    /// Per-zone energy-flow breakdown, ordered by `zone_id` (unzoned last).
    pub zones: Vec<ZoneFlow>,
}

/// Per-zone energy flow: production, consumption, and their net within one
/// `zone_id`. `net_flow > 0` = zone is a net exporter; `< 0` = net importer.
#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct ZoneFlow {
    /// Zone partition id; `None` groups meters with no assigned zone.
    pub zone_id: Option<i32>,
    /// Energy produced by this zone's meters.
    pub total_produced: f64,
    /// Energy consumed by this zone's meters.
    pub total_consumed: f64,
    /// `total_produced - total_consumed` for the zone.
    pub net_flow: f64,
    /// Readings counted in this zone.
    pub reading_count: i64,
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
