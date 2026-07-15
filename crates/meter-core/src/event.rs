//! Meter domain events emitted on registry mutations.
//!
//! On a successful meter registration (and, if one is ever added, update) the
//! service publishes a [`MeterEvent`] to Kafka. Downstream **read-models** (the
//! trading service and the aggregator bridge) consume these to keep their own
//! projections of the meter registry in sync without querying this service.
//!
//! The JSON on the wire is intentionally fixed and **must stay compatible with
//! the consumers** — top-level `id` / `event_type` / `timestamp` / `source` /
//! `data`, mirroring the IAM platform `Event` shape.

use serde::Serialize;
use uuid::Uuid;

use crate::domain::meter::Meter;

/// Fixed `source` stamped on every emitted event.
pub const EVENT_SOURCE: &str = "gridtokenx-meter-service";

/// Event type for a newly registered meter.
pub const EVENT_METER_REGISTERED: &str = "MeterRegistered";

/// Event type for an updated meter (reserved — no update path exists today).
pub const EVENT_METER_UPDATED: &str = "MeterUpdated";

/// A meter domain event, serialized to JSON for Kafka.
///
/// Wire shape (do not reorder/rename without updating every consumer):
/// ```json
/// { "id": "...", "event_type": "MeterRegistered", "timestamp": "...",
///   "source": "gridtokenx-meter-service", "data": { ... } }
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct MeterEvent {
    /// Unique event id (fresh per emission, not the meter id).
    pub id: Uuid,
    /// Event discriminator (e.g. `MeterRegistered`).
    pub event_type: String,
    /// RFC-3339 emission timestamp.
    pub timestamp: String,
    /// Emitting service.
    pub source: String,
    /// Event payload.
    pub data: MeterEventData,
}

/// Payload for a [`MeterEvent`].
#[derive(Debug, Clone, Serialize)]
pub struct MeterEventData {
    /// Physical serial number of the meter.
    pub serial_number: String,
    /// Meter primary key (`meters.id`).
    pub meter_id: Uuid,
    /// Owning user (`meters.user_id`).
    pub user_id: Uuid,
    /// Zone partition id, if assigned.
    pub zone_id: Option<i32>,
    /// Coarse status derived from verification state (`verified` / `unverified`).
    pub status: String,
    /// Owner's wallet address, or `null` when unknown.
    pub wallet_address: Option<String>,
}

impl MeterEvent {
    /// Builds a `MeterRegistered` event from a freshly persisted [`Meter`].
    ///
    /// `wallet_address` collapses the persistence layer's empty-string "no
    /// wallet" sentinel to `None`; `status` is derived from `is_verified`
    /// (registration verifies at insert, so this is normally `verified`).
    #[must_use]
    pub fn meter_registered(user_id: Uuid, meter: &Meter) -> Self {
        Self::from_meter(EVENT_METER_REGISTERED, user_id, meter)
    }

    /// Builds a `MeterUpdated` event from a persisted [`Meter`]. Reserved for a
    /// future update path; kept alongside [`Self::meter_registered`] so the two
    /// stay wire-identical.
    #[must_use]
    pub fn meter_updated(user_id: Uuid, meter: &Meter) -> Self {
        Self::from_meter(EVENT_METER_UPDATED, user_id, meter)
    }

    fn from_meter(event_type: &str, user_id: Uuid, meter: &Meter) -> Self {
        let wallet_address = if meter.wallet_address.trim().is_empty() {
            None
        } else {
            Some(meter.wallet_address.clone())
        };
        let status = if meter.is_verified {
            "verified"
        } else {
            "unverified"
        };
        Self {
            id: Uuid::new_v4(),
            event_type: event_type.to_string(),
            timestamp: chrono::Utc::now().to_rfc3339(),
            source: EVENT_SOURCE.to_string(),
            data: MeterEventData {
                serial_number: meter.serial_number.clone(),
                meter_id: meter.id,
                user_id,
                zone_id: meter.zone_id,
                status: status.to_string(),
                wallet_address,
            },
        }
    }
}

/// Best-effort publisher of [`MeterEvent`]s.
///
/// Implementations MUST be **non-blocking** and **never** propagate failure to
/// the caller: publishing is fire-and-forget so a broker hiccup can never fail
/// or delay a meter registration. Errors are logged inside the implementation.
pub trait MeterEventPublisher: Send + Sync {
    /// Emit an event. Returns immediately; delivery happens in the background.
    fn publish(&self, event: MeterEvent);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::meter::Meter;

    fn meter(wallet: &str, verified: bool) -> Meter {
        Meter {
            id: Uuid::new_v4(),
            serial_number: "SN-1".to_string(),
            meter_type: "smart_meter".to_string(),
            location: String::new(),
            is_verified: verified,
            wallet_address: wallet.to_string(),
            latitude: None,
            longitude: None,
            zone_id: Some(3),
        }
    }

    #[test]
    fn registered_event_has_fixed_wire_shape() {
        let uid = Uuid::new_v4();
        let m = meter("WALLET123", true);
        let ev = MeterEvent::meter_registered(uid, &m);
        let v = serde_json::to_value(&ev).expect("serialize");

        assert_eq!(v["event_type"], "MeterRegistered");
        assert_eq!(v["source"], "gridtokenx-meter-service");
        assert!(v["id"].is_string());
        assert!(v["timestamp"].is_string());
        assert_eq!(v["data"]["serial_number"], "SN-1");
        assert_eq!(v["data"]["meter_id"], m.id.to_string());
        assert_eq!(v["data"]["user_id"], uid.to_string());
        assert_eq!(v["data"]["zone_id"], 3);
        assert_eq!(v["data"]["status"], "verified");
        assert_eq!(v["data"]["wallet_address"], "WALLET123");
    }

    #[test]
    fn empty_wallet_becomes_null_and_unverified_status() {
        let ev = MeterEvent::meter_registered(Uuid::new_v4(), &meter("", false));
        let v = serde_json::to_value(&ev).expect("serialize");
        assert!(v["data"]["wallet_address"].is_null());
        assert_eq!(v["data"]["status"], "unverified");
    }
}
