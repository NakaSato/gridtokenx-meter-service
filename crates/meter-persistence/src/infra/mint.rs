//! Mint gateway implementations.
//!
//! [`DisabledMintGateway`] is wired when no blockchain backend is configured
//! (the meter-service container has no NATS / Chain Bridge access). It returns
//! a `503` so the mint endpoints are present and contract-correct without
//! pretending to settle on-chain.
//!
//! The production gateway talks to Chain Bridge over NATS request-reply
//! (`chain.tx.submit` → `chain.tx.result.{id}`, see `gridtokenx-blockchain-core`)
//! and is added once NATS_URL, the Solana program config, and a platform-admin
//! signer are provisioned for this service.

use async_trait::async_trait;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use meter_core::domain::meter::MintOutcome;
use meter_core::error::{ApiError, Result};
use meter_core::traits::MintGateway;

/// Mint gateway used when no blockchain backend is configured.
///
/// Every call fails with [`ApiError::Unavailable`] (HTTP 503).
#[derive(Debug, Default, Clone)]
pub struct DisabledMintGateway;

#[async_trait]
impl MintGateway for DisabledMintGateway {
    async fn mint(
        &self,
        _wallet_address: &str,
        _kwh: f64,
        _meter_serial: &str,
        _timestamp_ms: i64,
    ) -> Result<MintOutcome> {
        Err(ApiError::Unavailable(
            "mint backend not configured for this service".to_string(),
        ))
    }
}

/// 15-minute window in milliseconds. Must match the aggregator's billing window
/// so the on-chain mint-record PDA `(meter_id, window_start_ms)` is stable per
/// (meter, window) and a replay of the same window is a no-op mint.
const WINDOW_MS: i64 = 15 * 60 * 1000;

/// Mirror of `gridtokenx_blockchain_core::rpc::nats_schema::MintEnergyMessage`.
/// Duplicated here so meter-service stays chain-light (no blockchain-core / Solana
/// dependency). Keep field names in sync with that type.
#[derive(Serialize)]
struct MintEnergyMessage {
    correlation_id: String,
    idempotency_key: String,
    reply_subject: String,
    recipient_wallet: String,
    energy_kwh: f64,
    meter_id: [u8; 16],
    window_start_ms: i64,
    service_identity: String,
    created_at_ms: u64,
    // `auth` is intentionally omitted — see NatsMintGateway security note.
}

/// Mirror of `MintEnergyResultMessage`.
#[derive(Deserialize)]
struct MintEnergyResultMessage {
    success: bool,
    signature: Option<String>,
    error: Option<String>,
    #[serde(default)]
    slot: u64,
}

/// Mints energy tokens by asking Chain Bridge to BUILD, sign (Vault
/// `platform_admin`), and submit the generation mint over NATS
/// (`chain.tx.mint` → reply on a per-request `chain.mint.result.{id}` subject).
/// meter-service sends only intent and carries no Solana types.
///
/// SECURITY: this sends the envelope **unsigned**. The bridge accepts unsigned
/// envelopes only when signature enforcement is off (dev). In production the
/// bridge MUST enforce signing and meter-service MUST attach an `EnvelopeAuth`
/// (its mTLS client cert) — an unsigned, spoofable `service_identity` must never
/// be allowed to mint. Tracked as a production-hardening follow-up.
pub struct NatsMintGateway {
    client: async_nats::Client,
    service_identity: String,
    request_timeout: std::time::Duration,
}

impl NatsMintGateway {
    /// Creates a gateway over a connected NATS client, asserting `service_identity`
    /// to Chain Bridge and bounding each mint request by `request_timeout`.
    #[must_use]
    pub fn new(
        client: async_nats::Client,
        service_identity: String,
        request_timeout: std::time::Duration,
    ) -> Self {
        Self {
            client,
            service_identity,
            request_timeout,
        }
    }

    /// Stable 16-byte meter id for the mint-record PDA. The serial is a UUID in
    /// this system; parse it, else derive a deterministic v5 id so a non-UUID
    /// serial still yields a stable per-meter key.
    fn meter_id_bytes(meter_serial: &str) -> [u8; 16] {
        match Uuid::parse_str(meter_serial) {
            Ok(u) => *u.as_bytes(),
            Err(_) => *Uuid::new_v5(&Uuid::NAMESPACE_OID, meter_serial.as_bytes()).as_bytes(),
        }
    }
}

#[async_trait]
impl MintGateway for NatsMintGateway {
    async fn mint(
        &self,
        wallet_address: &str,
        kwh: f64,
        meter_serial: &str,
        timestamp_ms: i64,
    ) -> Result<MintOutcome> {
        let correlation_id = Uuid::new_v4().to_string();
        let reply_subject = format!("chain.mint.result.{correlation_id}");
        let window_start_ms = (timestamp_ms / WINDOW_MS) * WINDOW_MS;
        let created_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));

        let msg = MintEnergyMessage {
            correlation_id,
            // Stable per (meter, window) — the bridge dedups replays; the
            // on-chain PDA is the ultimate backstop.
            idempotency_key: format!("mint:{meter_serial}:{window_start_ms}"),
            reply_subject: reply_subject.clone(),
            recipient_wallet: wallet_address.to_string(),
            energy_kwh: kwh,
            meter_id: Self::meter_id_bytes(meter_serial),
            window_start_ms,
            service_identity: self.service_identity.clone(),
            created_at_ms,
        };
        let payload = serde_json::to_vec(&msg)
            .map_err(|e| ApiError::Unavailable(format!("encode mint intent: {e}")))?;

        // Subscribe to the reply BEFORE publishing so the result can't be missed.
        let mut sub = self
            .client
            .subscribe(reply_subject.clone())
            .await
            .map_err(|e| ApiError::Unavailable(format!("subscribe mint reply: {e}")))?;

        self.client
            .publish("chain.tx.mint".to_string(), payload.into())
            .await
            .map_err(|e| ApiError::Unavailable(format!("publish mint intent: {e}")))?;
        let _ = self.client.flush().await;

        let reply = tokio::time::timeout(self.request_timeout, sub.next())
            .await
            .map_err(|_| ApiError::Unavailable("mint request timed out".to_string()))?
            .ok_or_else(|| ApiError::Unavailable("mint reply stream closed".to_string()))?;

        let result: MintEnergyResultMessage = serde_json::from_slice(&reply.payload)
            .map_err(|e| ApiError::Unavailable(format!("decode mint result: {e}")))?;

        if result.success {
            let signature = result
                .signature
                .ok_or_else(|| ApiError::Unavailable("mint succeeded without signature".to_string()))?;
            Ok(MintOutcome {
                signature,
                slot: result.slot,
            })
        } else {
            Err(ApiError::Unavailable(
                result.error.unwrap_or_else(|| "mint failed".to_string()),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meter_id_uses_uuid_bytes_when_serial_is_uuid() {
        let u = Uuid::new_v4();
        assert_eq!(NatsMintGateway::meter_id_bytes(&u.to_string()), *u.as_bytes());
    }

    #[test]
    fn meter_id_falls_back_to_stable_v5_for_non_uuid() {
        let a = NatsMintGateway::meter_id_bytes("METER-XYZ");
        let b = NatsMintGateway::meter_id_bytes("METER-XYZ");
        let c = NatsMintGateway::meter_id_bytes("METER-OTHER");
        assert_eq!(a, b, "non-UUID serial maps to a stable id");
        assert_ne!(a, c, "distinct serials map to distinct ids");
    }

    #[test]
    fn window_floors_to_15_min() {
        // 10:07:30 → window start 10:00:00 (matches the aggregator billing window).
        let ts = 1_700_000_850_000i64; // arbitrary ms
        let floored = (ts / WINDOW_MS) * WINDOW_MS;
        assert_eq!(floored % WINDOW_MS, 0);
        assert!(floored <= ts && ts - floored < WINDOW_MS);
    }
}
