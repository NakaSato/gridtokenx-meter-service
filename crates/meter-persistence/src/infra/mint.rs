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
    ) -> Result<String> {
        Err(ApiError::Unavailable(
            "mint backend not configured for this service".to_string(),
        ))
    }
}
