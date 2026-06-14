//! Meter service — applies request bounds and business rules before delegating
//! to the repository and mint gateway.

use std::sync::Arc;

use uuid::Uuid;

use meter_core::domain::meter::{
    Meter, MeterReading, MeterStats, MintResponse, RegisterMeterRequest, RegisterMeterResponse,
    SubmitReadingRequest,
};
use meter_core::error::{ApiError, Result};
use meter_core::traits::{MeterRepositoryTrait, MintGateway};

/// Max readings returned by a single page.
const MAX_READINGS_LIMIT: i64 = 500;

/// Service layer over [`MeterRepositoryTrait`] and [`MintGateway`].
#[derive(Clone)]
pub struct MeterService {
    repo: Arc<dyn MeterRepositoryTrait>,
    mint: Arc<dyn MintGateway>,
}

impl MeterService {
    /// Creates a new service over the given repository and mint gateway.
    #[must_use]
    pub fn new(repo: Arc<dyn MeterRepositoryTrait>, mint: Arc<dyn MintGateway>) -> Self {
        Self { repo, mint }
    }

    /// Lists the user's meters.
    ///
    /// # Errors
    /// Returns an error if the underlying query fails.
    pub async fn list_my_meters(&self, user_id: Uuid) -> Result<Vec<Meter>> {
        self.repo.list_user_meters(user_id).await
    }

    /// Lists a bounded page of the user's readings.
    ///
    /// # Errors
    /// Returns an error if the underlying query fails.
    pub async fn list_my_readings(
        &self,
        user_id: Uuid,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<MeterReading>> {
        let limit = limit.clamp(1, MAX_READINGS_LIMIT);
        let offset = offset.max(0);
        self.repo.list_user_readings(user_id, limit, offset).await
    }

    /// Aggregates the user's meter stats.
    ///
    /// # Errors
    /// Returns an error if the underlying query fails.
    pub async fn my_stats(&self, user_id: Uuid) -> Result<MeterStats> {
        self.repo.user_stats(user_id).await
    }

    /// Registers a new meter for the user.
    ///
    /// # Errors
    /// Returns [`ApiError::BadRequest`] on empty serial, [`ApiError::Conflict`]
    /// if the serial is already registered, or a database error.
    pub async fn register_meter(
        &self,
        user_id: Uuid,
        req: &RegisterMeterRequest,
    ) -> Result<RegisterMeterResponse> {
        if req.serial_number.trim().is_empty() {
            return Err(ApiError::BadRequest("serial_number is required".to_string()));
        }
        let meter = self.repo.register_meter(user_id, req).await?;
        Ok(RegisterMeterResponse {
            success: true,
            message: format!("Meter '{}' registered", meter.serial_number),
            meter: Some(meter),
        })
    }

    /// Submits a reading for one of the user's meters, optionally minting it.
    ///
    /// # Errors
    /// Returns [`ApiError::BadRequest`] on invalid kWh / missing wallet,
    /// [`ApiError::NotFound`] if the meter is not owned by the user, or a
    /// database / mint-backend error.
    pub async fn submit_reading(
        &self,
        user_id: Uuid,
        serial: &str,
        req: &SubmitReadingRequest,
        auto_mint: bool,
    ) -> Result<MeterReading> {
        if req.kwh < 0.0 || !req.kwh.is_finite() {
            return Err(ApiError::BadRequest("kwh must be a non-negative number".to_string()));
        }

        let meter = self
            .repo
            .find_meter_by_serial(user_id, serial)
            .await?
            .ok_or_else(|| ApiError::NotFound(format!("meter '{serial}' not found")))?;

        // Wallet from the request, falling back to the meter owner's wallet.
        let wallet = match &req.wallet_address {
            Some(w) if !w.trim().is_empty() => w.clone(),
            _ => meter.wallet_address.clone(),
        };
        if wallet.trim().is_empty() {
            return Err(ApiError::BadRequest(
                "no wallet_address provided and meter owner has none".to_string(),
            ));
        }

        let mut reading = self
            .repo
            .insert_reading(user_id, serial, req.kwh, &wallet, req.timestamp.as_deref())
            .await?;

        // Auto-mint is best-effort: the reading is always persisted. A mint
        // failure is reported in `message` rather than failing the submit, so
        // the reading can be minted later via the explicit mint endpoint.
        if auto_mint {
            match self.mint_existing(user_id, reading.id).await {
                Ok(res) => {
                    reading.minted = true;
                    reading.tx_signature = Some(res.transaction_signature);
                    reading.message = Some(res.message);
                }
                Err(e) => {
                    tracing::warn!("auto-mint failed for reading {}: {e}", reading.id);
                    reading.message = Some(format!("reading saved; mint deferred ({e})"));
                }
            }
        }

        Ok(reading)
    }

    /// Ingests a verified device reading forwarded by the aggregator bridge over
    /// NATS, then best-effort mints it. Returns `true` if the reading is minted.
    ///
    /// This path carries no JWT, so the reading is attributed by meter `serial`
    /// (resolved to its registered owner). The mint wallet is the owner's
    /// registered wallet — never a value taken off the wire.
    ///
    /// Idempotent: `reading_id` is the row primary key, so a duplicate delivery
    /// is a no-op insert, and the mint's `minted` guard prevents a second
    /// on-chain mint. The reading is always persisted; a mint-backend outage is
    /// logged and the reading can be minted later via the explicit endpoint.
    ///
    /// # Errors
    /// Returns [`ApiError::BadRequest`] on invalid kWh, [`ApiError::NotFound`]
    /// when the serial is not registered to any user (caller should skip it), or
    /// a database error.
    pub async fn ingest_device_reading(
        &self,
        reading_id: Uuid,
        serial: &str,
        kwh: f64,
        timestamp_ms: i64,
    ) -> Result<bool> {
        if kwh < 0.0 || !kwh.is_finite() {
            return Err(ApiError::BadRequest("kwh must be a non-negative number".to_string()));
        }

        let owner = self
            .repo
            .find_owner_by_serial(serial)
            .await?
            .ok_or_else(|| ApiError::NotFound(format!("meter '{serial}' not registered")))?;

        let inserted = self
            .repo
            .insert_device_reading(
                reading_id,
                owner.user_id,
                serial,
                kwh,
                &owner.wallet_address,
                timestamp_ms,
            )
            .await?;
        if !inserted {
            tracing::info!("reading {reading_id} already ingested; re-checking mint");
        }

        // Best-effort mint. `reading_id` is the row id, so this targets the row
        // just inserted (or the pre-existing one on a redelivery). `mint_existing`
        // rejects an already-minted reading, so a retry never double-mints.
        let minted = match self.mint_existing(owner.user_id, reading_id).await {
            Ok(_) => true,
            // Already minted — idempotent success on redelivery.
            Err(ApiError::Conflict(_)) => true,
            // Mint backend down/disabled — reading is saved, mint deferred.
            Err(ApiError::Unavailable(e)) => {
                tracing::warn!("mint deferred for reading {reading_id}: {e}");
                false
            }
            Err(e) => return Err(e),
        };

        Ok(minted)
    }

    /// Mints a previously submitted reading.
    ///
    /// # Errors
    /// Returns [`ApiError::NotFound`] if the reading is not owned by the user,
    /// [`ApiError::Conflict`] if it is already minted, or a mint-backend error.
    pub async fn mint_reading(&self, user_id: Uuid, reading_id: Uuid) -> Result<MintResponse> {
        self.mint_existing(user_id, reading_id).await
    }

    /// Shared mint path: loads the reading, mints it, and records the signature.
    async fn mint_existing(&self, user_id: Uuid, reading_id: Uuid) -> Result<MintResponse> {
        let info = self
            .repo
            .get_reading_for_mint(user_id, reading_id)
            .await?
            .ok_or_else(|| ApiError::NotFound("reading not found".to_string()))?;

        if info.minted {
            return Err(ApiError::Conflict("reading already minted".to_string()));
        }
        if info.wallet_address.trim().is_empty() {
            return Err(ApiError::BadRequest(
                "reading has no wallet_address to mint to".to_string(),
            ));
        }

        let signature = self
            .mint
            .mint(&info.wallet_address, info.kwh, &info.meter_serial, info.timestamp_ms)
            .await?;

        self.repo.mark_reading_minted(reading_id, &signature).await?;

        Ok(MintResponse {
            message: "Reading minted".to_string(),
            transaction_signature: signature,
            kwh_amount: info.kwh.to_string(),
            wallet_address: info.wallet_address,
        })
    }
}
