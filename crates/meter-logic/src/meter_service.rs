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
        let serial = req.serial_number.trim();
        if serial.is_empty() {
            return Err(ApiError::BadRequest("serial_number is required".to_string()));
        }
        // Persist the canonical (trimmed) serial. Device readings forwarded over
        // NATS arrive with a trimmed `meter_serial` and are matched by exact
        // equality (`find_owner_by_serial`), so storing a whitespace-padded serial
        // here would make those readings un-attributable and silently dropped.
        let normalized = RegisterMeterRequest {
            serial_number: serial.to_string(),
            meter_type: req.meter_type.clone(),
            location: req.location.clone(),
            latitude: req.latitude,
            longitude: req.longitude,
        };
        let meter = self.repo.register_meter(user_id, &normalized).await?;
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

        // Match registration, which stores the trimmed serial: trim the path
        // serial so a padded value still resolves the meter (and is stored
        // canonically on the reading).
        let serial = serial.trim();
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
            // Minted now, or already minted (Conflict) — idempotent success on redelivery.
            Ok(_) | Err(ApiError::Conflict(_)) => true,
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

        let outcome = self
            .mint
            .mint(&info.wallet_address, info.kwh, &info.meter_serial, info.timestamp_ms)
            .await?;

        self.repo
            .mark_reading_minted(reading_id, &outcome.signature, outcome.slot)
            .await?;

        Ok(MintResponse {
            message: "Reading minted".to_string(),
            transaction_signature: outcome.signature,
            kwh_amount: info.kwh.to_string(),
            wallet_address: info.wallet_address,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use meter_core::domain::meter::{MeterStats, MintOutcome, ReadingMintInfo, ReadingOwner};

    const OWNER_WALLET: &str = "owner-wallet";
    const WIRE_WALLET: &str = "attacker-supplied-wallet";

    /// How the fake mint gateway responds.
    #[derive(Clone, Copy)]
    enum MintMode {
        Ok,
        Unavailable,
    }

    /// Configurable fake repository. Config fields are set before wrapping in
    /// `Arc`; captures use interior mutability.
    struct FakeRepo {
        /// `Some(wallet)` = a meter exists with that owner wallet (`Meter` isn't `Clone`).
        meter_wallet: Option<String>,
        owner: Option<ReadingOwner>,
        /// Wallet returned by `get_reading_for_mint` (the value minted to).
        mint_wallet: String,
        /// Whether the loaded reading is already minted.
        already_minted: bool,
        /// `insert_device_reading` return (false = duplicate delivery).
        device_inserted: bool,
        /// Captures.
        readings_page: Mutex<Option<(i64, i64)>>,
        inserted_wallet: Mutex<Option<String>>,
        inserted_serial: Mutex<Option<String>>,
        device_wallet: Mutex<Option<String>>,
        registered_serial: Mutex<Option<String>>,
        marked: Mutex<bool>,
    }

    impl Default for FakeRepo {
        fn default() -> Self {
            Self {
                meter_wallet: None,
                owner: None,
                mint_wallet: OWNER_WALLET.to_string(),
                already_minted: false,
                device_inserted: true,
                readings_page: Mutex::new(None),
                inserted_wallet: Mutex::new(None),
                inserted_serial: Mutex::new(None),
                device_wallet: Mutex::new(None),
                registered_serial: Mutex::new(None),
                marked: Mutex::new(false),
            }
        }
    }

    fn meter(wallet: &str) -> Meter {
        Meter {
            id: Uuid::nil(),
            serial_number: "M1".to_string(),
            meter_type: "solar".to_string(),
            location: String::new(),
            is_verified: true,
            wallet_address: wallet.to_string(),
            latitude: None,
            longitude: None,
            zone_id: None,
        }
    }

    fn reading() -> MeterReading {
        MeterReading {
            id: Uuid::nil(),
            meter_serial: "M1".to_string(),
            kwh: 1.0,
            timestamp: String::new(),
            submitted_at: String::new(),
            minted: false,
            tx_signature: None,
            message: None,
            energy_generated: None,
            energy_consumed: None,
            voltage: None,
            current: None,
        }
    }

    #[async_trait::async_trait]
    impl MeterRepositoryTrait for FakeRepo {
        async fn list_user_meters(&self, _user_id: Uuid) -> Result<Vec<Meter>> {
            Ok(self.meter_wallet.iter().map(|w| meter(w)).collect())
        }

        async fn list_user_readings(
            &self,
            _user_id: Uuid,
            limit: i64,
            offset: i64,
        ) -> Result<Vec<MeterReading>> {
            *self.readings_page.lock().expect("lock") = Some((limit, offset));
            Ok(vec![])
        }

        async fn user_stats(&self, _user_id: Uuid) -> Result<MeterStats> {
            Ok(MeterStats {
                total_produced: 0.0,
                total_consumed: 0.0,
                last_reading_time: None,
                total_minted: 0.0,
                total_minted_count: 0,
                pending_mint: 0.0,
                pending_mint_count: 0,
            })
        }

        async fn register_meter(
            &self,
            _user_id: Uuid,
            req: &RegisterMeterRequest,
        ) -> Result<Meter> {
            *self.registered_serial.lock().expect("lock") = Some(req.serial_number.clone());
            let mut m = meter(OWNER_WALLET);
            m.serial_number = req.serial_number.clone();
            Ok(m)
        }

        async fn find_meter_by_serial(
            &self,
            _user_id: Uuid,
            _serial: &str,
        ) -> Result<Option<Meter>> {
            Ok(self.meter_wallet.as_deref().map(meter))
        }

        async fn find_owner_by_serial(&self, _serial: &str) -> Result<Option<ReadingOwner>> {
            Ok(self.owner.clone())
        }

        async fn insert_device_reading(
            &self,
            _reading_id: Uuid,
            _user_id: Uuid,
            _meter_serial: &str,
            _kwh: f64,
            wallet_address: &str,
            _timestamp_ms: i64,
        ) -> Result<bool> {
            *self.device_wallet.lock().expect("lock") = Some(wallet_address.to_string());
            Ok(self.device_inserted)
        }

        async fn insert_reading(
            &self,
            _user_id: Uuid,
            meter_serial: &str,
            _kwh: f64,
            wallet_address: &str,
            _timestamp: Option<&str>,
        ) -> Result<MeterReading> {
            *self.inserted_wallet.lock().expect("lock") = Some(wallet_address.to_string());
            *self.inserted_serial.lock().expect("lock") = Some(meter_serial.to_string());
            Ok(reading())
        }

        async fn get_reading_for_mint(
            &self,
            _user_id: Uuid,
            reading_id: Uuid,
        ) -> Result<Option<ReadingMintInfo>> {
            Ok(Some(ReadingMintInfo {
                id: reading_id,
                meter_serial: "M1".to_string(),
                wallet_address: self.mint_wallet.clone(),
                kwh: 1.0,
                timestamp_ms: 0,
                minted: self.already_minted,
            }))
        }

        async fn mark_reading_minted(
            &self,
            _reading_id: Uuid,
            _signature: &str,
            _slot: u64,
        ) -> Result<()> {
            *self.marked.lock().expect("lock") = true;
            Ok(())
        }
    }

    struct FakeMint {
        mode: MintMode,
    }

    #[async_trait::async_trait]
    impl MintGateway for FakeMint {
        async fn mint(
            &self,
            _wallet_address: &str,
            _kwh: f64,
            _meter_serial: &str,
            _timestamp_ms: i64,
        ) -> Result<MintOutcome> {
            match self.mode {
                MintMode::Ok => Ok(MintOutcome {
                    signature: "sig123".to_string(),
                    slot: 42,
                }),
                MintMode::Unavailable => {
                    Err(ApiError::Unavailable("mint backend down".to_string()))
                }
            }
        }
    }

    fn service(repo: FakeRepo, mode: MintMode) -> MeterService {
        MeterService::new(Arc::new(repo), Arc::new(FakeMint { mode }))
    }

    fn submit_req(kwh: f64, wallet: Option<&str>) -> SubmitReadingRequest {
        SubmitReadingRequest {
            kwh,
            wallet_address: wallet.map(str::to_string),
            timestamp: None,
        }
    }

    // --- page clamping -----------------------------------------------------

    #[tokio::test]
    async fn list_readings_clamps_to_500_1_and_0() {
        let repo = Arc::new(FakeRepo::default());
        let svc = MeterService::new(repo.clone(), Arc::new(FakeMint { mode: MintMode::Ok }));

        let _ = svc.list_my_readings(Uuid::nil(), 10_000, -5).await.expect("ok");
        assert_eq!(*repo.readings_page.lock().expect("lock"), Some((500, 0)));

        let _ = svc.list_my_readings(Uuid::nil(), 0, 7).await.expect("ok");
        assert_eq!(*repo.readings_page.lock().expect("lock"), Some((1, 7)));
    }

    // --- register ----------------------------------------------------------

    #[tokio::test]
    async fn register_meter_rejects_empty_serial() {
        let svc = service(FakeRepo::default(), MintMode::Ok);
        let req = RegisterMeterRequest {
            serial_number: "   ".to_string(),
            meter_type: None,
            location: None,
            latitude: None,
            longitude: None,
        };
        let err = svc.register_meter(Uuid::nil(), &req).await.expect_err("should reject");
        assert!(matches!(err, ApiError::BadRequest(_)));
    }

    #[tokio::test]
    async fn register_meter_persists_trimmed_serial() {
        // Regression: a whitespace-padded serial must be stored canonically, else
        // NATS-forwarded readings (matched by exact, trimmed serial) are dropped.
        let repo = Arc::new(FakeRepo::default());
        let svc = MeterService::new(repo.clone(), Arc::new(FakeMint { mode: MintMode::Ok }));
        let req = RegisterMeterRequest {
            serial_number: "  M-9  ".to_string(),
            meter_type: None,
            location: None,
            latitude: None,
            longitude: None,
        };
        let resp = svc.register_meter(Uuid::nil(), &req).await.expect("ok");
        assert_eq!(*repo.registered_serial.lock().expect("lock"), Some("M-9".to_string()));
        assert_eq!(resp.meter.expect("meter").serial_number, "M-9");
    }

    // --- submit_reading: kwh validation ------------------------------------

    #[tokio::test]
    async fn submit_rejects_negative_kwh() {
        let svc = service(FakeRepo { meter_wallet: Some(OWNER_WALLET.to_string()), ..Default::default() }, MintMode::Ok);
        let err = svc
            .submit_reading(Uuid::nil(), "M1", &submit_req(-1.0, None), false)
            .await
            .expect_err("should reject");
        assert!(matches!(err, ApiError::BadRequest(_)));
    }

    #[tokio::test]
    async fn submit_rejects_non_finite_kwh() {
        let svc = service(FakeRepo { meter_wallet: Some(OWNER_WALLET.to_string()), ..Default::default() }, MintMode::Ok);
        let err = svc
            .submit_reading(Uuid::nil(), "M1", &submit_req(f64::NAN, None), false)
            .await
            .expect_err("should reject");
        assert!(matches!(err, ApiError::BadRequest(_)));
    }

    // --- submit_reading: meter lookup --------------------------------------

    #[tokio::test]
    async fn submit_unknown_meter_is_not_found() {
        let svc = service(FakeRepo { meter_wallet: None, ..Default::default() }, MintMode::Ok);
        let err = svc
            .submit_reading(Uuid::nil(), "M1", &submit_req(1.0, None), false)
            .await
            .expect_err("should 404");
        assert!(matches!(err, ApiError::NotFound(_)));
    }

    // --- submit_reading: wallet fallback -----------------------------------

    #[tokio::test]
    async fn submit_falls_back_to_owner_wallet_when_request_blank() {
        let repo = Arc::new(FakeRepo { meter_wallet: Some(OWNER_WALLET.to_string()), ..Default::default() });
        let svc = MeterService::new(repo.clone(), Arc::new(FakeMint { mode: MintMode::Ok }));
        svc.submit_reading(Uuid::nil(), "M1", &submit_req(1.0, Some("  ")), false)
            .await
            .expect("ok");
        assert_eq!(*repo.inserted_wallet.lock().expect("lock"), Some(OWNER_WALLET.to_string()));
    }

    #[tokio::test]
    async fn submit_trims_path_serial_before_persisting() {
        let repo = Arc::new(FakeRepo { meter_wallet: Some(OWNER_WALLET.to_string()), ..Default::default() });
        let svc = MeterService::new(repo.clone(), Arc::new(FakeMint { mode: MintMode::Ok }));
        svc.submit_reading(Uuid::nil(), "  M-9  ", &submit_req(1.0, None), false)
            .await
            .expect("ok");
        assert_eq!(*repo.inserted_serial.lock().expect("lock"), Some("M-9".to_string()));
    }

    #[tokio::test]
    async fn submit_uses_request_wallet_when_present() {
        let repo = Arc::new(FakeRepo { meter_wallet: Some(OWNER_WALLET.to_string()), ..Default::default() });
        let svc = MeterService::new(repo.clone(), Arc::new(FakeMint { mode: MintMode::Ok }));
        svc.submit_reading(Uuid::nil(), "M1", &submit_req(1.0, Some("req-wallet")), false)
            .await
            .expect("ok");
        assert_eq!(*repo.inserted_wallet.lock().expect("lock"), Some("req-wallet".to_string()));
    }

    #[tokio::test]
    async fn submit_rejects_when_no_wallet_anywhere() {
        let svc = service(FakeRepo { meter_wallet: Some(String::new()), ..Default::default() }, MintMode::Ok);
        let err = svc
            .submit_reading(Uuid::nil(), "M1", &submit_req(1.0, None), false)
            .await
            .expect_err("should reject");
        assert!(matches!(err, ApiError::BadRequest(_)));
    }

    // --- submit_reading: auto-mint best-effort -----------------------------

    #[tokio::test]
    async fn submit_without_auto_mint_persists_unminted() {
        let svc = service(FakeRepo { meter_wallet: Some(OWNER_WALLET.to_string()), ..Default::default() }, MintMode::Ok);
        let r = svc
            .submit_reading(Uuid::nil(), "M1", &submit_req(1.0, None), false)
            .await
            .expect("ok");
        assert!(!r.minted);
        assert!(r.tx_signature.is_none());
    }

    #[tokio::test]
    async fn submit_with_auto_mint_marks_minted() {
        let svc = service(FakeRepo { meter_wallet: Some(OWNER_WALLET.to_string()), ..Default::default() }, MintMode::Ok);
        let r = svc
            .submit_reading(Uuid::nil(), "M1", &submit_req(1.0, None), true)
            .await
            .expect("ok");
        assert!(r.minted);
        assert_eq!(r.tx_signature.as_deref(), Some("sig123"));
    }

    #[tokio::test]
    async fn submit_auto_mint_failure_still_persists_reading() {
        let svc = service(FakeRepo { meter_wallet: Some(OWNER_WALLET.to_string()), ..Default::default() }, MintMode::Unavailable);
        let r = svc
            .submit_reading(Uuid::nil(), "M1", &submit_req(1.0, None), true)
            .await
            .expect("submit must succeed despite mint failure");
        assert!(!r.minted);
        assert!(r.message.unwrap_or_default().contains("mint deferred"));
    }

    // --- ingest_device_reading ---------------------------------------------

    #[tokio::test]
    async fn ingest_rejects_negative_kwh() {
        let svc = service(FakeRepo::default(), MintMode::Ok);
        let err = svc
            .ingest_device_reading(Uuid::nil(), "M1", -1.0, 0)
            .await
            .expect_err("should reject");
        assert!(matches!(err, ApiError::BadRequest(_)));
    }

    #[tokio::test]
    async fn ingest_unregistered_serial_is_not_found() {
        let svc = service(FakeRepo { owner: None, ..Default::default() }, MintMode::Ok);
        let err = svc
            .ingest_device_reading(Uuid::nil(), "M1", 1.0, 0)
            .await
            .expect_err("should 404");
        assert!(matches!(err, ApiError::NotFound(_)));
    }

    #[tokio::test]
    async fn ingest_credits_owner_wallet_not_wire_value() {
        let owner = ReadingOwner {
            user_id: Uuid::nil(),
            wallet_address: OWNER_WALLET.to_string(),
        };
        let repo = Arc::new(FakeRepo { owner: Some(owner), ..Default::default() });
        let svc = MeterService::new(repo.clone(), Arc::new(FakeMint { mode: MintMode::Ok }));
        // The wire value is never passed to ingest; assert the persisted wallet is the owner's.
        let _ = svc.ingest_device_reading(Uuid::nil(), "M1", 1.0, 0).await.expect("ok");
        let credited = repo.device_wallet.lock().expect("lock").clone();
        assert_eq!(credited, Some(OWNER_WALLET.to_string()));
        assert_ne!(credited, Some(WIRE_WALLET.to_string()));
    }

    #[tokio::test]
    async fn ingest_mint_unavailable_defers_but_succeeds() {
        let owner = ReadingOwner {
            user_id: Uuid::nil(),
            wallet_address: OWNER_WALLET.to_string(),
        };
        let svc = service(FakeRepo { owner: Some(owner), ..Default::default() }, MintMode::Unavailable);
        let minted = svc.ingest_device_reading(Uuid::nil(), "M1", 1.0, 0).await.expect("ok");
        assert!(!minted, "mint deferred returns false but does not error");
    }

    #[tokio::test]
    async fn ingest_already_minted_is_idempotent_success() {
        let owner = ReadingOwner {
            user_id: Uuid::nil(),
            wallet_address: OWNER_WALLET.to_string(),
        };
        let svc = service(
            FakeRepo { owner: Some(owner), already_minted: true, ..Default::default() },
            MintMode::Ok,
        );
        let minted = svc.ingest_device_reading(Uuid::nil(), "M1", 1.0, 0).await.expect("ok");
        assert!(minted, "redelivery of a minted reading is treated as success");
    }

    // --- mint_existing guards ----------------------------------------------

    #[tokio::test]
    async fn mint_existing_already_minted_conflicts() {
        let svc = service(FakeRepo { already_minted: true, ..Default::default() }, MintMode::Ok);
        let err = svc.mint_reading(Uuid::nil(), Uuid::nil()).await.expect_err("conflict");
        assert!(matches!(err, ApiError::Conflict(_)));
    }

    #[tokio::test]
    async fn mint_existing_rejects_empty_wallet() {
        let svc = service(
            FakeRepo { mint_wallet: String::new(), ..Default::default() },
            MintMode::Ok,
        );
        let err = svc.mint_reading(Uuid::nil(), Uuid::nil()).await.expect_err("bad request");
        assert!(matches!(err, ApiError::BadRequest(_)));
    }
}
