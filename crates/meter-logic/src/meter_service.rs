//! Meter service — applies request bounds and business rules before delegating
//! to the repository.

use std::sync::Arc;

use uuid::Uuid;

use meter_core::domain::meter::{
    Meter, MeterReading, MeterStats, RegisterMeterRequest, RegisterMeterResponse,
    SubmitReadingRequest,
};
use meter_core::error::{ApiError, Result};
use meter_core::traits::MeterRepositoryTrait;

/// Max readings returned by a single page.
const MAX_READINGS_LIMIT: i64 = 500;

/// Service layer over [`MeterRepositoryTrait`].
#[derive(Clone)]
pub struct MeterService {
    repo: Arc<dyn MeterRepositoryTrait>,
}

impl MeterService {
    /// Creates a new service over the given repository.
    #[must_use]
    pub fn new(repo: Arc<dyn MeterRepositoryTrait>) -> Self {
        Self { repo }
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
        // Persist the canonical (trimmed) serial so a reading submitted with a
        // whitespace-padded serial still resolves the meter by exact equality.
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

    /// Submits a reading for one of the user's meters and persists it.
    ///
    /// # Errors
    /// Returns [`ApiError::BadRequest`] on invalid kWh / missing wallet,
    /// [`ApiError::NotFound`] if the meter is not owned by the user, or a
    /// database error.
    pub async fn submit_reading(
        &self,
        user_id: Uuid,
        serial: &str,
        req: &SubmitReadingRequest,
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

        self.repo
            .insert_reading(user_id, serial, req.kwh, &wallet, req.timestamp.as_deref())
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use meter_core::domain::meter::MeterStats;

    const OWNER_WALLET: &str = "owner-wallet";

    /// Configurable fake repository. Config fields are set before wrapping in
    /// `Arc`; captures use interior mutability.
    #[derive(Default)]
    struct FakeRepo {
        /// `Some(wallet)` = a meter exists with that owner wallet.
        meter_wallet: Option<String>,
        /// Captures.
        readings_page: Mutex<Option<(i64, i64)>>,
        inserted_wallet: Mutex<Option<String>>,
        inserted_serial: Mutex<Option<String>>,
        registered_serial: Mutex<Option<String>>,
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
            energy_generated: None,
            energy_consumed: None,
            voltage: None,
            current: None,
            mint_status: "pending".to_string(),
            mint_tx_signature: None,
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
                minted_count: 0,
                pending_count: 0,
                denied_count: 0,
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
    }

    fn service(repo: FakeRepo) -> MeterService {
        MeterService::new(Arc::new(repo))
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
        let svc = MeterService::new(repo.clone());

        let _ = svc.list_my_readings(Uuid::nil(), 10_000, -5).await.expect("ok");
        assert_eq!(*repo.readings_page.lock().expect("lock"), Some((500, 0)));

        let _ = svc.list_my_readings(Uuid::nil(), 0, 7).await.expect("ok");
        assert_eq!(*repo.readings_page.lock().expect("lock"), Some((1, 7)));
    }

    // --- register ----------------------------------------------------------

    #[tokio::test]
    async fn register_meter_rejects_empty_serial() {
        let svc = service(FakeRepo::default());
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
        let repo = Arc::new(FakeRepo::default());
        let svc = MeterService::new(repo.clone());
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
        let svc = service(FakeRepo { meter_wallet: Some(OWNER_WALLET.to_string()), ..Default::default() });
        let err = svc
            .submit_reading(Uuid::nil(), "M1", &submit_req(-1.0, None))
            .await
            .expect_err("should reject");
        assert!(matches!(err, ApiError::BadRequest(_)));
    }

    #[tokio::test]
    async fn submit_rejects_non_finite_kwh() {
        let svc = service(FakeRepo { meter_wallet: Some(OWNER_WALLET.to_string()), ..Default::default() });
        let err = svc
            .submit_reading(Uuid::nil(), "M1", &submit_req(f64::NAN, None))
            .await
            .expect_err("should reject");
        assert!(matches!(err, ApiError::BadRequest(_)));
    }

    // --- submit_reading: meter lookup --------------------------------------

    #[tokio::test]
    async fn submit_unknown_meter_is_not_found() {
        let svc = service(FakeRepo { meter_wallet: None, ..Default::default() });
        let err = svc
            .submit_reading(Uuid::nil(), "M1", &submit_req(1.0, None))
            .await
            .expect_err("should 404");
        assert!(matches!(err, ApiError::NotFound(_)));
    }

    // --- submit_reading: wallet fallback -----------------------------------

    #[tokio::test]
    async fn submit_falls_back_to_owner_wallet_when_request_blank() {
        let repo = Arc::new(FakeRepo { meter_wallet: Some(OWNER_WALLET.to_string()), ..Default::default() });
        let svc = MeterService::new(repo.clone());
        svc.submit_reading(Uuid::nil(), "M1", &submit_req(1.0, Some("  ")))
            .await
            .expect("ok");
        assert_eq!(*repo.inserted_wallet.lock().expect("lock"), Some(OWNER_WALLET.to_string()));
    }

    #[tokio::test]
    async fn submit_trims_path_serial_before_persisting() {
        let repo = Arc::new(FakeRepo { meter_wallet: Some(OWNER_WALLET.to_string()), ..Default::default() });
        let svc = MeterService::new(repo.clone());
        svc.submit_reading(Uuid::nil(), "  M-9  ", &submit_req(1.0, None))
            .await
            .expect("ok");
        assert_eq!(*repo.inserted_serial.lock().expect("lock"), Some("M-9".to_string()));
    }

    #[tokio::test]
    async fn submit_uses_request_wallet_when_present() {
        let repo = Arc::new(FakeRepo { meter_wallet: Some(OWNER_WALLET.to_string()), ..Default::default() });
        let svc = MeterService::new(repo.clone());
        svc.submit_reading(Uuid::nil(), "M1", &submit_req(1.0, Some("req-wallet")))
            .await
            .expect("ok");
        assert_eq!(*repo.inserted_wallet.lock().expect("lock"), Some("req-wallet".to_string()));
    }

    #[tokio::test]
    async fn submit_rejects_when_no_wallet_anywhere() {
        let svc = service(FakeRepo { meter_wallet: Some(String::new()), ..Default::default() });
        let err = svc
            .submit_reading(Uuid::nil(), "M1", &submit_req(1.0, None))
            .await
            .expect_err("should reject");
        assert!(matches!(err, ApiError::BadRequest(_)));
    }
}
