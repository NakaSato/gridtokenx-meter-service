//! Meter service — applies request bounds and business rules before delegating
//! to the repository.

use std::sync::Arc;

use uuid::Uuid;

use meter_core::domain::meter::{
    Meter, MeterMapPoint, MeterReading, MeterStats, RegisterMeterRequest, RegisterMeterResponse,
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

    /// Lists all located meters across every user for the map view.
    ///
    /// # Errors
    /// Returns an error if the underlying query fails.
    pub async fn list_map_points(&self) -> Result<Vec<MeterMapPoint>> {
        self.repo.list_map_meters().await
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

    /// Lists a bounded page of readings plus pagination metadata: the total
    /// count across all the user's readings and whether more pages follow.
    /// `has_more` is computed from the **clamped** offset, so it stays correct
    /// when the caller passes an out-of-range `limit`/`offset`.
    ///
    /// # Errors
    /// Returns an error if either underlying query fails.
    pub async fn list_my_readings_page(
        &self,
        user_id: Uuid,
        limit: i64,
        offset: i64,
    ) -> Result<(Vec<MeterReading>, i64, bool)> {
        let limit = limit.clamp(1, MAX_READINGS_LIMIT);
        let offset = offset.max(0);
        let items = self.repo.list_user_readings(user_id, limit, offset).await?;
        let total = self.repo.count_user_readings(user_id).await?;
        let has_more = offset + i64::try_from(items.len()).unwrap_or(i64::MAX) < total;
        Ok((items, total, has_more))
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
            return Err(ApiError::BadRequest(
                "serial_number is required".to_string(),
            ));
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

    /// Readiness probe: verifies the backing store is reachable.
    ///
    /// # Errors
    /// Returns an error if the repository ping fails (e.g. Postgres unreachable).
    pub async fn check_ready(&self) -> Result<()> {
        self.repo.ping().await
    }

    /// Newest resolved-mint readings (minted/denied) with their owning `user_id`,
    /// for the mint-status SSE poller. Bounded by `limit`.
    ///
    /// # Errors
    /// Returns an error if the underlying query fails.
    pub async fn poll_resolved_mints(&self, limit: i64) -> Result<Vec<(Uuid, MeterReading)>> {
        self.repo.list_resolved_mint_readings(limit).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use meter_core::domain::meter::{MeterStats, ZoneFlow};

    const OWNER_WALLET: &str = "owner-wallet";

    /// Configurable fake repository. Config fields are set before wrapping in
    /// `Arc`; captures use interior mutability.
    #[derive(Default)]
    struct FakeRepo {
        /// `Some(wallet)` = a meter exists with that owner wallet.
        meter_wallet: Option<String>,
        /// When true, `ping` returns an error (simulates an unreachable store).
        ping_should_fail: bool,
        /// Total returned by `count_user_readings` (pagination metadata).
        readings_count: i64,
        /// Returned by `list_resolved_mint_readings`.
        resolved_mints: Vec<(Uuid, MeterReading)>,
        /// Per-zone flow returned by `user_stats`.
        stats_zones: Vec<ZoneFlow>,
        /// Captures.
        readings_page: Mutex<Option<(i64, i64)>>,
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

    #[async_trait::async_trait]
    impl MeterRepositoryTrait for FakeRepo {
        async fn list_user_meters(&self, _user_id: Uuid) -> Result<Vec<Meter>> {
            Ok(self.meter_wallet.iter().map(|w| meter(w)).collect())
        }

        async fn list_map_meters(&self) -> Result<Vec<MeterMapPoint>> {
            Ok(vec![])
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
                zones: self.stats_zones.clone(),
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

        async fn count_user_readings(&self, _user_id: Uuid) -> Result<i64> {
            Ok(self.readings_count)
        }

        async fn list_resolved_mint_readings(
            &self,
            _limit: i64,
        ) -> Result<Vec<(Uuid, MeterReading)>> {
            Ok(self.resolved_mints.clone())
        }

        async fn ping(&self) -> Result<()> {
            if self.ping_should_fail {
                Err(ApiError::Unavailable("store unreachable".to_string()))
            } else {
                Ok(())
            }
        }
    }

    fn service(repo: FakeRepo) -> MeterService {
        MeterService::new(Arc::new(repo))
    }

    // --- page clamping -----------------------------------------------------

    #[tokio::test]
    async fn list_readings_clamps_to_500_1_and_0() {
        let repo = Arc::new(FakeRepo::default());
        let svc = MeterService::new(repo.clone());

        let _ = svc
            .list_my_readings(Uuid::nil(), 10_000, -5)
            .await
            .expect("ok");
        assert_eq!(*repo.readings_page.lock().expect("lock"), Some((500, 0)));

        let _ = svc.list_my_readings(Uuid::nil(), 0, 7).await.expect("ok");
        assert_eq!(*repo.readings_page.lock().expect("lock"), Some((1, 7)));
    }

    #[tokio::test]
    async fn readings_page_reports_total_and_has_more() {
        // FakeRepo returns an empty page but a configurable total, so this pins
        // the metadata arithmetic: has_more = clamped_offset + items.len < total.
        let svc = service(FakeRepo {
            readings_count: 5,
            ..Default::default()
        });
        let (items, total, has_more) = svc
            .list_my_readings_page(Uuid::nil(), 10, 0)
            .await
            .expect("ok");
        assert!(items.is_empty());
        assert_eq!(total, 5);
        assert!(has_more, "0 + 0 < 5 should report more pages");

        // No readings → no more pages.
        let svc = service(FakeRepo::default());
        let (_, total, has_more) = svc
            .list_my_readings_page(Uuid::nil(), 10, 0)
            .await
            .expect("ok");
        assert_eq!(total, 0);
        assert!(!has_more, "0 < 0 should report no more pages");
    }

    // --- stats: per-zone flow ----------------------------------------------

    #[tokio::test]
    async fn my_stats_surfaces_per_zone_flow() {
        // The service passes the repository's per-zone flow through unchanged;
        // net_flow sign distinguishes a net-exporter zone from a net-importer.
        let svc = service(FakeRepo {
            stats_zones: vec![
                ZoneFlow {
                    zone_id: Some(1),
                    total_produced: 30.0,
                    total_consumed: 10.0,
                    net_flow: 20.0,
                    reading_count: 3,
                },
                ZoneFlow {
                    zone_id: None,
                    total_produced: 5.0,
                    total_consumed: 12.0,
                    net_flow: -7.0,
                    reading_count: 2,
                },
            ],
            ..Default::default()
        });
        let stats = svc.my_stats(Uuid::nil()).await.expect("ok");
        assert_eq!(stats.zones.len(), 2);
        assert_eq!(stats.zones[0].zone_id, Some(1));
        assert!(stats.zones[0].net_flow > 0.0, "zone 1 is a net exporter");
        assert_eq!(stats.zones[1].zone_id, None);
        assert!(
            stats.zones[1].net_flow < 0.0,
            "unzoned group is a net importer"
        );
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
        let err = svc
            .register_meter(Uuid::nil(), &req)
            .await
            .expect_err("should reject");
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
        assert_eq!(
            *repo.registered_serial.lock().expect("lock"),
            Some("M-9".to_string())
        );
        assert_eq!(resp.meter.expect("meter").serial_number, "M-9");
    }

    // --- readiness ---------------------------------------------------------

    #[tokio::test]
    async fn check_ready_ok_when_store_reachable() {
        let svc = service(FakeRepo::default());
        svc.check_ready().await.expect("ready");
    }

    #[tokio::test]
    async fn check_ready_errors_when_store_unreachable() {
        let svc = service(FakeRepo {
            ping_should_fail: true,
            ..Default::default()
        });
        let err = svc.check_ready().await.expect_err("should fail");
        assert!(matches!(err, ApiError::Unavailable(_)));
    }
}
