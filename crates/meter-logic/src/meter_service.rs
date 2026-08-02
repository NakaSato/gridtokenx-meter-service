//! Meter service — applies request bounds and business rules before delegating
//! to the repository.

use std::sync::Arc;

use uuid::Uuid;

use meter_core::domain::meter::{
    Meter, MeterAttestation, MeterMapPoint, MeterReading, MeterStats, MeterVerificationAttempt,
    RegisterMeterRequest, RegisterMeterResponse, VerifyMeterResponse,
    VERIFICATION_METHOD_TELEMETRY,
};
use meter_core::error::{ApiError, Result};
use meter_core::event::{MeterEvent, MeterEventPublisher};
use meter_core::traits::MeterRepositoryTrait;

/// Max readings returned by a single page.
const MAX_READINGS_LIMIT: i64 = 500;

/// Max stored serial length; mirrors the `meters.serial_number varchar(100)`
/// column so an over-long serial fails as a `400` here rather than a raw DB
/// error at insert.
const MAX_SERIAL_LEN: usize = 100;

/// Canonicalizes a meter serial for storage and lookup.
///
/// Trims surrounding whitespace, then — when the trimmed value is a UUID in any
/// accepted form (hyphenated or the 32-hex "simple" form) — rewrites it to the
/// canonical lowercase hyphenated form. This makes the same physical meter
/// claim once regardless of the dash style or case the client sent, so the
/// `meters_serial_number_key` UNIQUE constraint rejects a second claim of the
/// same UUID (e.g. simulator sends `3eb1…-…-…`, a user sends `3eb1…` undashed).
/// Non-UUID serials pass through trimmed, unchanged.
///
/// The Aggregator Bridge attributes readings by exact `meter_serial` match, and
/// devices emit the hyphenated form, so canonicalizing to that form keeps the
/// reading→owner JOIN resolving to the single surviving `meters` row.
#[must_use]
fn canonicalize_serial(raw: &str) -> String {
    let trimmed = raw.trim();
    Uuid::parse_str(trimmed).map_or_else(|_| trimmed.to_string(), |u| u.to_string())
}

/// Default freshness window for the telemetry attestation, in hours (30 days).
/// Wide on purpose: it exists to reject a meter that has *never* proven itself,
/// not to un-verify one that went quiet for a fortnight. Verification is a
/// one-way latch — a meter that stops reporting keeps `is_verified` — so a tight
/// window here would only make first-time verification flaky, never revoke
/// anything. Override with `METER_VERIFY_WINDOW_HOURS`.
pub const DEFAULT_VERIFY_WINDOW_HOURS: i64 = 720;

/// Service layer over [`MeterRepositoryTrait`].
#[derive(Clone)]
pub struct MeterService {
    repo: Arc<dyn MeterRepositoryTrait>,
    /// Optional best-effort publisher for meter domain events. `None` disables
    /// emission entirely (default); wired in `startup` only when
    /// `METER_EVENTS_ENABLED` is set. Publishing never blocks or fails a call.
    event_publisher: Option<Arc<dyn MeterEventPublisher>>,
    /// How far back [`Self::verify_meter`] looks for attested telemetry.
    verify_window_hours: i64,
}

impl MeterService {
    /// Creates a new service over the given repository, with event emission
    /// disabled. Existing callers/tests keep this signature.
    #[must_use]
    pub fn new(repo: Arc<dyn MeterRepositoryTrait>) -> Self {
        Self {
            repo,
            event_publisher: None,
            verify_window_hours: DEFAULT_VERIFY_WINDOW_HOURS,
        }
    }

    /// Creates a service that emits meter domain events via `publisher` on
    /// successful mutations. `None` behaves exactly like [`Self::new`].
    #[must_use]
    pub fn with_event_publisher(
        repo: Arc<dyn MeterRepositoryTrait>,
        publisher: Option<Arc<dyn MeterEventPublisher>>,
    ) -> Self {
        Self {
            repo,
            event_publisher: publisher,
            verify_window_hours: DEFAULT_VERIFY_WINDOW_HOURS,
        }
    }

    /// Overrides the telemetry-attestation freshness window. Values below 1 hour
    /// are clamped up — a zero/negative window would make every verification
    /// fail with a message that reads like a device fault.
    #[must_use]
    pub fn with_verify_window_hours(mut self, hours: i64) -> Self {
        self.verify_window_hours = hours.max(1);
        self
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
    /// if the serial is already registered, or a database error. The conflict
    /// distinguishes a serial the caller already holds from one held by another
    /// account, so the message can't be misread as "you registered this".
    pub async fn register_meter(
        &self,
        user_id: Uuid,
        req: &RegisterMeterRequest,
    ) -> Result<RegisterMeterResponse> {
        if req.serial_number.trim().is_empty() {
            return Err(ApiError::BadRequest(
                "serial_number is required".to_string(),
            ));
        }
        // Persist the canonical serial (trimmed, UUIDs hyphenated-lowercased) so
        // the same physical meter claims once regardless of dash style/case, and
        // a reading submitted with a whitespace-padded serial still resolves the
        // meter by exact equality.
        let serial = canonicalize_serial(&req.serial_number);
        if serial.chars().count() > MAX_SERIAL_LEN {
            return Err(ApiError::BadRequest(format!(
                "serial_number too long (max {MAX_SERIAL_LEN} characters)"
            )));
        }
        let normalized = RegisterMeterRequest {
            serial_number: serial,
            meter_type: req.meter_type.clone(),
            location: req.location.clone(),
            latitude: req.latitude,
            longitude: req.longitude,
        };
        let meter = match self.repo.register_meter(user_id, &normalized).await {
            Ok(meter) => meter,
            // The unique-constraint hit proves the serial is taken but says
            // nothing about *by whom*, and the bare "already registered" reads
            // as if this caller had registered it — which sends someone hunting
            // for a meter that is not on their account. Naming the two cases
            // apart leaks nothing: a caller can already tell which of their own
            // serials exist from `GET /api/v1/me/meters`.
            Err(ApiError::Conflict(unrefined)) => {
                let owned = self
                    .repo
                    .find_meter_by_serial(user_id, &normalized.serial_number)
                    .await;
                return Err(match owned {
                    Ok(Some(_)) => ApiError::Conflict(format!(
                        "you have already registered meter '{}'",
                        normalized.serial_number
                    )),
                    Ok(None) => ApiError::Conflict(format!(
                        "meter '{}' is already registered to another account",
                        normalized.serial_number
                    )),
                    // The refinement is a courtesy, not the answer — if the
                    // ownership lookup fails, still report the conflict we
                    // actually proved rather than turning it into a 500.
                    Err(_) => ApiError::Conflict(unrefined),
                });
            }
            Err(e) => return Err(e),
        };

        // Best-effort domain event AFTER the row is committed. Non-blocking and
        // failure-isolated: the publisher spawns delivery and never returns an
        // error, so a broker hiccup can't fail or delay this registration.
        if let Some(publisher) = &self.event_publisher {
            publisher.publish(MeterEvent::meter_registered(user_id, &meter));
        }

        Ok(RegisterMeterResponse {
            success: true,
            message: format!("Meter '{}' registered", meter.serial_number),
            meter: Some(meter),
        })
    }

    /// Verifies one of the caller's meters by **telemetry attestation**.
    ///
    /// Registration only records a claim on a serial. This proves the claim: it
    /// passes when the Aggregator Bridge has already accepted at least one
    /// Ed25519-signed reading from that serial *attributed to this owner* inside
    /// the freshness window. That makes verification a statement about the
    /// physical device — it exists, it holds the provisioned device key, and its
    /// readings resolve to this account — rather than about the HTTP session.
    ///
    /// Idempotent: verifying an already-verified meter is a success that changes
    /// nothing (`already_verified = true`), so a client retrying after a dropped
    /// response never sees a spurious failure.
    ///
    /// The flip is a one-way latch. Nothing here un-verifies a meter that goes
    /// quiet: revocation is a different decision (an operator's, on evidence of
    /// tampering or transfer) and giving a network outage the power to strand a
    /// prosumer's open orders would be worse than the risk it addresses.
    ///
    /// # Errors
    /// [`ApiError::NotFound`] when the caller owns no meter with that serial —
    /// ownership is enforced by the owner-scoped lookup, so another user's meter
    /// is indistinguishable from a nonexistent one. [`ApiError::Conflict`] when
    /// no attested telemetry exists yet. Database errors propagate.
    pub async fn verify_meter(&self, user_id: Uuid, serial: &str) -> Result<VerifyMeterResponse> {
        // Canonicalized the same way registration stores it, so a serial typed
        // with different dash styling still resolves the row it claimed.
        let serial = canonicalize_serial(serial);

        let meter = self
            .repo
            .find_meter_by_serial(user_id, &serial)
            .await?
            .ok_or_else(|| {
                ApiError::NotFound(format!("meter '{serial}' is not registered to you"))
            })?;

        let attested = self
            .repo
            .count_attested_readings(user_id, &serial, self.verify_window_hours)
            .await?;
        let attestation = MeterAttestation {
            attested_readings: attested,
            window_hours: self.verify_window_hours,
        };

        if meter.is_verified {
            return Ok(VerifyMeterResponse {
                success: true,
                message: format!("Meter '{serial}' is already verified"),
                already_verified: true,
                attestation,
                meter: Some(meter),
            });
        }

        if attested == 0 {
            let reason = format!(
                "no signature-verified telemetry from meter '{serial}' in the last {}h",
                self.verify_window_hours
            );
            self.record_attempt(user_id, &serial, false, Some(reason.clone()))
                .await;
            return Err(ApiError::Conflict(format!(
                "{reason} — bring the device online and let it stream before selling"
            )));
        }

        let verified = self
            .repo
            .mark_meter_verified(user_id, &serial)
            .await?
            .ok_or_else(|| {
                // The owner-scoped lookup above found it, so losing the row between
                // the two statements means it was deleted concurrently.
                ApiError::NotFound(format!("meter '{serial}' is not registered to you"))
            })?;

        self.record_attempt(user_id, &serial, true, None).await;

        // Best-effort domain event AFTER the flip is committed, so downstream
        // read-models (trading's sell-side gate) learn the meter is sellable.
        if let Some(publisher) = &self.event_publisher {
            publisher.publish(MeterEvent::meter_updated(user_id, &verified));
        }

        Ok(VerifyMeterResponse {
            success: true,
            message: format!(
                "Meter '{serial}' verified from {attested} attested reading(s); you can now open sell orders"
            ),
            already_verified: false,
            attestation,
            meter: Some(verified),
        })
    }

    /// Appends a verification attempt to the audit trail, best-effort.
    ///
    /// A failed audit write is logged, never propagated. On the success path the
    /// flip is already committed, so returning an error here would tell the owner
    /// their meter is unverified when it is verified — a worse outcome than a gap
    /// in the trail. On the failure path the caller is already getting an error.
    async fn record_attempt(
        &self,
        user_id: Uuid,
        serial: &str,
        succeeded: bool,
        failure_reason: Option<String>,
    ) {
        let attempt = MeterVerificationAttempt {
            user_id,
            meter_serial: serial.to_string(),
            verification_method: VERIFICATION_METHOD_TELEMETRY.to_string(),
            succeeded,
            failure_reason,
        };
        if let Err(e) = self.repo.record_verification_attempt(&attempt).await {
            tracing::warn!(
                "meter verification attempt for '{serial}' (succeeded={succeeded}) not audited: {e}"
            );
        }
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
    // Independent, orthogonal failure switches — a state machine or enums would
    // obscure that any combination is legal, which is the point of the double.
    #[allow(clippy::struct_excessive_bools)]
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
        /// `is_verified` on the meter `find_meter_by_serial` hands back.
        meter_verified: bool,
        /// Returned by `count_attested_readings` — the telemetry evidence.
        attested_readings: i64,
        /// When true, `record_verification_attempt` fails (audit-write outage).
        audit_should_fail: bool,
        /// When true, `register_meter` hits the serial unique constraint.
        register_conflicts: bool,
        /// Captures.
        readings_page: Mutex<Option<(i64, i64)>>,
        registered_serial: Mutex<Option<String>>,
        /// Serial passed to `mark_meter_verified`, if it was called at all.
        verified_serial: Mutex<Option<String>>,
        /// Window (hours) `count_attested_readings` was asked for.
        attested_window: Mutex<Option<i64>>,
        /// Every audited attempt: `(serial, succeeded, failure_reason)`.
        attempts: Mutex<Vec<(String, bool, Option<String>)>>,
    }

    fn meter(wallet: &str) -> Meter {
        meter_with(wallet, true)
    }

    fn meter_with(wallet: &str, is_verified: bool) -> Meter {
        Meter {
            id: Uuid::nil(),
            serial_number: "M1".to_string(),
            meter_type: "solar".to_string(),
            location: String::new(),
            is_verified,
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
            if self.register_conflicts {
                // Mirrors the persistence layer's 23505 mapping: proves the
                // serial is taken, without saying by whom.
                return Err(ApiError::Conflict(format!(
                    "meter '{}' already registered",
                    req.serial_number
                )));
            }
            let mut m = meter(OWNER_WALLET);
            m.serial_number = req.serial_number.clone();
            Ok(m)
        }

        async fn find_meter_by_serial(
            &self,
            _user_id: Uuid,
            serial: &str,
        ) -> Result<Option<Meter>> {
            Ok(self.meter_wallet.as_deref().map(|w| {
                let mut m = meter_with(w, self.meter_verified);
                m.serial_number = serial.to_string();
                m
            }))
        }

        async fn count_attested_readings(
            &self,
            _user_id: Uuid,
            _serial: &str,
            within_hours: i64,
        ) -> Result<i64> {
            *self.attested_window.lock().expect("lock") = Some(within_hours);
            Ok(self.attested_readings)
        }

        async fn mark_meter_verified(&self, _user_id: Uuid, serial: &str) -> Result<Option<Meter>> {
            *self.verified_serial.lock().expect("lock") = Some(serial.to_string());
            Ok(self.meter_wallet.as_deref().map(|w| {
                let mut m = meter_with(w, true);
                m.serial_number = serial.to_string();
                m
            }))
        }

        async fn record_verification_attempt(
            &self,
            attempt: &MeterVerificationAttempt,
        ) -> Result<()> {
            if self.audit_should_fail {
                return Err(ApiError::Unavailable("audit store down".to_string()));
            }
            self.attempts.lock().expect("lock").push((
                attempt.meter_serial.clone(),
                attempt.succeeded,
                attempt.failure_reason.clone(),
            ));
            Ok(())
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

    #[tokio::test]
    async fn register_meter_canonicalizes_uuid_serial() {
        // A bare 32-hex UUID (as a user might paste) is stored hyphenated-lower,
        // so it collides on the UNIQUE serial with the simulator's dashed form
        // instead of creating a second `meters` row for the same physical meter.
        let repo = Arc::new(FakeRepo::default());
        let svc = MeterService::new(repo.clone());
        let req = RegisterMeterRequest {
            serial_number: "  3EB13B9046684257BDD640FB06671AD1  ".to_string(),
            meter_type: None,
            location: None,
            latitude: None,
            longitude: None,
        };
        svc.register_meter(Uuid::nil(), &req).await.expect("ok");
        assert_eq!(
            *repo.registered_serial.lock().expect("lock"),
            Some("3eb13b90-4668-4257-bdd6-40fb06671ad1".to_string())
        );
    }

    fn register_req(serial: &str) -> RegisterMeterRequest {
        RegisterMeterRequest {
            serial_number: serial.to_string(),
            meter_type: None,
            location: None,
            latitude: None,
            longitude: None,
        }
    }

    #[tokio::test]
    async fn register_meter_rejects_over_long_serial() {
        let repo = Arc::new(FakeRepo::default());
        let svc = MeterService::new(repo.clone());
        let req = RegisterMeterRequest {
            serial_number: "X".repeat(MAX_SERIAL_LEN + 1),
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
        assert!(repo.registered_serial.lock().expect("lock").is_none());
    }

    /// A taken serial the caller does NOT own must not be described in words
    /// that imply they registered it — that reading sent a real operator
    /// hunting for a meter that was never on their account.
    #[tokio::test]
    async fn register_conflict_on_another_users_serial_says_another_account() {
        // meter_wallet: None => the owner-scoped lookup finds nothing, i.e. the
        // serial belongs to somebody else.
        let svc = service(FakeRepo {
            register_conflicts: true,
            meter_wallet: None,
            ..Default::default()
        });
        let err = svc
            .register_meter(Uuid::nil(), &register_req("SN-TAKEN"))
            .await
            .expect_err("should conflict");

        let ApiError::Conflict(msg) = err else {
            panic!("expected Conflict, got {err:?}")
        };
        assert!(
            msg.contains("another account"),
            "message must attribute the claim elsewhere, got {msg:?}"
        );
    }

    #[tokio::test]
    async fn register_conflict_on_own_serial_says_already_yours() {
        // meter_wallet: Some(..) => the owner-scoped lookup finds it, so the
        // caller is re-registering a serial they already hold.
        let svc = service(FakeRepo {
            register_conflicts: true,
            meter_wallet: Some(OWNER_WALLET.to_string()),
            ..Default::default()
        });
        let err = svc
            .register_meter(Uuid::nil(), &register_req("SN-MINE"))
            .await
            .expect_err("should conflict");

        let ApiError::Conflict(msg) = err else {
            panic!("expected Conflict, got {err:?}")
        };
        assert!(
            msg.contains("you have already registered"),
            "message must name the caller as the holder, got {msg:?}"
        );
    }

    #[test]
    fn canonicalize_serial_leaves_non_uuid_untouched() {
        assert_eq!(canonicalize_serial("  METER-XYZ-1  "), "METER-XYZ-1");
        assert_eq!(
            canonicalize_serial("3eb13b90-4668-4257-bdd6-40fb06671ad1"),
            "3eb13b90-4668-4257-bdd6-40fb06671ad1"
        );
    }

    // --- verify ------------------------------------------------------------

    /// A repo holding one unverified meter with `attested` signature-verified
    /// readings behind it.
    fn verify_repo(attested: i64) -> FakeRepo {
        FakeRepo {
            meter_wallet: Some(OWNER_WALLET.to_string()),
            meter_verified: false,
            attested_readings: attested,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn verify_meter_flips_when_attested_telemetry_exists() {
        let repo = Arc::new(verify_repo(3));
        let svc = MeterService::new(repo.clone());

        let resp = svc.verify_meter(Uuid::nil(), "M1").await.expect("verified");

        assert!(resp.success);
        assert!(!resp.already_verified);
        assert_eq!(resp.attestation.attested_readings, 3);
        assert!(resp.meter.expect("meter").is_verified);
        assert_eq!(
            *repo.verified_serial.lock().expect("lock"),
            Some("M1".to_string()),
            "the flip must actually reach the repository"
        );
        assert_eq!(
            *repo.attempts.lock().expect("lock"),
            vec![("M1".to_string(), true, None)],
            "a successful verification must leave an audit row"
        );
    }

    #[tokio::test]
    async fn verify_meter_refuses_without_attested_telemetry() {
        // The whole point of the gate: a registered-but-unproven meter stays
        // unverified, so Trading keeps refusing sell orders backed by it.
        let repo = Arc::new(verify_repo(0));
        let svc = MeterService::new(repo.clone());

        let err = svc
            .verify_meter(Uuid::nil(), "M1")
            .await
            .expect_err("should refuse");

        assert!(matches!(err, ApiError::Conflict(_)), "got {err:?}");
        assert!(
            repo.verified_serial.lock().expect("lock").is_none(),
            "a refused verification must not flip the flag"
        );
        let attempts = repo.attempts.lock().expect("lock");
        assert_eq!(attempts.len(), 1, "the failed attempt must be audited");
        assert!(!attempts[0].1);
        assert!(
            attempts[0]
                .2
                .as_deref()
                .is_some_and(|r| r.contains("no signature-verified telemetry")),
            "the audit row must record why: {:?}",
            attempts[0].2
        );
    }

    #[tokio::test]
    async fn verify_meter_is_idempotent_on_an_already_verified_meter() {
        // A client retrying after a dropped response must not see a failure, and
        // the retry must not write a second audit row or re-issue the event.
        let repo = Arc::new(FakeRepo {
            meter_wallet: Some(OWNER_WALLET.to_string()),
            meter_verified: true,
            attested_readings: 0,
            ..Default::default()
        });
        let svc = MeterService::new(repo.clone());

        let resp = svc.verify_meter(Uuid::nil(), "M1").await.expect("ok");

        assert!(resp.success);
        assert!(resp.already_verified);
        assert!(repo.verified_serial.lock().expect("lock").is_none());
        assert!(repo.attempts.lock().expect("lock").is_empty());
    }

    #[tokio::test]
    async fn verify_meter_404s_a_meter_the_caller_does_not_own() {
        // `meter_wallet: None` = the owner-scoped lookup finds nothing. Another
        // user's meter is indistinguishable from a nonexistent one, on purpose:
        // a distinguishable 403 would confirm which serials are registered.
        let repo = Arc::new(FakeRepo {
            attested_readings: 99,
            ..Default::default()
        });
        let svc = MeterService::new(repo.clone());

        let err = svc
            .verify_meter(Uuid::nil(), "SOMEONE-ELSES")
            .await
            .expect_err("should 404");

        assert!(matches!(err, ApiError::NotFound(_)), "got {err:?}");
        assert!(
            repo.verified_serial.lock().expect("lock").is_none(),
            "abundant telemetry must not verify a meter the caller does not own"
        );
    }

    #[tokio::test]
    async fn verify_meter_canonicalizes_the_serial() {
        // Same normalization registration applies, so a UUID serial typed in the
        // bare 32-hex form still resolves the row it claimed.
        let repo = Arc::new(verify_repo(1));
        let svc = MeterService::new(repo.clone());

        svc.verify_meter(Uuid::nil(), "  3EB13B9046684257BDD640FB06671AD1  ")
            .await
            .expect("verified");

        assert_eq!(
            *repo.verified_serial.lock().expect("lock"),
            Some("3eb13b90-4668-4257-bdd6-40fb06671ad1".to_string())
        );
    }

    #[tokio::test]
    async fn verify_meter_uses_the_configured_window() {
        let repo = Arc::new(verify_repo(1));
        let svc = MeterService::new(repo.clone()).with_verify_window_hours(24);

        let resp = svc.verify_meter(Uuid::nil(), "M1").await.expect("verified");

        assert_eq!(*repo.attested_window.lock().expect("lock"), Some(24));
        assert_eq!(resp.attestation.window_hours, 24);

        // A non-positive window would make every verification unsatisfiable, so
        // it clamps to 1 rather than silently locking every prosumer out.
        let repo = Arc::new(verify_repo(1));
        let svc = MeterService::new(repo.clone()).with_verify_window_hours(0);
        svc.verify_meter(Uuid::nil(), "M1").await.expect("verified");
        assert_eq!(*repo.attested_window.lock().expect("lock"), Some(1));
    }

    #[tokio::test]
    async fn verify_meter_succeeds_even_if_the_audit_write_fails() {
        // The flip is already committed by then; failing the response would tell
        // the owner their meter is unverified when it is verified.
        let repo = Arc::new(FakeRepo {
            meter_wallet: Some(OWNER_WALLET.to_string()),
            meter_verified: false,
            attested_readings: 2,
            audit_should_fail: true,
            ..Default::default()
        });
        let svc = MeterService::new(repo.clone());

        let resp = svc.verify_meter(Uuid::nil(), "M1").await.expect("verified");
        assert!(resp.success);
        assert!(repo.verified_serial.lock().expect("lock").is_some());
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
