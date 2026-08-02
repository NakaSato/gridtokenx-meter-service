//! Background poller that pushes mint-status transitions to SSE subscribers.
//!
//! This service never writes the mint columns (other services do, out-of-band in
//! Postgres). To surface `pending → minted`/`denied` transitions on the realtime
//! stream without a DB trigger (the `meter_readings` schema is owned by IAM), the
//! poller periodically snapshots the newest resolved-mint readings and diffs each
//! snapshot against the previous one, broadcasting only what changed. On startup
//! it primes its seen-set without broadcasting, so the existing minted backlog is
//! not replayed — only transitions that happen while the service is running are
//! pushed. It is best-effort: clients still reconcile by re-fetching list/stats.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast;
use tracing::{debug, info, warn};
use uuid::Uuid;

use meter_api::ReadingEvent;
use meter_core::domain::meter::MeterReading;
use meter_logic::MeterService;

/// Max readings pulled per poll (newest first). Bounds both the query and the
/// retained seen-set.
const POLL_LIMIT: i64 = 500;

/// Diff a fresh snapshot against the previous mint-status-by-id map. Returns the
/// readings to broadcast — those whose id is newly resolved, or whose resolved
/// status changed — together with the next seen-map (bounded to this snapshot, so
/// it never grows without limit).
fn diff_transitions(
    batch: Vec<(Uuid, MeterReading)>,
    prev: &HashMap<Uuid, String>,
) -> (Vec<(Uuid, MeterReading)>, HashMap<Uuid, String>) {
    let mut next = HashMap::with_capacity(batch.len());
    let mut out = Vec::new();
    for (user_id, reading) in batch {
        let changed = prev
            .get(&reading.id)
            .is_none_or(|s| s != &reading.mint_status);
        next.insert(reading.id, reading.mint_status.clone());
        if changed {
            out.push((user_id, reading));
        }
    }
    (out, next)
}

/// Spawn the poller as a detached task. A no-op when `interval_secs == 0`.
pub fn spawn(service: MeterService, tx: broadcast::Sender<Arc<ReadingEvent>>, interval_secs: u64) {
    if interval_secs == 0 {
        info!("mint-status poller disabled (METER_MINT_POLL_SECS=0)");
        return;
    }
    info!("mint-status poller started ({interval_secs}s interval)");
    tokio::spawn(async move {
        let mut seen: HashMap<Uuid, String> = HashMap::new();

        // Prime: record the current resolved state WITHOUT broadcasting.
        match service.poll_resolved_mints(POLL_LIMIT).await {
            Ok(batch) => {
                let (_skipped, primed) = diff_transitions(batch, &HashMap::new());
                info!(primed = primed.len(), "mint-status poller primed seen-set");
                seen = primed;
            }
            Err(e) => warn!("mint-status poller prime failed: {e}"),
        }

        let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));
        ticker.tick().await; // consume the immediate first tick
        loop {
            ticker.tick().await;
            match service.poll_resolved_mints(POLL_LIMIT).await {
                Ok(batch) => {
                    let snapshot_len = batch.len();
                    let (transitions, next) = diff_transitions(batch, &seen);
                    seen = next;
                    if !transitions.is_empty() {
                        info!(
                            transitions = transitions.len(),
                            snapshot = snapshot_len,
                            "mint-status poll: broadcasting transitions"
                        );
                    }
                    for (user_id, reading) in transitions {
                        debug!(
                            reading_id = %reading.id,
                            %user_id,
                            mint_status = %reading.mint_status,
                            "mint-status transition pushed to SSE"
                        );
                        // Send error only means no subscribers; never fatal.
                        let _ = tx.send(Arc::new(ReadingEvent { user_id, reading }));
                    }
                }
                Err(e) => warn!("mint-status poll failed: {e}"),
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    use meter_core::domain::meter::{
        Meter, MeterMapPoint, MeterStats, MeterVerificationAttempt, RegisterMeterRequest,
    };
    use meter_core::error::Result;
    use meter_core::traits::MeterRepositoryTrait;

    /// Repo whose data methods are never reached: the disabled-poller path
    /// returns before any query, so this only has to satisfy the trait.
    struct UnusedRepo;

    #[async_trait::async_trait]
    impl MeterRepositoryTrait for UnusedRepo {
        async fn list_user_meters(&self, _user_id: Uuid) -> Result<Vec<Meter>> {
            Ok(vec![])
        }
        async fn list_map_meters(&self) -> Result<Vec<MeterMapPoint>> {
            Ok(vec![])
        }
        async fn list_user_readings(
            &self,
            _user_id: Uuid,
            _limit: i64,
            _offset: i64,
        ) -> Result<Vec<MeterReading>> {
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
                zones: vec![],
            })
        }
        async fn register_meter(
            &self,
            _user_id: Uuid,
            _req: &RegisterMeterRequest,
        ) -> Result<Meter> {
            unreachable!("disabled poller never registers")
        }
        async fn find_meter_by_serial(
            &self,
            _user_id: Uuid,
            _serial: &str,
        ) -> Result<Option<Meter>> {
            Ok(None)
        }
        async fn count_user_readings(&self, _user_id: Uuid) -> Result<i64> {
            Ok(0)
        }
        async fn count_attested_readings(
            &self,
            _user_id: Uuid,
            _serial: &str,
            _within_hours: i64,
        ) -> Result<i64> {
            Ok(0)
        }
        async fn mark_meter_verified(
            &self,
            _user_id: Uuid,
            _serial: &str,
        ) -> Result<Option<Meter>> {
            unreachable!("disabled poller never verifies")
        }
        async fn record_verification_attempt(
            &self,
            _attempt: &MeterVerificationAttempt,
        ) -> Result<()> {
            unreachable!("disabled poller never verifies")
        }
        async fn list_resolved_mint_readings(
            &self,
            _limit: i64,
        ) -> Result<Vec<(Uuid, MeterReading)>> {
            Ok(vec![])
        }
        async fn ping(&self) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn disabled_poller_is_a_noop() {
        // interval_secs == 0 must early-return: it spawns no task and never
        // touches the repo, so the branch runs without a Tokio runtime and
        // nothing is ever broadcast.
        let service = MeterService::new(Arc::new(UnusedRepo));
        let (tx, mut rx) = broadcast::channel::<Arc<ReadingEvent>>(4);
        spawn(service, tx, 0);
        assert!(
            rx.try_recv().is_err(),
            "disabled poller must not broadcast anything"
        );
    }

    fn reading(id: Uuid, mint_status: &str) -> MeterReading {
        MeterReading {
            id,
            meter_serial: "M1".to_string(),
            kwh: 1.0,
            timestamp: String::new(),
            submitted_at: String::new(),
            mint_status: mint_status.to_string(),
            mint_tx_signature: None,
            ..Default::default()
        }
    }

    #[test]
    fn newly_resolved_reading_is_a_transition() {
        let user = Uuid::new_v4();
        let id = Uuid::new_v4();
        let batch = vec![(user, reading(id, "minted"))];
        let (out, next) = diff_transitions(batch, &HashMap::new());
        assert_eq!(
            out.len(),
            1,
            "first sight of a resolved reading should emit"
        );
        assert_eq!(next.get(&id).map(String::as_str), Some("minted"));
    }

    #[test]
    fn unchanged_status_is_not_re_emitted() {
        let user = Uuid::new_v4();
        let id = Uuid::new_v4();
        let mut prev = HashMap::new();
        prev.insert(id, "minted".to_string());
        let batch = vec![(user, reading(id, "minted"))];
        let (out, _next) = diff_transitions(batch, &prev);
        assert!(out.is_empty(), "same status must not re-broadcast");
    }

    #[test]
    fn changed_status_is_a_transition() {
        let user = Uuid::new_v4();
        let id = Uuid::new_v4();
        let mut prev = HashMap::new();
        prev.insert(id, "pending".to_string());
        let batch = vec![(user, reading(id, "denied"))];
        let (out, _next) = diff_transitions(batch, &prev);
        assert_eq!(out.len(), 1, "status change must broadcast");
        assert_eq!(out[0].1.mint_status, "denied");
    }

    #[test]
    fn seen_map_is_bounded_to_current_snapshot() {
        // A stale id present in `prev` but absent from the batch is dropped from
        // `next`, so the map can't grow unbounded over time.
        let mut prev = HashMap::new();
        prev.insert(Uuid::new_v4(), "minted".to_string());
        let (_out, next) = diff_transitions(Vec::new(), &prev);
        assert!(next.is_empty(), "next map should reflect only the snapshot");
    }
}
