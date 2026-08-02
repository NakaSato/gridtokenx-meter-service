//! Service startup — connects the DB, wires dependencies as traits, builds the
//! router, and serves until shutdown.
//!
//! Dependency direction: server → api → logic → persistence → core.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use axum::routing::{get, post};
use axum::Router;
use sqlx::postgres::PgPoolOptions;
use tokio::sync::broadcast;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::info;

use meter_api::handlers::{meter, system};
use meter_api::{AppState, ReadingEvent};
use meter_core::config::Config;
use meter_core::event::MeterEventPublisher;
use meter_core::traits::MeterRepositoryTrait;
use meter_logic::MeterService;
use meter_persistence::{KafkaMeterEventPublisher, MeterRepository};

/// Capacity of the realtime readings broadcast channel. Lagged subscribers skip
/// missed events rather than blocking publishers.
const READINGS_CHANNEL_CAP: usize = 256;

/// Connects dependencies and serves the meter service until the process exits.
///
/// # Errors
/// Returns an error if the Postgres pool cannot be created or the TCP listener
/// fails to bind / serve.
pub async fn run(config: Config) -> anyhow::Result<()> {
    // 1. Database pool
    let pool = PgPoolOptions::new()
        .max_connections(config.database_max_connections)
        .acquire_timeout(std::time::Duration::from_secs(10))
        .connect(&config.database_url)
        .await
        .context("Failed to connect to PostgreSQL")?;
    info!("✅ Connected to Postgres");

    // 2. Repository (as a trait) → service (DI).
    let repo: Arc<dyn MeterRepositoryTrait> = Arc::new(MeterRepository::new(pool));

    // 2a. Optional Kafka event publisher for meter domain events (feeds the
    //     trading + aggregator read-models). Gated OFF by default; a bad broker
    //     config degrades to disabled rather than failing startup.
    let event_publisher: Option<Arc<dyn MeterEventPublisher>> = if config.events_enabled {
        match KafkaMeterEventPublisher::new(&config.kafka_bootstrap_servers, &config.events_topic) {
            Ok(publisher) => {
                info!(
                    "✅ Meter event publisher enabled (topic: {})",
                    config.events_topic
                );
                Some(Arc::new(publisher))
            }
            Err(e) => {
                tracing::warn!("Meter event publisher disabled — producer init failed: {e}");
                None
            }
        }
    } else {
        info!("Meter event publisher disabled (METER_EVENTS_ENABLED unset)");
        None
    };
    let meter_service = MeterService::with_event_publisher(repo, event_publisher)
        .with_verify_window_hours(config.verify_window_hours);

    // 3. Realtime readings broadcast channel (submit → SSE subscribers).
    let (readings_tx, _) = broadcast::channel::<Arc<ReadingEvent>>(READINGS_CHANNEL_CAP);

    // 3a. Mint-status poller: pushes pending→minted/denied transitions onto the
    //     same channel (the mint columns are written out-of-band by other
    //     services). Disabled when `mint_poll_secs == 0`.
    crate::mint_poller::spawn(
        meter_service.clone(),
        readings_tx.clone(),
        config.mint_poll_secs,
    );

    let state = AppState {
        meter_service,
        jwt_secret: Arc::from(config.jwt_secret.as_str()),
        readings_tx,
    };

    // 4. Router
    let app = build_app(state);

    // 5. Serve
    let addr = SocketAddr::from(([0, 0, 0, 0], config.port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!("🔌 meter-service listening on {addr}");
    axum::serve(listener, app).await?;
    Ok(())
}

/// Serves Prometheus metrics in text-exposition format. Gated to internal CIDRs
/// at the APISIX gateway (same policy as `/health`).
async fn metrics_handler() -> String {
    crate::metrics::render()
}

/// Builds the meter-service router from a wired [`AppState`].
///
/// Extracted from [`run`] so integration tests can drive the exact same route
/// table in-process (e.g. via `tower::ServiceExt::oneshot`) without binding a
/// socket.
pub fn build_app(state: AppState) -> Router {
    Router::new()
        .route("/health", get(system::health))
        .route("/health/ready", get(system::ready))
        .route("/metrics", get(metrics_handler))
        // ---- Canonical surface: caller-scoped routes live under `/api/v1/me` ----
        // Every route below is scoped to the JWT `sub`, so the path says so. This
        // matches the platform user-self convention (IAM `/me/wallets`, Trading
        // `/me/orders`, Noti `/me/notifications`); APISIX route 12 carries
        // priority 20 so `/api/v1/me/meters*` reaches this service rather than
        // being swallowed by IAM's `/api/v1/me/*` route 11.
        .route(
            "/api/v1/me/meters",
            get(meter::get_my_meters).post(meter::register_meter),
        )
        .route("/api/v1/me/meters/readings", get(meter::get_my_readings))
        .route(
            "/api/v1/me/meters/readings/stream",
            get(meter::stream_readings),
        )
        .route("/api/v1/me/meters/stats", get(meter::get_meter_stats))
        // Possession proof. Distinct from registration (which only claims the
        // serial): until this passes, Trading refuses sell orders on the meter.
        // No conflict with the static `readings`/`stats` segments above — those
        // are one segment past `meters`, this is two.
        .route(
            "/api/v1/me/meters/{serial}/verify",
            post(meter::verify_meter),
        )
        // Grid-wide, deliberately NOT caller-scoped (the map shows every located
        // meter across all users) — so it stays off the `/me` base.
        .route("/api/v1/meters/map", get(meter::get_meters_map))
        // ---- Legacy aliases (dual-served, same handlers) ----
        // Pre-unification paths, kept so the Trading UI and any live client keep
        // working while callers migrate to the `/api/v1/me/meters*` forms above.
        // Remove only once no caller hits them; each maps to the identical
        // handler, so behavior is byte-for-byte the same on either path.
        .route("/api/v1/meters", post(meter::register_meter))
        .route("/api/v1/meters/readings", get(meter::get_my_readings))
        .route(
            "/api/v1/meters/readings/stream",
            get(meter::stream_readings),
        )
        .route("/api/v1/meters/stats", get(meter::get_meter_stats))
        .route("/api/v1/meters/{serial}/verify", post(meter::verify_meter))
        // Reading ingest removed: meter telemetry now flows **only** via the
        // Aggregator Bridge (Ed25519-signed IoT gateway). This service no longer
        // accepts direct reading writes; it serves the dashboard read paths and
        // pushes mint-status transitions over SSE.
        .layer(axum::middleware::from_fn(crate::metrics::track_http))
        // INFO-level request span so traces export to Tempo (the default
        // make_span is DEBUG and is filtered out under the standard `info` env).
        .layer(TraceLayer::new_for_http().make_span_with(
            |request: &axum::http::Request<axum::body::Body>| {
                tracing::info_span!(
                    "http_request",
                    method = %request.method(),
                    uri = %request.uri().path(),
                )
            },
        ))
        .layer(CorsLayer::permissive())
        .with_state(state)
}
