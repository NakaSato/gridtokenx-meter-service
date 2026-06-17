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
use meter_core::traits::MeterRepositoryTrait;
use meter_logic::MeterService;
use meter_persistence::MeterRepository;

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
    let meter_service = MeterService::new(repo);

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

/// Builds the meter-service router from a wired [`AppState`].
///
/// Extracted from [`run`] so integration tests can drive the exact same route
/// table in-process (e.g. via `tower::ServiceExt::oneshot`) without binding a
/// socket.
pub fn build_app(state: AppState) -> Router {
    Router::new()
        .route("/health", get(system::health))
        .route("/health/ready", get(system::ready))
        .route("/api/v1/me/meters", get(meter::get_my_meters))
        .route("/api/v1/meters", post(meter::register_meter))
        .route("/api/v1/meters/readings", get(meter::get_my_readings))
        .route(
            "/api/v1/meters/readings/stream",
            get(meter::stream_readings),
        )
        .route("/api/v1/meters/stats", get(meter::get_meter_stats))
        .route(
            "/api/v1/meters/{serial}/readings",
            post(meter::submit_reading),
        )
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(state)
}
