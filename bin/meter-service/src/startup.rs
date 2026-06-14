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
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::info;

use meter_api::handlers::{meter, system};
use meter_api::AppState;
use meter_core::config::Config;
use meter_core::traits::{MeterRepositoryTrait, MintGateway};
use meter_logic::MeterService;
use meter_persistence::{DisabledMintGateway, MeterRepository};

/// Connects dependencies and serves the meter service until the process exits.
pub async fn run(config: Config) -> anyhow::Result<()> {
    // 1. Database pool
    let pool = PgPoolOptions::new()
        .max_connections(config.database_max_connections)
        .acquire_timeout(std::time::Duration::from_secs(10))
        .connect(&config.database_url)
        .await
        .context("Failed to connect to PostgreSQL")?;
    info!("✅ Connected to Postgres");

    // 2. Repository + mint gateway (as traits) → service (DI).
    //    No blockchain backend is configured for this service, so minting is
    //    disabled (mint endpoints return 503). Swap in a NATS-backed gateway
    //    once Chain Bridge access is provisioned.
    let repo: Arc<dyn MeterRepositoryTrait> = Arc::new(MeterRepository::new(pool));
    let mint: Arc<dyn MintGateway> = Arc::new(DisabledMintGateway);
    let meter_service = MeterService::new(repo, mint);

    // 2b. Device-reading consumer (aggregator bridge → meter-service mint
    //     handoff). Degraded by design: unset NATS_URL or a connect failure
    //     leaves the HTTP API running normally.
    if let Some(nats_url) = config.nats_url.clone() {
        let subject = config.meter_reading_subject.clone();
        let consumer_service = meter_service.clone();
        tokio::spawn(async move {
            if let Err(e) =
                crate::reading_consumer::run(nats_url, subject, consumer_service).await
            {
                tracing::warn!("device reading consumer stopped: {e}");
            }
        });
    } else {
        info!("NATS_URL unset — device reading consumer disabled");
    }

    let state = AppState {
        meter_service,
        jwt_secret: Arc::from(config.jwt_secret.as_str()),
    };

    // 3. Router
    let app = Router::new()
        .route("/health", get(system::health))
        .route("/api/v1/me/meters", get(meter::get_my_meters))
        .route("/api/v1/meters", post(meter::register_meter))
        .route("/api/v1/meters/readings", get(meter::get_my_readings))
        .route("/api/v1/meters/stats", get(meter::get_meter_stats))
        .route(
            "/api/v1/meters/{serial}/readings",
            post(meter::submit_reading),
        )
        .route(
            "/api/v1/meters/readings/{reading_id}/mint",
            post(meter::mint_reading),
        )
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(state);

    // 4. Serve
    let addr = SocketAddr::from(([0, 0, 0, 0], config.port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!("🔌 meter-service listening on {addr}");
    axum::serve(listener, app).await?;
    Ok(())
}
