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
use meter_persistence::{DisabledMintGateway, MeterRepository, NatsMintGateway};

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
    //    Mint via Chain Bridge over NATS when MINT_VIA_CHAIN_BRIDGE is set and a
    //    NATS URL is configured; otherwise minting is disabled (503). Degraded by
    //    design: a NATS connect failure falls back to disabled, the API still serves.
    let repo: Arc<dyn MeterRepositoryTrait> = Arc::new(MeterRepository::new(pool));
    let mint: Arc<dyn MintGateway> = match (config.mint_via_chain_bridge, &config.nats_url) {
        (true, Some(url)) => match async_nats::connect(url).await {
            Ok(client) => {
                info!("✅ mint via Chain Bridge enabled (NATS {url})");
                Arc::new(NatsMintGateway::new(
                    client,
                    config.mint_service_identity.clone(),
                    std::time::Duration::from_secs(30),
                ))
            }
            Err(e) => {
                tracing::warn!("mint backend NATS connect failed ({e}); minting disabled");
                Arc::new(DisabledMintGateway)
            }
        },
        (true, None) => {
            tracing::warn!("MINT_VIA_CHAIN_BRIDGE set but NATS_URL unset; minting disabled");
            Arc::new(DisabledMintGateway)
        }
        (false, _) => Arc::new(DisabledMintGateway),
    };
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
