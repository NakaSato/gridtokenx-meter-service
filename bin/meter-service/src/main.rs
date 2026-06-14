//! gridtokenx-meter-service — user-facing Smart Meter read API.
//!
//! Serves the trading UI's meter dashboard from the shared `gridtokenx` DB
//! (meters + meter_readings tables; ownership by user_id). JWT-authed via the
//! IAM-issued token forwarded by APISIX.

use gridtokenx_meter_service::{startup, telemetry};
use meter_core::config::Config;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    telemetry::init();

    let config = Config::from_env()?;
    startup::run(config).await
}
