//! Structured JSON logging init.

use tracing_subscriber::EnvFilter;

/// Initializes the global tracing subscriber (JSON, env-filtered).
pub fn init() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| "info,sqlx=warn".into()),
        )
        .json()
        .init();
}
