//! Runtime configuration loaded from the environment.

/// Service configuration sourced from environment variables.
#[derive(Debug, Clone)]
pub struct Config {
    /// PostgreSQL connection URL (`DATABASE_URL`).
    pub database_url: String,
    /// Shared HS256 secret IAM signs JWTs with (`JWT_SECRET`).
    pub jwt_secret: String,
    /// TCP port to bind (`METER_SERVICE_PORT` | `PORT`, default 8080).
    pub port: u16,
    /// Max Postgres pool connections.
    pub database_max_connections: u32,
    /// Optional NATS URL for the device-reading consumer (`NATS_URL`). When
    /// unset, the consumer is disabled and the service runs HTTP-only.
    pub nats_url: Option<String>,
    /// Subject the aggregator bridge forwards mintable readings on
    /// (`METER_SERVICE_NATS_SUBJECT`, default `meter.reading`).
    pub meter_reading_subject: String,
    /// When true (and `nats_url` is set), mint via Chain Bridge over NATS
    /// (`MINT_VIA_CHAIN_BRIDGE`); otherwise minting is disabled (503).
    pub mint_via_chain_bridge: bool,
    /// SPIFFE identity this service asserts to Chain Bridge for mint RBAC
    /// (`CHAIN_BRIDGE_SERVICE_IDENTITY`, default the meter-service SPIFFE URI).
    pub mint_service_identity: String,
}

impl Config {
    /// Builds the configuration from process environment variables.
    ///
    /// # Errors
    /// Returns an error if `JWT_SECRET` is unset.
    pub fn from_env() -> anyhow::Result<Self> {
        let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgresql://gridtokenx_user:gridtokenx_password@postgres:5432/gridtokenx".to_string()
        });
        let jwt_secret = std::env::var("JWT_SECRET")
            .map_err(|_| anyhow::anyhow!("JWT_SECRET must be set (same value IAM signs tokens with)"))?;
        let port: u16 = std::env::var("METER_SERVICE_PORT")
            .or_else(|_| std::env::var("PORT"))
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8080);

        let nats_url = std::env::var("NATS_URL")
            .ok()
            .filter(|s| !s.trim().is_empty());
        let meter_reading_subject = std::env::var("METER_SERVICE_NATS_SUBJECT")
            .unwrap_or_else(|_| "meter.reading".to_string());
        let mint_via_chain_bridge = std::env::var("MINT_VIA_CHAIN_BRIDGE")
            .is_ok_and(|v| v.eq_ignore_ascii_case("true"));
        let mint_service_identity = std::env::var("CHAIN_BRIDGE_SERVICE_IDENTITY")
            .unwrap_or_else(|_| "spiffe://gridtokenx.th/prod/meter-service".to_string());

        Ok(Self {
            database_url,
            jwt_secret,
            port,
            database_max_connections: 10,
            nats_url,
            meter_reading_subject,
            mint_via_chain_bridge,
            mint_service_identity,
        })
    }
}
