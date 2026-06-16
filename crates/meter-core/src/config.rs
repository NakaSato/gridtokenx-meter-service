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

        Ok(Self {
            database_url,
            jwt_secret,
            port,
            database_max_connections: 10,
        })
    }
}
