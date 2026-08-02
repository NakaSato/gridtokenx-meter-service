//! Runtime configuration loaded from the environment.

/// Service configuration sourced from environment variables.
#[derive(Debug, Clone)]
pub struct Config {
    /// `PostgreSQL` connection URL (`DATABASE_URL`).
    pub database_url: String,
    /// Shared HS256 secret IAM signs JWTs with (`JWT_SECRET`).
    pub jwt_secret: String,
    /// TCP port to bind (`METER_SERVICE_PORT` | `PORT`, default 8080).
    pub port: u16,
    /// Max Postgres pool connections.
    pub database_max_connections: u32,
    /// Interval (seconds) for the mint-status SSE poller
    /// (`METER_MINT_POLL_SECS`, default 15). `0` disables the poller.
    pub mint_poll_secs: u64,
    /// Whether to emit meter domain events to Kafka (`METER_EVENTS_ENABLED`,
    /// default `false`). When off, no producer is created and registration
    /// behaves exactly as before.
    pub events_enabled: bool,
    /// Kafka topic for meter domain events (`METER_EVENTS_TOPIC`, default
    /// `meter_events`).
    pub events_topic: String,
    /// Kafka bootstrap servers for the event producer
    /// (`KAFKA_BOOTSTRAP_SERVERS`, default `kafka:9092`).
    pub kafka_bootstrap_servers: String,
    /// How far back meter verification looks for signature-verified telemetry
    /// (`METER_VERIFY_WINDOW_HOURS`, default 720 = 30 days). Clamped to >= 1.
    pub verify_window_hours: i64,
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
        let jwt_secret = std::env::var("JWT_SECRET").map_err(|_| {
            anyhow::anyhow!("JWT_SECRET must be set (same value IAM signs tokens with)")
        })?;
        let port: u16 = std::env::var("METER_SERVICE_PORT")
            .or_else(|_| std::env::var("PORT"))
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8080);

        let mint_poll_secs: u64 = std::env::var("METER_MINT_POLL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(15);

        let events_enabled = std::env::var("METER_EVENTS_ENABLED")
            .is_ok_and(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes"));
        let events_topic =
            std::env::var("METER_EVENTS_TOPIC").unwrap_or_else(|_| "meter_events".to_string());
        let kafka_bootstrap_servers =
            std::env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "kafka:9092".to_string());

        // Clamped to >= 1: a zero/negative window would fail every verification
        // with a message blaming the device for an operator's misconfiguration.
        let verify_window_hours: i64 = std::env::var("METER_VERIFY_WINDOW_HOURS")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .map_or(720, |h| h.max(1));

        Ok(Self {
            database_url,
            jwt_secret,
            port,
            database_max_connections: 10,
            mint_poll_secs,
            events_enabled,
            events_topic,
            kafka_bootstrap_servers,
            verify_window_hours,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::Config;

    /// `from_env` mutates *process* env, so all cases live in ONE test (threads
    /// in a test binary share env); it snapshots and restores the vars it edits.
    #[test]
    fn from_env_defaults_fallbacks_and_required_secret() {
        let keys = [
            "DATABASE_URL",
            "JWT_SECRET",
            "METER_SERVICE_PORT",
            "PORT",
            "METER_MINT_POLL_SECS",
            "METER_VERIFY_WINDOW_HOURS",
        ];
        let saved: Vec<(&str, Option<String>)> =
            keys.iter().map(|k| (*k, std::env::var(k).ok())).collect();
        for k in keys {
            std::env::remove_var(k);
        }

        // JWT_SECRET is the one hard requirement.
        assert!(Config::from_env().is_err(), "missing JWT_SECRET must error");

        // Defaults with only the secret set.
        std::env::set_var("JWT_SECRET", "x".repeat(32));
        let c = Config::from_env().expect("defaults");
        assert_eq!(c.port, 8080, "default port");
        assert_eq!(c.mint_poll_secs, 15, "default poll secs");
        assert!(c.database_url.contains("postgres:5432"), "default db url");

        // METER_SERVICE_PORT takes precedence over PORT.
        std::env::set_var("METER_SERVICE_PORT", "9999");
        std::env::set_var("PORT", "1111");
        assert_eq!(Config::from_env().expect("explicit port").port, 9999);

        // PORT is the fallback when METER_SERVICE_PORT is unset.
        std::env::remove_var("METER_SERVICE_PORT");
        assert_eq!(Config::from_env().expect("port fallback").port, 1111);

        // Unparseable port → default 8080.
        std::env::set_var("PORT", "not-a-number");
        assert_eq!(Config::from_env().expect("bad port").port, 8080);

        // Poll interval override (0 disables).
        std::env::set_var("METER_MINT_POLL_SECS", "0");
        assert_eq!(Config::from_env().expect("poll override").mint_poll_secs, 0);

        // Verification window: default, override, and the >= 1 clamp. Unlike the
        // poll interval, 0 must NOT mean "disabled" — it would silently make every
        // verification unsatisfiable, so it clamps up instead.
        assert_eq!(
            Config::from_env()
                .expect("default window")
                .verify_window_hours,
            720
        );
        std::env::set_var("METER_VERIFY_WINDOW_HOURS", "24");
        assert_eq!(
            Config::from_env()
                .expect("window override")
                .verify_window_hours,
            24
        );
        for bad in ["0", "-5"] {
            std::env::set_var("METER_VERIFY_WINDOW_HOURS", bad);
            assert_eq!(
                Config::from_env()
                    .expect("clamped window")
                    .verify_window_hours,
                1,
                "window {bad} must clamp to 1, not disable verification"
            );
        }

        // Restore prior env.
        for (k, v) in saved {
            match v {
                Some(val) => std::env::set_var(k, val),
                None => std::env::remove_var(k),
            }
        }
    }
}
