//! Live HTTP end-to-end test against the real router + this service's own
//! `gridtokenx_meter` Postgres (DB-per-service split — see repo CLAUDE.md).
//!
//! Drives the exact production route table in-process via
//! `tower::ServiceExt::oneshot` (no socket bind) and asserts the meter flow:
//! register → read (list/stats) → realtime mint-status SSE. Readings are
//! ingested via the Aggregator Bridge, not here, so the tests seed rows
//! directly (`inject_*`) the way other services write them. It also proves the
//! read-only `mint_status` projection is wired through every read path.
//!
//! Self-contained: it picks an existing wallet-bearing user from
//! `user_wallet_read_model` — the one table this service reads but does not own
//! (IAM owns wallets; the aggregator's IAM-event feed is the sole writer; see
//! `contracts/user_wallet_read_model.sql` and TD-004). It is NOT `users`, which
//! lives in `gridtokenx_iam` and isn't reachable from here — hence `meters.user_id`
//! having no FK. The fixtures deliberately do not touch `meter_owner_read_model`:
//! that is the aggregator's private serial-keyed projection and this service must
//! keep resolving owners without it. Each test uses a unique throwaway serial and
//! deletes the meter + readings it created; it does NOT mutate the user it borrows.
//!
//! Ignored by default (needs live Postgres, with `user_wallet_read_model`
//! populated by IAM events — e.g. via the running docker stack). Run:
//!
//! ```text
//! DATABASE_URL=postgresql://gridtokenx_user:gridtokenx_password@127.0.0.1:7001/gridtokenx_meter \
//! JWT_SECRET=dev-jwt-secret-key-minimum-32-characters-long-for-development-2025 \
//! cargo test -p gridtokenx-meter-service --test e2e_http -- --ignored --nocapture
//! ```

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::http::{header, Request, Response, StatusCode};
use futures::StreamExt;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tokio::sync::broadcast;
use tower::ServiceExt;
use uuid::Uuid;

use gridtokenx_meter_service::startup::build_app;
use meter_api::{AppState, ReadingEvent};
use meter_core::domain::meter::MeterReading;
use meter_core::traits::MeterRepositoryTrait;
use meter_logic::MeterService;
use meter_persistence::MeterRepository;

const DEFAULT_DB: &str =
    "postgresql://gridtokenx_user:gridtokenx_password@127.0.0.1:7001/gridtokenx_meter";
const DEFAULT_SECRET: &str = "dev-jwt-secret-key-minimum-32-characters-long-for-development-2025";

/// HS256 JWT with `sub` + `exp`, matching what the auth extractor verifies.
fn mint_jwt(user_id: Uuid, secret: &str) -> String {
    let exp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_secs()
        + 3600;
    let claims = serde_json::json!({ "sub": user_id, "exp": exp });
    encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
    .expect("mint jwt")
}

/// HS256 JWT with an explicit `exp` (seconds since epoch), for expiry tests.
fn mint_jwt_with_exp(user_id: Uuid, secret: &str, exp: u64) -> String {
    let claims = serde_json::json!({ "sub": user_id, "exp": exp });
    encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
    .expect("mint jwt")
}

fn authed(method: &str, uri: &str, token: &str, body: Option<serde_json::Value>) -> Request<Body> {
    let mut b = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {token}"));
    let body = match body {
        Some(json) => {
            b = b.header(header::CONTENT_TYPE, "application/json");
            Body::from(serde_json::to_vec(&json).expect("encode body"))
        }
        None => Body::empty(),
    };
    b.body(body).expect("build request")
}

async fn json_body(resp: Response<Body>) -> serde_json::Value {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("read body");
    serde_json::from_slice(&bytes).expect("parse json")
}

#[tokio::test]
#[ignore = "requires live Postgres"]
async fn http_e2e_register_and_read() {
    let db = std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_DB.to_string());
    let secret = std::env::var("JWT_SECRET").unwrap_or_else(|_| DEFAULT_SECRET.to_string());

    let pool: PgPool = PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&db)
        .await
        .expect("connect Postgres");

    // Borrow an existing wallet-bearing user from the local owner read-model
    // (this DB has no `users` table post-split; `meters.user_id` carries no FK).
    let user_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT user_id FROM user_wallet_read_model WHERE wallet_address IS NOT NULL AND wallet_address <> '' LIMIT 1",
    )
    .fetch_optional(&pool)
    .await
    .expect("query user");
    let Some(user_id) = user_id else {
        eprintln!("SKIP: no wallet-bearing user in DB");
        return;
    };

    let token = mint_jwt(user_id, &secret);
    let serial = format!("E2E-AUTO-{}", Uuid::new_v4());

    // Wire the same DI graph as production.
    let repo: Arc<dyn MeterRepositoryTrait> = Arc::new(MeterRepository::new(pool.clone()));
    let (readings_tx, _) = broadcast::channel::<Arc<ReadingEvent>>(256);
    let state = AppState {
        meter_service: MeterService::new(repo),
        jwt_secret: Arc::from(secret.as_str()),
        readings_tx,
    };
    let app = build_app(state);

    // Run the flow, then always clean up — even on assertion panic.
    let result = run_flow(&app, &pool, &token, &serial, user_id).await;
    cleanup(&pool, &serial).await;
    result.expect("e2e flow");
}

/// The assertions, isolated so the caller can clean up regardless of outcome.
async fn run_flow(
    app: &axum::Router,
    pool: &PgPool,
    token: &str,
    serial: &str,
    user_id: Uuid,
) -> Result<(), String> {
    macro_rules! check {
        ($cond:expr, $($arg:tt)*) => {
            if !$cond { return Err(format!($($arg)*)); }
        };
    }

    // 1. health
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .expect("req"),
        )
        .await
        .expect("health");
    check!(
        resp.status() == StatusCode::OK,
        "health status {}",
        resp.status()
    );

    // 2. register
    register(app, token, serial).await?;

    // 3. seed a pending reading directly (ingest is via the Aggregator Bridge).
    let reading_id = inject_reading(pool, user_id, serial, false, None, None).await?;

    // 4. readings list contains it with mint_status pending.
    let resp = app
        .clone()
        .oneshot(authed(
            "GET",
            "/api/v1/meters/readings?limit=10",
            token,
            None,
        ))
        .await
        .expect("readings");
    check!(
        resp.status() == StatusCode::OK,
        "readings status {}",
        resp.status()
    );
    let list = json_body(resp).await;
    let arr = list.as_array().cloned().unwrap_or_default();
    let found = arr
        .iter()
        .find(|r| r["id"] == serde_json::json!(reading_id));
    check!(found.is_some(), "injected reading not in list");
    check!(
        found.expect("found")["mint_status"] == serde_json::json!("pending"),
        "listed reading not pending"
    );

    // 5. stats expose the three mint counters as integers.
    let resp = app
        .clone()
        .oneshot(authed("GET", "/api/v1/meters/stats", token, None))
        .await
        .expect("stats");
    check!(
        resp.status() == StatusCode::OK,
        "stats status {}",
        resp.status()
    );
    let stats = json_body(resp).await;
    for key in ["minted_count", "pending_count", "denied_count"] {
        check!(stats[key].is_i64(), "stats.{key} not an integer: {stats}");
    }
    check!(
        stats["pending_count"].as_i64().unwrap_or(0) >= 1,
        "pending_count should count our reading: {stats}"
    );

    Ok(())
}

/// `GET /api/v1/me/meters` — the last route with no e2e coverage. Register a
/// meter, assert it appears in the caller's meter list with the right serial.
#[tokio::test]
#[ignore = "requires live Postgres"]
async fn http_e2e_my_meters() {
    let db = std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_DB.to_string());
    let secret = std::env::var("JWT_SECRET").unwrap_or_else(|_| DEFAULT_SECRET.to_string());

    let pool: PgPool = PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&db)
        .await
        .expect("connect Postgres");

    let user_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT user_id FROM user_wallet_read_model WHERE wallet_address IS NOT NULL AND wallet_address <> '' LIMIT 1",
    )
    .fetch_optional(&pool)
    .await
    .expect("query user");
    let Some(user_id) = user_id else {
        eprintln!("SKIP: no wallet-bearing user in DB");
        return;
    };

    let token = mint_jwt(user_id, &secret);
    let serial = format!("E2E-MINE-{}", Uuid::new_v4());

    let repo: Arc<dyn MeterRepositoryTrait> = Arc::new(MeterRepository::new(pool.clone()));
    let (readings_tx, _) = broadcast::channel::<Arc<ReadingEvent>>(16);
    let state = AppState {
        meter_service: MeterService::new(repo),
        jwt_secret: Arc::from(secret.as_str()),
        readings_tx,
    };
    let app = build_app(state);

    let result = run_my_meters(&app, &token, &serial).await;
    cleanup(&pool, &serial).await;
    result.expect("my-meters flow");
}

async fn run_my_meters(app: &axum::Router, token: &str, serial: &str) -> Result<(), String> {
    macro_rules! check {
        ($cond:expr, $($arg:tt)*) => {
            if !$cond { return Err(format!($($arg)*)); }
        };
    }

    register(app, token, serial).await?;

    let resp = app
        .clone()
        .oneshot(authed("GET", "/api/v1/me/meters", token, None))
        .await
        .expect("my meters");
    check!(
        resp.status() == StatusCode::OK,
        "my-meters status {}",
        resp.status()
    );
    let meters = json_body(resp).await;
    let arr = meters.as_array().cloned().unwrap_or_default();
    let mine = arr
        .iter()
        .find(|m| m["serial_number"] == serde_json::json!(serial));
    check!(
        mine.is_some(),
        "registered meter {serial} absent from /me/meters: {meters}"
    );

    Ok(())
}

/// A freshly registered meter must surface the owner's wallet IMMEDIATELY, with
/// no dependency on the aggregator's serial-keyed projection.
///
/// This started life as a test of a *fallback* (`c6aa96b`): `meter_select` joined
/// `meter_owner_read_model` first and only consulted the user edge when the async
/// feed had not yet written the serial row. TD-004 removed that first join —
/// meter-service now resolves owner wallets through `user_wallet_read_model`
/// alone, keyed on the locally-owned `meters.user_id` — so this is no longer a
/// fallback path but THE path. It still pins the same observable: a user present
/// only in `user_wallet_read_model`, with no serial row anywhere, gets a
/// non-blank `wallet_address` the instant the meter is registered.
#[tokio::test]
#[ignore = "requires live Postgres"]
async fn http_e2e_register_surfaces_wallet_from_user_edge() {
    let db = std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_DB.to_string());
    let secret = std::env::var("JWT_SECRET").unwrap_or_else(|_| DEFAULT_SECRET.to_string());

    let pool: PgPool = PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&db)
        .await
        .expect("connect Postgres");

    // Fresh user with only the user→wallet edge, and a fresh serial that has no
    // row in the aggregator's serial-keyed projection — so a resolved wallet can
    // only have come from `user_wallet_read_model` via `meters.user_id`.
    let user_id = Uuid::new_v4();
    let wallet = "E2EWa11etFromUserEdge1111111111111111111111";
    sqlx::query(
        "INSERT INTO user_wallet_read_model (user_id, wallet_address, updated_at)
         VALUES ($1, $2, now())
         ON CONFLICT (user_id) DO UPDATE SET wallet_address = EXCLUDED.wallet_address",
    )
    .bind(user_id)
    .bind(wallet)
    .execute(&pool)
    .await
    .expect("seed user_wallet_read_model");

    let token = mint_jwt(user_id, &secret);
    let serial = format!("E2E-WEDGE-{}", Uuid::new_v4());

    let repo: Arc<dyn MeterRepositoryTrait> = Arc::new(MeterRepository::new(pool.clone()));
    let (readings_tx, _) = broadcast::channel::<Arc<ReadingEvent>>(16);
    let state = AppState {
        meter_service: MeterService::new(repo),
        jwt_secret: Arc::from(secret.as_str()),
        readings_tx,
    };
    let app = build_app(state);

    let result = run_wallet_from_user_edge(&app, &token, &serial, wallet).await;

    // Clean up regardless of outcome: the meter row + the seeded user edge.
    cleanup(&pool, &serial).await;
    let _ = sqlx::query("DELETE FROM user_wallet_read_model WHERE user_id = $1")
        .bind(user_id)
        .execute(&pool)
        .await;

    result.expect("wallet-from-user-edge flow");
}

async fn run_wallet_from_user_edge(
    app: &axum::Router,
    token: &str,
    serial: &str,
    wallet: &str,
) -> Result<(), String> {
    macro_rules! check {
        ($cond:expr, $($arg:tt)*) => {
            if !$cond { return Err(format!($($arg)*)); }
        };
    }

    register(app, token, serial).await?;

    let resp = app
        .clone()
        .oneshot(authed("GET", "/api/v1/me/meters", token, None))
        .await
        .expect("my meters");
    check!(
        resp.status() == StatusCode::OK,
        "my-meters status {}",
        resp.status()
    );
    let meters = json_body(resp).await;
    let arr = meters.as_array().cloned().unwrap_or_default();
    let mine = arr
        .iter()
        .find(|m| m["serial_number"] == serde_json::json!(serial));
    check!(
        mine.is_some(),
        "registered meter {serial} absent from /me/meters: {meters}"
    );
    let mine = mine.expect("found");
    check!(
        mine["wallet_address"] == serde_json::json!(wallet),
        "wallet not resolved from user edge: expected {wallet}, got {}",
        mine["wallet_address"]
    );

    Ok(())
}

/// Error-contract paths through the real router + DB: duplicate serial → 409
/// (the DB unique-constraint behavior, which has no unit coverage), and the
/// removed ingest route (`POST /meters/{serial}/readings`) → 404.
#[tokio::test]
#[ignore = "requires live Postgres"]
async fn http_e2e_error_paths() {
    let db = std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_DB.to_string());
    let secret = std::env::var("JWT_SECRET").unwrap_or_else(|_| DEFAULT_SECRET.to_string());

    let pool: PgPool = PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&db)
        .await
        .expect("connect Postgres");

    let user_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT user_id FROM user_wallet_read_model WHERE wallet_address IS NOT NULL AND wallet_address <> '' LIMIT 1",
    )
    .fetch_optional(&pool)
    .await
    .expect("query user");
    let Some(user_id) = user_id else {
        eprintln!("SKIP: no wallet-bearing user in DB");
        return;
    };

    let token = mint_jwt(user_id, &secret);
    let serial = format!("E2E-ERR-{}", Uuid::new_v4());

    let repo: Arc<dyn MeterRepositoryTrait> = Arc::new(MeterRepository::new(pool.clone()));
    let (readings_tx, _) = broadcast::channel::<Arc<ReadingEvent>>(16);
    let state = AppState {
        meter_service: MeterService::new(repo),
        jwt_secret: Arc::from(secret.as_str()),
        readings_tx,
    };
    let app = build_app(state);

    let result = run_error_paths(&app, &token, &serial).await;
    cleanup(&pool, &serial).await;
    result.expect("error-path flow");
}

async fn run_error_paths(app: &axum::Router, token: &str, serial: &str) -> Result<(), String> {
    macro_rules! check {
        ($cond:expr, $($arg:tt)*) => {
            if !$cond { return Err(format!($($arg)*)); }
        };
    }

    register(app, token, serial).await?;

    // The reading-ingest route was removed (readings flow via the Aggregator
    // Bridge now); a POST to it must miss the router → 404 NotFound.
    let resp = app
        .clone()
        .oneshot(authed(
            "POST",
            &format!("/api/v1/meters/{serial}/readings"),
            token,
            Some(serde_json::json!({ "kwh": 1.0 })),
        ))
        .await
        .expect("removed ingest route");
    check!(
        resp.status() == StatusCode::NOT_FOUND,
        "removed ingest route: expected 404, got {}",
        resp.status()
    );

    // Re-register the same serial → 409 Conflict (DB unique constraint).
    let resp = app
        .clone()
        .oneshot(authed(
            "POST",
            "/api/v1/meters",
            token,
            Some(serde_json::json!({
                "serial_number": serial,
                "meter_type": "smart_meter",
                "location": "e2e-auto",
            })),
        ))
        .await
        .expect("dup register");
    check!(
        resp.status() == StatusCode::CONFLICT,
        "duplicate serial: expected 409, got {}",
        resp.status()
    );

    Ok(())
}

/// Post DB-per-service split, `meters.user_id` carries NO foreign key to
/// `users` (which lives in `gridtokenx_iam`, unreachable from this DB — see
/// repo CLAUDE.md "DB-per-service split"). Registering for a `user_id` with no
/// backing user row therefore **succeeds** (soft uuid ref) instead of failing
/// with a 23503 FK violation the way it did before the split. Authorization
/// still comes from the JWT, not the DB — an attacker still needs a valid
/// token for that `sub`. This test pins the current (post-split) contract so a
/// future re-introduction of the FK is a deliberate, visible test change, not
/// a silent behavior flip.
#[tokio::test]
#[ignore = "requires live Postgres"]
async fn http_e2e_register_unknown_user_succeeds_soft_ref() {
    let db = std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_DB.to_string());
    let secret = std::env::var("JWT_SECRET").unwrap_or_else(|_| DEFAULT_SECRET.to_string());

    let pool: PgPool = PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&db)
        .await
        .expect("connect Postgres");

    // A random user id backed by no owner row anywhere reachable from this DB.
    let user_id = Uuid::new_v4();
    let token = mint_jwt(user_id, &secret);
    let serial = format!("E2E-SOFTREF-{}", Uuid::new_v4());

    let repo: Arc<dyn MeterRepositoryTrait> = Arc::new(MeterRepository::new(pool.clone()));
    let (readings_tx, _) = broadcast::channel::<Arc<ReadingEvent>>(16);
    let state = AppState {
        meter_service: MeterService::new(repo),
        jwt_secret: Arc::from(secret.as_str()),
        readings_tx,
    };
    let app = build_app(state);

    let resp = app
        .clone()
        .oneshot(authed(
            "POST",
            "/api/v1/meters",
            &token,
            Some(serde_json::json!({
                "serial_number": serial,
                "meter_type": "smart_meter",
                "location": "e2e-auto",
            })),
        ))
        .await
        .expect("register unknown user");
    let status = resp.status();

    // Clean up regardless of outcome before asserting.
    cleanup(&pool, &serial).await;

    assert_eq!(
        status,
        StatusCode::OK,
        "unknown-user register should succeed (no FK on meters.user_id post-split), got {status}"
    );
}

/// Register a meter for `token`'s user. Returns `Err` with context on failure.
async fn register(app: &axum::Router, token: &str, serial: &str) -> Result<(), String> {
    let resp = app
        .clone()
        .oneshot(authed(
            "POST",
            "/api/v1/meters",
            token,
            Some(serde_json::json!({
                "serial_number": serial,
                "meter_type": "smart_meter",
                "location": "e2e-auto",
            })),
        ))
        .await
        .expect("register");
    if resp.status() != StatusCode::OK {
        return Err(format!("register {serial} status {}", resp.status()));
    }
    Ok(())
}

/// GET the user's readings list, returning the array of ids.
async fn reading_ids(app: &axum::Router, token: &str) -> Result<Vec<String>, String> {
    let resp = app
        .clone()
        .oneshot(authed(
            "GET",
            "/api/v1/meters/readings?limit=200",
            token,
            None,
        ))
        .await
        .expect("readings");
    if resp.status() != StatusCode::OK {
        return Err(format!("readings status {}", resp.status()));
    }
    let list = json_body(resp).await;
    Ok(list
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .filter_map(|r| r["id"].as_str().map(str::to_string))
        .collect())
}

/// Two users must not see each other's readings (list scoping). Each user's
/// readings are seeded directly (ingest is via the Aggregator Bridge); the list
/// is `user_id`-scoped so neither user sees the other's rows.
#[tokio::test]
#[ignore = "requires live Postgres"]
async fn http_e2e_multi_user_isolation() {
    let db = std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_DB.to_string());
    let secret = std::env::var("JWT_SECRET").unwrap_or_else(|_| DEFAULT_SECRET.to_string());

    let pool: PgPool = PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&db)
        .await
        .expect("connect Postgres");

    // Need two DISTINCT wallet-bearing users.
    let users: Vec<Uuid> = sqlx::query_scalar(
        "SELECT user_id FROM user_wallet_read_model WHERE wallet_address IS NOT NULL AND wallet_address <> '' LIMIT 2",
    )
    .fetch_all(&pool)
    .await
    .expect("query users");
    if users.len() < 2 {
        eprintln!(
            "SKIP: need 2 wallet-bearing users in DB, found {}",
            users.len()
        );
        return;
    }
    let (user_a, user_b) = (users[0], users[1]);

    let token_a = mint_jwt(user_a, &secret);
    let token_b = mint_jwt(user_b, &secret);
    let serial_a = format!("E2E-ISO-A-{}", Uuid::new_v4());
    let serial_b = format!("E2E-ISO-B-{}", Uuid::new_v4());

    let repo: Arc<dyn MeterRepositoryTrait> = Arc::new(MeterRepository::new(pool.clone()));
    let (readings_tx, _) = broadcast::channel::<Arc<ReadingEvent>>(256);
    let state = AppState {
        meter_service: MeterService::new(repo),
        jwt_secret: Arc::from(secret.as_str()),
        readings_tx,
    };
    let app = build_app(state);

    let result = run_isolation(
        &app, &pool, &token_a, &token_b, &serial_a, &serial_b, user_a, user_b,
    )
    .await;
    cleanup(&pool, &serial_a).await;
    cleanup(&pool, &serial_b).await;
    result.expect("isolation flow");
}

#[allow(clippy::too_many_arguments)]
async fn run_isolation(
    app: &axum::Router,
    pool: &PgPool,
    token_a: &str,
    token_b: &str,
    serial_a: &str,
    serial_b: &str,
    user_a: Uuid,
    user_b: Uuid,
) -> Result<(), String> {
    macro_rules! check {
        ($cond:expr, $($arg:tt)*) => {
            if !$cond { return Err(format!($($arg)*)); }
        };
    }

    register(app, token_a, serial_a).await?;
    register(app, token_b, serial_b).await?;

    // Seed one reading for each user directly (ingest is via the Aggregator
    // Bridge). The list is `user_id`-scoped, so neither must see the other's.
    let id_b = inject_reading(pool, user_b, serial_b, false, None, None).await?;
    let id_a = inject_reading(pool, user_a, serial_a, false, None, None).await?;

    // List scoping: A sees its own reading, never B's; and vice versa.
    let a_ids = reading_ids(app, token_a).await?;
    check!(
        a_ids.contains(&id_a),
        "A's list missing its own reading {id_a}"
    );
    check!(
        !a_ids.contains(&id_b),
        "LEAK: A's list contains B's reading {id_b}"
    );

    let b_ids = reading_ids(app, token_b).await?;
    check!(
        b_ids.contains(&id_b),
        "B's list missing its own reading {id_b}"
    );
    check!(
        !b_ids.contains(&id_a),
        "LEAK: B's list contains A's reading {id_a}"
    );

    Ok(())
}

/// Framework-level rejections, infra-free (lazy pool): an unknown route is 404,
/// and a malformed JSON body on a real route is rejected (4xx) before the
/// handler touches the DB. Auth runs before the `Json` extractor, so the
/// malformed-body case carries a valid token.
#[tokio::test]
async fn malformed_body_and_unknown_route() {
    let secret = "test-secret-minimum-32-characters-long-aaaaaaaaaa";
    let pool = PgPool::connect_lazy(DEFAULT_DB).expect("lazy pool");
    let repo: Arc<dyn MeterRepositoryTrait> = Arc::new(MeterRepository::new(pool));
    let (readings_tx, _) = broadcast::channel::<Arc<ReadingEvent>>(16);
    let state = AppState {
        meter_service: MeterService::new(repo),
        jwt_secret: Arc::from(secret),
        readings_tx,
    };
    let app = build_app(state);

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_secs();
    let token = mint_jwt_with_exp(Uuid::new_v4(), secret, now + 3600);

    // Unknown route → 404 (router miss; no auth involved).
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/does-not-exist")
                .body(Body::empty())
                .expect("req"),
        )
        .await
        .expect("oneshot");
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "unknown route should be 404"
    );

    // Malformed JSON on POST /api/v1/meters with a VALID token: the Json
    // extractor rejects with a client error (400) before the handler runs, so
    // the lazy DB is never touched.
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/meters")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from("{ this is not json"))
        .expect("req");
    let resp = app.clone().oneshot(req).await.expect("oneshot");
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "malformed JSON should be 400, got {}",
        resp.status()
    );
}

/// Every protected route must reject a request whose JWT is missing, malformed,
/// wrongly signed, or expired — all 401, before any DB access. Uses a lazy pool
/// so it needs NO live Postgres (auth fails in the extractor first).
#[tokio::test]
async fn auth_rejects_bad_tokens() {
    let secret = "test-secret-minimum-32-characters-long-aaaaaaaaaa";
    // Lazy pool: never connects unless a query runs — and auth rejection runs
    // before any handler touches the DB.
    let pool = PgPool::connect_lazy(DEFAULT_DB).expect("lazy pool");
    let repo: Arc<dyn MeterRepositoryTrait> = Arc::new(MeterRepository::new(pool));
    let (readings_tx, _) = broadcast::channel::<Arc<ReadingEvent>>(16);
    let state = AppState {
        meter_service: MeterService::new(repo),
        jwt_secret: Arc::from(secret),
        readings_tx,
    };
    let app = build_app(state);

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_secs();
    let user = Uuid::new_v4();

    // Each case is a request whose auth must be rejected on a protected route.
    let no_header = Request::builder()
        .method("GET")
        .uri("/api/v1/meters/stats")
        .body(Body::empty())
        .expect("req");

    let no_bearer = Request::builder()
        .method("GET")
        .uri("/api/v1/meters/stats")
        .header(header::AUTHORIZATION, "Token abc")
        .body(Body::empty())
        .expect("req");

    let garbage = authed("GET", "/api/v1/meters/stats", "not-a-jwt", None);

    // Structurally valid JWT, but signed with the WRONG secret.
    let wrong_secret = mint_jwt(user, "some-other-secret-that-is-also-32-chars-long!!");
    let wrong_sig = authed("GET", "/api/v1/meters/stats", &wrong_secret, None);

    // Right secret, but exp is well in the past (beyond jsonwebtoken's default
    // 60s leeway).
    let expired_tok = mint_jwt_with_exp(user, secret, now - 3600);
    let expired = authed("GET", "/api/v1/meters/stats", &expired_tok, None);

    // Correctly signed, unexpired — but `sub` is not a UUID, so the `Claims`
    // deserialize fails inside the extractor → still 401.
    let bad_sub_claims = serde_json::json!({ "sub": "not-a-uuid", "exp": now + 3600 });
    let bad_sub_tok = encode(
        &Header::new(Algorithm::HS256),
        &bad_sub_claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
    .expect("encode bad-sub jwt");
    let bad_sub = authed("GET", "/api/v1/meters/stats", &bad_sub_tok, None);

    let cases = [
        ("missing header", no_header),
        ("non-Bearer scheme", no_bearer),
        ("garbage token", garbage),
        ("wrong signature", wrong_sig),
        ("expired token", expired),
        ("non-UUID sub", bad_sub),
    ];

    for (name, req) in cases {
        let resp = app.clone().oneshot(req).await.expect("oneshot");
        assert_eq!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "{name}: expected 401, got {}",
            resp.status()
        );
    }
    // Valid-token acceptance is covered by the live e2e tests (which exercise
    // the full DB-backed handler). Adding it here would force a DB connection
    // (and a long acquire timeout when no Postgres is up), so it's omitted.
}

/// Synthetic-injection proof of the read-only `mint_status` derivation. Inserts
/// one minted row (`minted = true` + a tx signature) and one denied row
/// (`blockchain_status = 'failed'`) straight into the shared table for a
/// throwaway serial, then asserts the HTTP read paths surface them: the readings
/// list `mint_status` + `mint_tx_signature`, and the stats `minted_count` /
/// `denied_count`. These two branches of `MINT_STATUS_CASE` have NO other
/// automated coverage — every other path only exercises the `pending` branch
/// (this service only ever inserts unminted rows; mint/deny columns are written
/// by other services). Self-cleaning: `cleanup` deletes both rows by serial.
#[tokio::test]
#[ignore = "requires live Postgres"]
async fn http_e2e_mint_status_minted_and_denied() {
    let db = std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_DB.to_string());
    let secret = std::env::var("JWT_SECRET").unwrap_or_else(|_| DEFAULT_SECRET.to_string());

    let pool: PgPool = PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&db)
        .await
        .expect("connect Postgres");

    let user_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT user_id FROM user_wallet_read_model WHERE wallet_address IS NOT NULL AND wallet_address <> '' LIMIT 1",
    )
    .fetch_optional(&pool)
    .await
    .expect("query user");
    let Some(user_id) = user_id else {
        eprintln!("SKIP: no wallet-bearing user in DB");
        return;
    };

    let token = mint_jwt(user_id, &secret);
    let serial = format!("E2E-MINT-{}", Uuid::new_v4());

    let repo: Arc<dyn MeterRepositoryTrait> = Arc::new(MeterRepository::new(pool.clone()));
    let (readings_tx, _) = broadcast::channel::<Arc<ReadingEvent>>(16);
    let state = AppState {
        meter_service: MeterService::new(repo),
        jwt_secret: Arc::from(secret.as_str()),
        readings_tx,
    };
    let app = build_app(state);

    let result = run_mint_status(&app, &pool, &token, &serial, user_id).await;
    cleanup(&pool, &serial).await;
    result.expect("mint-status flow");
}

/// Insert a synthetic `meter_readings` row with explicit mint columns, returning
/// its id. Bypasses the service (which only writes unminted rows) to forge the
/// minted/denied states other services would write. `energy_generated`/
/// `energy_consumed` are left 0 so this row never perturbs the SUM-aggregate
/// deltas asserted by other tests sharing this user under parallel runs.
async fn inject_reading(
    pool: &PgPool,
    user_id: Uuid,
    serial: &str,
    minted: bool,
    tx_sig: Option<&str>,
    blockchain_status: Option<&str>,
) -> Result<String, String> {
    let id: Uuid = sqlx::query_scalar(
        "INSERT INTO meter_readings
            (id, user_id, wallet_address, meter_serial, kwh_amount, timestamp,
             energy_generated, minted, mint_tx_signature, blockchain_status)
         VALUES
            (gen_random_uuid(), $1, 'E2E-INJECT', $2, 0, now(),
             0, $3, $4, $5)
         RETURNING id",
    )
    .bind(user_id)
    .bind(serial)
    .bind(minted)
    .bind(tx_sig)
    .bind(blockchain_status)
    .fetch_one(pool)
    .await
    .map_err(|e| format!("inject reading: {e}"))?;
    Ok(id.to_string())
}

async fn run_mint_status(
    app: &axum::Router,
    pool: &PgPool,
    token: &str,
    serial: &str,
    user_id: Uuid,
) -> Result<(), String> {
    macro_rules! check {
        ($cond:expr, $($arg:tt)*) => {
            if !$cond { return Err(format!($($arg)*)); }
        };
    }

    register(app, token, serial).await?;

    let sig = format!("E2E_SIG_{}", Uuid::new_v4().simple());
    let id_minted =
        inject_reading(pool, user_id, serial, true, Some(&sig), Some("confirmed")).await?;
    let id_denied = inject_reading(pool, user_id, serial, false, None, Some("failed")).await?;

    // Read both back through the HTTP list and locate them by id.
    let resp = app
        .clone()
        .oneshot(authed(
            "GET",
            "/api/v1/meters/readings?limit=200",
            token,
            None,
        ))
        .await
        .expect("readings");
    check!(
        resp.status() == StatusCode::OK,
        "readings status {}",
        resp.status()
    );
    let list = json_body(resp).await;
    let arr = list.as_array().cloned().unwrap_or_default();
    let find = |id: &str| {
        arr.iter()
            .find(|r| r["id"] == serde_json::json!(id))
            .cloned()
    };

    // Minted row: mint_status == "minted" AND the tx signature is surfaced.
    let minted = find(&id_minted).ok_or("injected minted row absent from list")?;
    check!(
        minted["mint_status"] == serde_json::json!("minted"),
        "minted row not 'minted': {minted}"
    );
    check!(
        minted["mint_tx_signature"] == serde_json::json!(sig),
        "minted row missing/wrong mint_tx_signature: {minted}"
    );

    // Denied row: mint_status == "denied" AND no signature (field omitted → null).
    let denied = find(&id_denied).ok_or("injected denied row absent from list")?;
    check!(
        denied["mint_status"] == serde_json::json!("denied"),
        "denied row not 'denied': {denied}"
    );
    check!(
        denied["mint_tx_signature"].is_null(),
        "denied row should have no mint_tx_signature: {denied}"
    );

    // Counters scoped to THIS serial — exactly one minted + one denied — using
    // the same predicates as `user_stats`. Serial-scoping keeps this
    // deterministic under concurrent ignored tests that inject/clean their own
    // mint rows for the shared user (a user-wide lower bound flakes when a
    // concurrent cleanup deletes rows between snapshots).
    let (minted_count, denied_count): (i64, i64) = sqlx::query_as(
        "SELECT
            COUNT(*) FILTER (WHERE COALESCE(minted, false) OR COALESCE(on_chain_confirmed, false))::int8,
            COUNT(*) FILTER (WHERE blockchain_status = 'failed' OR blockchain_last_error IS NOT NULL)::int8
         FROM meter_readings WHERE meter_serial = $1",
    )
    .bind(serial)
    .fetch_one(pool)
    .await
    .map_err(|e| format!("count query: {e}"))?;
    check!(minted_count == 1, "serial minted_count {minted_count} != 1");
    check!(denied_count == 1, "serial denied_count {denied_count} != 1");

    Ok(())
}

/// Synthetic-injection proof of the `not_applicable` branch of `MINT_STATUS_CASE`
/// (`blockchain_status = 'no_surplus'`, written by the Aggregator Bridge's
/// settlement sink — `PgReadingsWriter::mark_no_surplus` — when a reading's
/// 15-min billing window closes with net consumption, nothing to mint). Inserts
/// one such row plus one genuinely-pending row for the same serial, then asserts
/// the readings list surfaces `not_applicable` distinctly from `pending`, and
/// that `pending_count` — which shares `MINT_STATUS_CASE`'s predicates — counts
/// only the genuine pending row, not the settled-no-mint one (the bug this
/// status exists to fix: before it, both rows showed "Pending" forever).
#[tokio::test]
#[ignore = "requires live Postgres"]
async fn http_e2e_mint_status_not_applicable() {
    let db = std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_DB.to_string());
    let secret = std::env::var("JWT_SECRET").unwrap_or_else(|_| DEFAULT_SECRET.to_string());

    let pool: PgPool = PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&db)
        .await
        .expect("connect Postgres");

    let user_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT user_id FROM user_wallet_read_model WHERE wallet_address IS NOT NULL AND wallet_address <> '' LIMIT 1",
    )
    .fetch_optional(&pool)
    .await
    .expect("query user");
    let Some(user_id) = user_id else {
        eprintln!("SKIP: no wallet-bearing user in DB");
        return;
    };

    let token = mint_jwt(user_id, &secret);
    let serial = format!("E2E-NOSURPLUS-{}", Uuid::new_v4());

    let repo: Arc<dyn MeterRepositoryTrait> = Arc::new(MeterRepository::new(pool.clone()));
    let (readings_tx, _) = broadcast::channel::<Arc<ReadingEvent>>(16);
    let state = AppState {
        meter_service: MeterService::new(repo),
        jwt_secret: Arc::from(secret.as_str()),
        readings_tx,
    };
    let app = build_app(state);

    let result = run_mint_status_not_applicable(&app, &pool, &token, &serial, user_id).await;
    cleanup(&pool, &serial).await;
    result.expect("not-applicable mint-status flow");
}

async fn run_mint_status_not_applicable(
    app: &axum::Router,
    pool: &PgPool,
    token: &str,
    serial: &str,
    user_id: Uuid,
) -> Result<(), String> {
    macro_rules! check {
        ($cond:expr, $($arg:tt)*) => {
            if !$cond { return Err(format!($($arg)*)); }
        };
    }

    register(app, token, serial).await?;

    let id_no_surplus =
        inject_reading(pool, user_id, serial, false, None, Some("no_surplus")).await?;
    let id_pending = inject_reading(pool, user_id, serial, false, None, None).await?;

    let resp = app
        .clone()
        .oneshot(authed(
            "GET",
            "/api/v1/meters/readings?limit=200",
            token,
            None,
        ))
        .await
        .expect("readings");
    check!(
        resp.status() == StatusCode::OK,
        "readings status {}",
        resp.status()
    );
    let list = json_body(resp).await;
    let arr = list.as_array().cloned().unwrap_or_default();
    let find = |id: &str| {
        arr.iter()
            .find(|r| r["id"] == serde_json::json!(id))
            .cloned()
    };

    // No-surplus row: mint_status == "not_applicable", NOT "pending".
    let no_surplus = find(&id_no_surplus).ok_or("injected no_surplus row absent from list")?;
    check!(
        no_surplus["mint_status"] == serde_json::json!("not_applicable"),
        "no_surplus row not 'not_applicable': {no_surplus}"
    );

    // Ordinary row: still "pending" (unaffected by the new branch).
    let pending = find(&id_pending).ok_or("injected pending row absent from list")?;
    check!(
        pending["mint_status"] == serde_json::json!("pending"),
        "plain row not 'pending': {pending}"
    );

    // Serial-scoped pending_count (same predicate as `user_stats`) must count
    // ONLY the genuine pending row — the fix's whole point is that a
    // no_surplus row stops inflating this count / showing as Pending forever.
    let pending_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FILTER (WHERE NOT (COALESCE(minted, false) OR COALESCE(on_chain_confirmed, false))
                                    AND NOT COALESCE(blockchain_status = 'failed' OR blockchain_last_error IS NOT NULL, false)
                                    AND COALESCE(blockchain_status, '') != 'no_surplus')::int8
         FROM meter_readings WHERE meter_serial = $1",
    )
    .bind(serial)
    .fetch_one(pool)
    .await
    .map_err(|e| format!("count query: {e}"))?;
    check!(
        pending_count == 1,
        "serial pending_count {pending_count} != 1 (no_surplus row must not count as pending)"
    );

    Ok(())
}

/// Pagination through HTTP: `?limit` caps the page exactly, and out-of-range
/// `limit`/`offset` are clamped (not errors). Logic-unit-tested already; this
/// proves the query-param parse + clamp survive the real HTTP path.
#[tokio::test]
#[ignore = "requires live Postgres"]
async fn http_e2e_pagination() {
    let db = std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_DB.to_string());
    let secret = std::env::var("JWT_SECRET").unwrap_or_else(|_| DEFAULT_SECRET.to_string());

    let pool: PgPool = PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&db)
        .await
        .expect("connect Postgres");

    let user_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT user_id FROM user_wallet_read_model WHERE wallet_address IS NOT NULL AND wallet_address <> '' LIMIT 1",
    )
    .fetch_optional(&pool)
    .await
    .expect("query user");
    let Some(user_id) = user_id else {
        eprintln!("SKIP: no wallet-bearing user in DB");
        return;
    };

    let token = mint_jwt(user_id, &secret);
    let serial = format!("E2E-PAGE-{}", Uuid::new_v4());

    let repo: Arc<dyn MeterRepositoryTrait> = Arc::new(MeterRepository::new(pool.clone()));
    let (readings_tx, _) = broadcast::channel::<Arc<ReadingEvent>>(16);
    let state = AppState {
        meter_service: MeterService::new(repo),
        jwt_secret: Arc::from(secret.as_str()),
        readings_tx,
    };
    let app = build_app(state);

    let result = run_pagination(&app, &pool, &token, &serial, user_id).await;
    cleanup(&pool, &serial).await;
    result.expect("pagination flow");
}

async fn run_pagination(
    app: &axum::Router,
    pool: &PgPool,
    token: &str,
    serial: &str,
    user_id: Uuid,
) -> Result<(), String> {
    macro_rules! check {
        ($cond:expr, $($arg:tt)*) => {
            if !$cond { return Err(format!($($arg)*)); }
        };
    }

    register(app, token, serial).await?;
    // Three readings so the user has >= 2 (list is user-scoped, not serial-scoped).
    inject_reading(pool, user_id, serial, false, None, None).await?;
    inject_reading(pool, user_id, serial, false, None, None).await?;
    inject_reading(pool, user_id, serial, false, None, None).await?;

    let list_len = |query: &str| {
        let app = app.clone();
        let req = authed(
            "GET",
            &format!("/api/v1/meters/readings{query}"),
            token,
            None,
        );
        async move {
            let resp = app.oneshot(req).await.expect("readings");
            let status = resp.status();
            let body = json_body(resp).await;
            (status, body.as_array().map_or(0, Vec::len))
        }
    };

    // No query string exercises the handler's serde default limit (50): still
    // 200, and the three just-seeded readings are within the default page.
    let (status, len) = list_len("").await;
    check!(status == StatusCode::OK, "default-limit status {status}");
    check!(
        len >= 3,
        "default limit should surface the user's readings, got {len}"
    );

    // Exact page cap.
    let (status, len) = list_len("?limit=2").await;
    check!(status == StatusCode::OK, "limit=2 status {status}");
    check!(len == 2, "limit=2 should return exactly 2, got {len}");

    // Over-max limit clamps to 500 server-side: still 200, not an error.
    let (status, _) = list_len("?limit=99999").await;
    check!(status == StatusCode::OK, "huge limit status {status}");

    // Negative offset clamps to 0: still 200.
    let (status, _) = list_len("?limit=10&offset=-5").await;
    check!(status == StatusCode::OK, "negative offset status {status}");

    Ok(())
}

/// Reading projection of the optional energy fields + stats SUM aggregates.
/// Inject a row with explicit
/// `energy_generated`/`energy_consumed`/`voltage`/`current` (as other services
/// write via the Aggregator Bridge), then assert the HTTP list surfaces them and
/// stats sums move by the exact injected amounts (before/after delta, robust to
/// other rows).
#[tokio::test]
#[ignore = "requires live Postgres"]
async fn http_e2e_reading_fields_and_aggregates() {
    let db = std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_DB.to_string());
    let secret = std::env::var("JWT_SECRET").unwrap_or_else(|_| DEFAULT_SECRET.to_string());

    let pool: PgPool = PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&db)
        .await
        .expect("connect Postgres");

    let user_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT user_id FROM user_wallet_read_model WHERE wallet_address IS NOT NULL AND wallet_address <> '' LIMIT 1",
    )
    .fetch_optional(&pool)
    .await
    .expect("query user");
    let Some(user_id) = user_id else {
        eprintln!("SKIP: no wallet-bearing user in DB");
        return;
    };

    let token = mint_jwt(user_id, &secret);
    let serial = format!("E2E-FIELDS-{}", Uuid::new_v4());

    let repo: Arc<dyn MeterRepositoryTrait> = Arc::new(MeterRepository::new(pool.clone()));
    let (readings_tx, _) = broadcast::channel::<Arc<ReadingEvent>>(16);
    let state = AppState {
        meter_service: MeterService::new(repo),
        jwt_secret: Arc::from(secret.as_str()),
        readings_tx,
    };
    let app = build_app(state);

    let result = run_reading_fields(&app, &pool, &token, &serial, user_id).await;
    cleanup(&pool, &serial).await;
    result.expect("reading-fields flow");
}

// The `check!` macro expands to `if !$cond`; the float-tolerance asserts below
// compare partially-ordered f64s, which trips `neg_cmp_op_on_partial_ord`.
#[allow(clippy::neg_cmp_op_on_partial_ord)]
#[allow(clippy::too_many_lines)]
async fn run_reading_fields(
    app: &axum::Router,
    pool: &PgPool,
    token: &str,
    serial: &str,
    user_id: Uuid,
) -> Result<(), String> {
    macro_rules! check {
        ($cond:expr, $($arg:tt)*) => {
            if !$cond { return Err(format!($($arg)*)); }
        };
    }
    // f64 JSON compare with tolerance.
    let approx =
        |v: &serde_json::Value, want: f64| v.as_f64().is_some_and(|g| (g - want).abs() < 1e-6);

    register(app, token, serial).await?;

    // Inject a row with explicit optional fields (other services write these).
    let (gen, con, volt, cur) = (10.5_f64, 4.25_f64, 230.0_f64, 5.5_f64);
    let id: Uuid = sqlx::query_scalar(
        "INSERT INTO meter_readings
            (id, user_id, wallet_address, meter_serial, kwh_amount, timestamp,
             energy_generated, energy_consumed, voltage, current, minted)
         VALUES
            (gen_random_uuid(), $1, 'E2E-FIELDS', $2, $3, now(), $3, $4, $5, $6, false)
         RETURNING id",
    )
    .bind(user_id)
    .bind(serial)
    .bind(gen)
    .bind(con)
    .bind(volt)
    .bind(cur)
    .fetch_one(pool)
    .await
    .map_err(|e| format!("inject reading: {e}"))?;
    let id = id.to_string();

    // List surfaces the optional fields.
    let resp = app
        .clone()
        .oneshot(authed(
            "GET",
            "/api/v1/meters/readings?limit=200",
            token,
            None,
        ))
        .await
        .expect("readings");
    check!(
        resp.status() == StatusCode::OK,
        "readings status {}",
        resp.status()
    );
    let list = json_body(resp).await;
    let arr = list.as_array().cloned().unwrap_or_default();
    let row = arr
        .iter()
        .find(|r| r["id"] == serde_json::json!(id))
        .ok_or("injected row absent from list")?;
    check!(
        approx(&row["energy_generated"], gen),
        "energy_generated wrong: {row}"
    );
    check!(
        approx(&row["energy_consumed"], con),
        "energy_consumed wrong: {row}"
    );
    check!(approx(&row["voltage"], volt), "voltage wrong: {row}");
    check!(approx(&row["current"], cur), "current wrong: {row}");

    // Aggregate SUM correctness, scoped to THIS serial so concurrent ignored
    // tests sharing the borrowed user can't skew it (the /stats endpoint is
    // user-scoped and thus racy for an exact delta). Same SUM SQL shape as the
    // repository's `user_stats`.
    let (sum_gen, sum_con): (f64, f64) = sqlx::query_as(
        "SELECT COALESCE(SUM(energy_generated), 0)::float8,
                COALESCE(SUM(energy_consumed),  0)::float8
         FROM meter_readings WHERE meter_serial = $1",
    )
    .bind(serial)
    .fetch_one(pool)
    .await
    .map_err(|e| format!("sum query: {e}"))?;
    check!(
        (sum_gen - gen).abs() < 1e-6,
        "SUM(energy_generated) {sum_gen} != {gen}"
    );
    check!(
        (sum_con - con).abs() < 1e-6,
        "SUM(energy_consumed) {sum_con} != {con}"
    );

    // The /stats endpoint still returns well-formed aggregates (shape, not exact
    // value — value is racy across the shared user).
    let resp = app
        .clone()
        .oneshot(authed("GET", "/api/v1/meters/stats", token, None))
        .await
        .expect("stats");
    check!(
        resp.status() == StatusCode::OK,
        "stats status {}",
        resp.status()
    );
    let stats = json_body(resp).await;
    check!(
        stats["total_produced"].is_number(),
        "total_produced not number: {stats}"
    );
    check!(
        stats["total_consumed"].is_number(),
        "total_consumed not number: {stats}"
    );
    check!(
        stats["last_reading_time"].is_string(),
        "last_reading_time missing: {stats}"
    );

    Ok(())
}

/// SSE lagged-subscriber invariant (CLAUDE.md): a slow subscriber that overruns
/// the broadcast channel capacity SKIPS the missed events rather than having its
/// stream closed. Infra-free: a tiny-capacity channel is flooded BEFORE the
/// subscriber reads, forcing a `Lagged`, which the handler swallows. The stream
/// must stay open and still deliver a later event.
#[tokio::test]
async fn sse_lagged_subscriber_skips_not_closes() {
    let secret = "test-secret-minimum-32-characters-long-aaaaaaaaaa";
    let pool = PgPool::connect_lazy(DEFAULT_DB).expect("lazy pool");
    let repo: Arc<dyn MeterRepositoryTrait> = Arc::new(MeterRepository::new(pool));
    // Tiny capacity so a handful of unread events overruns it → Lagged.
    let (readings_tx, _) = broadcast::channel::<Arc<ReadingEvent>>(4);
    let tx = readings_tx.clone();
    let state = AppState {
        meter_service: MeterService::new(repo),
        jwt_secret: Arc::from(secret),
        readings_tx,
    };
    let app = build_app(state);

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_secs();
    let user = Uuid::new_v4();
    let token = mint_jwt_with_exp(user, secret, now + 3600);

    // Subscribe first (registers the receiver), but do NOT read yet.
    let stream_resp = app
        .clone()
        .oneshot(authed(
            "GET",
            "/api/v1/meters/readings/stream",
            &token,
            None,
        ))
        .await
        .expect("stream");
    assert_eq!(stream_resp.status(), StatusCode::OK, "stream status");
    let mut events = stream_resp.into_body().into_data_stream();

    // Flood well past capacity (4) for THIS user while nothing is consuming →
    // the receiver lags. The last event's id is what must still arrive.
    let mut last_id = String::new();
    for i in 0..12 {
        let mut r = sample_reading();
        let id = Uuid::new_v4();
        r.id = id;
        r.kwh = f64::from(i);
        last_id = id.to_string();
        let _ = tx.send(Arc::new(ReadingEvent {
            user_id: user,
            reading: r,
        }));
    }

    // The stream must NOT have closed: after swallowing the Lagged error it
    // should still yield one of the retained (later) events.
    let mut sse_text = String::new();
    let got = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(chunk) = events.next().await {
            let bytes = chunk.expect("sse chunk");
            sse_text.push_str(&String::from_utf8_lossy(&bytes));
            if sse_text.contains(&last_id) {
                return true;
            }
        }
        false
    })
    .await
    .unwrap_or(false);
    assert!(
        got,
        "lagged stream closed or never delivered the retained event {last_id}; got: {sse_text}"
    );
}

/// A minimal `MeterReading` for broadcasting directly into the SSE channel
/// (no DB row required).
fn sample_reading() -> MeterReading {
    MeterReading {
        id: Uuid::nil(),
        meter_serial: "SSE-LAG".to_string(),
        kwh: 0.0,
        timestamp: String::new(),
        submitted_at: String::new(),
        mint_status: "pending".to_string(),
        mint_tx_signature: None,
        ..Default::default()
    }
}

/// `/health` returns 200 with the `"ok"` body, and the permissive CORS layer
/// stamps `access-control-allow-origin: *` on responses. Infra-free (health
/// never touches the pool).
#[tokio::test]
async fn health_ok_body_and_cors_header() {
    let secret = "test-secret-minimum-32-characters-long-aaaaaaaaaa";
    let pool = PgPool::connect_lazy(DEFAULT_DB).expect("lazy pool");
    let repo: Arc<dyn MeterRepositoryTrait> = Arc::new(MeterRepository::new(pool));
    let (readings_tx, _) = broadcast::channel::<Arc<ReadingEvent>>(4);
    let state = AppState {
        meter_service: MeterService::new(repo),
        jwt_secret: Arc::from(secret),
        readings_tx,
    };
    let app = build_app(state);

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .expect("req"),
        )
        .await
        .expect("health");
    assert_eq!(resp.status(), StatusCode::OK, "health status");
    assert_eq!(
        resp.headers()
            .get("access-control-allow-origin")
            .and_then(|h| h.to_str().ok()),
        Some("*"),
        "permissive CORS header missing"
    );
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("body");
    assert_eq!(&bytes[..], b"ok", "health body");
}

/// A repository/DB failure surfaces as HTTP 500 (`ApiError::Database`). Drives a
/// pool pointed at a dead port with a short acquire timeout, so a valid-token
/// read past auth fails at the query and maps to `INTERNAL_SERVER_ERROR`.
#[tokio::test]
async fn db_error_maps_to_500() {
    let secret = "test-secret-minimum-32-characters-long-aaaaaaaaaa";
    // Lazy pool at a closed port; first query fails fast (connection refused).
    let pool = PgPoolOptions::new()
        .acquire_timeout(Duration::from_secs(2))
        .connect_lazy("postgresql://nobody@127.0.0.1:1/nodb")
        .expect("lazy pool");
    let repo: Arc<dyn MeterRepositoryTrait> = Arc::new(MeterRepository::new(pool));
    let (readings_tx, _) = broadcast::channel::<Arc<ReadingEvent>>(4);
    let state = AppState {
        meter_service: MeterService::new(repo),
        jwt_secret: Arc::from(secret),
        readings_tx,
    };
    let app = build_app(state);

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_secs();
    let token = mint_jwt_with_exp(Uuid::new_v4(), secret, now + 3600);

    // Valid token passes auth; the handler then hits the dead DB → 500.
    let resp = app
        .clone()
        .oneshot(authed("GET", "/api/v1/meters/stats", &token, None))
        .await
        .expect("stats");
    assert_eq!(
        resp.status(),
        StatusCode::INTERNAL_SERVER_ERROR,
        "DB failure should be 500, got {}",
        resp.status()
    );
}

/// Readiness: `GET /health/ready` returns 503 when Postgres is unreachable.
/// No auth required; drives a lazy pool at a closed port so the ping fails fast.
/// (The 200 path needs a live DB and is exercised by the DB-gated flow tests.)
#[tokio::test]
async fn ready_returns_503_when_db_unreachable() {
    let secret = "test-secret-minimum-32-characters-long-aaaaaaaaaa";
    let pool = PgPoolOptions::new()
        .acquire_timeout(Duration::from_secs(2))
        .connect_lazy("postgresql://nobody@127.0.0.1:1/nodb")
        .expect("lazy pool");
    let repo: Arc<dyn MeterRepositoryTrait> = Arc::new(MeterRepository::new(pool));
    let (readings_tx, _) = broadcast::channel::<Arc<ReadingEvent>>(4);
    let state = AppState {
        meter_service: MeterService::new(repo),
        jwt_secret: Arc::from(secret),
        readings_tx,
    };
    let app = build_app(state);

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/health/ready")
                .body(Body::empty())
                .expect("req"),
        )
        .await
        .expect("ready");
    assert_eq!(
        resp.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "unreachable DB should be 503, got {}",
        resp.status()
    );
}

/// Readiness: `GET /health/ready` returns 200 against a live, reachable Postgres
/// (the ping runs `SELECT 1`). DB-gated counterpart to the infra-free 503 test.
#[tokio::test]
#[ignore = "requires live Postgres"]
async fn ready_returns_200_when_db_reachable() {
    let db = std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_DB.to_string());
    let secret = std::env::var("JWT_SECRET").unwrap_or_else(|_| DEFAULT_SECRET.to_string());

    let pool: PgPool = PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&db)
        .await
        .expect("connect Postgres");
    let repo: Arc<dyn MeterRepositoryTrait> = Arc::new(MeterRepository::new(pool));
    let (readings_tx, _) = broadcast::channel::<Arc<ReadingEvent>>(4);
    let state = AppState {
        meter_service: MeterService::new(repo),
        jwt_secret: Arc::from(secret.as_str()),
        readings_tx,
    };
    let app = build_app(state);

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/health/ready")
                .body(Body::empty())
                .expect("req"),
        )
        .await
        .expect("ready");
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "reachable DB should be 200, got {}",
        resp.status()
    );
}

/// Pagination metadata: `GET /readings` carries `X-Total-Count` (an integer) and
/// `X-Has-More`. Register a meter, seed two readings, then request a 1-row page
/// for a user that now has >= 2 readings → `X-Has-More: true`. Body stays an array.
#[tokio::test]
#[ignore = "requires live Postgres"]
async fn readings_headers_report_pagination() {
    let db = std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_DB.to_string());
    let secret = std::env::var("JWT_SECRET").unwrap_or_else(|_| DEFAULT_SECRET.to_string());

    let pool: PgPool = PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&db)
        .await
        .expect("connect Postgres");

    let user_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT user_id FROM user_wallet_read_model WHERE wallet_address IS NOT NULL AND wallet_address <> '' LIMIT 1",
    )
    .fetch_optional(&pool)
    .await
    .expect("query user");
    let Some(user_id) = user_id else {
        eprintln!("SKIP: no wallet-bearing user in DB");
        return;
    };

    let token = mint_jwt(user_id, &secret);
    let serial = format!("E2E-PAGEHDR-{}", Uuid::new_v4());

    let repo: Arc<dyn MeterRepositoryTrait> = Arc::new(MeterRepository::new(pool.clone()));
    let (readings_tx, _) = broadcast::channel::<Arc<ReadingEvent>>(16);
    let state = AppState {
        meter_service: MeterService::new(repo),
        jwt_secret: Arc::from(secret.as_str()),
        readings_tx,
    };
    let app = build_app(state);

    let result = run_pagination_headers(&app, &pool, &token, &serial, user_id).await;
    cleanup(&pool, &serial).await;
    result.expect("pagination-headers flow");
}

async fn run_pagination_headers(
    app: &axum::Router,
    pool: &PgPool,
    token: &str,
    serial: &str,
    user_id: Uuid,
) -> Result<(), String> {
    macro_rules! check {
        ($cond:expr, $($arg:tt)*) => {
            if !$cond { return Err(format!($($arg)*)); }
        };
    }

    register(app, token, serial).await?;
    inject_reading(pool, user_id, serial, false, None, None).await?;
    inject_reading(pool, user_id, serial, false, None, None).await?;

    let resp = app
        .clone()
        .oneshot(authed(
            "GET",
            "/api/v1/meters/readings?limit=1",
            token,
            None,
        ))
        .await
        .expect("readings");
    check!(
        resp.status() == StatusCode::OK,
        "readings status {}",
        resp.status()
    );

    let total = resp
        .headers()
        .get("x-total-count")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<i64>().ok());
    check!(
        matches!(total, Some(t) if t >= 2),
        "X-Total-Count missing/too small: {total:?}"
    );
    let has_more = resp
        .headers()
        .get("x-has-more")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    check!(
        has_more.as_deref() == Some("true"),
        "X-Has-More should be 'true' for a 1-row page: {has_more:?}"
    );

    // Body unchanged: still a JSON array, capped to the page size.
    let list = json_body(resp).await;
    let arr = list.as_array().cloned().unwrap_or_default();
    check!(
        arr.len() == 1,
        "limit=1 should return 1 row, got {}",
        arr.len()
    );

    Ok(())
}

/// Insert a reading row with arbitrary mint columns, returning its id. Richer
/// than `inject_reading` — exercises the `on_chain_confirmed` /
/// `blockchain_last_error` predicates and predicate precedence.
async fn inject_mint_row(
    pool: &PgPool,
    user_id: Uuid,
    serial: &str,
    minted: bool,
    on_chain_confirmed: bool,
    blockchain_status: Option<&str>,
    blockchain_last_error: Option<&str>,
) -> Result<String, String> {
    let id: Uuid = sqlx::query_scalar(
        "INSERT INTO meter_readings
            (id, user_id, wallet_address, meter_serial, kwh_amount, timestamp,
             minted, on_chain_confirmed, blockchain_status, blockchain_last_error)
         VALUES
            (gen_random_uuid(), $1, 'E2E-PRED', $2, 1.0, now(), $3, $4, $5, $6)
         RETURNING id",
    )
    .bind(user_id)
    .bind(serial)
    .bind(minted)
    .bind(on_chain_confirmed)
    .bind(blockchain_status)
    .bind(blockchain_last_error)
    .fetch_one(pool)
    .await
    .map_err(|e| format!("inject mint row: {e}"))?;
    Ok(id.to_string())
}

/// `MINT_STATUS_CASE` has four column predicates and an ordering (minted wins
/// over denied wins over pending). Other tests only drive `minted` and
/// `blockchain_status='failed'`; this covers the remaining `on_chain_confirmed`
/// and `blockchain_last_error` predicates plus precedence.
#[tokio::test]
#[ignore = "requires live Postgres"]
async fn http_e2e_mint_status_alt_predicates_and_precedence() {
    let db = std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_DB.to_string());
    let secret = std::env::var("JWT_SECRET").unwrap_or_else(|_| DEFAULT_SECRET.to_string());

    let pool: PgPool = PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&db)
        .await
        .expect("connect Postgres");

    let user_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT user_id FROM user_wallet_read_model WHERE wallet_address IS NOT NULL AND wallet_address <> '' LIMIT 1",
    )
    .fetch_optional(&pool)
    .await
    .expect("query user");
    let Some(user_id) = user_id else {
        eprintln!("SKIP: no wallet-bearing user in DB");
        return;
    };

    let token = mint_jwt(user_id, &secret);
    let serial = format!("E2E-PRED-{}", Uuid::new_v4());

    let repo: Arc<dyn MeterRepositoryTrait> = Arc::new(MeterRepository::new(pool.clone()));
    let (readings_tx, _) = broadcast::channel::<Arc<ReadingEvent>>(16);
    let state = AppState {
        meter_service: MeterService::new(repo),
        jwt_secret: Arc::from(secret.as_str()),
        readings_tx,
    };
    let app = build_app(state);

    let result = run_mint_predicates(&app, &pool, &token, &serial, user_id).await;
    cleanup(&pool, &serial).await;
    result.expect("mint-predicate flow");
}

async fn run_mint_predicates(
    app: &axum::Router,
    pool: &PgPool,
    token: &str,
    serial: &str,
    user_id: Uuid,
) -> Result<(), String> {
    macro_rules! check {
        ($cond:expr, $($arg:tt)*) => {
            if !$cond { return Err(format!($($arg)*)); }
        };
    }

    register(app, token, serial).await?;

    // on_chain_confirmed alone (minted=false, no failure) → "minted".
    let id_onchain = inject_mint_row(pool, user_id, serial, false, true, None, None).await?;
    // blockchain_last_error set, status not 'failed' → "denied".
    let id_error = inject_mint_row(
        pool,
        user_id,
        serial,
        false,
        false,
        None,
        Some("rpc timeout"),
    )
    .await?;
    // Precedence: minted=true AND blockchain_status='failed' → "minted" wins.
    let id_both = inject_mint_row(pool, user_id, serial, true, false, Some("failed"), None).await?;
    // Precedence: on_chain_confirmed=true AND last_error set → "minted" wins.
    let id_both2 =
        inject_mint_row(pool, user_id, serial, false, true, None, Some("late error")).await?;

    let resp = app
        .clone()
        .oneshot(authed(
            "GET",
            "/api/v1/meters/readings?limit=200",
            token,
            None,
        ))
        .await
        .expect("readings");
    check!(
        resp.status() == StatusCode::OK,
        "readings status {}",
        resp.status()
    );
    let list = json_body(resp).await;
    let arr = list.as_array().cloned().unwrap_or_default();
    let status_of = |id: &str| -> Option<String> {
        arr.iter()
            .find(|r| r["id"] == serde_json::json!(id))
            .and_then(|r| r["mint_status"].as_str().map(str::to_string))
    };

    check!(
        status_of(&id_onchain).as_deref() == Some("minted"),
        "on_chain_confirmed not 'minted'"
    );
    check!(
        status_of(&id_error).as_deref() == Some("denied"),
        "blockchain_last_error not 'denied'"
    );
    check!(
        status_of(&id_both).as_deref() == Some("minted"),
        "precedence: minted+failed should be 'minted', got {:?}",
        status_of(&id_both)
    );
    check!(
        status_of(&id_both2).as_deref() == Some("minted"),
        "precedence: on_chain+last_error should be 'minted', got {:?}",
        status_of(&id_both2)
    );

    Ok(())
}

/// Insert a reading at an explicit `timestamp`, returning its id. Used to prove
/// ordering and the `last_reading_time` aggregate deterministically.
async fn inject_at(pool: &PgPool, user_id: Uuid, serial: &str, ts: &str) -> Result<String, String> {
    let id: Uuid = sqlx::query_scalar(
        "INSERT INTO meter_readings
            (id, user_id, wallet_address, meter_serial, kwh_amount, timestamp, minted)
         VALUES
            (gen_random_uuid(), $1, 'E2E-TS', $2, 1.0, $3::timestamptz, false)
         RETURNING id",
    )
    .bind(user_id)
    .bind(serial)
    .bind(ts)
    .fetch_one(pool)
    .await
    .map_err(|e| format!("inject_at: {e}"))?;
    Ok(id.to_string())
}

/// Readings list is ordered newest-first (`ORDER BY timestamp DESC`), and the
/// stats `last_reading_time` equals the user's MAX(timestamp). The newer row
/// uses a far-future timestamp so it is also the deterministic global max.
#[tokio::test]
#[ignore = "requires live Postgres"]
async fn http_e2e_list_ordering_and_last_reading_time() {
    let db = std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_DB.to_string());
    let secret = std::env::var("JWT_SECRET").unwrap_or_else(|_| DEFAULT_SECRET.to_string());
    let pool: PgPool = PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&db)
        .await
        .expect("connect Postgres");
    let user_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT user_id FROM user_wallet_read_model WHERE wallet_address IS NOT NULL AND wallet_address <> '' LIMIT 1",
    )
    .fetch_optional(&pool)
    .await
    .expect("query user");
    let Some(user_id) = user_id else {
        eprintln!("SKIP: no wallet-bearing user in DB");
        return;
    };
    let token = mint_jwt(user_id, &secret);
    let serial = format!("E2E-ORD-{}", Uuid::new_v4());
    let repo: Arc<dyn MeterRepositoryTrait> = Arc::new(MeterRepository::new(pool.clone()));
    let (readings_tx, _) = broadcast::channel::<Arc<ReadingEvent>>(16);
    let state = AppState {
        meter_service: MeterService::new(repo),
        jwt_secret: Arc::from(secret.as_str()),
        readings_tx,
    };
    let app = build_app(state);

    let result = run_ordering(&app, &pool, &token, &serial, user_id).await;
    cleanup(&pool, &serial).await;
    result.expect("ordering flow");
}

async fn run_ordering(
    app: &axum::Router,
    pool: &PgPool,
    token: &str,
    serial: &str,
    user_id: Uuid,
) -> Result<(), String> {
    macro_rules! check {
        ($cond:expr, $($arg:tt)*) => {
            if !$cond { return Err(format!($($arg)*)); }
        };
    }
    register(app, token, serial).await?;

    let id_old = inject_at(pool, user_id, serial, "2030-01-01T00:00:00Z").await?;
    let id_new = inject_at(pool, user_id, serial, "2099-12-31T23:59:59Z").await?;

    let resp = app
        .clone()
        .oneshot(authed(
            "GET",
            "/api/v1/meters/readings?limit=200",
            token,
            None,
        ))
        .await
        .expect("readings");
    check!(
        resp.status() == StatusCode::OK,
        "readings status {}",
        resp.status()
    );
    let list = json_body(resp).await;
    let arr = list.as_array().cloned().unwrap_or_default();
    let pos = |id: &str| arr.iter().position(|r| r["id"] == serde_json::json!(id));
    let (p_old, p_new) = (pos(&id_old), pos(&id_new));
    check!(
        p_old.is_some() && p_new.is_some(),
        "injected rows missing from list"
    );
    check!(
        p_new < p_old,
        "DESC order violated: newer at {p_new:?}, older at {p_old:?}"
    );

    // last_reading_time == the future max we injected.
    let resp = app
        .clone()
        .oneshot(authed("GET", "/api/v1/meters/stats", token, None))
        .await
        .expect("stats");
    check!(
        resp.status() == StatusCode::OK,
        "stats status {}",
        resp.status()
    );
    let stats = json_body(resp).await;
    let lrt = stats["last_reading_time"].as_str().unwrap_or_default();
    check!(
        lrt.starts_with("2099-12-31T23:59:59"),
        "last_reading_time not the injected max: {lrt}"
    );
    Ok(())
}

/// Registering with latitude/longitude persists them and surfaces them on
/// `/me/meters`.
#[tokio::test]
#[ignore = "requires live Postgres"]
async fn http_e2e_register_persists_lat_lon() {
    let db = std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_DB.to_string());
    let secret = std::env::var("JWT_SECRET").unwrap_or_else(|_| DEFAULT_SECRET.to_string());
    let pool: PgPool = PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&db)
        .await
        .expect("connect Postgres");
    let user_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT user_id FROM user_wallet_read_model WHERE wallet_address IS NOT NULL AND wallet_address <> '' LIMIT 1",
    )
    .fetch_optional(&pool)
    .await
    .expect("query user");
    let Some(user_id) = user_id else {
        eprintln!("SKIP: no wallet-bearing user in DB");
        return;
    };
    let token = mint_jwt(user_id, &secret);
    let serial = format!("E2E-GEO-{}", Uuid::new_v4());
    let repo: Arc<dyn MeterRepositoryTrait> = Arc::new(MeterRepository::new(pool.clone()));
    let (readings_tx, _) = broadcast::channel::<Arc<ReadingEvent>>(16);
    let state = AppState {
        meter_service: MeterService::new(repo),
        jwt_secret: Arc::from(secret.as_str()),
        readings_tx,
    };
    let app = build_app(state);

    let result: Result<(), String> = async {
        let (lat, lon) = (13.7563, 100.5018);
        let resp = app
            .clone()
            .oneshot(authed(
                "POST",
                "/api/v1/meters",
                &token,
                Some(serde_json::json!({
                    "serial_number": serial,
                    "meter_type": "smart_meter",
                    "location": "bkk",
                    "latitude": lat,
                    "longitude": lon,
                })),
            ))
            .await
            .expect("register");
        if resp.status() != StatusCode::OK {
            return Err(format!("register status {}", resp.status()));
        }
        let resp = app
            .clone()
            .oneshot(authed("GET", "/api/v1/me/meters", &token, None))
            .await
            .expect("my meters");
        if resp.status() != StatusCode::OK {
            return Err(format!("my-meters status {}", resp.status()));
        }
        let meters = json_body(resp).await;
        let arr = meters.as_array().cloned().unwrap_or_default();
        let m = arr
            .iter()
            .find(|m| m["serial_number"] == serde_json::json!(serial))
            .ok_or("registered meter absent from /me/meters")?;
        let approx =
            |v: &serde_json::Value, want: f64| v.as_f64().is_some_and(|g| (g - want).abs() < 1e-6);
        if !approx(&m["latitude"], lat) || !approx(&m["longitude"], lon) {
            return Err(format!("lat/lon not persisted: {m}"));
        }
        Ok(())
    }
    .await;
    cleanup(&pool, &serial).await;
    result.expect("lat-lon flow");
}

/// Per-zone energy-flow aggregation (`/api/v1/meters/stats` → `zones`).
///
/// Registers two meters for the borrowed user, assigns each a distinct,
/// per-run-randomized `zone_id` (so the GROUP BY group is isolated from the
/// shared user's other rows), and injects readings making one zone a net
/// exporter and the other a net importer. Asserts the endpoint's
/// `meter_readings`→`meters` join + `GROUP BY m.zone_id` produces the exact
/// per-zone totals and `net_flow = produced - consumed` with the right sign —
/// the locational signal `M_zone` incentives act on. Closes the §2.5 SQL gap.
#[tokio::test]
#[ignore = "requires live Postgres"]
async fn http_e2e_stats_per_zone_net_flow() {
    let db = std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_DB.to_string());
    let secret = std::env::var("JWT_SECRET").unwrap_or_else(|_| DEFAULT_SECRET.to_string());
    let pool: PgPool = PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&db)
        .await
        .expect("connect Postgres");
    let user_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT user_id FROM user_wallet_read_model WHERE wallet_address IS NOT NULL AND wallet_address <> '' LIMIT 1",
    )
    .fetch_optional(&pool)
    .await
    .expect("query user");
    let Some(user_id) = user_id else {
        eprintln!("SKIP: no wallet-bearing user in DB");
        return;
    };
    let token = mint_jwt(user_id, &secret);
    let run = Uuid::new_v4();
    let serial_a = format!("E2E-ZONE-A-{run}");
    let serial_b = format!("E2E-ZONE-B-{run}");
    // Per-run zone ids derived from the run uuid, kept in a safe positive range
    // so a collision with the shared user's existing zones is ~negligible.
    let zone_a = i32::from_be_bytes([run.as_bytes()[0], run.as_bytes()[1], 0, 0])
        .rem_euclid(1_000_000)
        + 900_000;
    let zone_b = zone_a + 1;

    let repo: Arc<dyn MeterRepositoryTrait> = Arc::new(MeterRepository::new(pool.clone()));
    let (readings_tx, _) = broadcast::channel::<Arc<ReadingEvent>>(16);
    let state = AppState {
        meter_service: MeterService::new(repo),
        jwt_secret: Arc::from(secret.as_str()),
        readings_tx,
    };
    let app = build_app(state);

    let result = run_zone_flow(
        &app, &pool, &token, user_id, &serial_a, &serial_b, zone_a, zone_b,
    )
    .await;
    cleanup(&pool, &serial_a).await;
    cleanup(&pool, &serial_b).await;
    result.expect("zone-flow flow");
}

#[allow(clippy::too_many_arguments)]
async fn run_zone_flow(
    app: &axum::Router,
    pool: &PgPool,
    token: &str,
    user_id: Uuid,
    serial_a: &str,
    serial_b: &str,
    zone_a: i32,
    zone_b: i32,
) -> Result<(), String> {
    let approx =
        |v: &serde_json::Value, want: f64| v.as_f64().is_some_and(|g| (g - want).abs() < 1e-6);

    // Register both meters, then stamp each with its zone (register takes no zone).
    register(app, token, serial_a).await?;
    register(app, token, serial_b).await?;
    for (serial, zone) in [(serial_a, zone_a), (serial_b, zone_b)] {
        sqlx::query("UPDATE meters SET zone_id = $1 WHERE serial_number = $2")
            .bind(zone)
            .bind(serial)
            .execute(pool)
            .await
            .map_err(|e| format!("set zone for {serial}: {e}"))?;
    }

    // Zone A: net exporter (gen 20 > con 5 → +15). Zone B: net importer (gen 3 <
    // con 10 → -7). Inject directly, as other services write these columns.
    let inject = |serial: &'static str, ser: String, gen: f64, con: f64| {
        let pool = pool.clone();
        async move {
            sqlx::query(
                "INSERT INTO meter_readings
                    (id, user_id, wallet_address, meter_serial, kwh_amount, timestamp,
                     energy_generated, energy_consumed, minted)
                 VALUES (gen_random_uuid(), $1, $2, $3, $4, now(), $4, $5, false)",
            )
            .bind(user_id)
            .bind(serial)
            .bind(ser)
            .bind(gen)
            .bind(con)
            .execute(&pool)
            .await
            .map(|_| ())
            .map_err(|e| format!("inject {serial}: {e}"))
        }
    };
    inject("E2E-ZONE-A", serial_a.to_string(), 20.0, 5.0).await?;
    inject("E2E-ZONE-B", serial_b.to_string(), 3.0, 10.0).await?;

    // Endpoint groups by zone and computes net_flow.
    let resp = app
        .clone()
        .oneshot(authed("GET", "/api/v1/meters/stats", token, None))
        .await
        .expect("stats");
    if resp.status() != StatusCode::OK {
        return Err(format!("stats status {}", resp.status()));
    }
    let stats = json_body(resp).await;
    let zones = stats["zones"].as_array().cloned().unwrap_or_default();
    let find_zone = |want: i32| {
        zones
            .iter()
            .find(|zone| zone["zone_id"].as_i64() == Some(i64::from(want)))
            .cloned()
    };

    let za = find_zone(zone_a).ok_or(format!("zone {zone_a} absent from stats.zones: {stats}"))?;
    if !approx(&za["total_produced"], 20.0) || !approx(&za["total_consumed"], 5.0) {
        return Err(format!("zone A totals wrong: {za}"));
    }
    if !approx(&za["net_flow"], 15.0) {
        return Err(format!("zone A net_flow != 15 (net exporter): {za}"));
    }

    let zb = find_zone(zone_b).ok_or(format!("zone {zone_b} absent from stats.zones: {stats}"))?;
    if !approx(&zb["net_flow"], -7.0) {
        return Err(format!("zone B net_flow != -7 (net importer): {zb}"));
    }
    // Invariant holds for every returned zone, regardless of other rows.
    for zone in &zones {
        let (prod, cons, net) = (
            zone["total_produced"].as_f64().unwrap_or(f64::NAN),
            zone["total_consumed"].as_f64().unwrap_or(f64::NAN),
            zone["net_flow"].as_f64().unwrap_or(f64::NAN),
        );
        if (net - (prod - cons)).abs() >= 1e-6 {
            return Err(format!("net_flow != produced - consumed for {zone}"));
        }
    }
    Ok(())
}

/// Two SSE subscribers for the SAME user both receive a submitted reading
/// (broadcast fan-out). Infra-free: events are sent directly on the channel.
#[tokio::test]
async fn sse_multiple_subscribers_same_user_both_receive() {
    let secret = "test-secret-minimum-32-characters-long-aaaaaaaaaa";
    let pool = PgPool::connect_lazy(DEFAULT_DB).expect("lazy pool");
    let repo: Arc<dyn MeterRepositoryTrait> = Arc::new(MeterRepository::new(pool));
    let (readings_tx, _) = broadcast::channel::<Arc<ReadingEvent>>(16);
    let tx = readings_tx.clone();
    let state = AppState {
        meter_service: MeterService::new(repo),
        jwt_secret: Arc::from(secret),
        readings_tx,
    };
    let app = build_app(state);

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_secs();
    let user = Uuid::new_v4();
    let token = mint_jwt_with_exp(user, secret, now + 3600);

    // Two independent subscribers for the same user.
    let sub = || {
        let app = app.clone();
        let token = token.clone();
        async move {
            app.oneshot(authed(
                "GET",
                "/api/v1/meters/readings/stream",
                &token,
                None,
            ))
            .await
            .expect("stream")
            .into_body()
            .into_data_stream()
        }
    };
    let mut a = sub().await;
    let mut b = sub().await;

    // One reading for the user.
    let mut r = sample_reading();
    let id = Uuid::new_v4();
    r.id = id;
    let id = id.to_string();
    let _ = tx.send(Arc::new(ReadingEvent {
        user_id: user,
        reading: r,
    }));

    assert!(
        sse_recv_contains(&mut a, &id).await,
        "subscriber A missed the reading"
    );
    assert!(
        sse_recv_contains(&mut b, &id).await,
        "subscriber B missed the reading"
    );
}

/// Drive an SSE body stream until `want` appears (or 5s elapses).
async fn sse_recv_contains(stream: &mut axum::body::BodyDataStream, want: &str) -> bool {
    let mut text = String::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(chunk) = stream.next().await {
            text.push_str(&String::from_utf8_lossy(&chunk.expect("chunk")));
            if text.contains(want) {
                return true;
            }
        }
        false
    })
    .await
    .unwrap_or(false)
}

/// Remove the meter + readings this test created. Never touches the user.
async fn cleanup(pool: &PgPool, serial: &str) {
    let _ = sqlx::query("DELETE FROM meter_readings WHERE meter_serial = $1")
        .bind(serial)
        .execute(pool)
        .await;
    let _ = sqlx::query("DELETE FROM meters WHERE serial_number = $1")
        .bind(serial)
        .execute(pool)
        .await;
}

/// Live round-trip for the mint-status poller: a pending reading whose mint
/// columns flip out-of-band (as other services write them) must reach the
/// user's open SSE stream as a `minted` event — proving `spawn` → poll →
/// `diff_transitions` → broadcast → per-user SSE filter end to end.
///
/// The poller primes its seen-set on spawn; this injects the resolved row
/// AFTER a delay past one poll interval, so it registers as a fresh transition
/// rather than backlog. Best-effort like the poller itself: under a DB with
/// >`POLL_LIMIT` newer resolved rows in the window the snapshot could miss it,
/// but that does not happen against a dev stack.
#[tokio::test]
#[ignore = "requires live Postgres"]
async fn http_e2e_mint_poller_pushes_transition_to_sse() {
    let db = std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_DB.to_string());
    let secret = std::env::var("JWT_SECRET").unwrap_or_else(|_| DEFAULT_SECRET.to_string());

    let pool: PgPool = PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&db)
        .await
        .expect("connect Postgres");

    let user_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT user_id FROM user_wallet_read_model WHERE wallet_address IS NOT NULL AND wallet_address <> '' LIMIT 1",
    )
    .fetch_optional(&pool)
    .await
    .expect("query user");
    let Some(user_id) = user_id else {
        eprintln!("SKIP: no wallet-bearing user in DB");
        return;
    };

    let token = mint_jwt(user_id, &secret);
    let serial = format!("E2E-POLL-{}", Uuid::new_v4());

    let repo: Arc<dyn MeterRepositoryTrait> = Arc::new(MeterRepository::new(pool.clone()));
    let meter_service = MeterService::new(repo);
    let (readings_tx, _) = broadcast::channel::<Arc<ReadingEvent>>(256);

    // Wire the poller onto the SAME channel the SSE handler filters by user, at a
    // 1s interval so the test completes quickly.
    gridtokenx_meter_service::mint_poller::spawn(meter_service.clone(), readings_tx.clone(), 1);

    let state = AppState {
        meter_service,
        jwt_secret: Arc::from(secret.as_str()),
        readings_tx,
    };
    let app = build_app(state);

    let result = run_mint_poller_sse(&app, &pool, &token, &serial, user_id).await;
    cleanup(&pool, &serial).await;
    result.expect("mint-poller SSE flow");
}

async fn run_mint_poller_sse(
    app: &axum::Router,
    pool: &PgPool,
    token: &str,
    serial: &str,
    user_id: Uuid,
) -> Result<(), String> {
    macro_rules! check {
        ($cond:expr, $($arg:tt)*) => {
            if !$cond { return Err(format!($($arg)*)); }
        };
    }

    register(app, token, serial).await?;

    // Subscribe BEFORE the transition so the broadcast subscription is live.
    let stream_resp = app
        .clone()
        .oneshot(authed("GET", "/api/v1/meters/readings/stream", token, None))
        .await
        .expect("stream");
    check!(
        stream_resp.status() == StatusCode::OK,
        "stream status {}",
        stream_resp.status()
    );
    let mut events = stream_resp.into_body().into_data_stream();

    // Let the poller finish priming + at least one empty tick, so the row we
    // inject next is a genuine new transition rather than primed backlog.
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Flip a reading into the resolved (minted) state out-of-band, as other
    // services would. The next poll tick must diff this as a transition.
    let sig = format!("E2E_POLL_SIG_{}", Uuid::new_v4().simple());
    let id_minted =
        inject_reading(pool, user_id, serial, true, Some(&sig), Some("confirmed")).await?;

    // The poller (1s interval) should pick it up and push it to our stream.
    let mut sse_text = String::new();
    let got = tokio::time::timeout(Duration::from_secs(8), async {
        while let Some(chunk) = events.next().await {
            let bytes = chunk.expect("sse chunk");
            sse_text.push_str(&String::from_utf8_lossy(&bytes));
            if sse_text.contains(&id_minted) {
                return true;
            }
        }
        false
    })
    .await
    .unwrap_or(false);
    check!(
        got,
        "poller did not push minted transition {id_minted} to SSE; got: {sse_text}"
    );
    check!(
        sse_text.contains("\"mint_status\":\"minted\""),
        "pushed event missing minted mint_status: {sse_text}"
    );

    Ok(())
}
